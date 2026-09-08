/** Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <functional>
#include <optional>
#include <utility>

#include "neug/execution/common/context.h"

namespace neug::execution {

// A single-consumer, synchronous pull stream. Construction does not read rows.
// A batch carries its anonymous head separately, keeping DataChunk a pure data
// container. Output tags describe the stream, including an empty stream.
// EOF and errors are terminal; an empty batch is NOT EOF.
template <typename T>
class Stream {
 public:
  struct Batch {
    T chunk;
    std::shared_ptr<IContextColumn> head;
  };
  using NextResult = result<std::optional<Batch>>;
  using Pull = std::function<NextResult()>;

  Stream() = default;
  explicit Stream(Pull pull, std::vector<int> tags = {})
      : tag_ids(std::move(tags)), pull_(std::move(pull)) {}
  Stream(Stream&&) = default;
  Stream& operator=(Stream&&) = default;
  Stream(const Stream&) = delete;
  Stream& operator=(const Stream&) = delete;

  NextResult Next() {
    if (error_) {
      return tl::unexpected(*error_);
    }
    if (!pull_) {
      return std::optional<Batch>{};
    }
    auto output = PullOne();
    if (!output) {
      error_ = output.error();
      pull_ = nullptr;
    } else if (!*output) {
      pull_ = nullptr;
    }
    return output;
  }

  std::vector<int> tag_ids;

 private:
  NextResult PullOne() {
    NextResult output = std::optional<Batch>{};
    TRY_HANDLE_ALL_WITH_EXCEPTION(
        NextResult, [&]() { return pull_(); },
        [&](const Status& status) { output = tl::unexpected(status); },
        [&](NextResult&& batch) { output = std::move(batch); });
    return output;
  }

  Pull pull_;
  std::optional<Status> error_;
};

// ContextChunk is only a DataChunk + anonymous head envelope. Moving this
// envelope does not collect batches or copy columns.
inline Stream<DataChunk>::Batch release_batch(ContextChunk chunk) {
  return {std::move(chunk.chunk()), std::move(chunk.head())};
}

// Exactly one upstream pull and one kernel invocation per downstream pull.
template <typename Transform>
Stream<DataChunk> map_chunks(Stream<DataChunk> input, Transform transform) {
  auto tags = input.tag_ids;
  auto upstream = std::make_shared<Stream<DataChunk>>(std::move(input));
  return Stream<DataChunk>(
      [upstream, transform = std::move(
                     transform)]() mutable -> Stream<DataChunk>::NextResult {
        GS_AUTO(next, upstream->Next());
        if (!next) {
          return std::optional<Stream<DataChunk>::Batch>{};
        }
        GS_AUTO(output, transform(ContextChunk(std::move(next->chunk),
                                               std::move(next->head))));
        return std::optional<Stream<DataChunk>::Batch>(
            release_batch(std::move(output)));
      },
      std::move(tags));
}

// Invoke a producer on first demand, without an intermediate Context.
template <typename Producer>
Stream<DataChunk> generate_chunk(Producer producer,
                                 std::vector<int> tags = {}) {
  return Stream<DataChunk>(
      [producer = std::move(producer),
       done = false]() mutable -> Stream<DataChunk>::NextResult {
        if (done) {
          return std::optional<Stream<DataChunk>::Batch>{};
        }
        done = true;
        GS_AUTO(chunk, producer());
        return std::optional<Stream<DataChunk>::Batch>(
            release_batch(std::move(chunk)));
      },
      std::move(tags));
}

// Explicit global-input boundary. Row-local operators never collect input.
inline result<ContextChunk> collect_chunk(Stream<DataChunk> input) {
  std::optional<ContextChunk> accumulated;
  while (true) {
    GS_AUTO(next, input.Next());
    if (!next) {
      return accumulated ? std::move(*accumulated) : ContextChunk{};
    }
    ContextChunk chunk(std::move(next->chunk), std::move(next->head));
    if (accumulated) {
      *accumulated = accumulated->union_with(chunk);
    } else {
      accumulated = std::move(chunk);
    }
  }
}

template <typename Reduce>
Stream<DataChunk> reduce_stream(Stream<DataChunk> input, Reduce reduce) {
  auto tags = input.tag_ids;
  auto upstream = std::make_shared<Stream<DataChunk>>(std::move(input));
  return generate_chunk(
      [upstream, reduce = std::move(reduce)]() mutable -> result<ContextChunk> {
        GS_AUTO(chunk, collect_chunk(std::move(*upstream)));
        return reduce(std::move(chunk));
      },
      std::move(tags));
}

// Buffer only when an operator must replay its input or stabilize it before
// mutations. Batches retain their boundaries and share column ownership.
inline result<std::vector<ContextChunk>> collect_batches(
    Stream<DataChunk> input) {
  std::vector<ContextChunk> chunks;
  while (true) {
    GS_AUTO(next, input.Next());
    if (!next)
      return chunks;
    chunks.emplace_back(std::move(next->chunk), std::move(next->head));
  }
}

inline Stream<DataChunk> stream_from_batches(std::vector<ContextChunk> chunks,
                                             std::vector<int> tags = {}) {
  auto batches = std::make_shared<std::vector<ContextChunk>>(std::move(chunks));
  return Stream<DataChunk>(
      [batches, index = size_t{0}]() mutable -> Stream<DataChunk>::NextResult {
        if (index == batches->size())
          return std::optional<Stream<DataChunk>::Batch>{};
        return std::optional<Stream<DataChunk>::Batch>(
            release_batch(std::move((*batches)[index++])));
      },
      std::move(tags));
}

inline Stream<DataChunk> stream_from_context(Context ctx) {
  auto tags = std::move(ctx.tag_ids);
  auto chunks =
      std::make_shared<std::vector<ContextChunk>>(std::move(ctx.chunks()));
  return Stream<DataChunk>(
      [chunks, index = size_t{0}]() mutable -> Stream<DataChunk>::NextResult {
        if (index == chunks->size()) {
          return std::optional<Stream<DataChunk>::Batch>{};
        }
        auto& cc = (*chunks)[index++];
        return std::optional<Stream<DataChunk>::Batch>(
            {std::move(cc.chunk()), std::move(cc.head())});
      },
      std::move(tags));
}

inline result<Context> materialize(Stream<DataChunk> stream) {
  Context ctx;
  ctx.tag_ids = std::move(stream.tag_ids);
  while (true) {
    auto next = stream.Next();
    if (!next) {
      return tl::unexpected(next.error());
    }
    if (!*next) {
      return ctx;
    }
    auto& batch = **next;
    ctx.append_chunk(std::move(batch.chunk), std::move(batch.head));
  }
}

}  // namespace neug::execution
