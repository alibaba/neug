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
// The stream yields T directly. Execution uses ContextChunk, which already
// owns the DataChunk and anonymous head. Tags also describe empty streams.
// EOF and errors are terminal; an empty batch is NOT EOF.
template <typename T>
class Stream {
 public:
  using NextResult = result<std::optional<T>>;
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
      return std::optional<T>{};
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
    NextResult output = std::optional<T>{};
    TRY_HANDLE_ALL_WITH_EXCEPTION(
        NextResult, [&]() { return pull_(); },
        [&](const Status& status) { output = tl::unexpected(status); },
        [&](NextResult&& batch) { output = std::move(batch); });
    return output;
  }

  Pull pull_;
  std::optional<Status> error_;
};

// An execution error is observed through Next(), just like a read error.
template <typename T>
Stream<T> error_stream(Status error) {
  return Stream<T>(
      [error = std::move(error)]() ->
      typename Stream<T>::NextResult { return tl::unexpected(error); });
}

// Own execution state until first demand. Initialization and its exceptions
// run inside Stream::Next's error boundary, exactly once.
template <typename Initialize>
Stream<ContextChunk> defer_stream(Stream<ContextChunk> input,
                                  Initialize initialize) {
  auto tags = input.tag_ids;
  struct State {
    Stream<ContextChunk> input;
    std::optional<Stream<ContextChunk>> output;
  };
  auto state = std::make_shared<State>(State{std::move(input), std::nullopt});
  return Stream<ContextChunk>(
      [state, initialize = std::move(
                  initialize)]() mutable -> Stream<ContextChunk>::NextResult {
        if (!state->output) {
          state->output.emplace(initialize(std::move(state->input)));
        }
        return state->output->Next();
      },
      std::move(tags));
}

// Put a batch pulled for initialization back in front of its remaining input.
inline Stream<ContextChunk> prepend_chunk(std::optional<ContextChunk> first,
                                          Stream<ContextChunk> input) {
  if (!first) {
    return std::move(input);
  }
  auto tags = input.tag_ids;
  auto pending =
      std::make_shared<std::optional<ContextChunk>>(std::move(first));
  auto upstream = std::make_shared<Stream<ContextChunk>>(std::move(input));
  return Stream<ContextChunk>(
      [pending, upstream]() -> Stream<ContextChunk>::NextResult {
        if (*pending) {
          auto chunk = std::move(*pending);
          pending->reset();
          return chunk;
        }
        return upstream->Next();
      },
      std::move(tags));
}

// Exactly one upstream pull and one kernel invocation per downstream pull.
template <typename Transform>
Stream<ContextChunk> map_chunks(Stream<ContextChunk> input,
                                Transform transform) {
  auto tags = input.tag_ids;
  auto upstream = std::make_shared<Stream<ContextChunk>>(std::move(input));
  return Stream<ContextChunk>(
      [upstream, transform = std::move(
                     transform)]() mutable -> Stream<ContextChunk>::NextResult {
        GS_AUTO(next, upstream->Next());
        if (!next) {
          return std::optional<ContextChunk>{};
        }
        GS_AUTO(output, transform(std::move(*next)));
        return std::optional<ContextChunk>(std::move(output));
      },
      std::move(tags));
}

// Invoke a producer on first demand, without an intermediate Context.
template <typename Producer>
Stream<ContextChunk> generate_chunk(Producer producer,
                                    std::vector<int> tags = {}) {
  return Stream<ContextChunk>(
      [producer = std::move(producer),
       done = false]() mutable -> Stream<ContextChunk>::NextResult {
        if (done) {
          return std::optional<ContextChunk>{};
        }
        done = true;
        GS_AUTO(chunk, producer());
        return std::optional<ContextChunk>(std::move(chunk));
      },
      std::move(tags));
}

// Explicit global-input boundary. Row-local operators never collect input.
inline result<ContextChunk> collect_chunk(Stream<ContextChunk> input) {
  std::optional<ContextChunk> accumulated;
  while (true) {
    GS_AUTO(next, input.Next());
    if (!next) {
      return accumulated ? std::move(*accumulated) : ContextChunk{};
    }
    ContextChunk chunk = std::move(*next);
    if (accumulated) {
      *accumulated = accumulated->union_with(chunk);
    } else {
      accumulated = std::move(chunk);
    }
  }
}

template <typename Reduce>
Stream<ContextChunk> reduce_stream(Stream<ContextChunk> input, Reduce reduce) {
  auto tags = input.tag_ids;
  auto upstream = std::make_shared<Stream<ContextChunk>>(std::move(input));
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
    Stream<ContextChunk> input) {
  std::vector<ContextChunk> chunks;
  while (true) {
    GS_AUTO(next, input.Next());
    if (!next) {
      return chunks;
    }
    chunks.push_back(std::move(*next));
  }
}

inline Stream<ContextChunk> stream_from_batches(
    std::vector<ContextChunk> chunks, std::vector<int> tags = {}) {
  auto batches = std::make_shared<std::vector<ContextChunk>>(std::move(chunks));
  return Stream<ContextChunk>(
      [batches,
       index = size_t{0}]() mutable -> Stream<ContextChunk>::NextResult {
        if (index == batches->size()) {
          return std::optional<ContextChunk>{};
        }
        return std::optional<ContextChunk>(std::move((*batches)[index++]));
      },
      std::move(tags));
}

inline Stream<ContextChunk> stream_from_context(Context ctx) {
  return stream_from_batches(std::move(ctx.chunks()), std::move(ctx.tag_ids));
}

inline result<Context> materialize(Stream<ContextChunk> stream) {
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
    ctx.append_chunk(std::move(batch));
  }
}

}  // namespace neug::execution
