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
