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

#include "neug/execution/common/context.h"

#include <glog/logging.h>

#include <utility>
#include <vector>

namespace neug {

namespace execution {

Context::Context() = default;

void Context::clear() {
  chunks_.clear();
  tag_ids.clear();
}

size_t Context::chunk_num() const { return chunks_.size(); }

ContextChunk& Context::chunk(size_t idx) { return chunks_[idx]; }

const ContextChunk& Context::chunk(size_t idx) const { return chunks_[idx]; }

std::vector<ContextChunk>& Context::chunks() { return chunks_; }

const std::vector<ContextChunk>& Context::chunks() const { return chunks_; }

void Context::append_chunk(DataChunk&& chunk) {
  chunks_.emplace_back(std::move(chunk));
}

void Context::append_chunk(DataChunk&& chunk,
                           std::shared_ptr<IContextColumn> head) {
  chunks_.emplace_back(std::move(chunk), std::move(head));
}

void Context::append_chunk(ContextChunk&& chunk) {
  chunks_.push_back(std::move(chunk));
}

void Context::flatten() {
  if (chunks_.size() <= 1) {
    return;
  }
  ContextChunk merged = std::move(chunks_[0]);
  for (size_t i = 1; i < chunks_.size(); ++i) {
    merged = merged.union_with(chunks_[i]);
  }
  chunks_.clear();
  chunks_.push_back(std::move(merged));
}

void Context::ensure_single_chunk(const char* caller) {
  if (chunks_.size() <= 1)
    return;
  LOG(WARNING) << caller << ": expected single-chunk Context, got "
               << chunks_.size() << " chunks; flattening";
  flatten();
}

size_t Context::col_num() const {
  return chunks_.empty() ? 0 : chunks_[0].columns().size();
}

size_t Context::row_num() const {
  size_t total = 0;
  for (const auto& cc : chunks_) {
    total += cc.row_num();
  }
  return total;
}

}  // namespace execution

}  // namespace neug
