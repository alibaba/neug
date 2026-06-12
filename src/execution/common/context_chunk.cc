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

#include "neug/execution/common/context_chunk.h"

#include <string>
#include <utility>
#include <vector>

#include "neug/utils/exception/exception.h"

namespace neug {

namespace execution {

ContextChunk::ContextChunk(DataChunk&& chunk) : chunk_(std::move(chunk)) {}

ContextChunk::ContextChunk(DataChunk&& chunk,
                           std::shared_ptr<IContextColumn> head)
    : chunk_(std::move(chunk)), head_(std::move(head)) {}

DataChunk& ContextChunk::chunk() { return chunk_; }

const DataChunk& ContextChunk::chunk() const { return chunk_; }

std::shared_ptr<IContextColumn>& ContextChunk::head() { return head_; }

const std::shared_ptr<IContextColumn>& ContextChunk::head() const {
  return head_;
}

std::vector<std::shared_ptr<IContextColumn>>& ContextChunk::columns() {
  return chunk_.columns;
}

const std::vector<std::shared_ptr<IContextColumn>>& ContextChunk::columns()
    const {
  return chunk_.columns;
}

void ContextChunk::clear() {
  chunk_.clear();
  head_.reset();
}

void ContextChunk::set(int alias, std::shared_ptr<IContextColumn> col) {
  head_ = col;
  if (alias < 0) {
    return;
  }
  if (chunk_.columns.size() <= static_cast<size_t>(alias)) {
    chunk_.columns.resize(alias + 1, nullptr);
  }
  if (chunk_.columns[alias] != nullptr) {
    THROW_RUNTIME_ERROR("column already set: " + std::to_string(alias));
  }
  chunk_.columns[alias] = std::move(col);
}

std::shared_ptr<IContextColumn> ContextChunk::get(int alias) const {
  if (alias == -1) {
    return head_;
  }
  if (alias < 0 || alias >= static_cast<int>(chunk_.columns.size())) {
    THROW_INTERNAL_EXCEPTION(
        "alias out of range: " + std::to_string(alias) +
        ", columns.size()=" + std::to_string(chunk_.columns.size()));
  }
  return chunk_.columns[alias];
}

void ContextChunk::set_with_reshuffle(int alias,
                                      std::shared_ptr<IContextColumn> col,
                                      const std::vector<size_t>& offsets) {
  head_.reset();
  if (alias >= 0 && chunk_.columns.size() > static_cast<size_t>(alias) &&
      chunk_.columns[alias] != nullptr) {
    chunk_.columns[alias] = nullptr;
  }
  reshuffle(offsets);
  set(alias, std::move(col));
}

void ContextChunk::remove(int alias) {
  if (alias < 0) {
    head_.reset();
  } else {
    chunk_.remove(alias);
  }
}

bool ContextChunk::exist(int alias) const {
  if (alias < 0) {
    return head_ != nullptr;
  }
  return chunk_.exist(alias);
}

size_t ContextChunk::row_num() const {
  size_t n = chunk_.row_num();
  if (n == 0 && chunk_.col_num() == 0 && head_ != nullptr) {
    return head_->size();
  }
  return n;
}

size_t ContextChunk::col_num() const { return chunk_.col_num(); }

void ContextChunk::reshuffle(const std::vector<size_t>& offsets) {
  auto& columns = chunk_.columns;
  std::vector<std::shared_ptr<IContextColumn>> new_cols;
  new_cols.reserve(columns.size());
  bool head_shuffled = false;
  for (size_t i = 0; i < columns.size(); ++i) {
    if (columns[i] == nullptr) {
      new_cols.push_back(nullptr);
      continue;
    }
    bool found_earlier = false;
    for (size_t j = 0; j < i; ++j) {
      if (columns[j].get() == columns[i].get()) {
        new_cols.push_back(new_cols[j]);
        found_earlier = true;
        break;
      }
    }
    if (!found_earlier) {
      auto shuffled = columns[i]->shuffle(offsets);
      if (!head_shuffled && head_ != nullptr &&
          head_.get() == columns[i].get()) {
        head_ = shuffled;
        head_shuffled = true;
      }
      new_cols.push_back(std::move(shuffled));
    } else if (!head_shuffled && head_ != nullptr &&
               head_.get() == columns[i].get()) {
      head_ = new_cols.back();
      head_shuffled = true;
    }
  }
  if (!head_shuffled && head_ != nullptr) {
    head_ = head_->shuffle(offsets);
  }
  columns = std::move(new_cols);
}

void ContextChunk::optional_reshuffle(const std::vector<size_t>& offsets) {
  auto& columns = chunk_.columns;
  std::vector<std::shared_ptr<IContextColumn>> new_cols;
  new_cols.reserve(columns.size());
  bool head_shuffled = false;
  for (size_t i = 0; i < columns.size(); ++i) {
    if (columns[i] == nullptr) {
      new_cols.push_back(nullptr);
      continue;
    }
    bool found_earlier = false;
    for (size_t j = 0; j < i; ++j) {
      if (columns[j].get() == columns[i].get()) {
        new_cols.push_back(new_cols[j]);
        found_earlier = true;
        break;
      }
    }
    if (!found_earlier) {
      auto shuffled = columns[i]->optional_shuffle(offsets);
      if (!head_shuffled && head_ != nullptr &&
          head_.get() == columns[i].get()) {
        head_ = shuffled;
        head_shuffled = true;
      }
      new_cols.push_back(std::move(shuffled));
    } else if (!head_shuffled && head_ != nullptr &&
               head_.get() == columns[i].get()) {
      head_ = new_cols.back();
      head_shuffled = true;
    }
  }
  if (!head_shuffled && head_ != nullptr) {
    head_ = head_->optional_shuffle(offsets);
  }
  columns = std::move(new_cols);
}

ContextChunk ContextChunk::union_with(const ContextChunk& other) const {
  DataChunk merged = chunk_.union_chunk(other.chunk_);
  std::shared_ptr<IContextColumn> merged_head;
  if (head_ != nullptr && other.head_ != nullptr) {
    bool aligned = false;
    for (size_t k = 0; k < chunk_.columns.size(); ++k) {
      if (chunk_.columns[k] != nullptr &&
          chunk_.columns[k].get() == head_.get() && k < merged.columns.size() &&
          merged.columns[k] != nullptr) {
        merged_head = merged.columns[k];
        aligned = true;
        break;
      }
    }
    if (!aligned) {
      merged_head = head_->union_col(other.head_);
    }
  }
  return ContextChunk(std::move(merged), std::move(merged_head));
}

}  // namespace execution
}  // namespace neug
