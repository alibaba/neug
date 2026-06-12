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

#include "neug/execution/common/data_chunk.h"

#include <glog/logging.h>

#include <string>
#include <utility>
#include <vector>

namespace neug {

namespace execution {

void DataChunk::clear() { columns.clear(); }

void DataChunk::set(int alias, std::shared_ptr<IContextColumn> col) {
  if (alias < 0) {
    THROW_RUNTIME_ERROR("DataChunk::set requires alias >= 0, got " +
                        std::to_string(alias));
  }
  if (columns.size() <= static_cast<size_t>(alias)) {
    columns.resize(alias + 1, nullptr);
  }
  if (columns[alias] != nullptr) {
    THROW_RUNTIME_ERROR("column already set: " + std::to_string(alias));
  }
  columns[alias] = std::move(col);
}

std::shared_ptr<IContextColumn> DataChunk::get(int alias) const {
  if (alias < 0 || alias >= static_cast<int>(columns.size())) {
    THROW_INTERNAL_EXCEPTION(
        "alias out of range: " + std::to_string(alias) +
        ", columns.size()=" + std::to_string(columns.size()));
  }
  return columns[alias];
}

void DataChunk::remove(int alias) {
  if (alias >= 0 && static_cast<size_t>(alias) < columns.size()) {
    columns[alias] = nullptr;
  }
}

bool DataChunk::exist(int alias) const {
  if (alias < 0 || static_cast<size_t>(alias) >= columns.size()) {
    return false;
  }
  return columns[alias] != nullptr;
}

size_t DataChunk::row_num() const {
  for (const auto& col : columns) {
    if (col != nullptr) {
      return col->size();
    }
  }
  return 0;
}

size_t DataChunk::col_num() const { return columns.size(); }

void DataChunk::reshuffle(const std::vector<size_t>& offsets) {
  std::vector<std::shared_ptr<IContextColumn>> new_cols;
  new_cols.reserve(columns.size());
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
      new_cols.push_back(columns[i]->shuffle(offsets));
    }
  }
  columns = std::move(new_cols);
}

void DataChunk::optional_reshuffle(const std::vector<size_t>& offsets) {
  std::vector<std::shared_ptr<IContextColumn>> new_cols;
  new_cols.reserve(columns.size());
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
      new_cols.push_back(columns[i]->optional_shuffle(offsets));
    }
  }
  columns = std::move(new_cols);
}

DataChunk DataChunk::union_chunk(const DataChunk& other) const {
  DataChunk out;
  CHECK(col_num() == other.col_num());
  for (size_t i = 0; i < col_num(); ++i) {
    if (columns[i] != nullptr) {
      out.set(i, columns[i]->union_col(other.get(i)));
    }
  }
  return out;
}

}  // namespace execution

}  // namespace neug
