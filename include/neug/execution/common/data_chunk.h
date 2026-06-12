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

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "neug/execution/common/columns/i_context_column.h"
#include "neug/utils/exception/exception.h"

namespace neug {
class StorageReadInterface;

namespace execution {

/**
 * @brief A DataChunk holds a set of columns that share the same row count.
 *
 * This is the basic unit of columnar data in the execution engine, analogous
 * to Arrow's RecordBatch. It is a pure data container — columns are indexed
 * by a non-negative alias and reshuffle / union operations only touch the
 * named columns. It does not carry any execution state.
 *
 * Execution-related transient state (e.g. the "head" column referenced by
 * anonymous outputs / inputs with alias = -1) lives in ContextChunk, one
 * layer above. DataChunk itself never sees alias = -1.
 */
class DataChunk {
 public:
  DataChunk() = default;
  ~DataChunk() = default;

  DataChunk(const DataChunk& other) = default;
  DataChunk& operator=(const DataChunk& other) = default;
  DataChunk(DataChunk&& other) noexcept = default;
  DataChunk& operator=(DataChunk&& other) noexcept = default;

  void clear();

  /// Stores the given column under the given alias (alias must be >= 0).
  void set(int alias, std::shared_ptr<IContextColumn> col);

  /// Returns the column at the given alias (alias must be >= 0).
  std::shared_ptr<IContextColumn> get(int alias) const;

  void remove(int alias);

  bool exist(int alias) const;

  size_t row_num() const;

  size_t col_num() const;

  void reshuffle(const std::vector<size_t>& offsets);

  void optional_reshuffle(const std::vector<size_t>& offsets);

  DataChunk union_chunk(const DataChunk& other) const;

  std::vector<std::shared_ptr<IContextColumn>> columns;
};

}  // namespace execution
}  // namespace neug
