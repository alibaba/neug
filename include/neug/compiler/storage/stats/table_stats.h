/**
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * This file is originally from the Kùzu project
 * (https://github.com/kuzudb/kuzu) Licensed under the MIT License. Modified by
 * Zhou Xiaoli in 2025 to support Neug-specific features.
 */

#pragma once

#include "neug/compiler/common/types/types.h"
#include "neug/compiler/storage/stats/column_stats.h"

namespace neug::common {
class LogicalType;
}
namespace neug {
namespace storage {

class TableStats {
 public:
  explicit TableStats(std::span<const common::LogicalType> dataTypes)
      : cardinality(0) {}

  EXPLICIT_COPY_DEFAULT_MOVE(TableStats);

  void incrementCardinality(common::cardinality_t increment) {
    cardinality += increment;
  }

  void merge(const TableStats& other) {
    std::vector<common::column_id_t> columnIDs;
    for (auto i = 0u; i < columnStats.size(); i++) {
      columnIDs.push_back(i);
    }
    merge(columnIDs, other);
  }

  void merge(const std::vector<common::column_id_t>& columnIDs,
             const TableStats& other) {
    cardinality += other.cardinality;
    NEUG_ASSERT(columnIDs.size() == other.columnStats.size());
    for (auto i = 0u; i < columnIDs.size(); ++i) {
      auto columnID = columnIDs[i];
      NEUG_ASSERT(columnID < columnStats.size());
      columnStats[columnID].merge(other.columnStats[i]);
    }
  }

  common::cardinality_t getTableCard() const { return cardinality; }

  common::cardinality_t getNumDistinctValues(
      common::column_id_t columnID) const {
    NEUG_ASSERT(columnID < columnStats.size());
    return columnStats[columnID].getNumDistinctValues();
  }

  void update(const std::vector<common::ValueVector*>& vectors,
              size_t numColumns = std::numeric_limits<size_t>::max());
  void update(const std::vector<common::column_id_t>& columnIDs,
              const std::vector<common::ValueVector*>& vectors,
              size_t numColumns = std::numeric_limits<size_t>::max());

  ColumnStats& addNewColumn(const common::LogicalType& dataType) {
    columnStats.emplace_back(dataType);
    return columnStats.back();
  }

  void serialize(common::Serializer& serializer) const;
  TableStats deserialize(common::Deserializer& deserializer);

 private:
  TableStats(const TableStats& other);

 private:
  common::cardinality_t cardinality;
  std::vector<ColumnStats> columnStats;
};

}  // namespace storage
}  // namespace neug
