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

#include <filesystem>
#include <string>
#include <unordered_map>
#include "neug/compiler/catalog/catalog.h"
#include "neug/compiler/catalog/catalog_entry/table_catalog_entry.h"
#include "neug/compiler/common/types/types.h"
#include "neug/storages/graph/property_graph.h"

namespace neug {
namespace main {
class MetadataManager;
}

namespace catalog {
class CatalogEntry;
}

namespace storage {

// 基于 PropertyGraph 重新实现 StatManager，避免序列化过程
// 直接返回 cardinality接口，而不是table，修改调用这些接口的地方
class StatsManager {
 public:
  StatsManager() = default;
  explicit StatsManager(const PropertyGraph& graph) : graph_(&graph) {}

  void UpdateGraph(const PropertyGraph& graph) { graph_ = &graph; }
#ifdef NEUG_BUILD_TEST
  void LoadFromJson(const Schema& schema, const std::string& stats_json);
#endif

  common::cardinality_t getTable(common::table_id_t tableID) const;
  common::cardinality_t getTable(common::table_id_t tableID,
                                 common::TableType tableType) const;
  common::cardinality_t getTable(catalog::SchemaEntry* tableEntry) const;
  common::cardinality_t getTableCardinality(common::table_id_t tableID) const {
    return getTable(tableID);
  }
  common::cardinality_t getTableCardinality(common::table_id_t tableID,
                                            common::TableType tableType) const {
    return getTable(tableID, tableType);
  }
  common::cardinality_t getTableCardinality(
      catalog::SchemaEntry* tableEntry) const {
    return getTable(tableEntry);
  }

 private:
  const PropertyGraph* graph_ = nullptr;
#ifdef NEUG_BUILD_TEST
  std::unordered_map<common::table_id_t, common::cardinality_t>
      table_cardinalities_;
#endif
};

}  // namespace storage
}  // namespace neug
