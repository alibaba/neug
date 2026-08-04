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

#include <cstdint>
#include <string>
#include <unordered_map>

#include "neug/storages/graph/graph_view.h"

namespace neug {
namespace main {
class MetadataManager;
}

namespace catalog {
class CatalogEntry;
}

// Compiler inputs derived from one GraphView. planning_generation is copied
// from the same pinned snapshot as view_, so cache lookup and compilation use
// a coherent set of planner inputs.
class GraphStats {
 public:
  GraphStats() = default;
  GraphStats(const GraphView& view, uint64_t planning_generation)
      : view_(&view), planning_generation_(planning_generation) {}

  void UpdateView(const GraphView& view, uint64_t planning_generation) {
    view_ = &view;
    planning_generation_ = planning_generation;
  }
  const Schema& schema() const { return view_->schema(); }
  uint64_t planning_generation() const { return planning_generation_; }
#ifdef NEUG_BUILD_TEST
  void LoadFromJson(const Schema& schema, const std::string& stats_json);
#endif

  uint64_t getTable(uint64_t tableID) const;
  uint64_t getTable(uint64_t tableID, SchemaEntryType tableType) const;
  uint64_t getTable(SchemaEntry* tableEntry) const;
  uint64_t getTableCardinality(uint64_t tableID) const {
    return getTable(tableID);
  }
  uint64_t getTableCardinality(uint64_t tableID,
                               SchemaEntryType tableType) const {
    return getTable(tableID, tableType);
  }
  uint64_t getTableCardinality(SchemaEntry* tableEntry) const {
    return getTable(tableEntry);
  }

 private:
  const GraphView* view_ = nullptr;
  uint64_t planning_generation_{0};
#ifdef NEUG_BUILD_TEST
  std::unordered_map<uint64_t, uint64_t> table_cardinalities_;
#endif
};

}  // namespace neug
