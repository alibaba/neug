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

/**
 * This file is originally from the Kùzu project
 * (https://github.com/kuzudb/kuzu) Licensed under the MIT License. Modified by
 * Zhou Xiaoli in 2025 to support Neug-specific features.
 */

#pragma once

#include <string>

#include "neug/compiler/binder/expression/node_expression.h"
#include "neug/compiler/common/assert.h"
#include "neug/compiler/common/copy_constructors.h"
#include "neug/compiler/common/types/types.h"
#include "neug/storages/graph/schema.h"

namespace neug {
namespace main {
class ClientContext;
}
namespace planner {
class Schema;
}
namespace function {
struct TableFuncBindInput;
}
namespace graph {

struct BoundGraphEntryTableInfo {
  SchemaEntry* entry;

  std::shared_ptr<binder::Expression> nodeOrRel;
  std::shared_ptr<binder::Expression> predicate;

  explicit BoundGraphEntryTableInfo(SchemaEntry* entry) : entry{entry} {}
  BoundGraphEntryTableInfo(SchemaEntry* entry,
                           std::shared_ptr<binder::Expression> nodeOrRel,
                           std::shared_ptr<binder::Expression> predicate)
      : entry{entry},
        nodeOrRel{std::move(nodeOrRel)},
        predicate{std::move(predicate)} {}
};

// Organize projected graph similar to CatalogEntry. When we want to share
// projected graph across statements, we need to migrate this class to catalog
// (or client context).
struct NEUG_API GraphEntry {
  std::vector<BoundGraphEntryTableInfo> nodeInfos;
  std::vector<BoundGraphEntryTableInfo> relInfos;

  GraphEntry() = default;
  GraphEntry(std::vector<SchemaEntry*> nodeEntries,
             std::vector<SchemaEntry*> relEntries);
  EXPLICIT_COPY_DEFAULT_MOVE(GraphEntry);

  bool isEmpty() const { return nodeInfos.empty() && relInfos.empty(); }

  std::vector<common::table_id_t> getNodeTableIDs() const;
  std::vector<common::table_id_t> getRelTableIDs() const;
  std::vector<SchemaEntry*> getNodeEntries() const;
  std::vector<SchemaEntry*> getRelEntries() const;

  const BoundGraphEntryTableInfo& getRelInfo(common::table_id_t tableID) const;

  void setRelPredicate(std::shared_ptr<binder::Expression> predicate);

  std::string toString() const {
    std::string result = "GraphEntry{";
    result += "nodeInfos=[";
    for (auto& nodeInfo : nodeInfos) {
      result += nodeInfo.entry->get_label() + ", ";
    }
    result += "], relInfos=[";
    for (auto& relInfo : relInfos) {
      result += relInfo.entry->get_label() + ", ";
    }
    result += "]}";
    return result;
  }

 private:
  GraphEntry(const GraphEntry& other)
      : nodeInfos{other.nodeInfos}, relInfos{other.relInfos} {}
};

// Validates `ProjectedGraphEntry` against the catalog and returns a bound
// [`GraphEntry`] (predicates are accepted as strings; expression binding is
// deferred).
class NEUG_API GDSFunction {
 public:
  static GraphEntry bindGraphEntry(main::ClientContext& clientContext,
                                   const ProjectedGraphEntry& entry);

  static BoundGraphEntryTableInfo bindNodeEntry(
      main::ClientContext& clientContext, const std::string& tableName,
      const std::string& predicate);

  static BoundGraphEntryTableInfo bindRelEntry(
      main::ClientContext& clientContext,
      const std::vector<std::string>& triplets, const std::string& predicate);

  static std::shared_ptr<binder::NodeExpression> bindNodeOutput(
      const function::TableFuncBindInput& bindInput,
      const std::vector<SchemaEntry*>& nodeEntries, const std::string& name,
      const std::optional<uint64_t>& yieldVariableIdx = std::nullopt);

  static std::shared_ptr<binder::Expression> bindRelOutput(
      const function::TableFuncBindInput& bindInput,
      const std::vector<SchemaEntry*>& relEntries,
      std::shared_ptr<binder::NodeExpression> srcNode,
      std::shared_ptr<binder::NodeExpression> dstNode, const std::string& name,
      const std::optional<uint64_t>& yieldVariableIdx = std::nullopt);
};

}  // namespace graph
}  // namespace neug
