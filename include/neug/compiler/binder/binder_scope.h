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

#include <algorithm>
#include <map>
#include <string>
#include <unordered_map>
#include <vector>
#include "neug/compiler/binder/expression/expression.h"
#include "neug/compiler/binder/expression/node_expression.h"
#include "neug/compiler/common/case_insensitive_map.h"

namespace neug {
namespace binder {

class BinderScope {
 public:
  BinderScope() = default;
  EXPLICIT_COPY_DEFAULT_MOVE(BinderScope);

  bool empty() const { return expressions.empty(); }
  bool contains(const std::string& varName) const {
    return nameToExprIdx.contains(varName);
  }
  std::shared_ptr<Expression> getExpression(const std::string& varName) const {
    NEUG_ASSERT(nameToExprIdx.contains(varName));
    return expressions[nameToExprIdx.at(varName)];
  }
  expression_vector getExpressions() const { return expressions; }
  void addExpression(const std::string& varName,
                     std::shared_ptr<Expression> expression);
  void replaceExpression(const std::string& oldName, const std::string& newName,
                         std::shared_ptr<Expression> expression);

  void memorizeTableEntries(const std::string& name,
                            std::vector<catalog::TableCatalogEntry*> entries) {
    memorizedNodeNameToEntries.insert({name, entries});
  }
  bool hasMemorizedTableIDs(const std::string& name) const {
    return memorizedNodeNameToEntries.contains(name);
  }
  std::vector<catalog::TableCatalogEntry*> getMemorizedTableEntries(
      const std::string& name) {
    NEUG_ASSERT(memorizedNodeNameToEntries.contains(name));
    return memorizedNodeNameToEntries.at(name);
  }

  void addNodeReplacement(std::shared_ptr<NodeExpression> node) {
    nodeReplacement.insert({node->getVariableName(), node});
  }
  bool hasNodeReplacement(const std::string& name) const {
    return nodeReplacement.contains(name);
  }
  std::shared_ptr<NodeExpression> getNodeReplacement(
      const std::string& name) const {
    NEUG_ASSERT(hasNodeReplacement(name));
    return nodeReplacement.at(name);
  }

  std::unordered_map<std::string, std::shared_ptr<Expression>>
  getNameToExprMap() const {
    std::unordered_map<std::string, std::shared_ptr<Expression>> nameToExprMap;
    for (const auto& pair : nameToExprIdx) {
      NEUG_ASSERT(pair.second < expressions.size());
      nameToExprMap[pair.first] = expressions[pair.second];
    }
    return nameToExprMap;
  }

  void clear();

 private:
  BinderScope(const BinderScope& other)
      : expressions{other.expressions},
        nameToExprIdx{other.nameToExprIdx},
        memorizedNodeNameToEntries{other.memorizedNodeNameToEntries} {}

 private:
  // Expressions in scope. Order should be preserved.
  expression_vector expressions;
  common::case_insensitive_map_t<common::idx_t> nameToExprIdx;
  // A node might be popped out of scope. But we may need to retain its table ID
  // information. E.g. MATCH (a:person) WITH collect(a) AS list_a UNWIND list_a
  // AS new_a MATCH (new_a)-[]->() It will be more performant if we can retain
  // the information that new_a has label person.
  common::case_insensitive_map_t<std::vector<catalog::TableCatalogEntry*>>
      memorizedNodeNameToEntries;
  // A node pattern may not always be bound as a node expression, e.g. in the
  // above query, (new_a) is bound as a variable rather than node expression.
  common::case_insensitive_map_t<std::shared_ptr<NodeExpression>>
      nodeReplacement;
};

}  // namespace binder
}  // namespace neug
