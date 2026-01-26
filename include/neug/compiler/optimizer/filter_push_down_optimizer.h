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

#include <vector>
#include "neug/compiler/common/types/types.h"
#include "neug/compiler/planner/operator/logical_plan.h"

namespace neug {
namespace main {
class ClientContext;
}
namespace optimizer {

struct PredicateSet {
  binder::expression_vector equalityPredicates;
  binder::expression_vector nonEqualityPredicates;

  PredicateSet() = default;
  EXPLICIT_COPY_DEFAULT_MOVE(PredicateSet);

  bool isEmpty() const {
    return equalityPredicates.empty() && nonEqualityPredicates.empty();
  }
  void clear() {
    equalityPredicates.clear();
    nonEqualityPredicates.clear();
  }

  void addPredicate(std::shared_ptr<binder::Expression> predicate);
  std::shared_ptr<binder::Expression> popNodePKEqualityComparison(
      const binder::Expression& nodeID,
      const std::vector<common::table_id_t>& tableIDs);
  binder::expression_vector getAllPredicates();

 private:
  PredicateSet(const PredicateSet& other)
      : equalityPredicates{other.equalityPredicates},
        nonEqualityPredicates{other.nonEqualityPredicates} {}
};

class FilterPushDownOptimizer {
 public:
  explicit FilterPushDownOptimizer(main::ClientContext* context)
      : context{context} {
    predicateSet = PredicateSet();
  }
  explicit FilterPushDownOptimizer(main::ClientContext* context,
                                   PredicateSet predicateSet)
      : predicateSet{std::move(predicateSet)}, context{context} {}

  void rewrite(planner::LogicalPlan* plan);

 private:
  std::shared_ptr<planner::LogicalOperator> visitOperator(
      const std::shared_ptr<planner::LogicalOperator>& op);
  // Collect predicates in FILTER
  std::shared_ptr<planner::LogicalOperator> visitFilterReplace(
      const std::shared_ptr<planner::LogicalOperator>& op);
  // Push primary key lookup into CROSS_PRODUCT
  // E.g.
  //      Filter(a.ID=b.ID)
  //      CrossProduct                   to                  HashJoin
  //   S(a)           S(b)                            S(a)             S(b)
  std::shared_ptr<planner::LogicalOperator> visitCrossProductReplace(
      const std::shared_ptr<planner::LogicalOperator>& op);

  // Push FILTER into SCAN_NODE_TABLE, and turn index lookup into INDEX_SCAN.
  std::shared_ptr<planner::LogicalOperator> visitScanNodeTableReplace(
      const std::shared_ptr<planner::LogicalOperator>& op);
  // Push Filter into EXTEND.
  std::shared_ptr<planner::LogicalOperator> visitExtendReplace(
      const std::shared_ptr<planner::LogicalOperator>& op);
  // Push Filter into TABLE_FUNCTION_CALL
  std::shared_ptr<planner::LogicalOperator> visitTableFunctionCallReplace(
      const std::shared_ptr<planner::LogicalOperator>& op);

  // Finish the current push down optimization by apply remaining predicates as
  // a single filter. And heuristically reorder equality predicates first in the
  // filter.
  std::shared_ptr<planner::LogicalOperator> finishPushDown(
      std::shared_ptr<planner::LogicalOperator> op);
  std::shared_ptr<planner::LogicalOperator> appendFilters(
      const binder::expression_vector& predicates,
      std::shared_ptr<planner::LogicalOperator> child);

  std::shared_ptr<planner::LogicalOperator> appendScanNodeTable(
      std::shared_ptr<binder::Expression> nodeID,
      std::vector<common::table_id_t> nodeTableIDs,
      binder::expression_vector properties,
      std::shared_ptr<planner::LogicalOperator> child);
  std::shared_ptr<planner::LogicalOperator> appendFilter(
      std::shared_ptr<binder::Expression> predicate,
      std::shared_ptr<planner::LogicalOperator> child);

  std::shared_ptr<planner::LogicalOperator> visitChildren(
      const std::shared_ptr<planner::LogicalOperator>& op);

 private:
  PredicateSet predicateSet;
  main::ClientContext* context;
};

}  // namespace optimizer
}  // namespace neug
