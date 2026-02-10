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

#include "neug/compiler/optimizer/optimizer.h"

#include <fstream>
#include <iostream>

#include "neug/compiler/main/client_context.h"
#include "neug/compiler/optimizer/acc_hash_join_optimizer.h"
#include "neug/compiler/optimizer/agg_key_dependency_optimizer.h"
#include "neug/compiler/optimizer/cardinality_updater.h"
#include "neug/compiler/optimizer/common_pattern_reuse_optimizer.h"
#include "neug/compiler/optimizer/correlated_subquery_unnest_solver.h"
#include "neug/compiler/optimizer/expand_getv_fusion.h"
#include "neug/compiler/optimizer/factorization_rewriter.h"
#include "neug/compiler/optimizer/filter_push_down_optimizer.h"
#include "neug/compiler/optimizer/filter_push_down_pattern.h"
#include "neug/compiler/optimizer/flat_join_to_expand_optimizer.h"
#include "neug/compiler/optimizer/limit_push_down_optimizer.h"
#include "neug/compiler/optimizer/project_join_condition_optimizer.h"
#include "neug/compiler/optimizer/projection_push_down_optimizer.h"
#include "neug/compiler/optimizer/remove_factorization_rewriter.h"
#include "neug/compiler/optimizer/remove_subquery_as_join.h"
#include "neug/compiler/optimizer/remove_unnecessary_join_optimizer.h"
#include "neug/compiler/optimizer/rename_dependent_var_optimizer.h"
#include "neug/compiler/optimizer/schema_populator.h"
#include "neug/compiler/optimizer/top_k_optimizer.h"
#include "neug/compiler/optimizer/union_alias_map_optimizer.h"
#include "neug/compiler/planner/operator/logical_explain.h"

namespace neug {
namespace optimizer {

void Optimizer::optimize(
    planner::LogicalPlan* plan, main::ClientContext* context,
    const planner::CardinalityEstimator& cardinalityEstimator) {
  if (context->getClientConfig()->enablePlanOptimizer) {
    // Factorization structure should be removed before further optimization can
    // be applied.
    auto removeFactorizationRewriter = RemoveFactorizationRewriter();
    removeFactorizationRewriter.rewrite(plan);

    auto correlatedSubqueryUnnestSolver =
        CorrelatedSubqueryUnnestSolver(nullptr);
    correlatedSubqueryUnnestSolver.solve(plan->getLastOperator().get());

    auto removeUnnecessaryJoinOptimizer = RemoveUnnecessaryJoinOptimizer();
    removeUnnecessaryJoinOptimizer.rewrite(plan);

    auto filterPushDownOptimizer = FilterPushDownOptimizer(context);
    filterPushDownOptimizer.rewrite(plan);

    auto projectionPushDownOptimizer = ProjectionPushDownOptimizer(
        context->getClientConfig()->recursivePatternSemantic, context);
    projectionPushDownOptimizer.rewrite(plan);

    auto limitPushDownOptimizer = LimitPushDownOptimizer();
    limitPushDownOptimizer.rewrite(plan);

    // if (context->getClientConfig()->enableSemiMask) {
    //   // HashJoinSIPOptimizer should be applied after optimizers that
    //   manipulate
    //   // hash join.
    //   auto hashJoinSIPOptimizer = HashJoinSIPOptimizer();
    //   hashJoinSIPOptimizer.rewrite(plan);
    // }

    auto topKOptimizer = TopKOptimizer();
    topKOptimizer.rewrite(plan);

    // auto factorizationRewriter = FactorizationRewriter();
    // factorizationRewriter.rewrite(plan);

    // AggKeyDependencyOptimizer doesn't change factorization structure and thus
    // can be put after FactorizationRewriter.
    auto aggKeyDependencyOptimizer = AggKeyDependencyOptimizer();
    aggKeyDependencyOptimizer.rewrite(plan);

    auto filterPushDownPattern = FilterPushDownPattern();
    filterPushDownPattern.rewrite(plan);

    auto renameDependentVar = RenameDependentVarOpt();
    renameDependentVar.rewrite(plan);

    auto expandGetVFusion = ExpandGetVFusion(context->getCatalog());
    expandGetVFusion.rewrite(plan);

    auto flatJoinToExpandOptimizer = FlatJoinToExpandOptimizer();
    flatJoinToExpandOptimizer.rewrite(plan);

    auto commonPatternReuseOptimizer = CommonPatternReuseOptimizer(context);
    commonPatternReuseOptimizer.rewrite(plan);

    auto unionAliasMapOptimizer = UnionAliasMapOptimizer();
    unionAliasMapOptimizer.rewrite(plan);

    auto removeSubqueryAsJoin = RemoveSubqueryAsJoin();
    removeSubqueryAsJoin.rewrite(plan);

    auto cardinalityUpdater =
        CardinalityUpdater(cardinalityEstimator, context->getTransaction());
    cardinalityUpdater.rewrite(plan);

    auto projectJoinConditionOptimizer = ProjectJoinConditionOptimizer(context);
    projectJoinConditionOptimizer.rewrite(plan);
  }
}

}  // namespace optimizer
}  // namespace neug
