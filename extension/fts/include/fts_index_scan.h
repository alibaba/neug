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

#pragma once

#include <optional>
#include <string>

#include "neug/compiler/binder/expression/scalar_function_expression.h"
#include "neug/compiler/function/function.h"
#include "neug/compiler/function/neug_call_function.h"
#include "neug/compiler/optimizer/logical_rule.h"
#include "neug/compiler/planner/operator/logical_projection.h"
#include "neug/execution/common/context.h"
#include "neug/execution/expression/expr.h"

namespace neug::fts_ext {

struct FTSIndexScanFuncInput final : function::CallFuncInputBase {
  label_t label_id;
  std::string unique_index_name;
  std::unique_ptr<execution::ExprBase> query_string;
  Value bound_query_string;
  std::optional<uint64_t> limit;
  bool ascending{true};
  int32_t node_alias;
  int32_t score_alias;
  execution::Context context;

  std::unique_ptr<function::CallFuncInputBase> bindParams(
      const execution::ParamsMap& params) const override;

  std::unique_ptr<function::CallFuncInputBase> bindContext(
      execution::Context&& input_context) const override {
    auto bound = std::make_unique<FTSIndexScanFuncInput>();
    bound->label_id = label_id;
    bound->unique_index_name = unique_index_name;
    // bindParams() has already evaluated query_string. From this stage onward,
    // the per-Eval input needs only the immutable bound scalar, not a copy of
    // the expression tree.
    bound->bound_query_string = bound_query_string;
    bound->limit = limit;
    bound->ascending = ascending;
    bound->node_alias = node_alias;
    bound->score_alias = score_alias;
    bound->context = std::move(input_context);
    return bound;
  }
};

struct FTSIndexScanFunction {
  static constexpr const char* name = "FTS_INDEX_SCAN";

  static function::function_set getFunctionSet();
};

class FTSIndexScanOptimizer final : public optimizer::LogicalRule {
 public:
  static constexpr const char* name = "FTS_INDEX_SCAN_OPTIMIZER";

  void rewrite(main::ClientContext* context,
               planner::LogicalPlan* plan) override;

  std::shared_ptr<planner::LogicalOperator> visitOperator(
      const std::shared_ptr<planner::LogicalOperator>& op) override;

  std::shared_ptr<planner::LogicalOperator> visitOrderByReplace(
      std::shared_ptr<planner::LogicalOperator> op) override;

  std::shared_ptr<planner::LogicalOperator> visitProjectionReplace(
      std::shared_ptr<planner::LogicalOperator> op) override;

 private:
  function::TableFunction* GetIndexScanFunction(
      catalog::Catalog& catalog) const;

  void RewriteProjection(
      planner::LogicalProjection* projection,
      const std::shared_ptr<binder::ScalarFunctionExpression>& bm25,
      bool ascending, std::optional<uint64_t> limit);

  main::ClientContext* context_{nullptr};
};

}  // namespace neug::fts_ext
