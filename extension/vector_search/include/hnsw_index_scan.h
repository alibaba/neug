/** Copyright 2020 Alibaba Group Holding Limited.
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

#include <string>

#include "neug/compiler/function/function.h"
#include "neug/compiler/function/neug_call_function.h"
#include "neug/compiler/optimizer/logical_rule.h"
#include "neug/execution/common/context.h"
#include "neug/execution/expression/expr.h"

namespace neug::vector_search_ext {

struct HNSWIndexScanFuncInput final : function::CallFuncInputBase {
  label_t label_id;
  std::string unique_index_name;
  std::unique_ptr<execution::ExprBase> target_value;
  Value bound_target_value;
  uint32_t topk;
  int32_t vertex_alias;
  int32_t score_alias;
  execution::Context context;

  std::unique_ptr<function::CallFuncInputBase> bindParams(
      const execution::ParamsMap& params) const override;

  std::unique_ptr<function::CallFuncInputBase> bindContext(
      execution::Context&& input_context) const override {
    auto bound = std::make_unique<HNSWIndexScanFuncInput>();
    bound->label_id = label_id;
    bound->unique_index_name = unique_index_name;
    // bindParams() has already evaluated target_value. From this stage onward,
    // the per-Eval input needs only the immutable bound scalar, not a copy of
    // the expression tree.
    bound->bound_target_value = bound_target_value;
    bound->topk = topk;
    bound->vertex_alias = vertex_alias;
    bound->score_alias = score_alias;
    bound->context = std::move(input_context);
    return bound;
  }
};

struct HNSWIndexScanFunction {
  static constexpr const char* name = "HNSW_INDEX_SCAN";

  static function::function_set getFunctionSet();
};

class HNSWIndexScanOptimizer final : public optimizer::LogicalRule {
 public:
  static constexpr const char* name = "HNSW_INDEX_SCAN_OPTIMIZER";

  void rewrite(main::ClientContext* context,
               planner::LogicalPlan* plan) override;

  std::shared_ptr<planner::LogicalOperator> visitOrderByReplace(
      std::shared_ptr<planner::LogicalOperator> op) override;

 private:
  function::TableFunction* GetIndexScanFunction(
      catalog::Catalog& catalog) const;

  main::ClientContext* context_{nullptr};
};

}  // namespace neug::vector_search_ext
