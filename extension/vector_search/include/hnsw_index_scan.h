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

namespace neug::vector_search_ext {

struct HNSWIndexScanFuncInput final : function::CallFuncInputBase {
  label_t label_id;
  std::string unique_index_name;
  Value target_value;
  uint32_t topk;
  int32_t vertex_alias;
  int32_t score_alias;
  execution::Context context;

  void bindContext(execution::Context&& input_context) override {
    context = std::move(input_context);
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
