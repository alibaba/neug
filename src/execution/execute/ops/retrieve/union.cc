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

#include "neug/execution/execute/ops/retrieve/union.h"

#include "neug/execution/common/context.h"
#include "neug/execution/common/operators/retrieve/union.h"
#include "neug/execution/execute/pipeline.h"
#include "neug/execution/execute/plan_parser.h"
#include "neug/storages/graph/graph_interface.h"
#include "neug/utils/likely.h"

namespace neug {
class Schema;

namespace execution {
class OprTimer;

namespace ops {
class UnionOpr : public IOperator {
 public:
  explicit UnionOpr(std::vector<Pipeline>&& sub_plans)
      : sub_plans_(std::move(sub_plans)) {}

  std::string get_operator_name() const override { return "UnionOpr"; }

  neug::result<neug::execution::Context> Eval(
      IStorageInterface& graph, const ParamsMap& params,
      neug::execution::Context&& ctx,
      neug::execution::OprTimer* timer) override {
    std::vector<neug::execution::Context> ctxs;
    for (auto& plan : sub_plans_) {
      neug::execution::Context n_ctx = ctx;
      std::unique_ptr<neug::execution::OprTimer> sub_timer =
          (timer != nullptr) ? std::make_unique<neug::execution::OprTimer>()
                             : nullptr;
      auto ret = plan.Execute(graph, std::move(n_ctx), params, sub_timer.get());
      if (NEUG_UNLIKELY(timer != nullptr)) {
        timer->add_child(std::move(sub_timer));
      }
      if (!ret) {
        return ret;
      }
      ctxs.emplace_back(std::move(ret.value()));
    }
    return Union::union_op(std::move(ctxs));
  }

 private:
  std::vector<Pipeline> sub_plans_;
};
neug::result<OpBuildResultT> UnionOprBuilder::Build(
    const neug::Schema& schema, const ContextMeta& ctx_meta,
    const physical::PhysicalPlan& plan, int op_idx) {
  std::vector<Pipeline> sub_plans;
  std::vector<ContextMeta> sub_metas;
  ContextMeta ret_meta = ctx_meta;
  for (int i = 0; i < plan.plan(op_idx).opr().union_().sub_plans_size(); ++i) {
    auto& sub_plan = plan.plan(op_idx).opr().union_().sub_plans(i);
    auto pair_res = PlanParser::get().parse_execute_pipeline_with_meta(
        schema, ctx_meta, sub_plan);
    if (!pair_res) {
      return std::make_pair(nullptr, ContextMeta());
    }
    auto pair = std::move(pair_res.value());
    sub_plans.emplace_back(std::move(pair.first));
    sub_metas.push_back(pair.second);
  }

  return std::make_pair(std::make_unique<UnionOpr>(std::move(sub_plans)),
                        ret_meta);
}
}  // namespace ops
}  // namespace execution
}  // namespace neug