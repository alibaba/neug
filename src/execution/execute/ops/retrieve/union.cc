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

#include <map>
#include <memory>
#include <string>
#include <utility>

#include "neug/execution/common/context.h"
#include "neug/execution/common/graph_interface.h"
#include "neug/execution/common/operators/retrieve/union.h"
#include "neug/execution/execute/pipeline.h"
#include "neug/execution/execute/plan_parser.h"

namespace gs {
class Schema;

namespace runtime {
class OprTimer;

namespace ops {
class UnionOpr : public IReadOperator {
 public:
  explicit UnionOpr(std::vector<ReadPipeline>&& sub_plans)
      : sub_plans_(std::move(sub_plans)) {}

  std::string get_operator_name() const override { return "UnionOpr"; }

  gs::result<gs::runtime::Context> Eval(
      const gs::runtime::GraphReadInterface& graph,
      const std::map<std::string, std::string>& params,
      gs::runtime::Context&& ctx, gs::runtime::OprTimer* timer) override {
    std::vector<gs::runtime::Context> ctxs;
    for (auto& plan : sub_plans_) {
      gs::runtime::Context n_ctx = ctx;
      std::unique_ptr<gs::runtime::OprTimer> sub_timer =
          (timer != nullptr) ? std::make_unique<gs::runtime::OprTimer>()
                             : nullptr;
      auto ret = plan.Execute(graph, std::move(n_ctx), params, sub_timer.get());
      if (timer != nullptr)
        [[unlikely]] { timer->add_child(std::move(sub_timer)); }
      if (!ret) {
        return ret;
      }
      ctxs.emplace_back(std::move(ret.value()));
    }
    return Union::union_op(std::move(ctxs));
  }

 private:
  std::vector<ReadPipeline> sub_plans_;
};
gs::result<ReadOpBuildResultT> UnionOprBuilder::Build(
    const gs::Schema& schema, const ContextMeta& ctx_meta,
    const physical::PhysicalPlan& plan, int op_idx) {
  std::vector<ReadPipeline> sub_plans;
  std::vector<ContextMeta> sub_metas;
  for (int i = 0;
       i < plan.query_plan().plan(op_idx).opr().union_().sub_plans_size();
       ++i) {
    auto& sub_plan = plan.query_plan().plan(op_idx).opr().union_().sub_plans(i);
    auto pair_res = PlanParser::get().parse_read_pipeline_with_meta(
        schema, ctx_meta, sub_plan);
    if (!pair_res) {
      return std::make_pair(nullptr, ContextMeta());
    }
    auto pair = std::move(pair_res.value());
    sub_plans.emplace_back(std::move(pair.first));
    sub_metas.push_back(pair.second);
  }
  // TODO(liulexiao): check sub metas consisitency
  return std::make_pair(std::make_unique<UnionOpr>(std::move(sub_plans)),
                        *sub_metas.rbegin());
}
}  // namespace ops
}  // namespace runtime
}  // namespace gs
