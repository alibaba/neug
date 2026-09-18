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

#include "neug/execution/execute/ops/retrieve/limit.h"

#include "neug/execution/common/context.h"
#include "neug/execution/common/operators/retrieve/limit.h"
#include "neug/execution/execute/ops/retrieve/range_expression.h"
#include "neug/storages/graph/graph_interface.h"

namespace neug {
class Schema;

namespace execution {
class OprTimer;

namespace ops {
class LimitOpr : public IOperator {
 public:
  LimitOpr(const algebra::Limit& opr, const ContextMeta& ctx_meta)
      : range_(opr.range(), ctx_meta) {}

  std::string get_operator_name() const override { return "LimitOpr"; }

  neug::result<neug::execution::Context> Eval(
      IStorageInterface& graph, const ParamsMap& params,
      neug::execution::Context&& ctx,
      neug::execution::OprTimer* timer) override {
    auto range = range_.bind(&graph, params);
    ctx.ensure_single_chunk("LimitOpr");
    return ctx.apply_chunks(
        [&](ContextChunk&& chunk) -> neug::result<ContextChunk> {
          return Limit::limit(std::move(chunk), range.lower, range.upper);
        });
  }

 private:
  RangeExpression range_;
};

neug::result<OpBuildResultT> LimitOprBuilder::Build(
    const neug::Schema& schema, const ContextMeta& ctx_meta,
    const physical::PhysicalPlan& plan, int op_idx) {
  return std::make_pair(
      std::make_unique<LimitOpr>(plan.plan(op_idx).opr().limit(), ctx_meta),
      ctx_meta);
}

}  // namespace ops
}  // namespace execution
}  // namespace neug
