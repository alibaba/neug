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
#include "neug/execution/execute/ops/update/select.h"

#include <stddef.h>

#include <map>
#include <string>
#include <utility>

#include "neug/execution/common/context.h"
#include "neug/execution/common/graph_interface.h"
#include "neug/execution/common/operators/retrieve/select.h"
#include "neug/execution/utils/expr.h"
#include "neug/execution/utils/var.h"

namespace gs {
class Schema;

namespace runtime {
class OprTimer;

namespace ops {
class USelectOpr : public IOperator {
 public:
  explicit USelectOpr(const common::Expression& predicate)
      : predicate_(predicate) {}
  gs::result<Context> Eval(IStorageInterface& graph,
                           const std::map<std::string, std::string>& params,
                           Context&& ctx, OprTimer* timer) override {
    Expr expr(dynamic_cast<StorageUpdateInterface*>(&graph), ctx, params,
              predicate_, VarType::kPathVar);
    Arena arena;
    return Select::select(std::move(ctx), [&](size_t idx) {
      return expr.eval_path(idx, arena).as_bool();
    });
  }

  std::string get_operator_name() const override { return "USelectOpr"; }

 private:
  common::Expression predicate_;
};

gs::result<OpBuildResultT> USelectOprBuilder::Build(
    const Schema& schema, const ContextMeta& ctx_meta,
    const physical::PhysicalPlan& plan, int op_idx) {
  auto opr = plan.query_plan().plan(op_idx).opr().select();
  return std::make_pair(std::make_unique<USelectOpr>(opr.predicate()),
                        ContextMeta());
}
}  // namespace ops
}  // namespace runtime
}  // namespace gs
