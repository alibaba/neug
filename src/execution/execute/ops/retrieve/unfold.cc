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

#include "neug/execution/execute/ops/retrieve/unfold.h"

#include <optional>

#include "neug/execution/common/context.h"
#include "neug/execution/common/operators/retrieve/unfold.h"
#include "neug/execution/expression/expr.h"
#include "neug/storages/graph/graph_interface.h"

namespace neug {
class Schema;

namespace execution {
class OprTimer;

namespace ops {
class UnfoldOpr : public IOperator {
 public:
  explicit UnfoldOpr(std::optional<int32_t> key,
                     std::unique_ptr<neug::execution::ExprBase> expr, int alias)
      : key_(key), expr_(std::move(expr)), alias_(alias) {}

  std::string get_operator_name() const override { return "UnfoldOpr"; }

  neug::result<neug::execution::Context> Eval(
      IStorageInterface& graph, const ParamsMap& params,
      neug::execution::Context&& ctx,
      neug::execution::OprTimer* timer) override {
    if (key_.has_value()) {
      return Unfold::unfold(std::move(ctx), key_.value(), alias_);
    } else {
      auto expr = expr_->bind(&graph, params);
      auto& record_expr = expr->Cast<RecordExprBase>();
      return Unfold::unfold(std::move(ctx), record_expr, alias_);
    }
  }

 private:
  std::optional<int32_t> key_;
  std::unique_ptr<neug::execution::ExprBase> expr_;
  int alias_;
};

neug::result<OpBuildResultT> UnfoldOprBuilder::Build(
    const neug::Schema& schema, const ContextMeta& ctx_meta,
    const physical::PhysicalPlan& plan, int op_idx) {
  ContextMeta ret_meta = ctx_meta;
  int alias = plan.plan(op_idx).opr().unfold().alias().value();
  const auto& expression = plan.plan(op_idx).opr().unfold().input_expr();
  auto expr = neug::execution::parse_expression(
      expression, ctx_meta, neug::execution::VarType::kRecord);
  ret_meta.set(alias, ListType::GetChildType(expr->type()));
  bool unfold_col = expression.operators_size() == 1 &&
                    expression.operators(0).has_var() &&
                    (!expression.operators(0).var().has_property());
  std::optional<int32_t> key = std::nullopt;
  if (unfold_col) {
    key = expression.operators(0).var().tag().id();
  }
  return std::make_pair(
      std::make_unique<UnfoldOpr>(key, std::move(expr), alias), ret_meta);
}

}  // namespace ops
}  // namespace execution
}  // namespace neug