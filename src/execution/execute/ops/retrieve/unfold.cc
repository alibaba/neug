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

#include <google/protobuf/wrappers.pb.h>
#include <map>
#include <memory>
#include <string>
#include <utility>

#include "neug/execution/common/context.h"
#include "neug/execution/common/operators/retrieve/unfold.h"
#include "neug/execution/utils/expr.h"
#include "neug/storages/graph/graph_interface.h"

namespace gs {
class Schema;

namespace runtime {
class OprTimer;

namespace ops {
class UnfoldOpr : public IOperator {
 public:
  explicit UnfoldOpr(const ::common::Expression& expr, int alias)
      : expr_(expr), alias_(alias) {}

  std::string get_operator_name() const override { return "UnfoldOpr"; }

  gs::result<gs::runtime::Context> Eval(
      IStorageInterface& graph,
      const std::map<std::string, std::string>& params,
      gs::runtime::Context&& ctx, gs::runtime::OprTimer* timer) override {
    if (expr_.operators_size() == 1 && expr_.operators(0).has_var() &&
        (!expr_.operators(0).var().has_property())) {
      int key = expr_.operators(0).var().tag().id();
      return Unfold::unfold(std::move(ctx), key, alias_);
    } else {
      StorageReadInterface* r_graph = nullptr;
      if (graph.readable()) {
        r_graph = dynamic_cast<StorageReadInterface*>(&graph);
      }
      Expr expr(r_graph, ctx, params, expr_, VarType::kPathVar);
      return Unfold::unfold(std::move(ctx), expr, alias_);
    }
  }

 private:
  ::common::Expression expr_;
  int alias_;
};

gs::result<OpBuildResultT> UnfoldOprBuilder::Build(
    const gs::Schema& schema, const ContextMeta& ctx_meta,
    const physical::PhysicalPlan& plan, int op_idx) {
  ContextMeta ret_meta = ctx_meta;
  int alias = plan.plan(op_idx).opr().unfold().alias().value();
  ret_meta.set(alias);
  return std::make_pair(
      std::make_unique<UnfoldOpr>(plan.plan(op_idx).opr().unfold().input_expr(),
                                  alias),
      ret_meta);
}

}  // namespace ops
}  // namespace runtime
}  // namespace gs