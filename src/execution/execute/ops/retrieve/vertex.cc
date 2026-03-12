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

#include "neug/execution/execute/ops/retrieve/vertex.h"

#include "neug/execution/common/context.h"
#include "neug/execution/common/operators/retrieve/get_v.h"
#include "neug/execution/common/types/graph_types.h"
#include "neug/execution/expression/expr.h"
#include "neug/execution/utils/params.h"
#include "neug/execution/utils/pb_parse_utils.h"
#include "neug/storages/graph/graph_interface.h"
#include "neug/utils/property/types.h"

namespace neug {
class Schema;

namespace execution {
class OprTimer;

namespace ops {

class GetVFromEdgesOpr : public IOperator {
 public:
  GetVFromEdgesOpr(std::unique_ptr<ExprBase>&& pred, const GetVParams& p)
      : pred_(std::move(pred)), v_params_(p) {}

  std::string get_operator_name() const override { return "GetVFromEdgesOpr"; }

  neug::result<neug::execution::Context> Eval(
      IStorageInterface& graph, const ParamsMap& params,
      neug::execution::Context&& ctx,
      neug::execution::OprTimer* timer) override {
    if (pred_ != nullptr) {
      auto expr = pred_->bind(&graph, params);
      GeneralPred pred(std::move(expr));
      return GetV::get_vertex_from_edges(graph, std::move(ctx), v_params_,
                                         pred);
    } else {
      return GetV::get_vertex_from_edges(graph, std::move(ctx), v_params_,
                                         DummyPred());
    }
  }

 private:
  std::unique_ptr<ExprBase> pred_;
  GetVParams v_params_;
};

neug::result<OpBuildResultT> VertexOprBuilder::Build(
    const neug::Schema& schema, const ContextMeta& ctx_meta,
    const physical::PhysicalPlan& plan, int op_idx) {
  const auto& vertex = plan.plan(op_idx).opr().vertex();

  int alias = -1;
  if (vertex.has_alias()) {
    alias = plan.plan(op_idx).opr().vertex().alias().value();
  }

  ContextMeta ret_meta = ctx_meta;
  ret_meta.set(alias, DataType::VERTEX);

  int tag = -1;
  if (vertex.has_tag()) {
    tag = vertex.tag().value();
  }
  VOpt opt = parse_opt(vertex.opt());

  if (!vertex.has_params()) {
    LOG(ERROR) << "GetV should have params" << vertex.DebugString();
    return std::make_pair(nullptr, ContextMeta());
  }
  GetVParams p;
  p.opt = opt;
  p.tag = tag;
  p.tables = parse_tables(vertex.params());
  p.alias = alias;

  std::unique_ptr<ExprBase> predicate = nullptr;
  if (vertex.params().has_predicate()) {
    predicate = parse_expression(vertex.params().predicate(), ctx_meta,
                                 VarType::kVertex);
  }

  if (opt == VOpt::kEnd || opt == VOpt::kStart || opt == VOpt::kOther) {
    return std::make_pair(
        std::make_unique<GetVFromEdgesOpr>(std::move(predicate), p), ret_meta);
  } else {
    THROW_NOT_IMPLEMENTED_EXCEPTION(std::string("GetV with opt") +
                                    std::to_string(static_cast<int>(opt)) +
                                    " is not supported yet");
  }

  return std::make_pair(nullptr, ContextMeta());
}
}  // namespace ops
}  // namespace execution
}  // namespace neug