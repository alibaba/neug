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
#include "neug/execution/execute/ops/retrieve/select.h"

#include "neug/execution/common/context.h"
#include "neug/execution/common/operators/retrieve/select.h"
#include "neug/execution/expression/special_predicates.h"
#include "neug/storages/graph/graph_interface.h"
#include "neug/utils/property/types.h"

#include "neug/execution/common/columns/vertex_columns.h"
#include "neug/execution/expression/predicates.h"

namespace neug {
namespace execution {
class OprTimer;

namespace ops {

class SelectIdNeOpr : public IOperator {
 public:
  explicit SelectIdNeOpr(std::unique_ptr<neug::execution::ExprBase>&& pred,
                         int tag, const std::string& prop_name,
                         const std::string& param_name)
      : tag_(tag),
        prop_name_(prop_name),
        param_name_(param_name),
        pred_(std::move(pred)) {}

  std::string get_operator_name() const override { return "SelectIdNeOpr"; }

  neug::result<neug::execution::Context> Eval(
      IStorageInterface& graph_interface, const ParamsMap& params,
      neug::execution::Context&& ctx,
      neug::execution::OprTimer* timer) override {
    auto col = ctx.get(tag_);
    const auto& name = prop_name_;
    if ((!col->is_optional()) &&
        col->column_type() == ContextColumnType::kVertex) {
      auto vertex_col = std::dynamic_pointer_cast<IVertexColumn>(col);
      auto labels = vertex_col->get_labels_set();
      if (labels.size() == 1 &&
          name == graph_interface.schema().get_vertex_primary_key_name(
                      *labels.begin())) {
        auto label = *labels.begin();
        int64_t oid = params.at(param_name_).GetValue<int64_t>();
        vid_t vid;
        if (graph_interface.GetVertexIndex(label, Property::from_int64(oid),
                                           vid)) {
          if (vertex_col->vertex_column_type() == VertexColumnType::kSingle) {
            const SLVertexColumn& sl_vertex_col =
                *(dynamic_cast<const SLVertexColumn*>(vertex_col.get()));

            return Select::select(
                std::move(ctx),
                [&sl_vertex_col, vid](const Context& ctx, size_t i) {
                  return sl_vertex_col.get_vertex(i).vid_ != vid;
                });
          } else {
            return Select::select(
                std::move(ctx),
                [&vertex_col, vid](const Context& ctx, size_t i) {
                  return vertex_col->get_vertex(i).vid_ != vid;
                });
          }
        }
      }
    }

    auto expr = pred_->bind(&graph_interface, params);
    neug::execution::GeneralPred expr_wrapper(std::move(expr));
    return Select::select(std::move(ctx), expr_wrapper);
  }

 private:
  int tag_;
  std::string prop_name_;
  std::string param_name_;
  std::unique_ptr<neug::execution::ExprBase> pred_;
};

class SelectOpr : public IOperator {
 public:
  explicit SelectOpr(std::unique_ptr<neug::execution::ExprBase>&& expr)
      : pred_(std::move(expr)) {}

  std::string get_operator_name() const override { return "SelectOpr"; }

  neug::result<neug::execution::Context> Eval(
      IStorageInterface& graph, const ParamsMap& params,
      neug::execution::Context&& ctx,
      neug::execution::OprTimer* timer) override {
    auto expr = pred_->bind(&graph, params);

    neug::execution::GeneralPred expr_wrapper(std::move(expr));
    return Select::select(std::move(ctx), expr_wrapper);
  }

 private:
  std::unique_ptr<neug::execution::ExprBase> pred_;
};

neug::result<OpBuildResultT> SelectOprBuilder::Build(
    const neug::Schema& schema, const ContextMeta& ctx_meta,
    const physical::PhysicalPlan& plan, int op_idx) {
  auto opr = plan.plan(op_idx).opr().select();
  auto type = parse_sp_pred(opr.predicate());
  const auto& op2 = opr.predicate().operators(2);

  std::unique_ptr<neug::execution::ExprBase> pred =
      neug::execution::parse_expression(opr.predicate(), ctx_meta,
                                        neug::execution::VarType::kRecord);
  ContextMeta ret_meta = ctx_meta;
  if (type == SPPredicateType::kPropertyNE && op2.has_param()) {
    auto var = opr.predicate().operators(0).var();
    int tag = var.has_tag() ? var.tag().id() : -1;
    if (var.has_property()) {
      auto name = var.property().key().name();
      auto type = parse_from_ir_data_type(
          opr.predicate().operators(2).param().data_type());
      auto param_name = op2.param().name();
      if (name == "id" && type.id() == DataTypeId::kInt64) {
        return std::make_pair(std::make_unique<SelectIdNeOpr>(
                                  std::move(pred), tag, name, param_name),
                              ret_meta);
      }
    }
  }

  return std::make_pair(std::make_unique<SelectOpr>(std::move(pred)), ret_meta);
}

}  // namespace ops
}  // namespace execution
}  // namespace neug