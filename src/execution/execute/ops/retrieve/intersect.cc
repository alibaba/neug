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

#include "neug/execution/execute/ops/retrieve/intersect.h"

#include "neug/execution/common/operators/retrieve/intersect.h"
#include "neug/execution/expression/expr.h"
#include "neug/execution/utils/params.h"
#include "neug/execution/utils/pb_parse_utils.h"
#include "neug/storages/graph/graph_interface.h"

namespace neug {

namespace execution {

namespace ops {

class IntersectOprMultip : public IOperator {
 public:
  IntersectOprMultip(const std::vector<EdgeExpandParams>& eeps,
                     std::vector<std::unique_ptr<ExprBase>>&& vertex_preds,
                     std::vector<std::unique_ptr<ExprBase>>&& edge_preds,
                     int alias)
      : eeps_(eeps),
        vertex_preds_(std::move(vertex_preds)),
        edge_preds_(std::move(edge_preds)),
        alias_(alias) {}
  std::string get_operator_name() const override {
    return "IntersectOprMultip";
  }

  neug::result<neug::execution::Context> Eval(
      IStorageInterface& graph_interface, const ParamsMap& params,
      neug::execution::Context&& ctx,
      neug::execution::OprTimer* timer) override {
    const auto& graph =
        dynamic_cast<const StorageReadInterface&>(graph_interface);
    std::vector<EdgeAndNbrPredicate> preds;
    for (size_t i = 0; i < edge_preds_.size(); ++i) {
      std::unique_ptr<BindedExprBase> v_pred =
          vertex_preds_[i] ? vertex_preds_[i]->bind(&graph, params) : nullptr;
      std::unique_ptr<BindedExprBase> e_pred =
          edge_preds_[i] ? edge_preds_[i]->bind(&graph, params) : nullptr;
      preds.emplace_back(std::move(v_pred), std::move(e_pred));
    }
    if (eeps_.size() == 2) {
      return Intersect::Binary_Intersect(
          graph, params, std::move(ctx), std::move(preds[0]),
          std::move(preds[1]), eeps_[0], eeps_[1], alias_);
    }

    return Intersect::Multiple_Intersect(graph, params, std::move(ctx),
                                         std::move(preds), eeps_, alias_);
  }

  std::vector<EdgeExpandParams> eeps_;
  std::vector<std::unique_ptr<ExprBase>> vertex_preds_;
  std::vector<std::unique_ptr<ExprBase>> edge_preds_;
  int alias_;
};

class IntersectWithEdgeOpr : public IOperator {
 public:
  IntersectWithEdgeOpr(const EdgeExpandParams& eep0,
                       const EdgeExpandParams& eep1, int v_alias,
                       std::unique_ptr<ExprBase>&& left_v_pred,
                       std::unique_ptr<ExprBase>&& right_v_pred,
                       std::unique_ptr<ExprBase>&& left_e_pred,
                       std::unique_ptr<ExprBase>&& right_e_pred,
                       const std::vector<int>& edge_alias)
      : eep0_(eep0),
        eep1_(eep1),
        v_alias_(v_alias),
        left_v_pred_(std::move(left_v_pred)),
        right_v_pred_(std::move(right_v_pred)),
        left_e_pred_(std::move(left_e_pred)),
        right_e_pred_(std::move(right_e_pred)),
        edge_alias_(edge_alias) {}

  std::string get_operator_name() const override {
    return "IntersectWithEdgeOpr";
  }

  neug::result<neug::execution::Context> Eval(
      IStorageInterface& graph_interface, const ParamsMap& params,
      neug::execution::Context&& ctx,
      neug::execution::OprTimer* timer) override {
    const auto& graph =
        dynamic_cast<const StorageReadInterface&>(graph_interface);
    std::unique_ptr<BindedExprBase> left_v_pred =
        left_v_pred_ ? left_v_pred_->bind(&graph, params) : nullptr;
    std::unique_ptr<BindedExprBase> right_v_pred =
        right_v_pred_ ? right_v_pred_->bind(&graph, params) : nullptr;
    std::unique_ptr<BindedExprBase> left_e_pred =
        left_e_pred_ ? left_e_pred_->bind(&graph, params) : nullptr;
    std::unique_ptr<BindedExprBase> right_e_pred =
        right_e_pred_ ? right_e_pred_->bind(&graph, params) : nullptr;
    EdgeAndNbrPredicate left_pred(std::move(left_v_pred),
                                  std::move(left_e_pred));
    EdgeAndNbrPredicate right_pred(std::move(right_v_pred),
                                   std::move(right_e_pred));
    return Intersect::Binary_Intersect_With_Edge(
        graph, params, std::move(ctx), std::move(left_pred),
        std::move(right_pred), eep0_, eep1_, v_alias_, edge_alias_);
  }

 private:
  EdgeExpandParams eep0_;
  EdgeExpandParams eep1_;
  int v_alias_;
  std::unique_ptr<ExprBase> left_v_pred_;
  std::unique_ptr<ExprBase> right_v_pred_;
  std::unique_ptr<ExprBase> left_e_pred_;
  std::unique_ptr<ExprBase> right_e_pred_;
  std::vector<int> edge_alias_;
};

EdgeExpandParams parse_edge_params(
    const physical::EdgeExpand& edge,
    const physical::PhysicalOpr_MetaData& meta_data) {
  EdgeExpandParams eep;
  int alias = -1;
  if (edge.has_alias()) {
    alias = edge.alias().value();
  }
  int v_tag = edge.has_v_tag() ? edge.v_tag().value() : -1;
  Direction dir = parse_direction(edge.direction());
  bool is_optional = edge.is_optional();
  eep.labels = parse_label_triplets(meta_data);
  eep.v_tag = v_tag;
  eep.dir = dir;
  eep.alias = alias;
  eep.is_optional = is_optional;
  return eep;
}

void parse(const physical::PhysicalPlan& plan, EdgeExpandParams& params,
           std::unique_ptr<ExprBase>& vpred, std::unique_ptr<ExprBase>& epred,
           const ContextMeta& ctx_meta) {
  vpred = nullptr;
  epred = nullptr;
  if (plan.plan_size() > 2 || plan.plan_size() < 1) {
    THROW_RUNTIME_ERROR(
        "sub-plan of intersect operator should have 1 or 2 plans");
  }
  if (plan.plan_size() >= 1) {
    if (plan.plan(0).opr().op_kind_case() !=
        physical::PhysicalOpr_Operator::OpKindCase::kEdge) {
      THROW_RUNTIME_ERROR(
          "the first operator in sub-plan of intersect operator should be "
          "edge expand");
    }
    const auto& edge = plan.plan(0).opr().edge();
    params = parse_edge_params(edge, plan.plan(0).meta_data(0));
    if (edge.has_params() && edge.params().has_predicate()) {
      epred =
          parse_expression(edge.params().predicate(), ctx_meta, VarType::kEdge);
    }
  }
  if (plan.plan_size() == 2) {
    if (plan.plan(1).opr().op_kind_case() !=
        physical::PhysicalOpr_Operator::OpKindCase::kVertex) {
      THROW_RUNTIME_ERROR(
          "the second operator in sub-plan of intersect operator should be "
          "vertex");
    }
    const auto& vertex = plan.plan(1).opr().vertex();
    if (vertex.has_params() && vertex.params().has_predicate()) {
      vpred = parse_expression(vertex.params().predicate(), ctx_meta,
                               VarType::kVertex);
    }
  }
}
neug::result<OpBuildResultT> IntersectOprBuilder::Build(
    const Schema& schema, const ContextMeta& ctx_meta,
    const physical::PhysicalPlan& plan, int op_idx) {
  const auto& intersect_opr = plan.plan(op_idx).opr().intersect();
  std::vector<EdgeExpandParams> eeps_(intersect_opr.sub_plans_size());
  std::vector<std::unique_ptr<ExprBase>> vertex_preds_(
      intersect_opr.sub_plans_size());
  std::vector<std::unique_ptr<ExprBase>> edge_preds_(
      intersect_opr.sub_plans_size());
  for (int i = 0; i < intersect_opr.sub_plans_size(); ++i) {
    parse(intersect_opr.sub_plans(i), eeps_[i], vertex_preds_[i],
          edge_preds_[i],
          ctx_meta);  // Parse edge expand params and predicates
  }
  const auto& sub_left = intersect_opr.sub_plans(0);

  int alias = -1;
  // There are two different cases for Intersect
  // 1. The subplans are composed of EdgeExpand + GetV.
  // 2. The subplans only contains EdgeExpandV.
  if (sub_left.plan_size() == 1) {
    const auto& edge = sub_left.plan(0).opr().edge();
    if (edge.expand_opt() ==
        physical::EdgeExpand_ExpandOpt::EdgeExpand_ExpandOpt_VERTEX) {
      alias = edge.alias().value();
    } else {
      THROW_INTERNAL_EXCEPTION(
          "If there is only one plan, it must be an EdgeExpand + GetV.");
    }
  } else if (sub_left.plan_size() == 2) {
    if (sub_left.plan(1).opr().has_vertex()) {
      alias = sub_left.plan(1).opr().vertex().alias().value();
    } else {
      THROW_INTERNAL_EXCEPTION(
          "If there are two plans, the second plan must be a GetV.");
    }
  }
  std::vector<int> edge_aliases;
  bool keep_edge_alias = false;
  for (int i = 0; i < intersect_opr.sub_plans_size(); ++i) {
    const auto& sub_plan = intersect_opr.sub_plans(i);
    if (sub_plan.plan_size() == 2) {
      if (!sub_plan.plan(1).opr().has_vertex()) {
        THROW_INTERNAL_EXCEPTION(
            "If there are two plans, the second plan must be a GetV.");
      }
      int edge_alias = sub_plan.plan(0).opr().edge().has_alias()
                           ? sub_plan.plan(0).opr().edge().alias().value()
                           : -1;
      edge_aliases.push_back(edge_alias);
      if (edge_alias != -1) {
        keep_edge_alias = true;
      }
    }
  }

  ContextMeta meta = ctx_meta;
  meta.set(plan.plan(op_idx).opr().intersect().key(), DataType::VERTEX);
  if (keep_edge_alias) {
    for (const auto& ea : edge_aliases) {
      if (ea != -1) {
        meta.set(ea, DataType::EDGE);
      }
    }
    if (eeps_.size() != 2) {
      THROW_NOT_SUPPORTED_EXCEPTION(
          "Keeping edge aliases is only supported for binary intersect.");
    }
    return std::make_pair(
        std::make_unique<IntersectWithEdgeOpr>(
            eeps_[0], eeps_[1], alias, std::move(vertex_preds_[0]),
            std::move(vertex_preds_[1]), std::move(edge_preds_[0]),
            std::move(edge_preds_[1]), edge_aliases),
        meta);
  }

  return std::make_pair(
      std::make_unique<IntersectOprMultip>(eeps_, std::move(vertex_preds_),
                                           std::move(edge_preds_), alias),
      meta);
}

}  // namespace ops
}  // namespace execution
}  // namespace neug