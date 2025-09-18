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

#include <map>
#include <memory>
#include <string>
#include <type_traits>
#include <utility>

#include "neug/execution/common/context.h"
#include "neug/execution/common/graph_interface.h"
#include "neug/execution/common/operators/retrieve/intersect.h"
#include "neug/execution/execute/pipeline.h"
#include "neug/execution/execute/plan_parser.h"
#include "neug/execution/utils/params.h"
#include "neug/execution/utils/predicates.h"
#include "neug/execution/utils/special_predicates.h"
#include "neug/execution/utils/utils.h"

namespace gs {
class Schema;

namespace runtime {
class OprTimer;

namespace ops {

struct GeneralVertexPredWrapper {
  GeneralVertexPredWrapper(GeneralVertexPredicate&& pred)
      : pred_(std::move(pred)) {}

  inline bool operator()(label_t label, vid_t v, size_t path_idx) const {
    return pred_(label, v, path_idx, arena_);
  }
  mutable Arena arena_;

  GeneralVertexPredicate pred_;
};

struct GeneralEdgePredWrapper {
  GeneralEdgePredWrapper(GeneralEdgePredicate&& pred)
      : pred_(std::move(pred)) {}

  inline bool operator()(const LabelTriplet& label, vid_t src, vid_t dst,
                         const Any& edata, Direction dir,
                         size_t path_idx) const {
    return pred_(label, src, dst, edata, dir, path_idx, arena_);
  }
  mutable Arena arena_;

  GeneralEdgePredicate pred_;
};

class IntersectOprMultip : public IReadOperator {
 public:
  IntersectOprMultip(
      const std::vector<EdgeExpandParams>& eeps,
      std::vector<std::optional<common::Expression>>&& vertex_preds,
      std::vector<std::optional<common::Expression>>&& edge_preds, int alias,
      std::vector<int> edge_aliases)
      : eeps_(eeps),
        vertex_preds_(std::move(vertex_preds)),
        edge_preds_(std::move(edge_preds)),
        alias_(alias),
        edge_aliases_(std::move(edge_aliases)) {}
  std::string get_operator_name() const override {
    return "IntersectOprMultip";
  }

  gs::result<gs::runtime::Context> Eval_Impl(
      const gs::runtime::GraphReadInterface& graph,
      const std::map<std::string, std::string>& params,
      gs::runtime::Context&& ctx,
      const std::vector<std::function<bool(label_t, vid_t, size_t)>>& v_preds,
      const std::vector<std::function<bool(const LabelTriplet&, vid_t, vid_t,
                                           const Any&, Direction, size_t)>>&
          e_preds,
      gs::runtime::OprTimer* timer) {
    return Intersect::Multiple_Intersect(graph, params, std::move(ctx), v_preds,
                                         e_preds, eeps_, alias_, edge_aliases_);
  }
  gs::result<gs::runtime::Context> Eval(
      const gs::runtime::GraphReadInterface& graph,
      const std::map<std::string, std::string>& params,
      gs::runtime::Context&& ctx, gs::runtime::OprTimer* timer) override {
    std::vector<std::function<bool(label_t, vid_t, size_t)>> v_preds;
    std::vector<std::function<bool(const LabelTriplet&, vid_t, vid_t,
                                   const Any&, Direction, size_t)>>
        e_preds;
    std::vector<std::unique_ptr<GeneralVertexPredWrapper>> pred_wrappers;
    std::vector<std::unique_ptr<GeneralEdgePredWrapper>> edge_pred_wrappers;
    for (const auto& pred : vertex_preds_) {
      if (pred.has_value()) {
        auto vpred = std::make_unique<GeneralVertexPredWrapper>(
            GeneralVertexPredicate(graph, ctx, params, pred.value()));
        pred_wrappers.emplace_back(std::move(vpred));
        auto& vpred_ref = *pred_wrappers.back();
        v_preds.emplace_back([&vpred_ref](label_t label, vid_t v, size_t idx) {
          return vpred_ref(label, v, idx);
        });
      } else {
        v_preds.emplace_back([](label_t, vid_t, size_t) { return true; });
      }
    }
    for (const auto& pred : edge_preds_) {
      if (pred.has_value()) {
        auto epred = std::make_unique<GeneralEdgePredWrapper>(
            GeneralEdgePredicate(graph, ctx, params, pred.value()));
        edge_pred_wrappers.emplace_back(std::move(epred));
        auto& epred_ref = *edge_pred_wrappers.back();
        e_preds.emplace_back([&epred_ref](const LabelTriplet& label, vid_t src,
                                          vid_t dst, const Any& edata,
                                          Direction dir, size_t idx) {
          return epred_ref(label, src, dst, edata, dir, idx);
        });
      } else {
        e_preds.emplace_back([](const LabelTriplet&, vid_t, vid_t, const Any&,
                                Direction, size_t) { return true; });
      }
    }
    return Eval_Impl(graph, params, std::move(ctx), v_preds, e_preds, timer);
  }

  std::vector<EdgeExpandParams> eeps_;
  std::vector<std::optional<common::Expression>> vertex_preds_;
  std::vector<std::optional<common::Expression>> edge_preds_;
  int alias_;
  std::vector<int> edge_aliases_;
};
class IntersectOprBeta : public IReadOperator {
 public:
  IntersectOprBeta(const EdgeExpandParams& eep0, const EdgeExpandParams& eep1,
                   int v_alias,
                   const std::optional<common::Expression>& left_v_pred,
                   const std::optional<common::Expression>& right_v_pred,
                   const std::optional<common::Expression>& left_e_pred,
                   const std::optional<common::Expression>& right_e_pred,
                   const std::vector<int>& edge_alias)
      : eep0_(eep0),
        eep1_(eep1),
        v_alias_(v_alias),
        left_v_pred_(left_v_pred),
        right_v_pred_(right_v_pred),
        left_e_pred_(left_e_pred),
        right_e_pred_(right_e_pred),
        edge_alias_(edge_alias) {}

  std::string get_operator_name() const override { return "IntersectOprBeta"; }

  gs::result<gs::runtime::Context> Eval(
      const gs::runtime::GraphReadInterface& graph,
      const std::map<std::string, std::string>& params,
      gs::runtime::Context&& ctx, gs::runtime::OprTimer* timer) override {
    auto lambda = [](label_t label, vid_t v, size_t idx) {
      return true;  // Dummy predicate that always returns true
    };
    std::vector<std::function<bool(label_t, vid_t, size_t)>> v_preds;
    std::vector<std::function<bool(const LabelTriplet&, vid_t, vid_t,
                                   const Any&, Direction, size_t)>>
        e_preds;
    std::shared_ptr<GeneralVertexPredWrapper> vpred1_wrapper, vpred2_wrapper;
    std::shared_ptr<GeneralEdgePredWrapper> epred1_wrapper, epred2_wrapper;
    if (left_v_pred_.has_value()) {
      vpred1_wrapper = std::make_shared<GeneralVertexPredWrapper>(
          GeneralVertexPredicate(graph, ctx, params, left_v_pred_.value()));
      v_preds.emplace_back(
          [vpred1_wrapper](label_t label, vid_t v, size_t idx) {
            return (*vpred1_wrapper)(label, v, idx);
          });
    } else {
      v_preds.emplace_back(lambda);
    }

    if (right_v_pred_.has_value()) {
      vpred2_wrapper = std::make_shared<GeneralVertexPredWrapper>(
          GeneralVertexPredicate(graph, ctx, params, right_v_pred_.value()));
      v_preds.emplace_back(
          [vpred2_wrapper](label_t label, vid_t v, size_t idx) {
            return (*vpred2_wrapper)(label, v, idx);
          });
    } else {
      v_preds.emplace_back(lambda);
    }

    if (left_e_pred_.has_value()) {
      epred1_wrapper = std::make_shared<GeneralEdgePredWrapper>(
          GeneralEdgePredicate(graph, ctx, params, left_e_pred_.value()));
      e_preds.emplace_back(
          [epred1_wrapper](const LabelTriplet& label, vid_t src, vid_t dst,
                           const Any& edata, Direction dir, size_t idx) {
            return (*epred1_wrapper)(label, src, dst, edata, dir, idx);
          });
    } else {
      e_preds.emplace_back([](const LabelTriplet&, vid_t, vid_t, const Any&,
                              Direction, size_t) { return true; });
    }

    if (right_e_pred_.has_value()) {
      epred2_wrapper = std::make_shared<GeneralEdgePredWrapper>(
          GeneralEdgePredicate(graph, ctx, params, right_e_pred_.value()));
      e_preds.emplace_back(
          [epred2_wrapper](const LabelTriplet& label, vid_t src, vid_t dst,
                           const Any& edata, Direction dir, size_t idx) {
            return (*epred2_wrapper)(label, src, dst, edata, dir, idx);
          });
    } else {
      e_preds.emplace_back([](const LabelTriplet&, vid_t, vid_t, const Any&,
                              Direction, size_t) { return true; });
    }

    return Intersect::Binary_Intersect(
        graph, params, std::move(ctx), v_preds[0], v_preds[1], e_preds[0],
        e_preds[1], eep0_, eep1_, v_alias_, edge_alias_);
  }

 private:
  EdgeExpandParams eep0_;
  EdgeExpandParams eep1_;
  int v_alias_;
  std::optional<common::Expression> left_v_pred_;
  std::optional<common::Expression> right_v_pred_;
  std::optional<common::Expression> left_e_pred_;
  std::optional<common::Expression> right_e_pred_;
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
           std::optional<common::Expression>& vpred,
           std::optional<common::Expression>& epred) {
  vpred = std::nullopt;
  epred = std::nullopt;
  if (plan.query_plan().plan_size() > 2 || plan.query_plan().plan_size() < 1) {
    THROW_RUNTIME_ERROR(
        "sub-plan of intersect operator should have 1 or 2 plans");
  }
  if (plan.query_plan().plan_size() >= 1) {
    if (plan.query_plan().plan(0).opr().op_kind_case() !=
        physical::PhysicalOpr_Operator::OpKindCase::kEdge) {
      THROW_RUNTIME_ERROR(
          "the first operator in sub-plan of intersect operator should be "
          "edge expand");
    }
    const auto& edge = plan.query_plan().plan(0).opr().edge();
    params = parse_edge_params(edge, plan.query_plan().plan(0).meta_data(0));
    if (edge.has_params() && edge.params().has_predicate()) {
      epred = edge.params().predicate();
      LOG(INFO) << "Edge predicate found in sub-plan";
    }
  }
  if (plan.query_plan().plan_size() == 2) {
    if (plan.query_plan().plan(1).opr().op_kind_case() !=
        physical::PhysicalOpr_Operator::OpKindCase::kVertex) {
      THROW_RUNTIME_ERROR(
          "the second operator in sub-plan of intersect operator should be "
          "vertex");
    }
    const auto& vertex = plan.query_plan().plan(1).opr().vertex();
    if (vertex.has_params() && vertex.params().has_predicate()) {
      vpred = vertex.params().predicate();
      LOG(INFO) << "Vertex predicate found in sub-plan";
    }
  }
}
gs::result<ReadOpBuildResultT> IntersectOprBuilder::Build(
    const Schema& schema, const ContextMeta& ctx_meta,
    const physical::PhysicalPlan& plan, int op_idx) {
  const auto& intersect_opr = plan.query_plan().plan(op_idx).opr().intersect();
  std::vector<EdgeExpandParams> eeps_(intersect_opr.sub_plans_size());
  std::vector<std::optional<common::Expression>> vertex_preds_(
      intersect_opr.sub_plans_size());
  std::vector<std::optional<common::Expression>> edge_preds_(
      intersect_opr.sub_plans_size());
  for (int i = 0; i < intersect_opr.sub_plans_size(); ++i) {
    parse(intersect_opr.sub_plans(i), eeps_[i], vertex_preds_[i],
          edge_preds_[i]);  // Parse edge expand params and predicates
  }
  const auto& sub_left = intersect_opr.sub_plans(0);

  int alias = -1;
  std::vector<int> edge_aliases;
  // There are two different cases for Intersect
  // 1. The subplans are composed of EdgeExpand + GetV.
  // 2. The subplans only contains EdgeExpandV.
  if (sub_left.query_plan().plan_size() == 1) {
    const auto& edge = sub_left.query_plan().plan(0).opr().edge();
    if (edge.expand_opt() ==
        physical::EdgeExpand_ExpandOpt::EdgeExpand_ExpandOpt_VERTEX) {
      alias = edge.alias().value();
    } else {
      THROW_INTERNAL_EXCEPTION(
          "If there is only one plan, it must be an EdgeExpand + GetV.");
    }
  } else if (sub_left.query_plan().plan_size() == 2) {
    if (sub_left.query_plan().plan(1).opr().has_vertex()) {
      alias = sub_left.query_plan().plan(1).opr().vertex().alias().value();
      // Only get the edge aliases when there is an EdgeExpandE
      for (const auto& eep : eeps_) {
        edge_aliases.push_back(eep.alias);
      }
    } else {
      THROW_INTERNAL_EXCEPTION(
          "If there are two plans, the second plan must be a GetV.");
    }
  }

  ContextMeta meta = ctx_meta;
  meta.set(plan.query_plan().plan(op_idx).opr().intersect().key());
  if (eeps_.size() == 2) {
    return std::make_pair(
        std::make_unique<IntersectOprBeta>(
            eeps_[0], eeps_[1], alias, vertex_preds_[0], vertex_preds_[1],
            edge_preds_[0], edge_preds_[1], edge_aliases),
        meta);
  } else {
    return std::make_pair(std::make_unique<IntersectOprMultip>(
                              eeps_, std::move(vertex_preds_),
                              std::move(edge_preds_), alias, edge_aliases),
                          meta);
  }
}

}  // namespace ops
}  // namespace runtime
}  // namespace gs
