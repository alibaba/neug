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
  GeneralVertexPredWrapper(const GeneralVertexPredicate& pred) : pred_(pred) {}

  inline bool operator()(label_t label, vid_t v, size_t path_idx, int) const {
    return pred_(label, v, path_idx, arena_, 0);
  }

  inline bool operator()(label_t label, vid_t v, size_t path_idx) const {
    return pred_(label, v, path_idx, arena_);
  }
  mutable Arena arena_;

  const GeneralVertexPredicate& pred_;
};

class IntersectOprMultip : public IReadOperator {
 public:
  IntersectOprMultip(const std::vector<EdgeExpandParams>& eeps,
                     std::vector<std::optional<common::Expression>>&& preds,
                     int alias)
      : eeps_(eeps), preds_(std::move(preds)), alias_(alias) {}
  std::string get_operator_name() const override {
    return "IntersectOprMultip";
  }

  gs::result<gs::runtime::Context> Eval_Impl(
      const gs::runtime::GraphReadInterface& graph,
      const std::map<std::string, std::string>& params,
      gs::runtime::Context&& ctx,
      const std::vector<std::function<bool(label_t, vid_t, size_t)>>& preds,
      gs::runtime::OprTimer* timer) {
    return Intersect::Multiple_Intersect(graph, params, std::move(ctx), preds,
                                         eeps_, alias_);
  }
  gs::result<gs::runtime::Context> Eval(
      const gs::runtime::GraphReadInterface& graph,
      const std::map<std::string, std::string>& params,
      gs::runtime::Context&& ctx, gs::runtime::OprTimer* timer) override {
    std::vector<std::function<bool(label_t, vid_t, size_t)>> preds;
    std::vector<std::unique_ptr<GeneralVertexPredWrapper>> pred_wrappers;
    for (const auto& pred : preds_) {
      if (pred.has_value()) {
        GeneralVertexPredicate p(graph, ctx, params, pred.value());
        auto vpred = std::make_unique<GeneralVertexPredWrapper>(p);

        pred_wrappers.emplace_back(std::move(vpred));
        auto& vpred_ref = *pred_wrappers.back();
        preds.emplace_back([&vpred_ref](label_t label, vid_t v, size_t idx) {
          return vpred_ref(label, v, idx);
        });
      } else {
        preds.emplace_back([](label_t, vid_t, size_t) { return true; });
      }
    }
    return Eval_Impl(graph, params, std::move(ctx), preds, timer);
  }

  std::vector<EdgeExpandParams> eeps_;
  std::vector<std::optional<common::Expression>> preds_;
  int alias_;
};
class IntersectOprBeta : public IReadOperator {
 public:
  IntersectOprBeta(const EdgeExpandParams& eep0, const EdgeExpandParams& eep1,
                   int alias,
                   const std::optional<common::Expression>& left_pred,
                   const std::optional<common::Expression>& right_pred)
      : eep0_(eep0),
        eep1_(eep1),
        alias_(alias),
        left_pred_(left_pred),
        right_pred_(right_pred) {}

  std::string get_operator_name() const override { return "IntersectOprBeta"; }

  gs::result<gs::runtime::Context> Eval(
      const gs::runtime::GraphReadInterface& graph,
      const std::map<std::string, std::string>& params,
      gs::runtime::Context&& ctx, gs::runtime::OprTimer* timer) override {
    auto lambda = [](label_t label, vid_t v, size_t idx) {
      return true;  // Dummy predicate that always returns true
    };
    if (left_pred_.has_value()) {
      GeneralVertexPredicate pred(graph, ctx, params, left_pred_.value());
      GeneralVertexPredWrapper vpred(pred);
      if (right_pred_.has_value()) {
        GeneralVertexPredicate pred(graph, ctx, params, right_pred_.value());
        GeneralVertexPredWrapper rpred(pred);
        return Intersect::Binary_Intersect(graph, params, std::move(ctx), vpred,
                                           rpred, eep0_, eep1_, alias_);
      } else {
        return Intersect::Binary_Intersect(graph, params, std::move(ctx), vpred,
                                           lambda, eep0_, eep1_, alias_);
      }
    } else {
      if (right_pred_.has_value()) {
        GeneralVertexPredicate pred(graph, ctx, params, right_pred_.value());
        GeneralVertexPredWrapper rpred(pred);
        return Intersect::Binary_Intersect(graph, params, std::move(ctx),
                                           lambda, rpred, eep0_, eep1_, alias_);
      } else {
        return Intersect::Binary_Intersect(graph, params, std::move(ctx),
                                           lambda, lambda, eep0_, eep1_,
                                           alias_);
      }
    }
  }

 private:
  EdgeExpandParams eep0_;
  EdgeExpandParams eep1_;
  int alias_;
  std::optional<common::Expression> left_pred_;
  std::optional<common::Expression> right_pred_;
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
           std::optional<common::Expression>& pred) {
  const auto& edge = plan.query_plan().plan(0).opr().edge();
  CHECK(plan.query_plan().plan_size() <= 2)
      << "sub-plan of intersect operator should have at most two plans";
  CHECK(plan.query_plan().plan(0).opr().op_kind_case() ==
        physical::PhysicalOpr_Operator::OpKindCase::kEdge);
  params = parse_edge_params(edge, plan.query_plan().plan(0).meta_data(0));
  if (plan.query_plan().plan_size() == 2) {
    CHECK(plan.query_plan().plan(1).opr().op_kind_case() ==
          physical::PhysicalOpr_Operator::OpKindCase::kVertex);
    const auto& vertex = plan.query_plan().plan(1).opr().vertex();
    if (vertex.has_params() && vertex.params().has_predicate()) {
      pred = vertex.params().predicate();
    } else {
      pred = std::nullopt;
    }
  } else {
    pred = std::nullopt;  // No predicate if no vertex operator is present
  }
}
gs::result<ReadOpBuildResultT> IntersectOprBuilder::Build(
    const Schema& schema, const ContextMeta& ctx_meta,
    const physical::PhysicalPlan& plan, int op_idx) {
  const auto& intersect_opr = plan.query_plan().plan(op_idx).opr().intersect();
  std::vector<EdgeExpandParams> eeps_(intersect_opr.sub_plans_size());
  std::vector<std::optional<common::Expression>> preds_(
      intersect_opr.sub_plans_size());
  for (int i = 0; i < intersect_opr.sub_plans_size(); ++i) {
    parse(intersect_opr.sub_plans(i), eeps_[i],
          preds_[i]);  // Parse edge expand params and predicates
  }
  const auto& sub_left = intersect_opr.sub_plans(0);
  const auto& edge_left = sub_left.query_plan().plan(0).opr().edge();

  int alias = -1;
  if (edge_left.has_alias()) {
    alias = edge_left.alias().value();
  }
  if (sub_left.query_plan().plan_size() == 2) {
    alias = sub_left.query_plan().plan(1).opr().vertex().alias().value();
  }
  ContextMeta meta = ctx_meta;
  meta.set(plan.query_plan().plan(op_idx).opr().intersect().key());
  if (eeps_.size() == 2) {
    return std::make_pair(std::make_unique<IntersectOprBeta>(
                              eeps_[0], eeps_[1], alias, preds_[0], preds_[1]),
                          meta);
  } else {
    return std::make_pair(
        std::make_unique<IntersectOprMultip>(eeps_, std::move(preds_), alias),
        meta);
  }
}

}  // namespace ops
}  // namespace runtime
}  // namespace gs
