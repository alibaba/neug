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

#include "neug/execution/execute/ops/retrieve/path.h"

#include <glog/logging.h>
#include <google/protobuf/wrappers.pb.h>
#include <stdint.h>

#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <ostream>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>

#include "neug/execution/common/context.h"
#include "neug/execution/common/operators/retrieve/path_expand.h"
#include "neug/execution/common/types.h"
#include "neug/execution/utils/expr_impl.h"
#include "neug/execution/utils/predicates.h"
#include "neug/execution/utils/special_predicates.h"
#include "neug/execution/utils/utils.h"
#include "neug/execution/utils/var.h"
#include "neug/storages/csr/mutable_csr.h"
#include "neug/storages/graph/graph_interface.h"
#include "neug/utils/property/types.h"
#include "neug/utils/result.h"

namespace gs {
class Schema;

namespace runtime {
class OprTimer;

namespace ops {

static bool is_shortest_path_with_order_by_limit(
    const physical::PhysicalPlan& plan, int i, int& path_len_alias,
    int& vertex_alias, int& limit_upper) {
  int opr_num = plan.plan_size();
  const auto& opr = plan.plan(i).opr();
  int start_tag = opr.path().start_tag().value();
  // must be any shortest path
  if (opr.path().path_opt() !=
          physical::PathExpand_PathOpt::PathExpand_PathOpt_ANY_SHORTEST ||
      opr.path().result_opt() !=
          physical::PathExpand_ResultOpt::PathExpand_ResultOpt_ALL_V) {
    return false;
  }
  if (i + 5 < opr_num) {
    const auto& get_v_opr = plan.plan(i + 1).opr();
    const auto& select_opr = plan.plan(i + 2).opr();
    const auto& project_opr = plan.plan(i + 3).opr();
    const auto& order_by_opr = plan.plan(i + 5).opr();
    if (!get_v_opr.has_vertex() || !project_opr.has_project() ||
        !order_by_opr.has_order_by()) {
      return false;
    }
    if (get_v_opr.vertex().opt() != physical::GetV::OTHER) {
      return false;
    }

    int path_alias = opr.path().has_alias() ? opr.path().alias().value() : -1;
    int get_v_tag =
        get_v_opr.vertex().has_tag() ? get_v_opr.vertex().tag().value() : -1;
    int get_v_alias = get_v_opr.vertex().has_alias()
                          ? get_v_opr.vertex().alias().value()
                          : -1;
    if (path_alias != get_v_tag && get_v_tag != -1) {
      return false;
    }

    if (!select_opr.has_select()) {
      return false;
    }
    if (!select_opr.select().has_predicate()) {
      return false;
    }
    auto pred = select_opr.select().predicate();
    if (pred.operators_size() != 3) {
      return false;
    }
    if (!pred.operators(0).has_var() ||
        !(pred.operators(1).item_case() == common::ExprOpr::kLogical) ||
        pred.operators(1).logical() != common::Logical::NE ||
        !pred.operators(2).has_var()) {
      return false;
    }

    if (!pred.operators(0).var().has_tag() ||
        !pred.operators(2).var().has_tag()) {
      return false;
    }
    if (pred.operators(0).var().tag().id() != get_v_alias &&
        pred.operators(2).var().tag().id() != get_v_alias) {
      return false;
    }

    if (pred.operators(0).var().tag().id() != start_tag &&
        pred.operators(2).var().tag().id() != start_tag) {
      return false;
    }

    // only vertex and length(path)
    if (project_opr.project().mappings_size() != 2 ||
        project_opr.project().is_append()) {
      return false;
    }

    auto mapping = project_opr.project().mappings();
    bool flag = false;
    for (const auto& m : mapping) {
      if (m.expr().operators_size() == 1 && m.expr().operators(0).has_var()) {
        auto var = m.expr().operators(0).var();
        if (var.has_property() && var.property().has_len() &&
            var.tag().has_id() && var.tag().id() == path_alias) {
          flag = true;
          path_len_alias = m.alias().value();
        } else {
          vertex_alias = m.alias().value();
        }
      } else {
        vertex_alias = m.alias().value();
      }
    }
    if (!flag) {
      return false;
    }

    // must has order by limit
    if (!order_by_opr.order_by().has_limit()) {
      return false;
    }
    limit_upper = order_by_opr.order_by().limit().upper();
    if (order_by_opr.order_by().pairs_size() < 0) {
      return false;
    }
    if (!order_by_opr.order_by().pairs()[0].has_key()) {
      return false;
    }
    if (!order_by_opr.order_by().pairs()[0].key().has_tag()) {
      return false;
    }
    if (order_by_opr.order_by().pairs()[0].key().tag().id() != path_len_alias) {
      return false;
    }
    if (order_by_opr.order_by().pairs()[0].order() !=
        algebra::OrderBy_OrderingPair_Order::OrderBy_OrderingPair_Order_ASC) {
      return false;
    }
    return true;
  }
  return false;
}

static bool is_all_shortest_path(const physical::PhysicalPlan& plan, int i) {
  int opr_num = plan.plan_size();
  const auto& opr = plan.plan(i).opr();
  if (opr.path().path_opt() !=
      physical::PathExpand_PathOpt::PathExpand_PathOpt_ALL_SHORTEST) {
    return false;
  }

  if (i + 1 < opr_num) {
    const auto& get_v_opr = plan.plan(i + 1).opr();
    if (!get_v_opr.has_vertex()) {
      return false;
    }
    if (get_v_opr.vertex().opt() != physical::GetV::OTHER) {
      return false;
    }

    int path_alias = opr.path().has_alias() ? opr.path().alias().value() : -1;
    int get_v_tag =
        get_v_opr.vertex().has_tag() ? get_v_opr.vertex().tag().value() : -1;
    if (path_alias != get_v_tag && get_v_tag != -1) {
      return false;
    }

    return true;
  }
  return false;
}

static bool is_shortest_path(const physical::PhysicalPlan& plan, int i) {
  int opr_num = plan.plan_size();
  const auto& opr = plan.plan(i).opr();
  // must be any shortest path
  if (opr.path().path_opt() !=
      physical::PathExpand_PathOpt::PathExpand_PathOpt_ANY_SHORTEST) {
    return false;
  }
  if (i + 1 < opr_num) {
    const auto& get_v_opr = plan.plan(i + 1).opr();
    if (!get_v_opr.has_vertex()) {
      return false;
    }
    if (get_v_opr.vertex().opt() != physical::GetV::OTHER) {
      return false;
    }

    int path_alias = opr.path().has_alias() ? opr.path().alias().value() : -1;
    int get_v_tag =
        get_v_opr.vertex().has_tag() ? get_v_opr.vertex().tag().value() : -1;

    if (path_alias != get_v_tag && get_v_tag != -1) {
      return false;
    }

    return true;
  }
  return false;
}

struct OrderByLimitSPOp {
  template <typename PRED_T>
  static gs::result<Context> eval_with_predicate(
      const PRED_T& pred, const IStorageInterface& graph_interface,
      Context&& ctx, const ShortestPathParams& spp, int limit) {
    const auto& graph =
        dynamic_cast<const StorageReadInterface&>(graph_interface);

    return PathExpand::single_source_shortest_path_with_order_by_length_limit(
        graph, std::move(ctx), spp, pred, limit);
  }
};

class SPOrderByLimitOpr : public IOperator {
 public:
  SPOrderByLimitOpr(const ShortestPathParams& spp, int limit,
                    const SpecialVertexPredicateConfig& config)
      : spp_(spp), limit_(limit), config_(config) {}

  std::string get_operator_name() const override { return "SPOrderByLimitOpr"; }

  gs::result<gs::runtime::Context> Eval(
      IStorageInterface& graph_interface,
      const std::map<std::string, std::string>& params,
      gs::runtime::Context&& ctx, gs::runtime::OprTimer* timer) override {
    const auto& graph =
        dynamic_cast<const StorageReadInterface&>(graph_interface);
    std::set<label_t> expected_labels;
    for (auto label : spp_.labels) {
      expected_labels.insert(label.src_label);
      expected_labels.insert(label.dst_label);
    }
    return dispatch_vertex_predicate<OrderByLimitSPOp>(
        graph, expected_labels, config_, params, graph, std::move(ctx), spp_,
        limit_);
  }

 private:
  ShortestPathParams spp_;
  int limit_;
  SpecialVertexPredicateConfig config_;
};

class SPOrderByLimitWithGPredOpr : public IOperator {
 public:
  SPOrderByLimitWithGPredOpr(const ShortestPathParams& spp, int limit,
                             const std::optional<common::Expression>& pred)
      : spp_(spp), limit_(limit), pred_(pred) {}

  std::string get_operator_name() const override {
    return "SPOrderByLimitWithGPredOpr";
  }

  gs::result<gs::runtime::Context> Eval(
      IStorageInterface& graph_interface,
      const std::map<std::string, std::string>& params,
      gs::runtime::Context&& ctx, gs::runtime::OprTimer* timer) override {
    const auto& graph =
        dynamic_cast<const StorageReadInterface&>(graph_interface);
    if (pred_) {
      Arena arena;
      auto v_pred = parse_expression(&graph, std::move(ctx), params,
                                     pred_.value(), VarType::kVertexVar);
      auto pred = [&v_pred, &arena](label_t label, vid_t vid) {
        return v_pred->eval_vertex(label, vid, arena).as_bool();
      };

      return PathExpand::single_source_shortest_path_with_order_by_length_limit(
          graph, std::move(ctx), spp_, pred, limit_);
    } else {
      return PathExpand::single_source_shortest_path_with_order_by_length_limit(
          graph, std::move(ctx), spp_, [](label_t, vid_t) { return true; },
          limit_);
    }
  }

 private:
  ShortestPathParams spp_;
  int limit_;
  std::optional<common::Expression> pred_;
};

gs::result<OpBuildResultT> SPOrderByLimitOprBuilder::Build(
    const gs::Schema& schema, const ContextMeta& ctx_meta,
    const physical::PhysicalPlan& plan, int op_idx) {
  const auto& opr = plan.plan(op_idx).opr().path();
  int path_len_alias = -1;
  int vertex_alias = -1;
  int limit_upper = -1;
  if (is_shortest_path_with_order_by_limit(plan, op_idx, path_len_alias,
                                           vertex_alias, limit_upper)) {
    ContextMeta ret_meta = ctx_meta;
    ret_meta.set(vertex_alias);
    ret_meta.set(path_len_alias);
    if (!opr.has_start_tag()) {
      LOG(ERROR) << "Shortest path with order by limit must have start tag";
      return std::make_pair(nullptr, ContextMeta());
    }
    int start_tag = opr.start_tag().value();
    if (opr.is_optional()) {
      LOG(ERROR) << "Currently only support non-optional shortest path with "
                    "order by limit";
      return std::make_pair(nullptr, ContextMeta());
    }
    ShortestPathParams spp;
    spp.start_tag = start_tag;
    spp.dir = parse_direction(opr.base().edge_expand().direction());
    spp.v_alias = vertex_alias;
    spp.alias = path_len_alias;
    spp.hop_lower = opr.hop_range().lower();
    spp.hop_upper = opr.hop_range().upper();
    spp.labels = parse_label_triplets(plan.plan(op_idx).meta_data(0));
    if (spp.labels.size() != 1) {
      LOG(ERROR) << "only support one label triplet";
      return std::make_pair(nullptr, ContextMeta());
    }
    const auto& get_v_opr = plan.plan(op_idx + 1).opr().vertex();
    if (get_v_opr.has_params() && get_v_opr.params().has_predicate()) {
      SpecialVertexPredicateConfig sp_config;
      if (is_special_vertex_predicate(get_v_opr.params().predicate(),
                                      sp_config)) {
        return std::make_pair(
            std::make_unique<SPOrderByLimitOpr>(spp, limit_upper, sp_config),
            ret_meta);
      }
    }
    std::optional<common::Expression> g_pred = std::nullopt;
    if (get_v_opr.params().has_predicate()) {
      g_pred = std::make_optional(get_v_opr.params().predicate());
    }
    return std::make_pair(
        std::make_unique<SPOrderByLimitWithGPredOpr>(spp, limit_upper, g_pred),
        ret_meta);

  } else {
    return std::make_pair(nullptr, ContextMeta());
  }
}

class SPSPredOpr : public IOperator {
 public:
  SPSPredOpr(const ShortestPathParams& spp,
             const SpecialVertexPredicateConfig& config)
      : spp_(spp), config_(config) {}

  std::string get_operator_name() const override { return "SPSPredOpr"; }

  gs::result<gs::runtime::Context> Eval(
      IStorageInterface& graph_interface,
      const std::map<std::string, std::string>& params,
      gs::runtime::Context&& ctx, gs::runtime::OprTimer* timer) override {
    const auto& graph =
        dynamic_cast<const StorageReadInterface&>(graph_interface);
    return PathExpand::
        single_source_shortest_path_with_special_vertex_predicate(
            graph, std::move(ctx), spp_, config_, params);
  }

 private:
  ShortestPathParams spp_;
  SpecialVertexPredicateConfig config_;
};

class SPGPredOpr : public IOperator {
 public:
  SPGPredOpr(const ShortestPathParams& spp, const common::Expression& pred)
      : spp_(spp), pred_(pred) {}

  std::string get_operator_name() const override { return "SPGPredOpr"; }

  gs::result<gs::runtime::Context> Eval(
      IStorageInterface& graph_interface,
      const std::map<std::string, std::string>& params,
      gs::runtime::Context&& ctx, gs::runtime::OprTimer* timer) override {
    const auto& graph =
        dynamic_cast<const StorageReadInterface&>(graph_interface);
    auto predicate = parse_expression(&graph, std::move(ctx), params, pred_,
                                      VarType::kVertexVar);
    Arena arena;
    auto pred = [&arena, &predicate](label_t label, vid_t v) {
      return predicate->eval_vertex(label, v, arena).as_bool();
    };

    return PathExpand::single_source_shortest_path(graph, std::move(ctx), spp_,
                                                   pred);
  }

 private:
  ShortestPathParams spp_;
  common::Expression pred_;
};
class SPWithoutPredOpr : public IOperator {
 public:
  explicit SPWithoutPredOpr(const ShortestPathParams& spp) : spp_(spp) {}

  std::string get_operator_name() const override { return "SPWithoutPredOpr"; }

  gs::result<gs::runtime::Context> Eval(
      IStorageInterface& graph_interface,
      const std::map<std::string, std::string>& params,
      gs::runtime::Context&& ctx, gs::runtime::OprTimer* timer) override {
    const auto& graph =
        dynamic_cast<const StorageReadInterface&>(graph_interface);
    return PathExpand::single_source_shortest_path(
        graph, std::move(ctx), spp_, [](label_t, vid_t) { return true; });
  }

 private:
  ShortestPathParams spp_;
};

class ASPOpr : public IOperator {
 public:
  ASPOpr(const gs::Schema& schema, const physical::PathExpand& opr,
         const physical::PhysicalOpr_MetaData& meta,
         const physical::GetV& get_v_opr, int v_alias) {
    int start_tag = opr.start_tag().value();
    aspp_.start_tag = start_tag;
    aspp_.dir = parse_direction(opr.base().edge_expand().direction());
    aspp_.v_alias = v_alias;
    aspp_.alias = opr.has_alias() ? opr.alias().value() : -1;
    aspp_.hop_lower = opr.hop_range().lower();
    aspp_.hop_upper = opr.hop_range().upper();

    aspp_.labels = parse_label_triplets(meta);
    CHECK(aspp_.labels.size() == 1) << "only support one label triplet";
    CHECK(aspp_.labels[0].src_label == aspp_.labels[0].dst_label)
        << "only support same src and dst label";
    CHECK(is_pk_oid_exact_check(schema, aspp_.labels[0].src_label,
                                get_v_opr.params().predicate(), oid_getter_));
  }

  std::string get_operator_name() const override { return "ASPOpr"; }

  gs::result<gs::runtime::Context> Eval(
      IStorageInterface& graph_interface,
      const std::map<std::string, std::string>& params,
      gs::runtime::Context&& ctx, gs::runtime::OprTimer* timer) override {
    const auto& graph =
        dynamic_cast<const StorageReadInterface&>(graph_interface);
    Property oid = oid_getter_(params);
    vid_t vid;
    if (!graph.GetVertexIndex(aspp_.labels[0].dst_label, oid, vid)) {
      LOG(ERROR) << "vertex not found "
                 << static_cast<int>(aspp_.labels[0].dst_label) << " "
                 << oid.to_string();
      RETURN_UNSUPPORTED_ERROR(
          "vertex not found" +
          std::to_string(static_cast<int>(aspp_.labels[0].dst_label)) + " " +
          std::string(oid.to_string()));
    }

    auto v = std::make_pair(aspp_.labels[0].dst_label, vid);
    return PathExpand::all_shortest_paths_with_given_source_and_dest(
        graph, std::move(ctx), aspp_, v);
  }

 private:
  ShortestPathParams aspp_;
  std::function<Property(const std::map<std::string, std::string>&)>
      oid_getter_;
};

class SSSDSPOpr : public IOperator {
 public:
  SSSDSPOpr(
      const ShortestPathParams& spp,
      const std::function<Property(const std::map<std::string, std::string>&)>&
          oid_getter)
      : spp_(spp), oid_getter_(oid_getter) {}

  std::string get_operator_name() const override { return "SSSDSPOpr"; }

  gs::result<gs::runtime::Context> Eval(
      IStorageInterface& graph_interface,
      const std::map<std::string, std::string>& params,
      gs::runtime::Context&& ctx, gs::runtime::OprTimer* timer) override {
    const auto& graph =
        dynamic_cast<const StorageReadInterface&>(graph_interface);
    Property vertex = oid_getter_(params);
    vid_t vid;
    if (!graph.GetVertexIndex(spp_.labels[0].dst_label, vertex, vid)) {
      LOG(ERROR) << "vertex not found" << spp_.labels[0].dst_label << " "
                 << vertex.to_string();
      RETURN_UNSUPPORTED_ERROR(
          "vertex not found" +
          std::to_string(static_cast<int>(spp_.labels[0].dst_label)) + " " +
          vertex.to_string());
    }

    auto v = std::make_pair(spp_.labels[0].dst_label, vid);

    return PathExpand::single_source_single_dest_shortest_path(
        graph, std::move(ctx), spp_, v);
  }

 private:
  ShortestPathParams spp_;
  std::function<Property(const std::map<std::string, std::string>&)>
      oid_getter_;
};
gs::result<OpBuildResultT> SPOprBuilder::Build(
    const gs::Schema& schema, const ContextMeta& ctx_meta,
    const physical::PhysicalPlan& plan, int op_idx) {
  ContextMeta ret_meta = ctx_meta;
  if (is_shortest_path(plan, op_idx)) {
    auto vertex = plan.plan(op_idx + 1).opr().vertex();
    auto path = plan.plan(op_idx).opr().path();
    int v_alias = vertex.has_alias() ? vertex.alias().value() : -1;
    int alias = path.has_alias() ? path.alias().value() : -1;
    ret_meta.set(v_alias);
    ret_meta.set(alias);

    if (!path.has_start_tag()) {
      LOG(ERROR) << "Shortest path must have start tag";
      return std::make_pair(nullptr, ContextMeta());
    }
    int start_tag = path.start_tag().value();
    if (path.is_optional()) {
      LOG(ERROR) << "Currently only support non-optional shortest path";
      return std::make_pair(nullptr, ContextMeta());
    }
    ShortestPathParams spp;
    spp.start_tag = start_tag;
    spp.dir = parse_direction(path.base().edge_expand().direction());
    spp.v_alias = v_alias;
    spp.alias = alias;
    spp.hop_lower = path.hop_range().lower();
    spp.hop_upper = path.hop_range().upper();
    spp.labels = parse_label_triplets(plan.plan(op_idx).meta_data(0));
    if (spp.labels.size() != 1) {
      LOG(ERROR) << "only support one label triplet";
      return std::make_pair(nullptr, ContextMeta());
    }
    if (spp.labels[0].src_label != spp.labels[0].dst_label) {
      LOG(ERROR) << "only support same src and dst label";
      return std::make_pair(nullptr, ContextMeta());
    }
    std::function<Property(const std::map<std::string, std::string>&)>
        oid_getter;
    if (vertex.has_params() && vertex.params().has_predicate() &&
        is_pk_oid_exact_check(schema, spp.labels[0].src_label,
                              vertex.params().predicate(), oid_getter)) {
      return std::make_pair(std::make_unique<SSSDSPOpr>(spp, oid_getter),
                            ret_meta);
    } else {
      if (vertex.has_params() && vertex.params().has_predicate()) {
        SpecialVertexPredicateConfig sp_config;
        if (is_special_vertex_predicate(vertex.params().predicate(),
                                        sp_config)) {
          return std::make_pair(std::make_unique<SPSPredOpr>(spp, sp_config),
                                ret_meta);
        } else {
          return std::make_pair(
              std::make_unique<SPGPredOpr>(spp, vertex.params().predicate()),
              ret_meta);
        }
      } else {
        return std::make_pair(std::make_unique<SPWithoutPredOpr>(spp),
                              ret_meta);
      }
    }
  } else if (is_all_shortest_path(plan, op_idx)) {
    auto vertex = plan.plan(op_idx + 1).opr().vertex();
    int v_alias = vertex.has_alias() ? vertex.alias().value() : -1;
    const auto& path = plan.plan(op_idx).opr().path();

    int alias = path.has_alias() ? path.alias().value() : -1;
    ret_meta.set(v_alias);
    ret_meta.set(alias);
    if (!path.has_start_tag()) {
      LOG(ERROR) << "Shortest path must have start tag";
      return std::make_pair(nullptr, ContextMeta());
    }
    if (path.is_optional()) {
      LOG(ERROR) << "Currently only support non-optional shortest path";
      return std::make_pair(nullptr, ContextMeta());
    }
    if ((!vertex.has_params()) || (!vertex.params().has_predicate())) {
      LOG(ERROR) << "Currently only support non-optional shortest path without "
                    "predicate";
      return std::make_pair(nullptr, ContextMeta());
    }
    return std::make_pair(std::make_unique<ASPOpr>(
                              schema, plan.plan(op_idx).opr().path(),
                              plan.plan(op_idx).meta_data(0), vertex, v_alias),
                          ret_meta);
  } else {
    return std::make_pair(nullptr, ContextMeta());
  }
}

class PathExpandVOpr : public IOperator {
 public:
  explicit PathExpandVOpr(const PathExpandParams& pep) : pep_(pep) {}

  gs::result<gs::runtime::Context> Eval(
      IStorageInterface& graph_interface,
      const std::map<std::string, std::string>& params,
      gs::runtime::Context&& ctx, gs::runtime::OprTimer* timer) override {
    const auto& graph =
        dynamic_cast<const StorageReadInterface&>(graph_interface);
    return PathExpand::edge_expand_v(graph, std::move(ctx), pep_);
  }
  std::string get_operator_name() const override { return "PathExpandVOpr"; }

 private:
  PathExpandParams pep_;
};

gs::result<OpBuildResultT> PathExpandVOprBuilder::Build(
    const gs::Schema& schema, const ContextMeta& ctx_meta,
    const physical::PhysicalPlan& plan, int op_idx) {
  const auto& opr = plan.plan(op_idx).opr().path();
  const auto& next_opr = plan.plan(op_idx + 1).opr().vertex();
  if (opr.result_opt() ==
          physical::PathExpand_ResultOpt::PathExpand_ResultOpt_END_V &&
      opr.base().edge_expand().expand_opt() ==
          physical::EdgeExpand_ExpandOpt::EdgeExpand_ExpandOpt_VERTEX) {
    int alias = -1;
    if (next_opr.has_alias()) {
      alias = next_opr.alias().value();
    }
    ContextMeta ret_meta = ctx_meta;
    ret_meta.set(alias);
    int start_tag = opr.has_start_tag() ? opr.start_tag().value() : -1;
    if (opr.path_opt() !=
        physical::PathExpand_PathOpt::PathExpand_PathOpt_ARBITRARY) {
      LOG(ERROR) << "Currently only support arbitrary path expand";
      return std::make_pair(nullptr, ContextMeta());
    }
    if (opr.is_optional()) {
      LOG(ERROR) << "Currently only support non-optional path expand without "
                    "predicate";
      return std::make_pair(nullptr, ContextMeta());
    }
    Direction dir = parse_direction(opr.base().edge_expand().direction());
    if (opr.base().edge_expand().is_optional()) {
      LOG(ERROR) << "Currently only support non-optional path expand without "
                    "predicate";
      return std::make_pair(nullptr, ContextMeta());
    }
    const algebra::QueryParams& query_params =
        opr.base().edge_expand().params();
    PathExpandParams pep;
    pep.alias = alias;
    pep.dir = dir;
    pep.hop_lower = opr.hop_range().lower();
    pep.hop_upper = opr.hop_range().upper();
    pep.start_tag = start_tag;
    pep.labels = parse_label_triplets(plan.plan(op_idx).meta_data(0));
    if (opr.base().edge_expand().expand_opt() !=
        physical::EdgeExpand_ExpandOpt::EdgeExpand_ExpandOpt_VERTEX) {
      LOG(ERROR) << "Currently only support vertex expand";
      return std::make_pair(nullptr, ContextMeta());
    }
    if (query_params.has_predicate()) {
      LOG(ERROR) << "Currently only support non-optional path expand without "
                    "predicate";
      return std::make_pair(nullptr, ContextMeta());
    }

    if (next_opr.has_params() && next_opr.params().has_predicate()) {
      LOG(ERROR) << "Currently only support path expand without vertex "
                    "predicate";
      return std::make_pair(nullptr, ContextMeta());
    }
    if (next_opr.has_params()) {
      std::vector<label_t> vertex_labels = parse_tables(next_opr.params());
      std::unordered_set<label_t> vertex_label_set(vertex_labels.begin(),
                                                   vertex_labels.end());
      for (const auto& lt : pep.labels) {
        if ((pep.dir == Direction::kOut || pep.dir == Direction::kBoth) &&
            vertex_label_set.find(lt.dst_label) == vertex_label_set.end()) {
          return std::make_pair(nullptr, ContextMeta());
        }
        if ((pep.dir == Direction::kIn || pep.dir == Direction::kBoth) &&
            vertex_label_set.find(lt.src_label) == vertex_label_set.end()) {
          return std::make_pair(nullptr, ContextMeta());
        }
      }
    }
    return std::make_pair(std::make_unique<PathExpandVOpr>(pep), ret_meta);
  } else {
    return std::make_pair(nullptr, ContextMeta());
  }
}

class PathExpandOpr : public IOperator {
 public:
  explicit PathExpandOpr(PathExpandParams pep) : pep_(pep) {}

  std::string get_operator_name() const override { return "PathExpandOpr"; }

  gs::result<gs::runtime::Context> Eval(
      IStorageInterface& graph_interface,
      const std::map<std::string, std::string>& params,
      gs::runtime::Context&& ctx, gs::runtime::OprTimer* timer) override {
    const auto& graph =
        dynamic_cast<const StorageReadInterface&>(graph_interface);
    return PathExpand::edge_expand_p(graph, std::move(ctx), pep_);
  }

 private:
  PathExpandParams pep_;
};

class PathExpandOprWithPred : public IOperator {
 public:
  PathExpandOprWithPred(PathExpandParams pep, const common::Expression& pred)
      : pep_(pep), pred_(pred) {}

  std::string get_operator_name() const override {
    return "PathExpandOprWithPred";
  }

  gs::result<gs::runtime::Context> Eval(
      IStorageInterface& graph_interface,
      const std::map<std::string, std::string>& params,
      gs::runtime::Context&& ctx, gs::runtime::OprTimer* timer) override {
    const auto& graph =
        dynamic_cast<const StorageReadInterface&>(graph_interface);
    GeneralEdgePredicate pred(graph, ctx, params, pred_);
    return PathExpand::edge_expand_p_with_pred(graph, std::move(ctx), pep_,
                                               pred);
  }

 private:
  PathExpandParams pep_;
  common::Expression pred_;
};

class AnyWeightedShortestPathOpr : public IOperator {
 public:
  AnyWeightedShortestPathOpr(PathExpandParams pep,
                             const common::Expression& weight)
      : pep_(pep), weight_(weight) {}

  std::string get_operator_name() const override {
    return "WeightedShortestPathOpr";
  }

  gs::result<gs::runtime::Context> Eval(
      IStorageInterface& graph_interface,
      const std::map<std::string, std::string>& params,
      gs::runtime::Context&& ctx, gs::runtime::OprTimer* timer) override {
    const auto& graph =
        dynamic_cast<const StorageReadInterface&>(graph_interface);
    Expr expr(&graph, ctx, params, weight_, VarType::kEdgeVar);
    Arena arena;
    auto weight_func = [&expr, &arena](const LabelTriplet& label, vid_t src,
                                       vid_t dst, const void* data_ptr) {
      return expr.eval_edge(label, src, dst, data_ptr, arena).as_double();
    };
    return PathExpand::any_weighted_shortest_path(graph, std::move(ctx), pep_,
                                                  weight_func);
  }

 private:
  PathExpandParams pep_;
  common::Expression weight_;
};

gs::result<OpBuildResultT> PathExpandOprBuilder::Build(
    const gs::Schema& schema, const ContextMeta& ctx_meta,
    const physical::PhysicalPlan& plan, int op_idx) {
  const auto& opr = plan.plan(op_idx).opr().path();
  int alias = -1;
  if (opr.has_alias()) {
    alias = opr.alias().value();
  }
  ContextMeta ret_meta = ctx_meta;
  ret_meta.set(alias);
  int start_tag = opr.has_start_tag() ? opr.start_tag().value() : -1;

  if (opr.is_optional()) {
    LOG(ERROR) << "Currently only support non-optional path expand without "
                  "predicate";
    return std::make_pair(nullptr, ContextMeta());
  }

  Direction dir = parse_direction(opr.base().edge_expand().direction());
  if (opr.base().edge_expand().is_optional()) {
    LOG(ERROR) << "Currently only support non-optional path expand without "
                  "predicate";
    return std::make_pair(nullptr, ContextMeta());
  }
  const algebra::QueryParams& query_params = opr.base().edge_expand().params();
  PathExpandParams pep;
  pep.alias = alias;
  pep.dir = dir;
  pep.hop_lower = opr.hop_range().lower();
  pep.hop_upper = opr.hop_range().upper();
  pep.start_tag = start_tag;
  pep.labels = parse_label_triplets(plan.plan(op_idx).meta_data(0));
  pep.opt = parse_path_opt(opr.path_opt());
  if (pep.opt == PathOpt::kAnyWeightedShortest) {
    auto prop = opr.extra_info().weight_expr();
    if (query_params.has_predicate()) {
      LOG(ERROR) << "Currently only support weighted shortest path without "
                    "predicate";
      return std::make_pair(nullptr, ContextMeta());
    } else {
      return std::make_pair(
          std::make_unique<AnyWeightedShortestPathOpr>(pep, prop), ret_meta);
    }
  }

  if (query_params.has_predicate()) {
    return std::make_pair(
        std::make_unique<PathExpandOprWithPred>(pep, query_params.predicate()),
        ret_meta);
  }
  return std::make_pair(std::make_unique<PathExpandOpr>(pep), ret_meta);
}

}  // namespace ops
}  // namespace runtime
}  // namespace gs