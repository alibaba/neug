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

#include "neug/execution/execute/ops/retrieve/edge.h"

#include "neug/execution/common/operators/retrieve/edge_expand.h"
#include "neug/execution/expression/expr.h"
#include "neug/execution/expression/predicates.h"
#include "neug/execution/utils/pb_parse_utils.h"
#include "neug/utils/property/types.h"

namespace neug {
class Schema;

namespace execution {
class OprTimer;

namespace ops {

bool edge_expand_get_v_fusable(const physical::PhysicalPlan& plan, int idx,
                               const physical::PhysicalOpr_MetaData& meta) {
  const auto& ee_opr = plan.plan(idx).opr().edge();
  const auto& v_opr = plan.plan(idx + 1).opr().vertex();
  if (ee_opr.expand_opt() !=
          physical::EdgeExpand_ExpandOpt::EdgeExpand_ExpandOpt_EDGE &&
      ee_opr.expand_opt() !=
          physical::EdgeExpand_ExpandOpt::EdgeExpand_ExpandOpt_VERTEX) {
    return false;
  }

  int alias = ee_opr.has_alias() ? ee_opr.alias().value() : -1;
  if (alias != -1) {
    if (idx + 2 < plan.plan_size() && plan.plan(idx + 2).opr().has_project()) {
      auto proj_opr = plan.plan(idx + 2).opr().project();
      if (proj_opr.is_append()) {
        return false;
      }
      const auto& mappings = proj_opr.mappings();
      for (auto i = 0; i < mappings.size(); ++i) {
        auto expr = mappings.Get(i).expr();
        for (auto j = 0; j < expr.operators_size(); ++j) {
          const auto& opr = expr.operators().Get(j);
          if (!opr.has_var()) {
            return false;
          }
          if (opr.has_var() && opr.var().has_tag() &&
              opr.var().tag().id() == alias) {
            return false;
          }
        }
      }
    } else {
      return false;
    }
  }

  int tag = v_opr.has_tag() ? v_opr.tag().value() : -1;
  if (tag != alias && tag != -1) {
    return false;
  }

  Direction dir = parse_direction(ee_opr.direction());
  if (ee_opr.expand_opt() ==
      physical::EdgeExpand_ExpandOpt::EdgeExpand_ExpandOpt_VERTEX) {
    if (v_opr.opt() == physical::GetV_VOpt::GetV_VOpt_ITSELF) {
      return true;
    }
  } else if (ee_opr.expand_opt() ==
             physical::EdgeExpand_ExpandOpt::EdgeExpand_ExpandOpt_EDGE) {
    if (dir == Direction::kOut &&
        v_opr.opt() == physical::GetV_VOpt::GetV_VOpt_END) {
      return true;
    }
    if (dir == Direction::kIn &&
        v_opr.opt() == physical::GetV_VOpt::GetV_VOpt_START) {
      return true;
    }
  }
  return false;
}

class EdgeExpandVWithEPCmpOpr : public IOperator {
 public:
  EdgeExpandVWithEPCmpOpr(const EdgeExpandParams& eep,
                          const SpecialPredicateConfig& config,
                          std::unique_ptr<ExprBase>&& pred)
      : eep_(eep), pred_(std::move(pred)), config_(config) {}

  std::string get_operator_name() const override {
    return "EdgeExpandVWithEPCmpOpr";
  }

  neug::result<neug::execution::Context> Eval(
      IStorageInterface& graph_interface, const ParamsMap& params,
      neug::execution::Context&& ctx,
      neug::execution::OprTimer* timer) override {
    const auto& graph =
        dynamic_cast<const StorageReadInterface&>(graph_interface);
    if ((!eep_.is_optional) &&
        (config_.ptype == SPPredicateType::kPropertyLT ||
         config_.ptype == SPPredicateType::kPropertyGT)) {
      const auto& param_value = params.at(config_.param_names[0]);
      auto ret = EdgeExpand::expand_vertex_ep_cmp(graph, std::move(ctx), eep_,
                                                  param_value, config_.ptype);
      if (ret) {
        return ret.value();
      }
    }

    auto expr = pred_->bind(&graph, params);
    GeneralPred expr_wrapper(std::move(expr));
    EdgePredicate<GeneralPred> pred(expr_wrapper);
    return EdgeExpand::expand_vertex<decltype(pred)>(graph, std::move(ctx),
                                                     eep_, pred);
  }

 private:
  EdgeExpandParams eep_;
  std::unique_ptr<ExprBase> pred_;
  SpecialPredicateConfig config_;
};

class EdgeExpandVOpr : public IOperator {
 public:
  EdgeExpandVOpr(const EdgeExpandParams& eep, std::unique_ptr<ExprBase>&& pred)
      : eep_(eep), pred_(std::move(pred)) {}

  std::string get_operator_name() const override { return "EdgeExpandVOpr"; }

  neug::result<neug::execution::Context> Eval(
      IStorageInterface& graph_interface, const ParamsMap& params,
      neug::execution::Context&& ctx,
      neug::execution::OprTimer* timer) override {
    const auto& graph =
        dynamic_cast<const StorageReadInterface&>(graph_interface);

    if (pred_ != nullptr) {
      auto expr = pred_->bind(&graph, params);
      GeneralPred expr_wrapper(std::move(expr));
      EdgePredicate pred(expr_wrapper);
      return EdgeExpand::expand_vertex<decltype(pred)>(graph, std::move(ctx),
                                                       eep_, pred);
    } else {
      return EdgeExpand::expand_vertex<DummyPred>(graph, std::move(ctx), eep_,
                                                  DummyPred());
    }
  }

 private:
  EdgeExpandParams eep_;
  std::unique_ptr<ExprBase> pred_;
};

class EdgeExpandEWithSPredOpr : public IOperator {
 public:
  EdgeExpandEWithSPredOpr(const EdgeExpandParams& eep,
                          const SpecialPredicateConfig& config)
      : eep_(eep), config_(config) {}

  std::string get_operator_name() const override {
    return "EdgeExpandEWithSPredOpr";
  }

  neug::result<neug::execution::Context> Eval(
      IStorageInterface& graph_interface, const ParamsMap& params,
      neug::execution::Context&& ctx,
      neug::execution::OprTimer* timer) override {
    const auto& graph =
        dynamic_cast<const StorageReadInterface&>(graph_interface);
    return EdgeExpand::expand_edge_with_special_edge_predicate(
        graph, std::move(ctx), eep_, config_,
        params.at(config_.param_names[0]));
  }

 private:
  EdgeExpandParams eep_;
  SpecialPredicateConfig config_;
};

class EdgeExpandEOpr : public IOperator {
 public:
  EdgeExpandEOpr(const EdgeExpandParams& eep, std::unique_ptr<ExprBase>&& pred)
      : eep_(eep), pred_(std::move(pred)) {}

  std::string get_operator_name() const override { return "EdgeExpandEOpr"; }

  neug::result<neug::execution::Context> Eval(
      IStorageInterface& graph_interface, const ParamsMap& params,
      neug::execution::Context&& ctx,
      neug::execution::OprTimer* timer) override {
    const auto& graph =
        dynamic_cast<const StorageReadInterface&>(graph_interface);
    if (pred_ == nullptr) {
      return EdgeExpand::expand_edge(graph, std::move(ctx), eep_, DummyPred());
    } else {
      auto expr = pred_->bind(&graph, params);
      GeneralPred expr_wrapper(std::move(expr));
      EdgePredicate pred(expr_wrapper);
      return EdgeExpand::expand_edge<decltype(pred)>(graph, std::move(ctx),
                                                     eep_, pred);
    }
  }

 private:
  EdgeExpandParams eep_;
  std::unique_ptr<ExprBase> pred_;
};

class EdgeExpandVWithSPVertexPredOpr : public IOperator {
 public:
  EdgeExpandVWithSPVertexPredOpr(const EdgeExpandParams& eep,
                                 const SpecialPredicateConfig& config)
      : eep_(eep), config_(config) {}

  std::string get_operator_name() const override {
    return "EdgeExpandVWithSPVertexPredOpr";
  }

  neug::result<neug::execution::Context> Eval(
      IStorageInterface& graph_interface, const ParamsMap& params,
      neug::execution::Context&& ctx,
      neug::execution::OprTimer* timer) override {
    const auto& graph =
        dynamic_cast<const StorageReadInterface&>(graph_interface);
    return EdgeExpand::expand_vertex_with_special_vertex_predicate(
        graph, std::move(ctx), eep_, config_, params);
  }

 private:
  EdgeExpandParams eep_;
  SpecialPredicateConfig config_;
};

class EdgeExpandVWithGPVertexPredOpr : public IOperator {
 public:
  EdgeExpandVWithGPVertexPredOpr(const EdgeExpandParams& eep,
                                 std::unique_ptr<ExprBase>&& pred)
      : eep_(eep), pred_(std::move(pred)) {}
  std::string get_operator_name() const override {
    return "EdgeExpandVWithGPVertexPredOpr";
  }

  neug::result<neug::execution::Context> Eval(
      IStorageInterface& graph_interface, const ParamsMap& params,
      neug::execution::Context&& ctx,
      neug::execution::OprTimer* timer) override {
    const auto& graph =
        dynamic_cast<const StorageReadInterface&>(graph_interface);
    auto expr = pred_->bind(&graph, params);
    GeneralPred expr_wrapper(std::move(expr));
    EdgeNbrPredicate vpred(expr_wrapper);
    return EdgeExpand::expand_vertex<EdgeNbrPredicate<GeneralPred>>(
        graph, std::move(ctx), eep_, vpred);
  }

 private:
  EdgeExpandParams eep_;
  std::unique_ptr<ExprBase> pred_;
};

class EdgeExpandDegreeOpr : public IOperator {
 public:
  EdgeExpandDegreeOpr(const EdgeExpandParams& eep) : eep_(eep) {}

  neug::result<neug::execution::Context> Eval(
      IStorageInterface& graph_interface, const ParamsMap& params,
      neug::execution::Context&& ctx,
      neug::execution::OprTimer* timer) override {
    const auto& graph =
        dynamic_cast<const StorageReadInterface&>(graph_interface);
    return EdgeExpand::expand_degree(graph, std::move(ctx), eep_);
  }

  std::string get_operator_name() const override {
    return "EdgeExpandDegreeOpr";
  }

 private:
  EdgeExpandParams eep_;
};

neug::result<OpBuildResultT> EdgeExpandOprBuilder::Build(
    const neug::Schema& schema, const ContextMeta& ctx_meta,
    const physical::PhysicalPlan& plan, int op_idx) {
  int alias = -1;
  if (plan.plan(op_idx).opr().edge().has_alias()) {
    alias = plan.plan(op_idx).opr().edge().alias().value();
  }
  ContextMeta meta = ctx_meta;

  auto opr = plan.plan(op_idx).opr().edge();
  int v_tag = opr.has_v_tag() ? opr.v_tag().value() : -1;
  Direction dir = parse_direction(opr.direction());
  bool is_optional = opr.is_optional();
  if (!opr.has_params()) {
    LOG(ERROR) << "EdgeExpandOprBuilder::Build: query_params is empty";
    return std::make_pair(nullptr, ContextMeta());
  }
  const auto& query_params = opr.params();
  EdgeExpandParams eep;
  eep.v_tag = v_tag;
  eep.labels = parse_label_triplets(plan.plan(op_idx).meta_data(0));
  eep.dir = dir;
  eep.alias = alias;
  eep.is_optional = is_optional;
  if (opr.expand_opt() == physical::EdgeExpand_ExpandOpt_VERTEX) {
    meta.set(alias, DataType::VERTEX);
    if (query_params.has_predicate()) {
      SpecialPredicateConfig config;
      auto pred =
          parse_expression(query_params.predicate(), ctx_meta, VarType::kEdge);
      if (is_special_edge_predicate(schema, eep.labels,
                                    query_params.predicate(), config)) {
        return std::make_pair(std::make_unique<EdgeExpandVWithEPCmpOpr>(
                                  eep, config, std::move(pred)),
                              meta);
      }
      return std::make_pair(
          std::make_unique<EdgeExpandVOpr>(eep, std::move(pred)), meta);

    } else {
      return std::make_pair(std::make_unique<EdgeExpandVOpr>(eep, nullptr),
                            meta);
    }
  } else if (opr.expand_opt() == physical::EdgeExpand_ExpandOpt_EDGE) {
    meta.set(alias, DataType::EDGE);
    if (query_params.has_predicate()) {
      SpecialPredicateConfig config;
      if (is_special_edge_predicate(schema, eep.labels,
                                    query_params.predicate(), config)) {
        return std::make_pair(
            std::make_unique<EdgeExpandEWithSPredOpr>(eep, config), meta);
      } else {
        auto pred = parse_expression(query_params.predicate(), ctx_meta,
                                     VarType::kEdge);
        return std::make_pair(
            std::make_unique<EdgeExpandEOpr>(eep, std::move(pred)), meta);
      }
    } else {
      return std::make_pair(std::make_unique<EdgeExpandEOpr>(eep, nullptr),
                            meta);
    }
  } else if (opr.expand_opt() == physical::EdgeExpand_ExpandOpt_DEGREE) {
    meta.set(alias, DataType::INT64);
    if (query_params.has_predicate()) {
      LOG(ERROR)
          << "EdgeExpandOprBuilder::Build: expand_opt is DEGREE with predicate"
          << opr.DebugString();
      return std::make_pair(nullptr, ContextMeta());
    } else {
      return std::make_pair(std::make_unique<EdgeExpandDegreeOpr>(eep), meta);
    }
  }
  return std::make_pair(nullptr, ContextMeta());
}

neug::result<OpBuildResultT> EdgeExpandGetVOprBuilder::Build(
    const neug::Schema& schema, const ContextMeta& ctx_meta,
    const physical::PhysicalPlan& plan, int op_idx) {
  if (edge_expand_get_v_fusable(plan, op_idx, plan.plan(op_idx).meta_data(0))) {
    int alias = -1;
    if (plan.plan(op_idx + 1).opr().vertex().has_alias()) {
      alias = plan.plan(op_idx + 1).opr().vertex().alias().value();
    }
    ContextMeta meta = ctx_meta;
    meta.set(alias, DataType::VERTEX);
    const auto& ee_opr = plan.plan(op_idx).opr().edge();
    const auto& v_opr = plan.plan(op_idx + 1).opr().vertex();
    auto vtables = parse_tables(v_opr.params());

    int v_tag = ee_opr.has_v_tag() ? ee_opr.v_tag().value() : -1;
    Direction dir = parse_direction(ee_opr.direction());
    bool is_optional = ee_opr.is_optional();
    if (!ee_opr.has_params()) {
      LOG(ERROR) << "EdgeExpandGetVOprBuilder::Build: query_params is empty"
                 << ee_opr.DebugString();
      return std::make_pair(nullptr, ContextMeta());
    }
    const auto& query_params = ee_opr.params();
    if (ee_opr.expand_opt() !=
            physical::EdgeExpand_ExpandOpt::EdgeExpand_ExpandOpt_EDGE &&
        ee_opr.expand_opt() !=
            physical::EdgeExpand_ExpandOpt::EdgeExpand_ExpandOpt_VERTEX) {
      LOG(ERROR) << "EdgeExpandGetVOprBuilder::Build: expand_opt is not EDGE "
                    "or VERTEX"
                 << ee_opr.DebugString();
      return std::make_pair(nullptr, ContextMeta());
    }

    EdgeExpandParams eep;
    eep.v_tag = v_tag;
    eep.labels = parse_label_triplets(plan.plan(op_idx).meta_data(0));
    std::vector<LabelTriplet> filtered_labels;
    for (auto label : eep.labels) {
      if (dir == Direction::kOut &&
          std::find(vtables.begin(), vtables.end(), label.dst_label) !=
              vtables.end()) {
        filtered_labels.push_back(label);
      } else if (dir == Direction::kIn &&
                 std::find(vtables.begin(), vtables.end(), label.src_label) !=
                     vtables.end()) {
        filtered_labels.push_back(label);
      } else if (dir == Direction::kBoth) {
        if (std::find(vtables.begin(), vtables.end(), label.src_label) !=
                vtables.end() &&
            std::find(vtables.begin(), vtables.end(), label.dst_label) !=
                vtables.end()) {
          filtered_labels.push_back(label);
        } else if ((std::find(vtables.begin(), vtables.end(),
                              label.src_label) != vtables.end()) ||
                   (std::find(vtables.begin(), vtables.end(),
                              label.dst_label) != vtables.end())) {
          return std::make_pair(nullptr, ContextMeta());
        }
      }
    }
    eep.labels = filtered_labels;
    eep.dir = dir;
    eep.alias = alias;
    eep.is_optional = is_optional;
    if (!v_opr.params().has_predicate()) {
      if (query_params.has_predicate()) {
        auto expr = parse_expression(query_params.predicate(), ctx_meta,
                                     VarType::kEdge);
        SpecialPredicateConfig config;
        if (is_special_edge_predicate(schema, eep.labels,
                                      query_params.predicate(), config)) {
          return std::make_pair(std::make_unique<EdgeExpandVWithEPCmpOpr>(
                                    eep, config, std::move(expr)),
                                meta);
        }
        return std::make_pair(
            std::make_unique<EdgeExpandVOpr>(eep, std::move(expr)), meta);
      } else {
        return std::make_pair(std::make_unique<EdgeExpandVOpr>(eep, nullptr),
                              meta);
      }
    }

    if (query_params.has_predicate()) {
      return std::make_pair(nullptr, meta);
    } else {
      SpecialPredicateConfig config;
      const auto& vertex_labels = parse_tables(v_opr.params());
      if (is_special_vertex_predicate(schema, vertex_labels,
                                      v_opr.params().predicate(), config)) {
        return std::make_pair(
            std::make_unique<EdgeExpandVWithSPVertexPredOpr>(eep, config),
            meta);

      } else {
        auto expr = parse_expression(v_opr.params().predicate(), ctx_meta,
                                     VarType::kVertex);
        return std::make_pair(std::make_unique<EdgeExpandVWithGPVertexPredOpr>(
                                  eep, std::move(expr)),
                              meta);
      }
    }
  }
  return std::make_pair(nullptr, ContextMeta());
}
}  // namespace ops

}  // namespace execution
}  // namespace neug