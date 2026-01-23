
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

#include "neug/execution/execute/ops/retrieve/project.h"

#include "neug/execution/common/context.h"
#include "neug/execution/common/operators/retrieve/project.h"
#include "neug/execution/execute/ops/retrieve/order_by_utils.h"
#include "neug/execution/execute/ops/retrieve/project_utils.h"
#include "neug/execution/utils/expr.h"
#include "neug/execution/utils/special_predicates.h"
#include "neug/storages/graph/graph_interface.h"
#include "neug/utils/property/types.h"
#include "neug/utils/result.h"

namespace gs {
namespace runtime {
class OprTimer;

namespace ops {

class ProjectOpr : public IOperator {
 public:
  ProjectOpr(const std::vector<std::tuple<common::Expression, int,
                                          std::optional<common::IrDataType>>>&
                 exprs_infos,
             bool is_append)
      : is_append_(is_append) {
    is_select_columns_ = true;
    for (auto& expr_info : exprs_infos) {
      int tag = -1;
      if (is_exchange_index(std::get<0>(expr_info), tag)) {
        select_columns_mapping_.emplace_back(tag, std::get<1>(expr_info));
      } else {
        is_select_columns_ = false;
        select_columns_mapping_.clear();
        break;
      }
    }
    if (is_select_columns_) {
      return;
    }
    for (auto& expr_info : exprs_infos) {
      std::unique_ptr<ProjectExprBuilderBase> builder = nullptr;
      builder = create_dummy_getter_builder(std::get<0>(expr_info),
                                            std::get<1>(expr_info));
      if (builder != nullptr) {
        expr_builders_.push_back(std::move(builder));
        fallback_expr_builders_.push_back(
            std::make_unique<GeneralProjectExprBuilder>(
                std::get<0>(expr_info), std::get<2>(expr_info),
                std::get<1>(expr_info)));
        continue;
      }

      builder = create_vertex_property_expr_builder(std::get<0>(expr_info),
                                                    std::get<1>(expr_info));
      if (builder != nullptr) {
        expr_builders_.push_back(std::move(builder));
        fallback_expr_builders_.push_back(
            std::make_unique<GeneralProjectExprBuilder>(
                std::get<0>(expr_info), std::get<2>(expr_info),
                std::get<1>(expr_info)));
        continue;
      }

      builder = create_case_when_builder(std::get<0>(expr_info),
                                         std::get<1>(expr_info));
      if (builder != nullptr) {
        expr_builders_.push_back(std::move(builder));
        fallback_expr_builders_.push_back(
            std::make_unique<GeneralProjectExprBuilder>(
                std::get<0>(expr_info), std::get<2>(expr_info),
                std::get<1>(expr_info)));
        continue;
      }
      expr_builders_.push_back(std::move(builder));
      fallback_expr_builders_.push_back(
          std::make_unique<GeneralProjectExprBuilder>(std::get<0>(expr_info),
                                                      std::get<2>(expr_info),
                                                      std::get<1>(expr_info)));
    }
  }
  ~ProjectOpr() {}

  gs::result<gs::runtime::Context> Eval(IStorageInterface& graph,
                                        const ParamsMap& params,
                                        gs::runtime::Context&& ctx,
                                        gs::runtime::OprTimer* timer) override {
    if (is_select_columns_) {
      Context ret;
      for (auto& p : select_columns_mapping_) {
        ret.set(p.second, ctx.get(p.first));
      }
      return ret;
    }

    std::vector<std::unique_ptr<ProjectExprBase>> exprs;

    for (size_t i = 0; i < expr_builders_.size(); ++i) {
      if (!expr_builders_[i]) {
        exprs.push_back(fallback_expr_builders_[i]->build(graph, ctx, params));
      } else {
        auto expr = expr_builders_[i]->build(graph, ctx, params);
        if (!expr) {
          exprs.push_back(
              fallback_expr_builders_[i]->build(graph, ctx, params));
        } else {
          exprs.push_back(std::move(expr));
        }
      }
    }

    auto ret = Project::project(std::move(ctx), exprs, is_append_);
    if (!ret) {
      return ret;
    }

    return ret;
  }

  std::string get_operator_name() const override { return "ProjectOpr"; }

 private:
  bool is_append_;

  bool is_select_columns_;
  std::vector<std::pair<int, int>> select_columns_mapping_;

  std::vector<std::unique_ptr<ProjectExprBuilderBase>> expr_builders_;
  std::vector<std::unique_ptr<ProjectExprBuilderBase>> fallback_expr_builders_;
};

gs::result<OpBuildResultT> ProjectOprBuilder::Build(
    const gs::Schema& schema, const ContextMeta& ctx_meta,
    const physical::PhysicalPlan& plan, int op_idx) {
  std::vector<common::IrDataType> data_types;
  int mappings_size = plan.plan(op_idx).opr().project().mappings_size();
  std::vector<
      std::tuple<common::Expression, int, std::optional<common::IrDataType>>>
      expr_infos;
  ContextMeta ret_meta;
  bool is_append = plan.plan(op_idx).opr().project().is_append();
  if (is_append) {
    ret_meta = ctx_meta;
  }

  if (plan.plan(op_idx).meta_data_size() == mappings_size) {
    for (int i = 0; i < plan.plan(op_idx).meta_data_size(); ++i) {
      data_types.push_back(plan.plan(op_idx).meta_data(i).type());
      const auto& m = plan.plan(op_idx).opr().project().mappings(i);
      int alias = m.has_alias() ? m.alias().value() : -1;
      ret_meta.set(alias);
      if (!m.has_expr()) {
        LOG(ERROR) << "expr is not set" << m.DebugString();
        return std::make_pair(nullptr, ret_meta);
      }
      auto expr = m.expr();
      expr_infos.emplace_back(expr, alias, data_types[i]);
    }
  } else {
    for (int i = 0; i < mappings_size; ++i) {
      auto& m = plan.plan(op_idx).opr().project().mappings(i);

      int alias = m.has_alias() ? m.alias().value() : -1;

      ret_meta.set(alias);
      if (!m.has_expr()) {
        LOG(ERROR) << "expr is not set" << m.DebugString();
        return std::make_pair(nullptr, ret_meta);
      }
      auto expr = m.expr();

      expr_infos.emplace_back(expr, alias, std::nullopt);
    }
  }

  return std::make_pair(
      std::make_unique<ProjectOpr>(std::move(expr_infos), is_append), ret_meta);
}

class ProjectOrderByOprBeta : public IOperator {
 public:
  ProjectOrderByOprBeta(
      const std::vector<std::tuple<common::Expression, int,
                                   std::optional<common::IrDataType>>>&
          exprs_infos,
      const std::set<int>& order_by_keys,
      const std::vector<std::pair<common::Variable, bool>>& order_by_pairs,
      int lower_bound, int upper_bound, const std::pair<int, bool>& first_pair)
      : exprs_infos_(exprs_infos),
        order_by_keys_(order_by_keys),
        order_by_pairs_(order_by_pairs),
        lower_bound_(lower_bound),
        upper_bound_(upper_bound),
        first_pair_(first_pair) {}

  std::string get_operator_name() const override {
    return "ProjectOrderByOprBeta";
  }

  gs::result<gs::runtime::Context> Eval(IStorageInterface& graph_interface,
                                        const ParamsMap& params,
                                        gs::runtime::Context&& ctx,
                                        gs::runtime::OprTimer* timer) override {
    const auto& graph =
        dynamic_cast<const StorageReadInterface&>(graph_interface);

    auto cmp_func = [&](const Context& ctx) -> GeneralComparer {
      GeneralComparer cmp;
      for (const auto& pair : order_by_pairs_) {
        Var v(&graph, ctx, pair.first, VarType::kPathVar);
        cmp.add_keys(std::move(v), pair.second);
      }
      return cmp;
    };
    std::vector<std::function<std::unique_ptr<ProjectExprBase>(
        const StorageReadInterface&, const ParamsMap&, const Context&)>>
        exprs;
    for (size_t i = 0; i < exprs_infos_.size(); ++i) {
      const auto& expr = std::get<0>(exprs_infos_[i]);
      const auto& alias = std::get<1>(exprs_infos_[i]);
      const auto& data_type = std::get<2>(exprs_infos_[i]);
      exprs.push_back([expr, alias, data_type](
                          const StorageReadInterface& graph,
                          const ParamsMap& params, const Context& ctx) {
        return create_project_expr(expr, alias, data_type, graph, ctx, params);
      });
    }
    auto fst_idx = first_pair_.first;
    const auto& fst_var = std::get<0>(exprs_infos_[fst_idx]);
    bool asc = first_pair_.second;
    auto index_generator = [&](const Context& ctx,
                               std::vector<size_t>& indices) -> bool {
      if (fst_var.operators_size() == 1 && fst_var.operators(0).has_var()) {
        const auto& var = fst_var.operators(0).var();
        if (var.has_tag()) {
          int tag = var.tag().id();
          auto col = ctx.get(tag);
          if (!var.has_property()) {
            if (col->column_type() == ContextColumnType::kValue) {
              if (col->order_by_limit(asc, upper_bound_, indices)) {
                return true;
              }
            }
          } else if (col->column_type() == ContextColumnType::kVertex) {
            std::string prop_name = var.property().key().name();
            auto vertex_col = std::dynamic_pointer_cast<IVertexColumn>(col);
            if (vertex_property_topN(asc, upper_bound_, vertex_col, graph,
                                     prop_name, indices)) {
              return true;
            }
          }
        }
      }
      auto expr = exprs[fst_idx](graph, params, ctx);
      if (expr->order_by_limit(ctx, asc, upper_bound_, indices)) {
        return true;
      }
      return false;
    };
    auto ret = Project::project_order_by_fuse<GeneralComparer>(
        graph, params, std::move(ctx), exprs, cmp_func, lower_bound_,
        upper_bound_, order_by_keys_, index_generator);
    if (!ret) {
      return ret;
    }
    return ret;
  }

 private:
  std::vector<
      std::tuple<common::Expression, int, std::optional<common::IrDataType>>>
      exprs_infos_;
  std::set<int> order_by_keys_;
  std::vector<std::pair<common::Variable, bool>> order_by_pairs_;
  int lower_bound_, upper_bound_;
  std::pair<int, bool> first_pair_;
};

static bool project_order_by_fusable_beta(
    const physical::Project& project_opr, const algebra::OrderBy& order_by_opr,
    const ContextMeta& ctx_meta,
    const std::vector<common::IrDataType>& data_types,
    std::set<int>& order_by_keys) {
  if (!order_by_opr.has_limit()) {
    return false;
  }
  if (project_opr.is_append()) {
    return false;
  }

  int mappings_size = project_opr.mappings_size();
  if (static_cast<size_t>(mappings_size) != data_types.size()) {
    return false;
  }

  std::set<int> new_generate_columns;
  for (int i = 0; i < mappings_size; ++i) {
    const physical::Project_ExprAlias& m = project_opr.mappings(i);
    if (m.has_alias()) {
      int alias = m.alias().value();
      if (new_generate_columns.find(alias) != new_generate_columns.end()) {
        return false;
      }
      new_generate_columns.insert(alias);
    }
  }

  int order_by_keys_num = order_by_opr.pairs_size();
  for (int k_i = 0; k_i < order_by_keys_num; ++k_i) {
    if (!order_by_opr.pairs(k_i).has_key()) {
      return false;
    }
    if (!order_by_opr.pairs(k_i).key().has_tag()) {
      return false;
    }
    if (!(order_by_opr.pairs(k_i).key().tag().item_case() ==
          common::NameOrId::ItemCase::kId)) {
      return false;
    }
    order_by_keys.insert(order_by_opr.pairs(k_i).key().tag().id());
  }
  for (auto key : order_by_keys) {
    if (new_generate_columns.find(key) == new_generate_columns.end() &&
        !ctx_meta.exist(key)) {
      return false;
    }
  }
  return true;
}

gs::result<OpBuildResultT> ProjectOrderByOprBuilder::Build(
    const gs::Schema& schema, const ContextMeta& ctx_meta,
    const physical::PhysicalPlan& plan, int op_idx) {
  std::vector<common::IrDataType> data_types;
  int mappings_size = plan.plan(op_idx).opr().project().mappings_size();
  if (plan.plan(op_idx).meta_data_size() == mappings_size) {
    for (int i = 0; i < plan.plan(op_idx).meta_data_size(); ++i) {
      data_types.push_back(plan.plan(op_idx).meta_data(i).type());
    }
  }
  std::set<int> order_by_keys;
  if (project_order_by_fusable_beta(plan.plan(op_idx).opr().project(),
                                    plan.plan(op_idx + 1).opr().order_by(),
                                    ctx_meta, data_types, order_by_keys)) {
    ContextMeta ret_meta;
    std::vector<
        std::tuple<common::Expression, int, std::optional<common::IrDataType>>>
        expr_infos;
    std::set<int> index_set;
    int first_key =
        plan.plan(op_idx + 1).opr().order_by().pairs(0).key().tag().id();
    int first_idx = -1;
    for (int i = 0; i < mappings_size; ++i) {
      auto& m = plan.plan(op_idx).opr().project().mappings(i);
      int alias = -1;
      if (m.has_alias()) {
        alias = m.alias().value();
      }
      ret_meta.set(alias);
      if (alias == first_key) {
        first_idx = i;
      }
      if (!m.has_expr()) {
        LOG(ERROR) << "expr is not set" << m.DebugString();
        return std::make_pair(nullptr, ret_meta);
      }
      auto expr = m.expr();
      expr_infos.emplace_back(expr, alias, data_types[i]);
      if (order_by_keys.find(alias) != order_by_keys.end()) {
        index_set.insert(i);
      }
    }

    auto order_by_opr = plan.plan(op_idx + 1).opr().order_by();
    int pair_size = order_by_opr.pairs_size();
    std::vector<std::pair<common::Variable, bool>> order_by_pairs;
    std::pair<int, bool> first_tuple;
    for (int i = 0; i < pair_size; ++i) {
      const auto& pair = order_by_opr.pairs(i);
      if (pair.order() != algebra::OrderBy_OrderingPair_Order::
                              OrderBy_OrderingPair_Order_ASC &&
          pair.order() != algebra::OrderBy_OrderingPair_Order::
                              OrderBy_OrderingPair_Order_DESC) {
        LOG(ERROR) << "order by order is not set" << pair.DebugString();
        return std::make_pair(nullptr, ContextMeta());
      }
      bool asc =
          pair.order() ==
          algebra::OrderBy_OrderingPair_Order::OrderBy_OrderingPair_Order_ASC;
      order_by_pairs.emplace_back(pair.key(), asc);
      if (i == 0) {
        first_tuple = std::make_pair(first_idx, asc);
        if (pair.key().has_property()) {
          LOG(ERROR) << "key has property" << pair.DebugString();
          return std::make_pair(nullptr, ContextMeta());
        }
      }
    }
    int lower = 0;
    int upper = std::numeric_limits<int>::max();
    if (order_by_opr.has_limit()) {
      lower = order_by_opr.limit().lower();
      upper = order_by_opr.limit().upper();
    }
    return std::make_pair(std::make_unique<ProjectOrderByOprBeta>(
                              std::move(expr_infos), index_set, order_by_pairs,
                              lower, upper, first_tuple),
                          ret_meta);
  } else {
    return std::make_pair(nullptr, ContextMeta());
  }
}

}  // namespace ops
}  // namespace runtime
}  // namespace gs