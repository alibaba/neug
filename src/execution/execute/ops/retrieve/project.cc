
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
#include "neug/execution/expression/special_predicates.h"

namespace neug {
namespace execution {
class OprTimer;

namespace ops {

class ProjectOpr : public IOperator {
 public:
  ProjectOpr(std::vector<std::pair<int, int>>&& select_columns_mapping,
             bool is_append)
      : is_append_(is_append),
        is_select_columns_(true),
        select_columns_mapping_(std::move(select_columns_mapping)) {}
  ProjectOpr(
      std::vector<std::unique_ptr<ProjectExprBuilderBase>>&& expr_builders,
      std::vector<std::unique_ptr<ProjectExprBuilderBase>>&&
          fallback_expr_builders,
      bool is_append)
      : is_append_(is_append),
        is_select_columns_(false),
        expr_builders_(std::move(expr_builders)),
        fallback_expr_builders_(std::move(fallback_expr_builders)) {}

  ~ProjectOpr() {}

  neug::result<neug::execution::Context> Eval(
      IStorageInterface& graph, const ParamsMap& params,
      neug::execution::Context&& ctx,
      neug::execution::OprTimer* timer) override {
    if (is_select_columns_) {
      Context ret;
      for (auto& p : select_columns_mapping_) {
        ret.set(p.second, ctx.get(p.first));
      }
      return ret;
    }

    std::vector<ProjectOp> exprs;

    for (size_t i = 0; i < expr_builders_.size(); ++i) {
      if (!expr_builders_[i]) {
        exprs.emplace_back(fallback_expr_builders_[i]->build(graph, params),
                           nullptr, fallback_expr_builders_[i]->alias());
        continue;
      } else {
        exprs.emplace_back(expr_builders_[i]->build(graph, params),
                           fallback_expr_builders_[i]->build(graph, params),
                           expr_builders_[i]->alias());
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

neug::result<OpBuildResultT> ProjectOprBuilder::Build(
    const neug::Schema& schema, const ContextMeta& ctx_meta,
    const physical::PhysicalPlan& plan, int op_idx) {
  std::vector<common::IrDataType> data_types;
  int mappings_size = plan.plan(op_idx).opr().project().mappings_size();
  std::vector<std::tuple<common::Expression, int, std::unique_ptr<ExprBase>>>
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
      ret_meta.set(alias, parse_from_ir_data_type(data_types[i]));
      if (!m.has_expr()) {
        LOG(ERROR) << "expr is not set" << m.DebugString();
        return std::make_pair(nullptr, ret_meta);
      }
      auto expr = m.expr();
      auto expr_ptr =
          parse_expression(expr, ctx_meta, neug::execution::VarType::kRecord);
      expr_infos.emplace_back(expr, alias, std::move(expr_ptr));
    }
  } else {
    LOG(ERROR) << "meta data size and mappings size mismatch "
               << plan.plan(op_idx).meta_data_size() << " vs " << mappings_size;
    return std::make_pair(nullptr, ret_meta);
  }

  std::vector<std::unique_ptr<ProjectExprBuilderBase>> expr_builders;
  std::vector<std::unique_ptr<ProjectExprBuilderBase>> fallback_expr_builders;
  std::vector<std::pair<int, int>> select_columns_mapping;

  bool is_select_columns = true;
  for (auto& expr_info : expr_infos) {
    int tag = -1;
    if (is_exchange_index(std::get<0>(expr_info), tag)) {
      select_columns_mapping.emplace_back(tag, std::get<1>(expr_info));
    } else {
      is_select_columns = false;
      select_columns_mapping.clear();
      break;
    }
  }
  if (is_select_columns) {
    return std::make_pair(std::make_unique<ProjectOpr>(
                              std::move(select_columns_mapping), is_append),
                          ret_meta);
  }

  create_project_expr_builders(std::move(expr_infos), expr_builders,
                               fallback_expr_builders);
  return std::make_pair(std::make_unique<ProjectOpr>(
                            std::move(expr_builders),
                            std::move(fallback_expr_builders), is_append),
                        ret_meta);
}

class ProjectOrderByOprBeta : public IOperator {
 public:
  ProjectOrderByOprBeta(
      std::vector<std::unique_ptr<ProjectExprBuilderBase>>&& expr_builders,
      std::vector<std::unique_ptr<ProjectExprBuilderBase>>&&
          fallback_expr_builders,
      const common::Expression& fst_expr, const std::set<int>& order_by_keys,
      const std::vector<std::pair<int32_t, bool>>& order_by_pairs,
      int lower_bound, int upper_bound, const std::pair<int, bool>& first_pair)
      : expr_builders_(std::move(expr_builders)),
        fallback_expr_builders_(std::move(fallback_expr_builders)),
        fst_expr_(fst_expr),
        order_by_keys_(order_by_keys),
        order_by_pairs_(order_by_pairs),
        lower_bound_(lower_bound),
        upper_bound_(upper_bound),
        first_pair_(first_pair) {}

  std::string get_operator_name() const override {
    return "ProjectOrderByOprBeta";
  }

  neug::result<neug::execution::Context> Eval(
      IStorageInterface& graph_interface, const ParamsMap& params,
      neug::execution::Context&& ctx,
      neug::execution::OprTimer* timer) override {
    const auto& graph =
        dynamic_cast<const StorageReadInterface&>(graph_interface);

    auto cmp_func = [&](const Context& ctx) -> GeneralComparer {
      GeneralComparer cmp;
      for (const auto& pair : order_by_pairs_) {
        cmp.add_keys(ctx.get(pair.first), pair.second);
      }
      return cmp;
    };

    std::vector<ProjectOp> exprs;

    for (size_t i = 0; i < expr_builders_.size(); ++i) {
      if (!expr_builders_[i]) {
        exprs.emplace_back(
            ProjectOp(fallback_expr_builders_[i]->build(graph, params), nullptr,
                      fallback_expr_builders_[i]->alias()));
        continue;
      }
      exprs.emplace_back(
          ProjectOp(expr_builders_[i]->build(graph, params),
                    fallback_expr_builders_[i]->build(graph, params),
                    expr_builders_[i]->alias()));
    }
    auto ret = Project::project_order_by_fuse<GeneralComparer>(
        graph, params, std::move(ctx), std::move(exprs), cmp_func, lower_bound_,
        upper_bound_, order_by_keys_, first_pair_);
    if (!ret) {
      return ret;
    }
    return ret;
  }

 private:
  std::vector<std::unique_ptr<ProjectExprBuilderBase>> expr_builders_;
  std::vector<std::unique_ptr<ProjectExprBuilderBase>> fallback_expr_builders_;
  ::common::Expression fst_expr_;
  std::set<int> order_by_keys_;
  std::vector<std::pair<int32_t, bool>> order_by_pairs_;
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
  return true;
}

neug::result<OpBuildResultT> ProjectOrderByOprBuilder::Build(
    const neug::Schema& schema, const ContextMeta& ctx_meta,
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
    std::vector<std::tuple<common::Expression, int, std::unique_ptr<ExprBase>>>
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
      ret_meta.set(alias, parse_from_ir_data_type(data_types[i]));
      if (alias == first_key) {
        first_idx = i;
      }
      if (!m.has_expr()) {
        LOG(ERROR) << "expr is not set" << m.DebugString();
        return std::make_pair(nullptr, ret_meta);
      }
      auto expr = m.expr();
      expr_infos.emplace_back(
          expr, alias, parse_expression(expr, ctx_meta, VarType::kRecord));
      if (order_by_keys.find(alias) != order_by_keys.end()) {
        index_set.insert(i);
      }
    }

    auto order_by_opr = plan.plan(op_idx + 1).opr().order_by();
    int pair_size = order_by_opr.pairs_size();
    std::vector<std::pair<int32_t, bool>> order_by_pairs;
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
      int32_t tag_id = pair.key().has_tag() ? pair.key().tag().id() : -1;
      order_by_pairs.emplace_back(tag_id, asc);
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
    const auto& first_expr = std::get<0>(expr_infos[first_idx]);
    std::vector<std::unique_ptr<ProjectExprBuilderBase>> expr_builders;
    std::vector<std::unique_ptr<ProjectExprBuilderBase>> fallback_expr_builders;
    create_project_expr_builders(std::move(expr_infos), expr_builders,
                                 fallback_expr_builders);
    return std::make_pair(
        std::make_unique<ProjectOrderByOprBeta>(
            std::move(expr_builders), std::move(fallback_expr_builders),
            first_expr, index_set, order_by_pairs, lower, upper, first_tuple),
        ret_meta);
  } else {
    return std::make_pair(nullptr, ContextMeta());
  }
}

}  // namespace ops
}  // namespace execution
}  // namespace neug