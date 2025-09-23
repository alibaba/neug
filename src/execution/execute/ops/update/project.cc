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
#include "neug/execution/execute/ops/update/project.h"

#include <glog/logging.h>
#include <google/protobuf/wrappers.pb.h>
#include <stddef.h>

#include <cstdint>
#include <functional>
#include <map>
#include <ostream>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "neug/execution/common/columns/value_columns.h"
#include "neug/execution/common/context.h"
#include "neug/execution/common/graph_interface.h"
#include "neug/execution/common/operators/update/project.h"
#include "neug/execution/common/rt_any.h"
#include "neug/execution/execute/ops/utils/project_utils.h"
#include "neug/execution/utils/var.h"
#include "neug/utils/result.h"

namespace gs {
class Schema;

namespace runtime {
class IContextColumn;
class OprTimer;

namespace ops {
class ProjectInsertOpr : public IInsertOperator {
 public:
  ProjectInsertOpr(
      const std::vector<std::function<std::unique_ptr<WriteProjectExprBase>(
          const std::map<std::string, std::string>&)>>& exprs)
      : exprs_(exprs) {}

  std::string get_operator_name() const override { return "ProjectInsertOpr"; }

  template <typename GraphInterface>
  gs::result<gs::runtime::WriteContext> eval_impl(
      GraphInterface& graph, const std::map<std::string, std::string>& params,
      gs::runtime::WriteContext&& ctx, gs::runtime::OprTimer* timer) {
    std::vector<std::unique_ptr<WriteProjectExprBase>> exprs;
    for (auto& expr : exprs_) {
      exprs.push_back(expr(params));
    }
    return IProject::project(std::move(ctx), exprs);
  }

  gs::result<gs::runtime::WriteContext> Eval(
      gs::runtime::GraphInsertInterface& graph,
      const std::map<std::string, std::string>& params,
      gs::runtime::WriteContext&& ctx, gs::runtime::OprTimer* timer) override {
    std::vector<std::unique_ptr<WriteProjectExprBase>> exprs;
    return eval_impl(graph, params, std::move(ctx), timer);
  }

  gs::result<gs::runtime::WriteContext> Eval(
      gs::runtime::GraphUpdateInterface& graph,
      const std::map<std::string, std::string>& params,
      gs::runtime::WriteContext&& ctx, gs::runtime::OprTimer* timer) override {
    std::vector<std::unique_ptr<WriteProjectExprBase>> exprs;
    return eval_impl(graph, params, std::move(ctx), timer);
  }

 private:
  std::vector<std::function<std::unique_ptr<WriteProjectExprBase>(
      const std::map<std::string, std::string>&)>>
      exprs_;
};

std::unique_ptr<IInsertOperator> ProjectInsertOprBuilder::Build(
    const Schema& schema, const physical::PhysicalPlan& plan, int op_id) {
  auto opr = plan.query_plan().plan(op_id).opr().project();
  int mappings_size = opr.mappings_size();
  std::vector<std::function<std::unique_ptr<WriteProjectExprBase>(
      const std::map<std::string, std::string>&)>>
      exprs;
  for (int i = 0; i < mappings_size; ++i) {
    const physical::Project_ExprAlias& m = opr.mappings(i);
    if (!m.has_alias()) {
      LOG(ERROR) << "project mapping should have alias";
      return nullptr;
    }
    if ((!m.has_expr()) || m.expr().operators_size() != 1) {
      LOG(ERROR) << "project mapping should have one expr";
      return nullptr;
    }
    if (m.expr().operators(0).item_case() == common::ExprOpr::kParam) {
      auto param = m.expr().operators(0).param();
      auto name = param.name();
      int alias = m.alias().value();
      exprs.emplace_back(
          [name, alias](const std::map<std::string, std::string>& params) {
            CHECK(params.find(name) != params.end());
            return std::make_unique<ParamsGetter>(params.at(name), alias);
          });
    } else if (m.expr().operators(0).item_case() == common::ExprOpr::kVar) {
      auto var = m.expr().operators(0).var();
      if (!var.has_tag()) {
        LOG(ERROR) << "project mapping should have tag";
        return nullptr;
      }
      if (var.has_property()) {
        LOG(ERROR) << "project mapping should not have property";
        return nullptr;
      }
      int tag = var.tag().id();
      int alias = m.alias().value();
      exprs.emplace_back(
          [tag, alias](const std::map<std::string, std::string>&) {
            return std::make_unique<DummyWGetter>(tag, alias);
          });
    } else if (m.expr().operators(0).has_udf_func()) {
      auto udf_func = m.expr().operators(0).udf_func();

      if (udf_func.name() == "gs.function.first") {
        if (udf_func.parameters_size() != 1 ||
            udf_func.parameters(0).operators_size() != 1) {
          LOG(ERROR) << "not support for " << m.expr().DebugString();
          return nullptr;
        }
        auto param = udf_func.parameters(0).operators(0);

        if (param.item_case() != common::ExprOpr::kVar) {
          LOG(ERROR) << "not support for " << m.expr().DebugString();
          return nullptr;
        }
        auto var = param.var();
        if (!var.has_tag()) {
          LOG(ERROR) << "project mapping should have tag";
          return nullptr;
        }
        if (var.has_property()) {
          LOG(ERROR) << "project mapping should not have property";
          return nullptr;
        }
        int tag = var.tag().id();
        int alias = m.alias().value();

        if (i + 1 < mappings_size) {
          const physical::Project_ExprAlias& next = opr.mappings(i + 1);
          if (!next.has_alias()) {
            LOG(ERROR) << "project mapping should have alias";
            return nullptr;
          }
          if (!next.has_expr()) {
            LOG(ERROR) << "project mapping should have expr";
            return nullptr;
          }
          if (next.expr().operators_size() != 1) {
            LOG(ERROR) << "project mapping should have one expr";
            return nullptr;
          }
          if (next.expr().operators(0).has_udf_func()) {
            auto next_udf_func = next.expr().operators(0).udf_func();
            if (next_udf_func.name() == "gs.function.second") {
              auto param = udf_func.parameters(0).operators(0);
              if (param.item_case() != common::ExprOpr::kVar) {
                LOG(ERROR) << "not support for " << m.expr().DebugString();
                return nullptr;
              }
              auto var = param.var();
              if (!var.has_tag()) {
                LOG(ERROR) << "project mapping should have tag";
                return nullptr;
              }
              if (var.has_property()) {
                LOG(ERROR) << "project mapping should not have property";
                return nullptr;
              }
              int next_tag = var.tag().id();
              int next_alias = next.alias().value();
              if (next_tag == tag) {
                exprs.emplace_back(
                    [tag, alias,
                     next_alias](const std::map<std::string, std::string>&) {
                      return std::make_unique<PairsGetter>(tag, alias,
                                                           next_alias);
                    });
                ++i;
                continue;
              }
            }
          }
        }
        exprs.emplace_back(
            [tag, alias](const std::map<std::string, std::string>&) {
              return std::make_unique<PairsFstGetter>(tag, alias);
            });

      } else if (udf_func.name() == "gs.function.second") {
        if (udf_func.parameters_size() != 1 ||
            udf_func.parameters(0).operators_size() != 1) {
          LOG(ERROR) << "not support for " << m.expr().DebugString();
          return nullptr;
        }
        auto param = udf_func.parameters(0).operators(0);

        if (param.item_case() != common::ExprOpr::kVar) {
          LOG(ERROR) << "not support for " << m.expr().DebugString();
          return nullptr;
        }
        auto var = param.var();
        if (!var.has_tag()) {
          LOG(ERROR) << "project mapping should have tag";
          return nullptr;
        }
        if (var.has_property()) {
          LOG(ERROR) << "project mapping should not have property";
          return nullptr;
        }
        int tag = var.tag().id();
        int alias = m.alias().value();
        exprs.emplace_back(
            [tag, alias](const std::map<std::string, std::string>&) {
              return std::make_unique<PairsSndGetter>(tag, alias);
            });
      } else {
        LOG(ERROR) << "not support for " << m.expr().DebugString();
        return nullptr;
      }
    } else {
      LOG(ERROR) << "not support for " << m.expr().DebugString();
      return nullptr;
    }
  }

  return std::make_unique<ProjectInsertOpr>(exprs);
}

class ProjectUpdateOpr : public IUpdateOperator {
 public:
  ProjectUpdateOpr(
      const std::vector<std::tuple<common::Expression, int,
                                   std::optional<common::IrDataType>>>&
          exprs_infos,
      const std::vector<std::pair<int, std::set<int>>>& dependencies,
      bool is_append)
      : exprs_infos_(exprs_infos),
        dependencies_(dependencies),
        is_append_(is_append) {}
  std::string get_operator_name() const override { return "ProjectUpdateOpr"; }

  gs::result<gs::runtime::Context> Eval(
      gs::runtime::GraphUpdateInterface& graph,
      const std::map<std::string, std::string>& params,
      gs::runtime::Context&& ctx, gs::runtime::OprTimer* timer) override {
    return ProjectEvalImpl(graph, params, std::move(ctx), exprs_infos_,
                           dependencies_, is_append_);
  }

 private:
  std::vector<
      std::tuple<common::Expression, int, std::optional<common::IrDataType>>>
      exprs_infos_;
  std::vector<std::pair<int, std::set<int>>> dependencies_;
  bool is_append_;
};

std::unique_ptr<IUpdateOperator> UProjectOprBuilder::Build(
    const Schema& schema, const physical::PhysicalPlan& plan, int op_idx) {
  std::vector<common::IrDataType> data_types;
  int mappings_size =
      plan.query_plan().plan(op_idx).opr().project().mappings_size();
  std::vector<
      std::tuple<common::Expression, int, std::optional<common::IrDataType>>>
      expr_infos;

  bool is_append = plan.query_plan().plan(op_idx).opr().project().is_append();

  std::vector<std::pair<int, std::set<int>>> dependencies;
  if (plan.query_plan().plan(op_idx).meta_data_size() == mappings_size) {
    for (int i = 0; i < plan.query_plan().plan(op_idx).meta_data_size(); ++i) {
      data_types.push_back(plan.query_plan().plan(op_idx).meta_data(i).type());
      const auto& m =
          plan.query_plan().plan(op_idx).opr().project().mappings(i);
      int alias = m.has_alias() ? m.alias().value() : -1;
      if (!m.has_expr()) {
        LOG(ERROR) << "expr is not set" << m.DebugString();
        return nullptr;
      }
      auto expr = m.expr();
      std::set<int> dependencies_set;
      parse_potential_dependencies(expr, dependencies_set);
      if (!dependencies_set.empty()) {
        dependencies.emplace_back(alias, dependencies_set);
      }
      expr_infos.emplace_back(expr, alias, data_types[i]);
    }
  } else {
    for (int i = 0; i < mappings_size; ++i) {
      auto& m = plan.query_plan().plan(op_idx).opr().project().mappings(i);

      int alias = m.has_alias() ? m.alias().value() : -1;

      if (!m.has_expr()) {
        LOG(ERROR) << "expr is not set" << m.DebugString();
        return nullptr;
      }
      auto expr = m.expr();
      std::set<int> dependencies_set;
      parse_potential_dependencies(expr, dependencies_set);
      if (!dependencies_set.empty()) {
        dependencies.emplace_back(alias, dependencies_set);
      }
      expr_infos.emplace_back(expr, alias, std::nullopt);
    }
  }

  return std::make_unique<ProjectUpdateOpr>(std::move(expr_infos), dependencies,
                                            is_append);
}

}  // namespace ops
}  // namespace runtime
}  // namespace gs