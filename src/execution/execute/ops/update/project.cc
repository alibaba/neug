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
#include "neug/execution/execute/ops/utils/project_utils.h"
#include "neug/execution/utils/var.h"
#include "neug/utils/result.h"

namespace gs {
class Schema;

namespace runtime {
class IContextColumn;
class OprTimer;

namespace ops {
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
