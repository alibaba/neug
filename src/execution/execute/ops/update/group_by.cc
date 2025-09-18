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

#include "neug/execution/execute/ops/retrieve/group_by.h"
#include "neug/execution/common/types.h"
#include "neug/execution/execute/ops/update/group_by.h"
#include "neug/execution/execute/ops/utils/group_by_utils.h"
namespace gs {
namespace runtime {
namespace ops {

class UGroupByOpr : public IUpdateOperator {
 public:
  UGroupByOpr(std::vector<common::Variable>&& vars,
              std::vector<std::pair<int, int>>&& mappings,
              std::vector<physical::GroupBy_AggFunc>&& aggrs,

              const std::vector<std::pair<int, int>>& dependencies)
      : vars_(std::move(vars)),
        mappings_(std::move(mappings)),
        aggrs_(std::move(aggrs)),
        dependencies_(dependencies) {}

  std::string get_operator_name() const override { return "UGroupByOpr"; }

  gs::result<gs::runtime::Context> Eval(
      gs::runtime::GraphUpdateInterface& graph,
      const std::map<std::string, std::string>& params,
      gs::runtime::Context&& ctx, gs::runtime::OprTimer* timer) override {
    return GroupByEvalImpl(graph, params, std::move(ctx), vars_, mappings_,
                           aggrs_, dependencies_);
  }

 private:
  std::vector<common::Variable> vars_;
  std::vector<std::pair<int, int>> mappings_;
  std::vector<physical::GroupBy_AggFunc> aggrs_;
  std::vector<std::pair<int, int>> dependencies_;
};

class UGroupByOprBeta : public IUpdateOperator {
 public:
  UGroupByOprBeta(
      std::vector<std::pair<common::Variable, int>>&& project_var_alias,
      std::vector<common::Variable>&& vars,
      std::vector<std::pair<int, int>>&& mappings,
      std::vector<physical::GroupBy_AggFunc>&& aggrs,
      std::vector<std::pair<int, int>> dependencies)
      : project_var_alias_(std::move(project_var_alias)),
        vars_(std::move(vars)),
        mappings_(std::move(mappings)),
        aggrs_(std::move(aggrs)),
        dependencies_(dependencies) {}

  std::string get_operator_name() const override { return "UGroupByOprBeta"; }

  gs::result<gs::runtime::Context> Eval(
      gs::runtime::GraphUpdateInterface& graph,
      const std::map<std::string, std::string>& params,
      gs::runtime::Context&& ctx, gs::runtime::OprTimer* timer) override {
    return GroupByBetaEvalImpl(graph, params, std::move(ctx),
                               project_var_alias_, vars_, mappings_, aggrs_,
                               dependencies_);
  }

 private:
  std::vector<std::pair<common::Variable, int>> project_var_alias_;
  std::vector<common::Variable> vars_;
  std::vector<std::pair<int, int>> mappings_;
  std::vector<physical::GroupBy_AggFunc> aggrs_;
  std::vector<std::pair<int, int>> dependencies_;
};

std::unique_ptr<IUpdateOperator> UGroupByOprBuilder::Build(
    const Schema& schema, const physical::PhysicalPlan& plan, int op_idx) {
  auto opr = plan.query_plan().plan(op_idx).opr().group_by();
  std::vector<std::pair<int, int>> mappings;
  std::vector<common::Variable> vars;
  std::vector<std::pair<common::Variable, int>> project_var_alias;
  std::vector<physical::GroupBy_AggFunc> reduce_funcs;
  std::vector<std::pair<int, int>> dependencies;
  if (!BuildGroupByUtils(opr, project_var_alias, vars, mappings, reduce_funcs,
                         dependencies)) {
    return nullptr;
  }
  if (project_var_alias.empty()) {
    return std::make_unique<UGroupByOpr>(std::move(vars), std::move(mappings),
                                         std::move(reduce_funcs),
                                         std::move(dependencies));
  } else {
    return std::make_unique<UGroupByOprBeta>(
        std::move(project_var_alias), std::move(vars), std::move(mappings),
        std::move(reduce_funcs), std::move(dependencies));
  }
}
}  // namespace ops
}  // namespace runtime
}  // namespace gs
