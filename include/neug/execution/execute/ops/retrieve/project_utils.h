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
#pragma once

#include "neug/execution/common/columns/value_columns.h"
#include "neug/execution/common/columns/vertex_columns.h"
#include "neug/execution/common/operators/retrieve/project.h"
#include "neug/execution/execute/ops/retrieve/order_by_utils.h"
#include "neug/execution/expression/expr.h"
#include "neug/execution/expression/special_predicates.h"
#include "neug/storages/graph/graph_interface.h"

namespace neug {
namespace execution {
namespace ops {

bool is_exchange_index(const common::Expression& expr, int& tag);

struct ProjectExprBuilderBase {
  virtual ~ProjectExprBuilderBase() = default;
  virtual std::unique_ptr<ProjectExprBase> build(const IStorageInterface& graph,
                                                 const ParamsMap& params) = 0;
  virtual bool is_general() const { return false; }
  virtual int alias() const = 0;
};

void create_project_expr_builders(
    std::vector<std::tuple<common::Expression, int,
                           std::unique_ptr<ExprBase>>>&& exprs_infos,
    std::vector<std::unique_ptr<ProjectExprBuilderBase>>& expr_builders,
    std::vector<std::unique_ptr<ProjectExprBuilderBase>>&
        fallback_expr_builders);

}  // namespace ops
}  // namespace execution
}  // namespace neug
