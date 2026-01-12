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

#include <glog/logging.h>
#include <google/protobuf/wrappers.pb.h>
#include <stddef.h>
#include <algorithm>
#include <cstdint>

#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <ostream>
#include <set>
#include <string>
#include <string_view>
#include <tuple>
#include <type_traits>
#include <typeindex>
#include <unordered_set>
#include <utility>

#include "neug/execution/common/columns/i_context_column.h"
#include "neug/execution/common/columns/value_columns.h"
#include "neug/execution/common/columns/vertex_columns.h"
#include "neug/execution/common/context.h"
#include "neug/execution/common/operators/retrieve/group_by.h"
#include "neug/execution/common/operators/retrieve/project.h"
#include "neug/execution/execute/ops/retrieve/group_by_utils.h"
#include "neug/execution/utils/var.h"
#include "neug/storages/graph/graph_interface.h"
#include "neug/utils/property/types.h"

namespace gs {
class Schema;

namespace runtime {
class OprTimer;

namespace ops {

class GroupByOpr : public IOperator {
 public:
  GroupByOpr(std::vector<common::Variable>&& vars,
             std::vector<std::pair<int, int>>&& mappings,
             std::vector<physical::GroupBy_AggFunc>&& aggrs,
             const std::vector<std::pair<int, int>>& dependencies)
      : vars_(std::move(vars)),
        mappings_(std::move(mappings)),
        aggrs_(std::move(aggrs)),
        dependencies_(dependencies) {}

  std::string get_operator_name() const override { return "GroupByOpr"; }

  gs::result<gs::runtime::Context> Eval(
      IStorageInterface& graph_interface,
      const std::map<std::string, std::string>& params,
      gs::runtime::Context&& ctx, gs::runtime::OprTimer* timer) override {
    const auto& graph =
        dynamic_cast<const StorageReadInterface&>(graph_interface);
    return GroupByEvalImpl(graph, params, std::move(ctx), vars_, mappings_,
                           aggrs_, dependencies_);
  }

 private:
  std::vector<common::Variable> vars_;
  std::vector<std::pair<int, int>> mappings_;
  std::vector<physical::GroupBy_AggFunc> aggrs_;
  std::vector<std::pair<int, int>> dependencies_;
};

gs::result<OpBuildResultT> GroupByOprBuilder::Build(
    const gs::Schema& schema, const ContextMeta& ctx_meta,
    const physical::PhysicalPlan& plan, int op_idx) {
  int mappings_num =
      plan.query_plan().plan(op_idx).opr().group_by().mappings_size();
  int func_num =
      plan.query_plan().plan(op_idx).opr().group_by().functions_size();
  ContextMeta meta;
  for (int i = 0; i < mappings_num; ++i) {
    auto& key = plan.query_plan().plan(op_idx).opr().group_by().mappings(i);
    if (key.has_alias()) {
      meta.set(key.alias().value());
    } else {
      meta.set(-1);
    }
  }
  for (int i = 0; i < func_num; ++i) {
    auto& func = plan.query_plan().plan(op_idx).opr().group_by().functions(i);
    if (func.has_alias()) {
      meta.set(func.alias().value());
    } else {
      meta.set(-1);
    }
  }

  auto opr = plan.query_plan().plan(op_idx).opr().group_by();
  std::vector<std::pair<int, int>> mappings;
  std::vector<common::Variable> vars;
  std::vector<physical::GroupBy_AggFunc> reduce_funcs;
  std::vector<std::pair<int, int>> dependencies;

  if (!BuildGroupByUtils(opr, vars, mappings, reduce_funcs, dependencies)) {
    return std::make_pair(nullptr, ContextMeta());
  }

  return std::make_pair(
      std::make_unique<GroupByOpr>(std::move(vars), std::move(mappings),

                                   std::move(reduce_funcs), dependencies),

      meta);
}

}  // namespace ops
}  // namespace runtime
}  // namespace gs