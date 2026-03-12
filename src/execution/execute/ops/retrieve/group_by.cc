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

#include "neug/execution/common/context.h"
#include "neug/execution/common/operators/retrieve/group_by.h"
#include "neug/execution/common/operators/retrieve/project.h"
#include "neug/execution/execute/ops/retrieve/group_by_utils.h"
#include "neug/storages/graph/graph_interface.h"
#include "neug/utils/property/types.h"

namespace neug {

namespace execution {
class OprTimer;

namespace ops {

class GroupByOpr : public IOperator {
 public:
  GroupByOpr(std::vector<std::pair<int, int>>&& mappings,
             std::vector<physical::GroupBy_AggFunc>&& aggrs)
      : mappings_(std::move(mappings)), aggrs_(std::move(aggrs)) {}

  std::string get_operator_name() const override { return "GroupByOpr"; }

  neug::result<neug::execution::Context> Eval(
      IStorageInterface& graph, const ParamsMap& params,
      neug::execution::Context&& ctx,
      neug::execution::OprTimer* timer) override {
    auto key = create_key_func(mappings_, graph, ctx);
    std::vector<ReduceOp> reducers;
    for (auto& aggr : aggrs_) {
      reducers.push_back(create_reduce_op(aggr, graph, ctx));
    }
    auto ret =
        GroupBy::group_by(std::move(ctx), std::move(key), std::move(reducers));
    if (!ret) {
      return ret;
    }

    return ret;
  }

 private:
  std::vector<std::pair<int, int>> mappings_;
  std::vector<physical::GroupBy_AggFunc> aggrs_;
};

neug::result<OpBuildResultT> GroupByOprBuilder::Build(
    const neug::Schema& schema, const ContextMeta& ctx_meta,
    const physical::PhysicalPlan& plan, int op_idx) {
  int mappings_num = plan.plan(op_idx).opr().group_by().mappings_size();
  int func_num = plan.plan(op_idx).opr().group_by().functions_size();
  ContextMeta meta;
  int metadata_num = plan.plan(op_idx).meta_data_size();
  if (func_num + mappings_num == metadata_num) {
    for (int i = 0; i < metadata_num; ++i) {
      meta.set(plan.plan(op_idx).meta_data(i).alias(),
               parse_from_ir_data_type(plan.plan(op_idx).meta_data(i).type()));
    }
  } else {
    LOG(FATAL) << "GroupBy metadata number mismatch.";
  }
  auto opr = plan.plan(op_idx).opr().group_by();
  std::vector<std::pair<int, int>> mappings;
  std::vector<physical::GroupBy_AggFunc> reduce_funcs;

  if (!BuildGroupByUtils(opr, mappings, reduce_funcs)) {
    return std::make_pair(nullptr, ContextMeta());
  }

  return std::make_pair(std::make_unique<GroupByOpr>(std::move(mappings),

                                                     std::move(reduce_funcs)),

                        meta);
}

}  // namespace ops
}  // namespace execution
}  // namespace neug