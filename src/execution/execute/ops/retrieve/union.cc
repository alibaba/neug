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

#include "neug/execution/execute/ops/retrieve/union.h"

#include "neug/execution/common/context.h"
#include "neug/execution/common/operators/retrieve/union.h"
#include "neug/execution/execute/pipeline.h"
#include "neug/execution/execute/plan_parser.h"
#include "neug/storages/graph/graph_interface.h"
#include "neug/utils/likely.h"

namespace neug {
class Schema;

namespace execution {
class OprTimer;

namespace ops {
class UnionOpr : public IOperator {
 public:
  explicit UnionOpr(std::vector<Pipeline>&& sub_plans)
      : sub_plans_(std::move(sub_plans)) {}

  std::string get_operator_name() const override { return "UnionOpr"; }

  Stream<ContextChunk> Eval(IStorageInterface& graph, const ParamsMap& params,
                            Stream<ContextChunk>&& input,
                            neug::execution::OprTimer* timer) override {
    struct State {
      Stream<ContextChunk> input;
      std::optional<std::vector<ContextChunk>> seed;
      Stream<ContextChunk> branch;
      size_t index = 0;
    };
    auto metadata = input.metadata();
    auto state = std::make_shared<State>();
    state->input = std::move(input);
    return Stream<ContextChunk>(
        [this, &graph, params, timer, metadata,
         state]() mutable -> Stream<ContextChunk>::NextResult {
          if (!state->seed) {
            GS_AUTO(seed, collect_batches(std::move(state->input)));
            state->seed = std::move(seed);
          }
          while (true) {
            GS_AUTO(next, state->branch.Next());
            if (next) {
              // UNION has no anonymous output head, matching the union kernel.
              next->head().reset();
              return next;
            }
            if (state->index == sub_plans_.size()) {
              return std::optional<ContextChunk>{};
            }
            auto sub_timer = timer ? std::make_unique<OprTimer>() : nullptr;
            auto* child = sub_timer.get();
            if (timer) {
              timer->add_child(std::move(sub_timer));
            }
            auto branch = sub_plans_[state->index++].ExecuteStream(
                graph, stream_from_batches(*state->seed, metadata), params,
                child);
            state->branch = std::move(branch);
          }
        });
  }

  void build_explain_children(OprTimer* parent_timer, const ParamsMap& params,
                              IStorageInterface& graph) override {
    // Build explain tree for each sub plan
    // and add them as children to the parent timer
    for (auto& plan : sub_plans_) {
      auto tree_result = plan.explain_tree(graph, params);
      if (tree_result && tree_result.value()) {
        parent_timer->add_child(std::move(tree_result.value()));
      }
    }
  }

 private:
  std::vector<Pipeline> sub_plans_;
};
neug::result<OpBuildResultT> UnionOprBuilder::Build(
    const neug::Schema& schema, const ContextMeta& ctx_meta,
    const physical::PhysicalPlan& plan, int op_idx) {
  const auto& union_op = plan.plan(op_idx).opr().union_();
  if (union_op.sub_plans_size() != 2) {
    RETURN_UNSUPPORTED_ERROR(
        "Union: exactly two sub-plans are supported, got " +
        std::to_string(union_op.sub_plans_size()));
  }

  std::vector<Pipeline> sub_plans;
  std::vector<ContextMeta> sub_metas;
  for (int i = 0; i < union_op.sub_plans_size(); ++i) {
    const auto& sub_plan = union_op.sub_plans(i);
    auto pair_res = PlanParser::get().parse_execute_pipeline_with_meta(
        schema, ctx_meta, sub_plan);
    if (!pair_res) {
      RETURN_ERROR(pair_res.error());
    }
    auto pair = std::move(pair_res.value());
    sub_plans.emplace_back(std::move(pair.first));
    sub_metas.push_back(pair.second);
  }

  ContextMeta ret_meta = ctx_meta;
  if (!sub_metas.empty()) {
    ret_meta = sub_metas.front();
    const auto& expected_columns = ret_meta.columns();
    for (size_t i = 1; i < sub_metas.size(); ++i) {
      const auto& actual_columns = sub_metas[i].columns();
      bool aliases_match = actual_columns.size() == expected_columns.size();
      if (aliases_match) {
        for (const auto& column : expected_columns) {
          if (actual_columns.find(column.first) == actual_columns.end()) {
            aliases_match = false;
            break;
          }
        }
      }
      if (!aliases_match) {
        RETURN_STATUS_ERROR(neug::StatusCode::ERR_SCHEMA_MISMATCH,
                            "Union: output aliases of branch " +
                                std::to_string(i) + " do not match branch 0");
      }
    }
  }

  return std::make_pair(std::make_unique<UnionOpr>(std::move(sub_plans)),
                        ret_meta);
}
}  // namespace ops
}  // namespace execution
}  // namespace neug
