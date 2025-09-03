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

#include "neug/execution/runtime/execute/ops/retrieve/join.h"

#include <glog/logging.h>
#include <algorithm>
#include <map>
#include <memory>
#include <ostream>
#include <set>
#include <string>
#include <type_traits>
#include <utility>

#include "neug/execution/runtime/common/context.h"
#include "neug/execution/runtime/common/graph_interface.h"
#include "neug/execution/runtime/common/operators/retrieve/join.h"
#include "neug/execution/runtime/common/types.h"
#include "neug/execution/runtime/execute/ops/update/join.h"
#include "neug/execution/runtime/execute/pipeline.h"
#include "neug/execution/runtime/execute/plan_parser.h"

namespace gs {
class Schema;

namespace runtime {
class OprTimer;

namespace ops {

class JoinUpdateOpr : public IUpdateOperator {
 public:
  JoinUpdateOpr(gs::runtime::UpdatePipeline&& left_pipeline,
                gs::runtime::UpdatePipeline&& right_pipeline,
                const JoinParams& join_params)
      : left_pipeline_(std::move(left_pipeline)),
        right_pipeline_(std::move(right_pipeline)),
        params_(join_params) {}

  std::string get_operator_name() const override { return "JoinOpr"; }

  virtual gs::result<Context> Eval(
      gs::runtime::GraphUpdateInterface& graph,
      const std::map<std::string, std::string>& params, Context&& ctx,
      OprTimer* timer) override {
    gs::runtime::Context ret_dup(ctx);
    std::unique_ptr<OprTimer> left_timer =
        (timer == nullptr ? nullptr : std::make_unique<OprTimer>());
    auto left_ctx =
        left_pipeline_.Execute(graph, std::move(ctx), params, left_timer.get());
    if (!left_ctx) {
      return left_ctx;
    }
    std::unique_ptr<OprTimer> right_timer =
        (timer == nullptr ? nullptr : std::make_unique<OprTimer>());
    auto right_ctx = right_pipeline_.Execute(graph, std::move(ret_dup), params,
                                             right_timer.get());
    if (!right_ctx) {
      return right_ctx;
    }
    if (timer != nullptr) {
      timer->add_child(std::move(left_timer));
      timer->add_child(std::move(right_timer));
    }
    return Join::join(std::move(left_ctx.value()), std::move(right_ctx.value()),
                      params_);
  }

 private:
  gs::runtime::UpdatePipeline left_pipeline_;
  gs::runtime::UpdatePipeline right_pipeline_;

  JoinParams params_;
};

std::unique_ptr<IUpdateOperator> UJoinUpdateOprBuilder::Build(
    const Schema& schema, const physical::PhysicalPlan& plan, int op_idx) {
  std::vector<int> right_columns;
  auto& opr = plan.query_plan().plan(op_idx).opr().join();
  JoinParams p;
  if (opr.left_keys().size() != opr.right_keys().size()) {
    LOG(ERROR) << "join keys size mismatch";
    THROW_INVALID_ARGUMENT_EXCEPTION(
        "join keys size mismatch: left_keys size = " +
        std::to_string(opr.left_keys().size()) +
        ", right_keys size = " + std::to_string(opr.right_keys().size()));
  }
  auto left_keys = opr.left_keys();

  for (int i = 0; i < left_keys.size(); i++) {
    if (!left_keys.Get(i).has_tag()) {
      LOG(ERROR) << "left_keys should have tag";
      THROW_INVALID_ARGUMENT_EXCEPTION("left_keys should have tag at index " +
                                       std::to_string(i));
    }
    p.left_columns.push_back(left_keys.Get(i).tag().id());
  }
  auto right_keys = opr.right_keys();
  for (int i = 0; i < right_keys.size(); i++) {
    if (!right_keys.Get(i).has_tag()) {
      LOG(ERROR) << "right_keys should have tag";
      THROW_INVALID_ARGUMENT_EXCEPTION("right_keys should have tag at index " +
                                       std::to_string(i));
    }
    p.right_columns.push_back(right_keys.Get(i).tag().id());
  }
  auto join_kind = plan.query_plan().plan(op_idx).opr().join().join_kind();

  switch (join_kind) {
  case physical::Join_JoinKind::Join_JoinKind_INNER:
    p.join_type = JoinKind::kInnerJoin;
    break;
  case physical::Join_JoinKind::Join_JoinKind_SEMI:
    p.join_type = JoinKind::kSemiJoin;
    break;
  case physical::Join_JoinKind::Join_JoinKind_ANTI:
    p.join_type = JoinKind::kAntiJoin;
    break;
  case physical::Join_JoinKind::Join_JoinKind_LEFT_OUTER:
    p.join_type = JoinKind::kLeftOuterJoin;
    break;
  case physical::Join_JoinKind::Join_JoinKind_TIMES:
    p.join_type = JoinKind::kTimes;
    break;
  default:
    LOG(ERROR) << "unsupported join kind" << opr.join_kind();
    THROW_INVALID_ARGUMENT_EXCEPTION("unsupported join kind: " +
                                     std::to_string(opr.join_kind()));
  }

  auto pair1_res = PlanParser::get().parse_update_pipeline(
      schema, plan.query_plan().plan(op_idx).opr().join().left_plan());
  if (!pair1_res) {
    THROW_INVALID_ARGUMENT_EXCEPTION("failed to parse left plan for join: ");
  }
  auto pair2_res = PlanParser::get().parse_update_pipeline(
      schema, plan.query_plan().plan(op_idx).opr().join().right_plan());
  if (!pair2_res) {
    THROW_INVALID_ARGUMENT_EXCEPTION("failed to parse right plan for join: ");
  }
  return std::make_unique<JoinUpdateOpr>(std::move(pair1_res.value()),
                                         std::move(pair2_res.value()), p);
}

}  // namespace ops
}  // namespace runtime
}  // namespace gs