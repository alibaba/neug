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

#include "neug/execution/execute/ops/update/sink.h"

#include <google/protobuf/wrappers.pb.h>

#include <map>
#include <string>
#include <vector>

#include "neug/execution/common/context.h"

namespace gs {
class Schema;

namespace runtime {
class GraphInsertInterface;
class GraphUpdateInterface;
class OprTimer;

namespace ops {

class SinkInsertOpr : public IInsertOperator {
 public:
  SinkInsertOpr() {}

  std::string get_operator_name() const override { return "SinkInsertOpr"; }

  template <typename GraphInterface>
  gs::result<gs::runtime::WriteContext> eval_impl(
      GraphInterface& graph, const std::map<std::string, std::string>& params,
      gs::runtime::WriteContext&& ctx, gs::runtime::OprTimer* timer) {
    return ctx;
  }

  gs::result<gs::runtime::WriteContext> Eval(
      gs::runtime::GraphInsertInterface& graph,
      const std::map<std::string, std::string>& params,
      gs::runtime::WriteContext&& ctx, gs::runtime::OprTimer* timer) override {
    return eval_impl(graph, params, std::move(ctx), timer);
  }

  gs::result<gs::runtime::WriteContext> Eval(
      gs::runtime::GraphUpdateInterface& graph,
      const std::map<std::string, std::string>& params,
      gs::runtime::WriteContext&& ctx, gs::runtime::OprTimer* timer) override {
    return eval_impl(graph, params, std::move(ctx), timer);
  }
};

std::unique_ptr<IInsertOperator> SinkInsertOprBuilder::Build(
    const Schema& schema, const physical::PhysicalPlan& plan, int op_idx) {
  return std::make_unique<SinkInsertOpr>();
}

class USinkOpr : public IUpdateOperator {
 public:
  explicit USinkOpr(const std::vector<int>& tag_ids) : tag_ids(tag_ids) {}

  std::string get_operator_name() const override { return "USinkOpr"; }

  gs::result<Context> Eval(GraphUpdateInterface& graph,
                           const std::map<std::string, std::string>& params,
                           Context&& ctx, OprTimer* timer) override {
    ctx.tag_ids = tag_ids;
    return ctx;
  }
  std::vector<int> tag_ids;
};

std::unique_ptr<IUpdateOperator> USinkOprBuilder::Build(
    const Schema& schema, const physical::PhysicalPlan& plan, int op_idx) {
  auto& opr = plan.query_plan().plan(op_idx).opr().sink();
  std::vector<int> tag_ids;
  for (auto& tag : opr.tags()) {
    tag_ids.push_back(tag.tag().value());
  }
  if (tag_ids.empty() && op_idx) {
    while (op_idx - 1 &&
           (!plan.query_plan().plan(op_idx - 1).opr().has_project()) &&
           (!plan.query_plan().plan(op_idx - 1).opr().has_group_by())) {
      op_idx--;
    }
    auto prev_opr = plan.query_plan().plan(op_idx - 1).opr();
    if (prev_opr.has_project()) {
      int mapping_size = prev_opr.project().mappings_size();
      for (int i = 0; i < mapping_size; ++i) {
        tag_ids.emplace_back(prev_opr.project().mappings(i).alias().value());
      }
    } else if (prev_opr.has_group_by()) {
      int mapping_size = prev_opr.group_by().mappings_size();
      for (int i = 0; i < mapping_size; ++i) {
        tag_ids.emplace_back(prev_opr.group_by().mappings(i).alias().value());
      }
      int function_size = prev_opr.group_by().functions_size();
      for (int i = 0; i < function_size; ++i) {
        tag_ids.emplace_back(prev_opr.group_by().functions(i).alias().value());
      }
    }
  }
  return std::make_unique<USinkOpr>(tag_ids);
}

}  // namespace ops
}  // namespace runtime
}  // namespace gs
