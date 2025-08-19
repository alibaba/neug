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

#ifndef RUNTIME_EXECUTE_PLAN_PARSER_H_
#define RUNTIME_EXECUTE_PLAN_PARSER_H_

#include <boost/leaf.hpp>
#include <map>
#include <memory>
#include <utility>
#include <vector>

#include "neug/engines/graph_db/runtime/execute/operator.h"
#include "neug/engines/graph_db/runtime/execute/pipeline.h"
#include "neug/utils/proto/plan/physical.pb.h"

namespace gs {
class Schema;

namespace runtime {
class ContextMeta;
class InsertPipeline;
class ReadPipeline;
class UpdatePipeline;

class PlanParser {
 public:
  PlanParser() { read_op_builders_.resize(64); }
  ~PlanParser() = default;

  void init();

  static PlanParser& get();

  void register_read_operator_builder(
      std::unique_ptr<IReadOperatorBuilder>&& builder);

  void register_write_operator_builder(
      std::unique_ptr<IInsertOperatorBuilder>&& builder);

  void register_update_operator_builder(
      std::unique_ptr<IUpdateOperatorBuilder>&& builder);

  bl::result<std::pair<ReadPipeline, ContextMeta>>
  parse_read_pipeline_with_meta(const gs::Schema& schema,
                                const ContextMeta& ctx_meta,
                                const physical::PhysicalPlan& plan);

  bl::result<ReadPipeline> parse_read_pipeline(
      const gs::Schema& schema, const ContextMeta& ctx_meta,
      const physical::PhysicalPlan& plan);

  bl::result<InsertPipeline> parse_write_pipeline(
      const gs::Schema& schema, const physical::PhysicalPlan& plan);

  bl::result<UpdatePipeline> parse_update_pipeline(
      const gs::Schema& schema, const physical::PhysicalPlan& plan);

 private:
  std::vector<std::vector<
      std::pair<std::vector<physical::PhysicalOpr_Operator::OpKindCase>,
                std::unique_ptr<IReadOperatorBuilder>>>>
      read_op_builders_;

  std::map<physical::PhysicalOpr_Operator::OpKindCase,
           std::unique_ptr<IInsertOperatorBuilder>>
      write_op_builders_;

  std::map<physical::PhysicalOpr_Operator::OpKindCase,
           std::unique_ptr<IUpdateOperatorBuilder>>
      update_op_builders_;
};

std::pair<runtime::Context, Status> ParseAndExecuteReadPipeline(
    const GraphReadInterface& graph, const physical::PhysicalPlan& plan,
    OprTimer& timer);

std::pair<runtime::Context, Status> ParseAndExecuteUpdatePipeline(
    GraphUpdateInterface& graph, const physical::PhysicalPlan& plan,
    OprTimer& timer);

}  // namespace runtime

}  // namespace gs

#endif  // RUNTIME_EXECUTE_PLAN_PARSER_H_