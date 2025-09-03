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

#include <map>
#include <memory>
#include <utility>
#include <vector>

#include "neug/execution/execute/operator.h"
#include "neug/execution/execute/pipeline.h"
#include "neug/utils/proto/plan/physical.pb.h"

namespace gs {
class Schema;

namespace runtime {
class ContextMeta;
class InsertPipeline;
class ReadPipeline;
class UpdatePipeline;

class PlanParser {
 private:
  PlanParser() { read_op_builders_.resize(64); }

 public:
  PlanParser(const PlanParser&) = delete;
  PlanParser(PlanParser&&) = delete;
  PlanParser& operator=(const PlanParser&) = delete;
  PlanParser& operator=(PlanParser&&) = delete;
  ~PlanParser() = default;

  void init();

  static PlanParser& get();

  void register_read_operator_builder(
      std::unique_ptr<IReadOperatorBuilder>&& builder);

  void register_write_operator_builder(
      std::unique_ptr<IInsertOperatorBuilder>&& builder);

  void register_update_operator_builder(
      std::unique_ptr<IUpdateOperatorBuilder>&& builder);

  gs::result<std::pair<ReadPipeline, ContextMeta>>
  parse_read_pipeline_with_meta(const gs::Schema& schema,
                                const ContextMeta& ctx_meta,
                                const physical::PhysicalPlan& plan);

  gs::result<ReadPipeline> parse_read_pipeline(
      const gs::Schema& schema, const ContextMeta& ctx_meta,
      const physical::PhysicalPlan& plan);

  gs::result<InsertPipeline> parse_write_pipeline(
      const gs::Schema& schema, const physical::PhysicalPlan& plan);

  gs::result<UpdatePipeline> parse_update_pipeline(
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

gs::result<runtime::Context> ParseAndExecuteReadPipeline(
    const GraphReadInterface& graph, const physical::PhysicalPlan& plan,
    OprTimer* timer);

gs::result<runtime::Context> ParseAndExecuteUpdatePipeline(
    GraphUpdateInterface& graph, const physical::PhysicalPlan& plan,
    OprTimer* timer);

}  // namespace runtime

}  // namespace gs

#endif  // RUNTIME_EXECUTE_PLAN_PARSER_H_