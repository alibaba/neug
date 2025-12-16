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

#include <map>
#include <memory>
#include <utility>
#include <vector>

#include "neug/execution/execute/operator.h"
#include "neug/execution/execute/pipeline.h"
#include "neug/generated/proto/plan/physical.pb.h"

namespace gs {
class Schema;

namespace runtime {
class ContextMeta;
class Pipeline;

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

  void register_operator_builder(std::unique_ptr<IOperatorBuilder>&& builder);

  void register_schema_operator_builder(
      std::unique_ptr<ISchemaOperatorBuilder>&& builder);

  void register_admin_operator_builder(
      std::unique_ptr<IAdminOperatorBuilder>&& builder);

  gs::result<std::pair<Pipeline, ContextMeta>> parse_execute_pipeline_with_meta(
      const gs::Schema& schema, const ContextMeta& ctx_meta,
      const physical::PhysicalPlan& plan);

  gs::result<Pipeline> parse_execute_pipeline(
      const gs::Schema& schema, const ContextMeta& ctx_meta,
      const physical::PhysicalPlan& plan);

  gs::result<Pipeline> parse_admin_pipeline(
      const gs::Schema& schema, const physical::AdminPlan& admin_plan);

  gs::result<Pipeline> parse_ddl_pipeline(const gs::Schema& schema,
                                          const physical::DDLPlan& ddl_plan);

 private:
  std::vector<std::vector<
      std::pair<std::vector<physical::PhysicalOpr_Operator::OpKindCase>,
                std::unique_ptr<IOperatorBuilder>>>>
      read_op_builders_;

  std::map<physical::AdminPlan_Operator::KindCase,
           std::unique_ptr<IAdminOperatorBuilder>>
      admin_op_builders_;

  std::map<physical::DDLPlan::PlanCase, std::unique_ptr<ISchemaOperatorBuilder>>
      schema_op_builders_;
};

gs::result<runtime::Context> ParseAndExecuteQueryPipeline(
    IStorageInterface& graph, const physical::PhysicalPlan& plan,
    OprTimer* timer);

gs::result<runtime::Context> ParseAndExecuteAdminPipeline(
    StorageUpdateInterface& graph, const physical::AdminPlan& admin_plan,
    OprTimer* timer);

gs::result<runtime::Context> ParseAndExecuteDDLPipeline(
    StorageUpdateInterface& graph, const physical::DDLPlan& ddl_plan,
    OprTimer* timer);

}  // namespace runtime

}  // namespace gs
