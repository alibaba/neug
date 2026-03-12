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

#include <memory>
#include <utility>
#include <vector>

#include "neug/execution/execute/operator.h"
#include "neug/execution/execute/pipeline.h"

namespace neug {
class Schema;

namespace execution {
class ContextMeta;
class Pipeline;

class PlanParser {
 private:
  PlanParser() { op_builders_.resize(128); }

 public:
  PlanParser(const PlanParser&) = delete;
  PlanParser(PlanParser&&) = delete;
  PlanParser& operator=(const PlanParser&) = delete;
  PlanParser& operator=(PlanParser&&) = delete;
  ~PlanParser() = default;

  void init();

  static PlanParser& get();

  void register_operator_builder(std::unique_ptr<IOperatorBuilder>&& builder);

  neug::result<std::pair<Pipeline, ContextMeta>>
  parse_execute_pipeline_with_meta(const neug::Schema& schema,
                                   const ContextMeta& ctx_meta,
                                   const physical::PhysicalPlan& plan);

  neug::result<std::pair<Pipeline, ContextMeta>> parse_execute_pipeline(
      const neug::Schema& schema, const ContextMeta& ctx_meta,
      const physical::PhysicalPlan& plan);

  static ParamsMetaMap parse_params_type(const physical::PhysicalPlan& plan);

 private:
  std::vector<std::vector<
      std::pair<std::vector<physical::PhysicalOpr_Operator::OpKindCase>,
                std::unique_ptr<IOperatorBuilder>>>>
      op_builders_;
};

}  // namespace execution

}  // namespace neug
