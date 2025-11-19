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

#ifndef INCLUDE_NEUG_EXECUTION_EXECUTE_PIPELINE_H_
#define INCLUDE_NEUG_EXECUTION_EXECUTE_PIPELINE_H_

#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "neug/execution/common/graph_interface.h"
#include "neug/execution/execute/operator.h"

namespace gs {

namespace runtime {
class Context;
class OprTimer;

class AdminPipeline {
 public:
  AdminPipeline() {}
  AdminPipeline(AdminPipeline&& rhs) : operators_(std::move(rhs.operators_)) {}
  explicit AdminPipeline(
      std::vector<std::unique_ptr<IAdminOperator>>&& operators)
      : operators_(std::move(operators)) {}
  ~AdminPipeline() = default;

  gs::result<Context> Execute(GraphUpdateInterface& graph, Context&& ctx,
                              const std::map<std::string, std::string>& params,
                              OprTimer* timer);

 private:
  std::vector<std::unique_ptr<IAdminOperator>> operators_;
};

class ReadPipeline {
 public:
  ReadPipeline() {}
  ReadPipeline(ReadPipeline&& rhs) : operators_(std::move(rhs.operators_)) {}
  explicit ReadPipeline(std::vector<std::unique_ptr<IReadOperator>>&& operators)
      : operators_(std::move(operators)) {}
  ~ReadPipeline() = default;

  gs::result<Context> Execute(const GraphReadInterface& graph, Context&& ctx,
                              const std::map<std::string, std::string>& params,
                              OprTimer* timer);

 private:
  std::vector<std::unique_ptr<IReadOperator>> operators_;
};

class InsertPipeline {
 public:
  InsertPipeline() = default;
  InsertPipeline(InsertPipeline&& rhs)
      : operators_(std::move(rhs.operators_)) {}
  explicit InsertPipeline(
      std::vector<std::unique_ptr<IInsertOperator>>&& operators)
      : operators_(std::move(operators)) {}
  ~InsertPipeline() = default;

  gs::result<Context> Execute(GraphInsertInterface& graph, Context&& ctx,
                              const std::map<std::string, std::string>& params,
                              OprTimer* timer);

 private:
  std::vector<std::unique_ptr<IInsertOperator>> operators_;
};

class UpdatePipeline {
 public:
  UpdatePipeline(UpdatePipeline&& rhs)
      : operators_(std::move(rhs.operators_)) {}
  explicit UpdatePipeline(
      std::vector<std::unique_ptr<IUpdateOperator>>&& operators)
      : operators_(std::move(operators)) {}
  ~UpdatePipeline() = default;

  gs::result<Context> Execute(GraphUpdateInterface& graph, Context&& ctx,
                              const std::map<std::string, std::string>& params,
                              OprTimer* timer);

 private:
  std::vector<std::unique_ptr<IUpdateOperator>> operators_;
};

}  // namespace runtime

}  // namespace gs

#endif  // INCLUDE_NEUG_EXECUTION_EXECUTE_PIPELINE_H_
