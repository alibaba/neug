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
#include <string>
#include <utility>
#include <vector>

#include "neug/execution/execute/operator.h"
#include "neug/storages/graph/graph_interface.h"

namespace gs {

namespace runtime {
class Context;
class OprTimer;

class AdminPipeline {
 public:
  AdminPipeline() {}
  AdminPipeline(AdminPipeline&& rhs) : operators_(std::move(rhs.operators_)) {}
  explicit AdminPipeline(std::vector<std::unique_ptr<IOperator>>&& operators)
      : operators_(std::move(operators)) {}
  ~AdminPipeline() = default;

  gs::result<Context> Execute(IStorageInterface& graph, Context&& ctx,
                              const std::map<std::string, std::string>& params,
                              OprTimer* timer);

 private:
  std::vector<std::unique_ptr<IOperator>> operators_;
};

class Pipeline {
 public:
  Pipeline() {}
  Pipeline(Pipeline&& rhs) : operators_(std::move(rhs.operators_)) {}
  explicit Pipeline(std::vector<std::unique_ptr<IOperator>>&& operators)
      : operators_(std::move(operators)) {}
  ~Pipeline() = default;

  gs::result<Context> Execute(IStorageInterface& graph, Context&& ctx,
                              const std::map<std::string, std::string>& params,
                              OprTimer* timer);

 private:
  std::vector<std::unique_ptr<IOperator>> operators_;
};

}  // namespace runtime

}  // namespace gs
