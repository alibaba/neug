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

#include "neug/execution/execute/operator.h"
#include "neug/storages/graph/graph_interface.h"

namespace neug {

namespace execution {
class Context;
class OprTimer;

class Pipeline {
 public:
  Pipeline() {}
  Pipeline(Pipeline&& rhs) : operators_(std::move(rhs.operators_)) {}
  explicit Pipeline(std::vector<std::unique_ptr<IOperator>>&& operators)
      : operators_(std::move(operators)) {}
  ~Pipeline() = default;

  neug::result<Context> Execute(IStorageInterface& graph, Context&& ctx,
                                const ParamsMap& params, OprTimer* timer);

 private:
  std::vector<std::unique_ptr<IOperator>> operators_;
};

}  // namespace execution

}  // namespace neug
