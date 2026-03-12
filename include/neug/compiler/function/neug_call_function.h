/**
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <vector>
#include "neug/compiler/function/function.h"
#include "neug/compiler/function/table/table_function.h"
#include "neug/generated/proto/plan/physical.pb.h"
#include "neug/storages/graph/graph_interface.h"

namespace neug {
class Schema;
class IStorageInterface;
namespace execution {
class ContextMeta;
class Context;
}  // namespace execution

namespace function {
struct CallFuncInputBase {
  virtual ~CallFuncInputBase() = default;
};

using call_bind_func_t = std::function<std::unique_ptr<CallFuncInputBase>(
    const Schema& schema, const execution::ContextMeta& ctx_meta,
    const ::physical::PhysicalPlan& plan, int op_idx)>;

using call_exec_func_t = std::function<execution::Context(
    const CallFuncInputBase& input, neug::IStorageInterface& graph)>;

using call_output_columns =
    std::vector<std::pair<std::string, common::LogicalTypeID>>;

struct NeugCallFunction : public TableFunction {
  call_output_columns outputColumns;
  call_bind_func_t bindFunc = nullptr;
  call_exec_func_t execFunc = nullptr;

  NeugCallFunction() = default;

  NeugCallFunction(std::string name,
                   std::vector<common::LogicalTypeID> inputTypes)
      : TableFunction{std::move(name), std::move(inputTypes)} {}
  NeugCallFunction(std::string name,
                   std::vector<common::LogicalTypeID> inputTypes,
                   call_output_columns outputColumns)
      : TableFunction{std::move(name), std::move(inputTypes)},
        outputColumns{std::move(outputColumns)} {}
};

}  // namespace function
}  // namespace neug