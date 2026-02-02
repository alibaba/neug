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

namespace neug {
class Schema;
class IStorageInterface;  // Forward declaration for graph interface

namespace runtime {
class ContextMeta;
class Context;
}  // namespace runtime

namespace function {
struct CallFuncInputBase {
  virtual ~CallFuncInputBase() = default;
};

using call_bind_func_t = std::function<std::unique_ptr<CallFuncInputBase>(
    const Schema& schema, const runtime::ContextMeta& ctx_meta,
    const ::physical::PhysicalPlan& plan, int op_idx)>;

// Extended exec function type that receives graph interface
using call_exec_func_t =
    std::function<runtime::Context(const CallFuncInputBase& input)>;

// New exec function type with graph access for extensions
using call_exec_with_graph_func_t =
    std::function<runtime::Context(IStorageInterface& graph, const CallFuncInputBase& input)>;

using call_output_columns =
    std::vector<std::pair<std::string, common::LogicalTypeID>>;

struct NeugCallFunction : public TableFunction {
  call_output_columns outputColumns;
  call_bind_func_t bindFunc = nullptr;
  call_exec_func_t execFunc = nullptr;
  call_exec_with_graph_func_t execWithGraphFunc = nullptr;  // New: exec with graph access

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