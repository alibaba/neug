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

#include "neug/compiler/function/gds/gds_algo_function.h"
#include "neug/compiler/function/neug_call_function.h"

namespace neug {
namespace gds {
struct NEUG_API BFSFunction {
  static constexpr const char* name = "bfs";
  static neug::execution::Context exec(const function::CallFuncInputBase& input,
                                       neug::IStorageInterface& graph);

  static std::unique_ptr<function::CallFuncInputBase> bind(
      const Schema& schema, const execution::ContextMeta& ctx_meta,
      const ::physical::PhysicalPlan& plan, int op_idx);

  static function::function_set getFunctionSet();
};
}  // namespace gds
}  // namespace neug