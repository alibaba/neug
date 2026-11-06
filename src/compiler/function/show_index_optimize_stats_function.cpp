/** Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "neug/compiler/function/show_index_optimize_stats_function.h"

#include <array>
#include "neug/common/columns/value_columns.h"
#include "neug/execution/common/context.h"
#include "neug/utils/exception/exception.h"

namespace neug::function {
namespace {
struct OptimizeStatsInput : CallFuncInputBase {
  std::array<std::string, 2> values;
  std::array<std::string, 2> parameters;

  std::unique_ptr<CallFuncInputBase> bindParams(
      const execution::ParamsMap& params) const override {
    auto bound = std::make_unique<OptimizeStatsInput>(*this);
    for (size_t i = 0; i < parameters.size(); ++i) {
      if (parameters[i].empty())
        continue;
      const auto it = params.find(parameters[i]);
      if (it == params.end() || it->second.IsNull() ||
          it->second.type().id() != DataTypeId::kVarchar)
        THROW_INVALID_ARGUMENT_EXCEPTION(
            "SHOW_INDEX_OPTIMIZE_STATS requires string parameters");
      bound->values[i] = it->second.GetValue<std::string>();
      if (bound->values[i].empty())
        THROW_INVALID_ARGUMENT_EXCEPTION(
            "SHOW_INDEX_OPTIMIZE_STATS requires nonempty parameters");
    }
    return bound;
  }
};
}  // namespace

function_set ShowIndexOptimizeStatsFunction::getFunctionSet() {
  const auto varchar = common::DataType(common::DataTypeId::kVarchar);
  const auto uint64 = common::DataType(common::DataTypeId::kUInt64);
  function_set functions;
  for (size_t arity = 0; arity <= 2; ++arity) {
    auto function = std::make_unique<NeugCallFunction>(
        name, call_input_types(arity, varchar),
        call_output_columns{{"type", varchar},
                            {"index_name", varchar},
                            {"pending_count", uint64}});
    function->isReadOnly = true;
    function->canParallelFunc = [] { return false; };
    function->namedParameters = {{"type", varchar}, {"index_name", varchar}};
    function->bindFunc = [](const Schema&, const execution::ContextMeta&,
                            const ::physical::PhysicalPlan& plan, int op) {
      auto input = std::make_unique<OptimizeStatsInput>();
      const auto& args =
          plan.plan(op).opr().procedure_call().query().arguments();
      if (args.size() != 2)
        THROW_INVALID_ARGUMENT_EXCEPTION(
            "Invalid SHOW_INDEX_OPTIMIZE_STATS arguments");
      for (int i = 0; i < args.size(); ++i) {
        if (args[i].has_param())
          input->parameters[i] = args[i].param().name();
        else if (args[i].has_const_() && args[i].const_().has_str())
          input->values[i] = args[i].const_().str();
        else
          THROW_INVALID_ARGUMENT_EXCEPTION(
              "SHOW_INDEX_OPTIMIZE_STATS requires string arguments");
      }
      return input;
    };
    function->execFunc = [](const CallFuncInputBase& input,
                            IStorageInterface& graph) {
      auto* read = dynamic_cast<StorageReadInterface*>(&graph);
      if (!read)
        THROW_INVALID_ARGUMENT_EXCEPTION(
            "SHOW_INDEX_OPTIMIZE_STATS requires a readable transaction");
      const auto& bound = dynamic_cast<const OptimizeStatsInput&>(input);
      IndexOptimizeOptions options;
      if (!bound.values[0].empty())
        options.type = bound.values[0];
      if (!bound.values[1].empty())
        options.index_name = bound.values[1];
      auto result = read->GetIndexOptimizePendingStats(options);
      if (!result)
        THROW_RUNTIME_ERROR(result.error().ToString());
      ValueColumnBuilder<std::string> types, names;
      ValueColumnBuilder<uint64_t> pending;
      for (const auto& row : result.value()) {
        types.push_back_opt(row.type);
        names.push_back_opt(row.index_name);
        pending.push_back_opt(row.pending_count);
      }
      execution::Context ctx;
      DataChunk chunk;
      chunk.set(0, types.finish());
      chunk.set(1, names.finish());
      chunk.set(2, pending.finish());
      ctx.append_chunk(std::move(chunk));
      ctx.tag_ids = {0, 1, 2};
      return ctx;
    };
    functions.push_back(std::move(function));
  }
  return functions;
}
}  // namespace neug::function
