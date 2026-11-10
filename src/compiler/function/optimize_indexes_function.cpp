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

#include "neug/compiler/function/optimize_indexes_function.h"

#include <array>
#include "neug/common/columns/value_columns.h"
#include "neug/execution/common/context.h"
#include "neug/utils/exception/exception.h"

namespace neug::function {
namespace {
struct OptimizeInput : CallFuncInputBase {
  std::array<std::string, 2> values;
  std::array<std::string, 2> parameters;

  std::unique_ptr<CallFuncInputBase> bindParams(
      const execution::ParamsMap& params) const override {
    auto bound = std::make_unique<OptimizeInput>(*this);
    for (size_t i = 0; i < parameters.size(); ++i) {
      if (parameters[i].empty())
        continue;
      const auto it = params.find(parameters[i]);
      if (it == params.end() || it->second.IsNull() ||
          it->second.type().id() != DataTypeId::kVarchar)
        THROW_INVALID_ARGUMENT_EXCEPTION(
            "OPTIMIZE_INDEXES requires string parameters");
      bound->values[i] = it->second.GetValue<std::string>();
      if (bound->values[i].empty())
        THROW_INVALID_ARGUMENT_EXCEPTION(
            "OPTIMIZE_INDEXES requires nonempty parameters");
    }
    return bound;
  }
};
}  // namespace

function_set OptimizeIndexesFunction::getFunctionSet() {
  const auto varchar = common::DataType(common::DataTypeId::kVarchar);
  const auto uint64 = common::DataType(common::DataTypeId::kUInt64);
  function_set functions;
  for (size_t arity = 0; arity <= 2; ++arity) {
    auto function = std::make_unique<NeugCallFunction>(
        name, call_input_types(arity, varchar),
        call_output_columns{
            {"type", varchar},
            {"index_name", varchar},
            {"indexed_count", uint64},
            {"skipped_count", uint64},
            {"duration_ms", common::DataType(common::DataTypeId::kDouble)}});
    function->isReadOnly = false;
    function->transactionalWrite = true;
    function->canParallelFunc = [] { return false; };
    function->namedParameters = {{"type", varchar}, {"name", varchar}};
    function->bindFunc = [](const Schema&, const execution::ContextMeta&,
                            const ::physical::PhysicalPlan& plan, int op) {
      auto input = std::make_unique<OptimizeInput>();
      const auto& args =
          plan.plan(op).opr().procedure_call().query().arguments();
      if (args.size() != 2)
        THROW_INVALID_ARGUMENT_EXCEPTION("Invalid OPTIMIZE_INDEXES arguments");
      for (int i = 0; i < args.size(); ++i) {
        if (args[i].has_param())
          input->parameters[i] = args[i].param().name();
        else if (args[i].has_const_() && args[i].const_().has_str())
          input->values[i] = args[i].const_().str();
        else
          THROW_INVALID_ARGUMENT_EXCEPTION(
              "OPTIMIZE_INDEXES requires string arguments");
      }
      return input;
    };
    function->execFunc = [](const CallFuncInputBase& input,
                            IStorageInterface& graph) {
      auto* update = dynamic_cast<StorageUpdateInterface*>(&graph);
      if (!update)
        THROW_INVALID_ARGUMENT_EXCEPTION(
            "OPTIMIZE_INDEXES requires a write transaction");
      const auto& bound = dynamic_cast<const OptimizeInput&>(input);
      IndexOptions options;
      if (!bound.values[0].empty())
        options.type = bound.values[0];
      if (!bound.values[1].empty())
        options.index_name = bound.values[1];
      auto result = update->OptimizeIndexes(options);
      if (!result)
        THROW_RUNTIME_ERROR(result.error().ToString());
      ValueColumnBuilder<std::string> types, names;
      ValueColumnBuilder<uint64_t> indexed, skipped;
      ValueColumnBuilder<double> durations;
      for (const auto& row : result.value()) {
        types.push_back_opt(row.type);
        names.push_back_opt(row.index_name);
        indexed.push_back_opt(row.stats.indexed_count);
        skipped.push_back_opt(row.stats.skipped_count);
        durations.push_back_opt(row.duration_ms);
      }
      execution::Context ctx;
      DataChunk chunk;
      chunk.set(0, types.finish());
      chunk.set(1, names.finish());
      chunk.set(2, indexed.finish());
      chunk.set(3, skipped.finish());
      chunk.set(4, durations.finish());
      ctx.append_chunk(std::move(chunk));
      ctx.tag_ids = {0, 1, 2, 3, 4};
      return ctx;
    };
    functions.push_back(std::move(function));
  }
  return functions;
}
}  // namespace neug::function
