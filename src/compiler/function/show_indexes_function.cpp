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

#include "neug/compiler/function/show_indexes_function.h"

#include <algorithm>
#include <array>
#include <optional>
#include <string>
#include <utility>
#include <vector>
#include "neug/compiler/common/string_utils.h"

#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

#include "neug/common/columns/value_columns.h"
#include "neug/execution/common/context.h"
#include "neug/storages/graph/graph_interface.h"
#include "neug/storages/index/storage_index.h"
#include "neug/storages/index/storage_index_manager.h"
#include "neug/utils/exception/exception.h"

namespace neug::function {
namespace {

struct ShowIndexesInput : CallFuncInputBase {
  std::array<std::string, 2> values;
  std::array<std::string, 2> parameters;

  std::unique_ptr<CallFuncInputBase> bindParams(
      const execution::ParamsMap& params) const override {
    auto bound = std::make_unique<ShowIndexesInput>(*this);
    for (size_t i = 0; i < parameters.size(); ++i) {
      if (parameters[i].empty())
        continue;
      const auto it = params.find(parameters[i]);
      if (it == params.end() || it->second.IsNull() ||
          it->second.type().id() != DataTypeId::kVarchar)
        THROW_INVALID_ARGUMENT_EXCEPTION(
            "SHOW_INDEXES requires string parameters");
      bound->values[i] = it->second.GetValue<std::string>();
      if (bound->values[i].empty())
        THROW_INVALID_ARGUMENT_EXCEPTION(
            "SHOW_INDEXES requires nonempty parameters");
    }
    return bound;
  }
};

std::string OptionsToJson(const IndexMeta& meta) {
  std::vector<std::pair<std::string, std::string>> options(meta.options.begin(),
                                                           meta.options.end());
  std::sort(options.begin(), options.end());

  rapidjson::StringBuffer buffer;
  rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
  writer.StartObject();
  for (const auto& [key, value] : options) {
    writer.Key(key.data(), static_cast<rapidjson::SizeType>(key.size()));
    writer.String(value.data(), static_cast<rapidjson::SizeType>(value.size()));
  }
  writer.EndObject();
  return buffer.GetString();
}

}  // namespace

function_set ShowIndexesFunction::getFunctionSet() {
  auto varchar = common::DataType(common::DataTypeId::kVarchar);
  function_set functionSet;
  for (size_t arity = 0; arity <= 2; ++arity) {
    auto function = std::make_unique<NeugCallFunction>(
        ShowIndexesFunction::name, call_input_types(arity, varchar),
        call_output_columns{
            {"name", varchar},
            {"type", varchar},
            {"label", varchar},
            {"property", varchar},
            {"options", varchar},
            {"state", varchar},
            {"buffered_count", common::DataType(common::DataTypeId::kUInt64)}});

    function->isReadOnly = true;
    function->namedParameters = {{"type", varchar}, {"name", varchar}};
    function->bindFunc = [](const Schema&, const execution::ContextMeta&,
                            const ::physical::PhysicalPlan& plan, int op) {
      auto input = std::make_unique<ShowIndexesInput>();
      const auto& args =
          plan.plan(op).opr().procedure_call().query().arguments();
      if (args.size() != 2)
        THROW_INVALID_ARGUMENT_EXCEPTION("Invalid SHOW_INDEXES arguments");
      for (int i = 0; i < args.size(); ++i) {
        if (args[i].has_param())
          input->parameters[i] = args[i].param().name();
        else if (args[i].has_const_() && args[i].const_().has_str())
          input->values[i] = args[i].const_().str();
        else
          THROW_INVALID_ARGUMENT_EXCEPTION(
              "SHOW_INDEXES requires string arguments");
      }
      return input;
    };
    function->execFunc = [](const CallFuncInputBase& input,
                            IStorageInterface& graph) {
      auto* readInterface = dynamic_cast<StorageReadInterface*>(&graph);
      if (!readInterface) {
        THROW_RUNTIME_ERROR(
            "SHOW_INDEXES requires a readable storage interface");
      }

      const auto& bound = dynamic_cast<const ShowIndexesInput&>(input);
      IndexOptions options;
      if (!bound.values[0].empty())
        options.type = bound.values[0];
      if (!bound.values[1].empty())
        options.index_name = bound.values[1];
      auto stats = readInterface->ShowIndexes(options);
      if (!stats)
        THROW_RUNTIME_ERROR("SHOW_INDEXES failed: " + stats.error().ToString());
      const auto& rows = stats.value();

      ValueColumnBuilder<std::string> nameBuilder;
      ValueColumnBuilder<std::string> typeBuilder;
      ValueColumnBuilder<std::string> labelBuilder;
      ValueColumnBuilder<std::string> propertyBuilder;
      ValueColumnBuilder<std::string> optionsBuilder;
      ValueColumnBuilder<std::string> stateBuilder;
      ValueColumnBuilder<uint64_t> bufferedBuilder;
      nameBuilder.reserve(rows.size());
      typeBuilder.reserve(rows.size());
      labelBuilder.reserve(rows.size());
      propertyBuilder.reserve(rows.size());
      optionsBuilder.reserve(rows.size());
      stateBuilder.reserve(rows.size());

      const auto& schema = graph.schema();
      for (const auto& [meta, state, buffered_count] : rows) {
        nameBuilder.push_back_opt(meta.name);
        typeBuilder.push_back_opt(meta.type);
        labelBuilder.push_back_opt(
            schema.get_vertex_label_name(meta.schema.label_id));
        if (meta.schema.columns.size() == 1) {
          propertyBuilder.push_back_opt(meta.schema.columns[0].property_name);
        } else {
          std::string properties = "[";
          for (size_t i = 0; i < meta.schema.columns.size(); ++i) {
            if (i > 0)
              properties += ",";
            properties += "\"" + meta.schema.columns[i].property_name + "\"";
          }
          properties += "]";
          propertyBuilder.push_back_opt(properties);
        }
        optionsBuilder.push_back_opt(OptionsToJson(meta));
        stateBuilder.push_back_opt(state == IndexState::kActive ? "active"
                                                                : "pending");
        if (buffered_count)
          bufferedBuilder.push_back_opt(*buffered_count);
        else
          bufferedBuilder.push_back_null();
      }

      execution::Context ctx;
      DataChunk chunk;
      chunk.set(0, nameBuilder.finish());
      chunk.set(1, typeBuilder.finish());
      chunk.set(2, labelBuilder.finish());
      chunk.set(3, propertyBuilder.finish());
      chunk.set(4, optionsBuilder.finish());
      chunk.set(5, stateBuilder.finish());
      chunk.set(6, bufferedBuilder.finish());
      ctx.append_chunk(std::move(chunk));
      ctx.tag_ids = {0, 1, 2, 3, 4, 5, 6};
      return ctx;
    };

    functionSet.push_back(std::move(function));
  }
  return functionSet;
}

}  // namespace neug::function
