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

/**
 * This file is originally from the Kùzu project
 * (https://github.com/kuzudb/kuzu) Licensed under the MIT License. Modified by
 * Zhou Xiaoli in 2025 to support Neug-specific features.
 */

#include "neug/compiler/common/vector/value_vector.h"
#include "neug/compiler/function/neug_scalar_function.h"
#include "neug/compiler/function/path/vector_path_functions.h"

#include <set>

#include "neug/common/types/value.h"

using namespace neug::binder;
using namespace neug::common;

namespace neug {
namespace function {

static std::unique_ptr<FunctionBindData> bindFunc(
    const ScalarBindFuncInput& input) {
  return FunctionBindData::getSimpleBindData(input.arguments,
                                             DataType(DataTypeId::kBoolean));
}

static const neug::Path& getPath(const std::vector<neug::Value>& args,
                                 const char* functionName) {
  if (args.size() != 1) {
    THROW_RUNTIME_ERROR(std::string(functionName) +
                        " expects exactly one argument");
  }
  if (args[0].type().id() != neug::DataTypeId::kPath) {
    THROW_RUNTIME_ERROR(std::string(functionName) + " expects a PATH argument");
  }
  return neug::PathValue::Get(args[0]);
}

static neug::Value isTrail(const std::vector<neug::Value>& args) {
  if (args.size() == 1 && args[0].IsNull()) {
    return neug::Value(neug::DataType::BOOLEAN);
  }
  const auto& path = getPath(args, IsTrailFunction::name);
  std::set<neug::EdgeRecord> relationships;
  for (const auto& relationship : path.relationships()) {
    if (!relationships.emplace(relationship).second) {
      return neug::Value::BOOLEAN(false);
    }
  }
  return neug::Value::BOOLEAN(true);
}

static neug::Value isACyclic(const std::vector<neug::Value>& args) {
  if (args.size() == 1 && args[0].IsNull()) {
    return neug::Value(neug::DataType::BOOLEAN);
  }
  const auto& path = getPath(args, IsACyclicFunction::name);
  std::set<neug::VertexRecord> vertices;
  for (const auto& vertex : path.nodes()) {
    if (!vertices.emplace(vertex).second) {
      return neug::Value::BOOLEAN(false);
    }
  }
  return neug::Value::BOOLEAN(true);
}

function_set IsTrailFunction::getFunctionSet() {
  function_set functionSet;
  auto function = std::make_unique<NeugScalarFunction>(
      name, std::vector<DataTypeId>{DataTypeId::kPath}, DataTypeId::kBoolean,
      isTrail);
  function->bindFunc = bindFunc;
  functionSet.push_back(std::move(function));
  return functionSet;
}

function_set IsACyclicFunction::getFunctionSet() {
  function_set functionSet;
  auto function = std::make_unique<NeugScalarFunction>(
      name, std::vector<DataTypeId>{DataTypeId::kPath}, DataTypeId::kBoolean,
      isACyclic);
  function->bindFunc = bindFunc;
  functionSet.push_back(std::move(function));
  return functionSet;
}

}  // namespace function
}  // namespace neug
