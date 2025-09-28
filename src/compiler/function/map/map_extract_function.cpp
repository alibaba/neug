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

#include "neug/compiler/function/map/functions/map_extract_function.h"

#include "neug/compiler/common/type_utils.h"
#include "neug/compiler/function/map/vector_map_functions.h"
#include "neug/compiler/function/scalar_function.h"
#include "neug/utils/exception/exception.h"

using namespace gs::common;

namespace gs {
namespace function {

static void validateKeyType(
    const std::shared_ptr<binder::Expression>& mapExpression,
    const std::shared_ptr<binder::Expression>& extractKeyExpression) {
  const auto& mapKeyType = MapType::getKeyType(mapExpression->dataType);
  if (mapKeyType != extractKeyExpression->dataType) {
    THROW_RUNTIME_ERROR("Unmatched map key type and extract key type");
  }
}

static std::unique_ptr<FunctionBindData> bindFunc(
    const ScalarBindFuncInput& input) {
  validateKeyType(input.arguments[0], input.arguments[1]);
  auto scalarFunction = neug_dynamic_cast<ScalarFunction*>(input.definition);
  TypeUtils::visit(
      input.arguments[1]->getDataType().getPhysicalType(), [&]<typename T>(T) {
        scalarFunction->execFunc = ScalarFunction::BinaryExecListStructFunction<
            list_entry_t, T, list_entry_t, MapExtract>;
      });
  auto resultType = LogicalType::LIST(
      MapType::getValueType(input.arguments[0]->dataType).copy());
  return FunctionBindData::getSimpleBindData(input.arguments, resultType);
}

function_set MapExtractFunctions::getFunctionSet() {
  function_set functionSet;
  auto function = std::make_unique<ScalarFunction>(
      name, std::vector<LogicalTypeID>{LogicalTypeID::MAP, LogicalTypeID::ANY},
      LogicalTypeID::LIST);
  function->bindFunc = bindFunc;
  functionSet.push_back(std::move(function));
  return functionSet;
}

}  // namespace function
}  // namespace gs
