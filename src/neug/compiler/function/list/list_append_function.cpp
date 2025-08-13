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

#include "neug/compiler/common/type_utils.h"
#include "neug/compiler/function/list/vector_list_functions.h"
#include "neug/compiler/function/scalar_function.h"
#include "neug/utils/exception/exception.h"
#include "neug/utils/exception/message.h"

using namespace gs::common;

namespace gs {
namespace function {

struct ListAppend {
  template <typename T>
  static void operation(common::list_entry_t& listEntry, T& value,
                        common::list_entry_t& result,
                        common::ValueVector& listVector,
                        common::ValueVector& valueVector,
                        common::ValueVector& resultVector) {
    result = common::ListVector::addList(&resultVector, listEntry.size + 1);
    auto listDataVector = common::ListVector::getDataVector(&listVector);
    auto listPos = listEntry.offset;
    auto resultDataVector = common::ListVector::getDataVector(&resultVector);
    auto resultPos = result.offset;
    for (auto i = 0u; i < listEntry.size; i++) {
      resultDataVector->copyFromVectorData(resultPos++, listDataVector,
                                           listPos++);
    }
    resultDataVector->copyFromVectorData(
        resultDataVector->getData() +
            resultPos * resultDataVector->getNumBytesPerValue(),
        &valueVector, reinterpret_cast<uint8_t*>(&value));
  }
};

static void validateArgumentType(const binder::expression_vector& arguments) {
  if (ListType::getChildType(arguments[0]->dataType) !=
      arguments[1]->getDataType()) {
    THROW_BINDER_EXCEPTION(
        ExceptionMessage::listFunctionIncompatibleChildrenType(
            ListAppendFunction::name, arguments[0]->getDataType().toString(),
            arguments[1]->getDataType().toString()));
  }
}

static std::unique_ptr<FunctionBindData> bindFunc(
    const ScalarBindFuncInput& input) {
  validateArgumentType(input.arguments);
  auto scalarFunction = input.definition->ptrCast<ScalarFunction>();
  TypeUtils::visit(
      input.arguments[1]->getDataType().getPhysicalType(),
      [&scalarFunction]<typename T>(T) {
        scalarFunction->execFunc = ScalarFunction::BinaryExecListStructFunction<
            list_entry_t, T, list_entry_t, ListAppend>;
      });
  return FunctionBindData::getSimpleBindData(input.arguments,
                                             input.arguments[0]->getDataType());
}

function_set ListAppendFunction::getFunctionSet() {
  function_set result;
  auto function = std::make_unique<ScalarFunction>(
      name, std::vector<LogicalTypeID>{LogicalTypeID::LIST, LogicalTypeID::ANY},
      LogicalTypeID::LIST);
  function->bindFunc = bindFunc;
  result.push_back(std::move(function));
  return result;
}

}  // namespace function
}  // namespace gs
