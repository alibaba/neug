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

#include "neug/compiler/function/list/functions/list_unique_function.h"

#include "neug/compiler/function/list/vector_list_functions.h"
#include "neug/compiler/function/scalar_function.h"

using namespace neug::common;

namespace neug {
namespace function {

uint64_t ListUnique::appendListElementsToValueSet(
    common::list_entry_t& input, common::ValueVector& inputVector,
    duplicate_value_handler duplicateValHandler,
    unique_value_handler uniqueValueHandler,
    null_value_handler nullValueHandler) {
  ValueSet uniqueKeys;
  auto dataVector = common::ListVector::getDataVector(&inputVector);
  auto val = common::Value::createDefaultValue(dataVector->dataType);
  for (auto i = 0u; i < input.size; i++) {
    if (dataVector->isNull(input.offset + i)) {
      if (nullValueHandler != nullptr) {
        nullValueHandler();
      }
      continue;
    }
    auto entryVal =
        common::ListVector::getListValuesWithOffset(&inputVector, input, i);
    val.copyFromColLayout(entryVal, dataVector);
    auto uniqueKey = uniqueKeys.insert(val).second;
    if (duplicateValHandler != nullptr && !uniqueKey) {
      duplicateValHandler(common::TypeUtils::entryToString(
          dataVector->dataType, entryVal, dataVector));
    }
    if (uniqueValueHandler != nullptr && uniqueKey) {
      uniqueValueHandler(*dataVector, input.offset + i);
    }
  }
  return uniqueKeys.size();
}

void ListUnique::operation(common::list_entry_t& input, int64_t& result,
                           common::ValueVector& inputVector,
                           common::ValueVector& /*resultVector*/) {
  result = appendListElementsToValueSet(input, inputVector);
}

static std::unique_ptr<FunctionBindData> bindFunc(
    const ScalarBindFuncInput& input) {
  return FunctionBindData::getSimpleBindData(input.arguments,
                                             LogicalType::INT64());
}

}  // namespace function
}  // namespace neug
