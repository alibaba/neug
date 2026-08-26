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

#include "neug/common/types/value.h"

namespace neug {
namespace function {

struct ListFunctionUtils {
  static bool isListLike(const DataType& type) {
    return type.id() == DataTypeId::kList || type.id() == DataTypeId::kArray;
  }

  static const DataType& getElementType(const DataType& type) {
    return type.id() == DataTypeId::kList ? ListType::GetChildType(type)
                                          : ArrayType::GetChildType(type);
  }

  static DataType getCoercedInputType(const DataType& inputType,
                                      const DataType& elementType) {
    if (inputType.id() == DataTypeId::kArray) {
      return DataType::Array(elementType.copy(),
                             ArrayType::GetNumElements(inputType));
    }
    return DataType::List(elementType.copy());
  }

  static const std::vector<Value>& getChildren(const Value& value) {
    return value.type().id() == DataTypeId::kList
               ? ListValue::GetChildren(value)
               : ArrayValue::GetChildren(value);
  }
};

}  // namespace function
}  // namespace neug
