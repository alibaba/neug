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

#include "neug/compiler/function/list/functions/list_concat_function.h"

#include "neug/compiler/function/list/vector_list_functions.h"
#include "neug/compiler/function/scalar_function.h"
#include "neug/utils/exception/exception.h"
#include "neug/utils/exception/message.h"

using namespace neug::common;

namespace neug {
namespace function {

void ListConcat::operation(common::list_entry_t& left,
                           common::list_entry_t& right,
                           common::list_entry_t& result,
                           common::ValueVector& leftVector,
                           common::ValueVector& rightVector,
                           common::ValueVector& resultVector) {
  result = common::ListVector::addList(&resultVector, left.size + right.size);
  auto resultDataVector = common::ListVector::getDataVector(&resultVector);
  auto resultPos = result.offset;
  auto leftDataVector = common::ListVector::getDataVector(&leftVector);
  auto leftPos = left.offset;
  for (auto i = 0u; i < left.size; i++) {
    resultDataVector->copyFromVectorData(resultPos++, leftDataVector,
                                         leftPos++);
  }
  auto rightDataVector = common::ListVector::getDataVector(&rightVector);
  auto rightPos = right.offset;
  for (auto i = 0u; i < right.size; i++) {
    resultDataVector->copyFromVectorData(resultPos++, rightDataVector,
                                         rightPos++);
  }
}

}  // namespace function
}  // namespace neug
