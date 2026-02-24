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

#include "neug/compiler/common/types/value/node.h"

#include "neug/compiler/common/constants.h"
#include "neug/compiler/common/string_format.h"
#include "neug/compiler/common/types/types.h"
#include "neug/compiler/common/types/value/value.h"

namespace neug {
namespace common {

std::vector<std::pair<std::string, std::unique_ptr<Value>>>
NodeVal::getProperties(const Value* val) {
  throwIfNotNode(val);
  std::vector<std::pair<std::string, std::unique_ptr<Value>>> properties;
  auto fieldNames = StructType::getFieldNames(val->dataType);
  for (auto i = 0u; i < val->childrenSize; ++i) {
    auto currKey = fieldNames[i];
    if (currKey == InternalKeyword::ID || currKey == InternalKeyword::LABEL) {
      continue;
    }
    properties.emplace_back(currKey, val->children[i]->copy());
  }
  return properties;
}

uint64_t NodeVal::getNumProperties(const Value* val) {
  throwIfNotNode(val);
  auto fieldNames = StructType::getFieldNames(val->dataType);
  return fieldNames.size() - OFFSET;
}

std::string NodeVal::getPropertyName(const Value* val, uint64_t index) {
  throwIfNotNode(val);
  auto fieldNames = StructType::getFieldNames(val->dataType);
  if (index >= fieldNames.size() - OFFSET) {
    return "";
  }
  return fieldNames[index + OFFSET];
}

Value* NodeVal::getPropertyVal(const Value* val, uint64_t index) {
  throwIfNotNode(val);
  auto fieldNames = StructType::getFieldNames(val->dataType);
  if (index >= fieldNames.size() - OFFSET) {
    return nullptr;
  }
  return val->children[index + OFFSET].get();
}

Value* NodeVal::getNodeIDVal(const Value* val) {
  throwIfNotNode(val);
  auto fieldIdx = StructType::getFieldIdx(val->dataType, InternalKeyword::ID);
  return val->children[fieldIdx].get();
}

Value* NodeVal::getLabelVal(const Value* val) {
  throwIfNotNode(val);
  auto fieldIdx =
      StructType::getFieldIdx(val->dataType, InternalKeyword::LABEL);
  return val->children[fieldIdx].get();
}

std::string NodeVal::toString(const Value* val) {
  throwIfNotNode(val);
  return val->toString();
}

void NodeVal::throwIfNotNode(const Value* val) {
  // LCOV_EXCL_START
  if (val->dataType.getLogicalTypeID() != LogicalTypeID::NODE) {
    THROW_EXCEPTION_WITH_FILE_LINE(stringFormat(
        "Expected NODE type, but got {} type", val->dataType.toString()));
  }
  // LCOV_EXCL_STOP
}

}  // namespace common
}  // namespace neug
