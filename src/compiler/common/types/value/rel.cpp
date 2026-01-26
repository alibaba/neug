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

#include "neug/compiler/common/types/value/rel.h"

#include "neug/compiler/common/constants.h"
#include "neug/compiler/common/string_format.h"
#include "neug/compiler/common/types/value/value.h"

namespace neug {
namespace common {

std::vector<std::pair<std::string, std::unique_ptr<Value>>>
RelVal::getProperties(const Value* val) {
  throwIfNotRel(val);
  std::vector<std::pair<std::string, std::unique_ptr<Value>>> properties;
  auto fieldNames = StructType::getFieldNames(val->dataType);
  for (auto i = 0u; i < val->childrenSize; ++i) {
    auto currKey = fieldNames[i];
    if (currKey == InternalKeyword::ID || currKey == InternalKeyword::LABEL ||
        currKey == InternalKeyword::SRC || currKey == InternalKeyword::DST) {
      continue;
    }
    auto currVal = val->children[i]->copy();
    properties.emplace_back(currKey, std::move(currVal));
  }
  return properties;
}

uint64_t RelVal::getNumProperties(const Value* val) {
  throwIfNotRel(val);
  auto fieldNames = StructType::getFieldNames(val->dataType);
  return fieldNames.size() - OFFSET;
}

std::string RelVal::getPropertyName(const Value* val, uint64_t index) {
  throwIfNotRel(val);
  auto fieldNames = StructType::getFieldNames(val->dataType);
  if (index >= fieldNames.size() - OFFSET) {
    return "";
  }
  return fieldNames[index + OFFSET];
}

Value* RelVal::getPropertyVal(const Value* val, uint64_t index) {
  throwIfNotRel(val);
  auto fieldNames = StructType::getFieldNames(val->dataType);
  if (index >= fieldNames.size() - OFFSET) {
    return nullptr;
  }
  return val->children[index + OFFSET].get();
}

Value* RelVal::getIDVal(const Value* val) {
  auto fieldIdx = StructType::getFieldIdx(val->dataType, InternalKeyword::ID);
  return val->children[fieldIdx].get();
}

Value* RelVal::getSrcNodeIDVal(const Value* val) {
  auto fieldIdx = StructType::getFieldIdx(val->dataType, InternalKeyword::SRC);
  return val->children[fieldIdx].get();
}

Value* RelVal::getDstNodeIDVal(const Value* val) {
  auto fieldIdx = StructType::getFieldIdx(val->dataType, InternalKeyword::DST);
  return val->children[fieldIdx].get();
}

Value* RelVal::getLabelVal(const Value* val) {
  auto fieldIdx =
      StructType::getFieldIdx(val->dataType, InternalKeyword::LABEL);
  return val->children[fieldIdx].get();
}

std::string RelVal::toString(const Value* val) {
  throwIfNotRel(val);
  return val->toString();
}

void RelVal::throwIfNotRel(const Value* val) {
  // LCOV_EXCL_START
  if (val->dataType.getLogicalTypeID() != LogicalTypeID::REL) {
    THROW_EXCEPTION_WITH_FILE_LINE(stringFormat(
        "Expected REL type, but got {} type", val->dataType.toString()));
  }
  // LCOV_EXCL_STOP
}

}  // namespace common
}  // namespace neug
