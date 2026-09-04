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

#include "neug/compiler/binder/expression/compact_literal_expression.h"

#include <memory>
#include <sstream>

#include "neug/common/types/value.h"
#include "neug/compiler/common/value_converter.h"
#include "neug/utils/exception/exception.h"

using namespace neug::common;

namespace neug {
namespace binder {
namespace {

compiler_impl::Value castScalarValue(const compiler_impl::Value& value,
                                     const DataType& targetType) {
  if (value.isNull()) {
    return compiler_impl::Value::createNullValue(targetType);
  }
  if (value.getDataType() == targetType) {
    auto result = value;
    result.setDataType(targetType);
    return result;
  }
  auto executionValue =
      common::convertToExecutionValue(value, value.getDataType());
  ::neug::Value casted;
  switch (targetType.id()) {
  case DataTypeId::kBoolean:
    THROW_CONVERSION_EXCEPTION("Unsupported compact default cast target: " +
                               targetType.ToString());
  case DataTypeId::kInt32:
    casted = ::neug::performCast<int32_t>(executionValue);
    break;
  case DataTypeId::kInt64:
    casted = ::neug::performCast<int64_t>(executionValue);
    break;
  case DataTypeId::kUInt32:
    casted = ::neug::performCast<uint32_t>(executionValue);
    break;
  case DataTypeId::kUInt64:
    casted = ::neug::performCast<uint64_t>(executionValue);
    break;
  case DataTypeId::kFloat:
    casted = ::neug::performCast<float>(executionValue);
    break;
  case DataTypeId::kDouble:
    casted = ::neug::performCast<double>(executionValue);
    break;
  case DataTypeId::kVarchar:
    casted = ::neug::performCastToString(executionValue);
    break;
  default:
    THROW_CONVERSION_EXCEPTION("Unsupported compact default cast target: " +
                               targetType.ToString());
  }
  return common::convertToCompilerValue(casted, targetType);
}

const DataType& getChildType(const DataType& type) {
  if (type.id() == DataTypeId::kArray) {
    return ArrayType::GetChildType(type);
  }
  if (type.id() == DataTypeId::kList) {
    return ListType::GetChildType(type);
  }
  THROW_CONVERSION_EXCEPTION("Compact literal can only be cast to LIST/ARRAY.");
}

}  // namespace

void CompactLiteralExpression::cast(const DataType& type) {
  if (type.id() != DataTypeId::kArray && type.id() != DataTypeId::kList) {
    THROW_CONVERSION_EXCEPTION(
        "Compact literal can only be cast to LIST/ARRAY.");
  }
  if (type.id() == DataTypeId::kArray &&
      getElementCount() != ArrayType::GetNumElements(type)) {
    THROW_CONVERSION_EXCEPTION("ARRAY value length mismatch: expected " +
                               std::to_string(ArrayType::GetNumElements(type)) +
                               ", got " + std::to_string(getElementCount()) +
                               ".");
  }
  const auto& childType = getChildType(type);
  for (auto& segment : segments_) {
    segment.value = castScalarValue(segment.value, childType);
  }
  dataType = type.copy();
}

uint64_t CompactLiteralExpression::getElementCount() const {
  uint64_t count = 0;
  for (const auto& segment : segments_) {
    count += segment.repeatCount;
  }
  return count;
}

compiler_impl::Value CompactLiteralExpression::materialize() const {
  std::vector<std::unique_ptr<compiler_impl::Value>> children;
  children.reserve(getElementCount());
  for (const auto& segment : segments_) {
    for (uint64_t i = 0; i < segment.repeatCount; ++i) {
      children.push_back(segment.value.copy());
    }
  }
  return compiler_impl::Value(dataType.copy(), std::move(children));
}

std::string CompactLiteralExpression::toStringInternal() const {
  std::ostringstream ss;
  ss << "[";
  for (auto i = 0u; i < segments_.size(); ++i) {
    if (i > 0) {
      ss << "; ";
    }
    ss << segments_[i].value.toString() << ":" << segments_[i].repeatCount;
  }
  ss << "]";
  return ss.str();
}

}  // namespace binder
}  // namespace neug
