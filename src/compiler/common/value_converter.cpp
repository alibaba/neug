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

#include "neug/compiler/common/value_converter.h"

#include <memory>
#include <vector>

#include "neug/compiler/common/types/date_t.h"
#include "neug/compiler/common/types/interval_t.h"
#include "neug/compiler/common/types/timestamp_t.h"

namespace neug {
namespace common {

namespace {
execution::date_t convertToExecutionDate(date_t value) {
  return execution::date_t(Date::toString(value));
}

execution::timestamp_ms_t convertToExecutionTimestamp(timestamp_ms_t value) {
  return execution::timestamp_ms_t(normalizeTimestampMillis(value));
}

execution::interval_t convertToExecutionInterval(interval_t value) {
  execution::interval_t result;
  result.months = value.months;
  result.days = value.days;
  result.micros = value.micros;
  return result;
}

date_t convertToCompilerDate(execution::date_t value) {
  const auto str = value.to_string();
  return Date::fromCString(str.c_str(), str.size());
}

timestamp_ms_t convertToCompilerTimestamp(execution::timestamp_ms_t value) {
  return timestamp_ms_t(value.milli_second);
}

interval_t convertToCompilerInterval(execution::interval_t value) {
  return interval_t(value.months, value.days, value.micros);
}
}  // namespace

int64_t normalizeTimestampMillis(timestamp_ms_t value) {
  constexpr int64_t kLikelyMicrosThreshold = 100000000000000LL;
  if (value.value > kLikelyMicrosThreshold ||
      value.value < -kLikelyMicrosThreshold) {
    return Timestamp::getEpochMilliSeconds(timestamp_t(value.value));
  }
  return value.value;
}

execution::Value convertToExecutionValue(const Value& value,
                                         const DataType& type) {
  if (value.isNull()) {
    return execution::Value(type.copy());
  }
  switch (type.id()) {
  case DataTypeId::kBoolean:
    return execution::Value::BOOLEAN(value.getValue<bool>());
  case DataTypeId::kInt32:
    return execution::Value::INT32(value.getValue<int32_t>());
  case DataTypeId::kUInt32:
    return execution::Value::UINT32(value.getValue<uint32_t>());
  case DataTypeId::kInt64:
    return execution::Value::INT64(value.getValue<int64_t>());
  case DataTypeId::kUInt64:
    return execution::Value::UINT64(value.getValue<uint64_t>());
  case DataTypeId::kFloat:
    return execution::Value::FLOAT(value.getValue<float>());
  case DataTypeId::kDouble:
    return execution::Value::DOUBLE(value.getValue<double>());
  case DataTypeId::kVarchar:
    return execution::Value::STRING(value.getValue<std::string>());
  case DataTypeId::kDate:
    return execution::Value::DATE(
        convertToExecutionDate(value.getValue<date_t>()));
  case DataTypeId::kTimestampMs:
    return execution::Value::TIMESTAMPMS(
        convertToExecutionTimestamp(value.getValue<timestamp_ms_t>()));
  case DataTypeId::kInterval:
    return execution::Value::INTERVAL(
        convertToExecutionInterval(value.getValue<interval_t>()));
  case DataTypeId::kArray: {
    std::vector<execution::Value> children;
    children.reserve(value.getChildrenSize());
    const auto& childType = ArrayType::GetChildType(type);
    for (auto i = 0u; i < value.getChildrenSize(); ++i) {
      children.push_back(
          convertToExecutionValue(*value.children[i], childType));
    }
    return execution::Value::ARRAY(type, std::move(children));
  }
  case DataTypeId::kList: {
    std::vector<execution::Value> children;
    children.reserve(value.getChildrenSize());
    const auto& childType = ListType::GetChildType(type);
    for (auto i = 0u; i < value.getChildrenSize(); ++i) {
      children.push_back(
          convertToExecutionValue(*value.children[i], childType));
    }
    return execution::Value::LIST(childType, std::move(children));
  }
  case DataTypeId::kStruct: {
    std::vector<execution::Value> children;
    children.reserve(value.getChildrenSize());
    const auto& childTypes = StructType::GetChildTypes(type);
    for (auto i = 0u; i < value.getChildrenSize(); ++i) {
      children.push_back(
          convertToExecutionValue(*value.children[i], childTypes[i]));
    }
    return execution::Value::STRUCT(type, std::move(children));
  }
  default:
    return execution::Value(type.copy());
  }
}

Value convertToCompilerValue(const execution::Value& value,
                             const DataType& type) {
  if (value.IsNull()) {
    return Value::createNullValue(type);
  }
  switch (type.id()) {
  case DataTypeId::kBoolean:
    return Value(value.GetValue<bool>());
  case DataTypeId::kInt32:
    return Value(value.GetValue<int32_t>());
  case DataTypeId::kUInt32:
    return Value(value.GetValue<uint32_t>());
  case DataTypeId::kInt64:
    return Value(value.GetValue<int64_t>());
  case DataTypeId::kUInt64:
    return Value(value.GetValue<uint64_t>());
  case DataTypeId::kFloat:
    return Value(value.GetValue<float>());
  case DataTypeId::kDouble:
    return Value(value.GetValue<double>());
  case DataTypeId::kVarchar:
    return Value(value.GetValue<std::string>());
  case DataTypeId::kDate:
    return Value(convertToCompilerDate(value.GetValue<execution::date_t>()));
  case DataTypeId::kTimestampMs:
    return Value(convertToCompilerTimestamp(
        value.GetValue<execution::timestamp_ms_t>()));
  case DataTypeId::kInterval:
    return Value(
        convertToCompilerInterval(value.GetValue<execution::interval_t>()));
  case DataTypeId::kArray: {
    std::vector<std::unique_ptr<Value>> children;
    const auto& childType = ArrayType::GetChildType(type);
    const auto& defaultChildren = execution::ArrayValue::GetChildren(value);
    children.reserve(defaultChildren.size());
    for (const auto& child : defaultChildren) {
      children.push_back(
          std::make_unique<Value>(convertToCompilerValue(child, childType)));
    }
    return Value(type.copy(), std::move(children));
  }
  case DataTypeId::kList: {
    std::vector<std::unique_ptr<Value>> children;
    const auto& childType = ListType::GetChildType(type);
    const auto& defaultChildren = execution::ListValue::GetChildren(value);
    children.reserve(defaultChildren.size());
    for (const auto& child : defaultChildren) {
      children.push_back(
          std::make_unique<Value>(convertToCompilerValue(child, childType)));
    }
    return Value(type.copy(), std::move(children));
  }
  case DataTypeId::kStruct: {
    std::vector<std::unique_ptr<Value>> children;
    const auto& childTypes = StructType::GetChildTypes(type);
    const auto& defaultChildren = execution::StructValue::GetChildren(value);
    children.reserve(defaultChildren.size());
    for (auto i = 0u; i < defaultChildren.size(); ++i) {
      children.push_back(std::make_unique<Value>(
          convertToCompilerValue(defaultChildren[i], childTypes[i])));
    }
    return Value(type.copy(), std::move(children));
  }
  default:
    return Value::createNullValue(type);
  }
}

}  // namespace common
}  // namespace neug
