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
::neug::Date convertToExecutionDate(compiler_impl::date_t value) {
  return ::neug::Date(compiler_impl::Date::toString(value));
}

::neug::DateTime convertToExecutionTimestamp(
    compiler_impl::timestamp_ms_t value) {
  return ::neug::DateTime(normalizeTimestampMillis(value));
}

::neug::Interval convertToExecutionInterval(compiler_impl::interval_t value) {
  ::neug::Interval result;
  result.months = value.months;
  result.days = value.days;
  result.micros = value.micros;
  return result;
}

compiler_impl::date_t convertToCompilerDate(::neug::Date value) {
  const auto str = value.to_string();
  return compiler_impl::Date::fromCString(str.c_str(), str.size());
}

compiler_impl::timestamp_ms_t convertToCompilerTimestamp(
    ::neug::DateTime value) {
  return compiler_impl::timestamp_ms_t(value.milli_second);
}

compiler_impl::interval_t convertToCompilerInterval(::neug::Interval value) {
  return compiler_impl::interval_t(value.months, value.days, value.micros);
}
}  // namespace

int64_t normalizeTimestampMillis(compiler_impl::timestamp_ms_t value) {
  constexpr int64_t kLikelyMicrosThreshold = 100000000000000LL;
  if (value.value > kLikelyMicrosThreshold ||
      value.value < -kLikelyMicrosThreshold) {
    return compiler_impl::Timestamp::getEpochMilliSeconds(
        compiler_impl::timestamp_t(value.value));
  }
  return value.value;
}

::neug::Value convertToExecutionValue(const compiler_impl::Value& value,
                                      const DataType& type) {
  if (value.isNull()) {
    return ::neug::Value(type.copy());
  }
  switch (type.id()) {
  case DataTypeId::kBoolean:
    return ::neug::Value::BOOLEAN(value.getValue<bool>());
  case DataTypeId::kInt32:
    return ::neug::Value::INT32(value.getValue<int32_t>());
  case DataTypeId::kUInt32:
    return ::neug::Value::UINT32(value.getValue<uint32_t>());
  case DataTypeId::kInt64:
    return ::neug::Value::INT64(value.getValue<int64_t>());
  case DataTypeId::kUInt64:
    return ::neug::Value::UINT64(value.getValue<uint64_t>());
  case DataTypeId::kFloat:
    return ::neug::Value::FLOAT(value.getValue<float>());
  case DataTypeId::kDouble:
    return ::neug::Value::DOUBLE(value.getValue<double>());
  case DataTypeId::kVarchar:
    return ::neug::Value::STRING(value.getValue<std::string>());
  case DataTypeId::kDate:
    return ::neug::Value::DATE(
        convertToExecutionDate(value.getValue<compiler_impl::date_t>()));
  case DataTypeId::kTimestampMs:
    return ::neug::Value::TIMESTAMPMS(convertToExecutionTimestamp(
        value.getValue<compiler_impl::timestamp_ms_t>()));
  case DataTypeId::kInterval:
    return ::neug::Value::INTERVAL(convertToExecutionInterval(
        value.getValue<compiler_impl::interval_t>()));
  case DataTypeId::kArray: {
    std::vector<::neug::Value> children;
    children.reserve(value.getChildrenSize());
    const auto& childType = ArrayType::GetChildType(type);
    for (auto i = 0u; i < value.getChildrenSize(); ++i) {
      children.push_back(
          convertToExecutionValue(*value.children[i], childType));
    }
    return ::neug::Value::ARRAY(type, std::move(children));
  }
  case DataTypeId::kList: {
    std::vector<::neug::Value> children;
    children.reserve(value.getChildrenSize());
    const auto& childType = ListType::GetChildType(type);
    for (auto i = 0u; i < value.getChildrenSize(); ++i) {
      children.push_back(
          convertToExecutionValue(*value.children[i], childType));
    }
    return ::neug::Value::LIST(childType, std::move(children));
  }
  case DataTypeId::kStruct: {
    std::vector<::neug::Value> children;
    children.reserve(value.getChildrenSize());
    const auto& childTypes = StructType::GetChildTypes(type);
    for (auto i = 0u; i < value.getChildrenSize(); ++i) {
      children.push_back(
          convertToExecutionValue(*value.children[i], childTypes[i]));
    }
    return ::neug::Value::STRUCT(type, std::move(children));
  }
  default:
    return ::neug::Value(type.copy());
  }
}

compiler_impl::Value convertToCompilerValue(const ::neug::Value& value,
                                            const DataType& type) {
  if (value.IsNull()) {
    return compiler_impl::Value::createNullValue(type);
  }
  switch (type.id()) {
  case DataTypeId::kBoolean:
    return compiler_impl::Value(value.GetValue<bool>());
  case DataTypeId::kInt32:
    return compiler_impl::Value(value.GetValue<int32_t>());
  case DataTypeId::kUInt32:
    return compiler_impl::Value(value.GetValue<uint32_t>());
  case DataTypeId::kInt64:
    return compiler_impl::Value(value.GetValue<int64_t>());
  case DataTypeId::kUInt64:
    return compiler_impl::Value(value.GetValue<uint64_t>());
  case DataTypeId::kFloat:
    return compiler_impl::Value(value.GetValue<float>());
  case DataTypeId::kDouble:
    return compiler_impl::Value(value.GetValue<double>());
  case DataTypeId::kVarchar:
    return compiler_impl::Value(value.GetValue<std::string>());
  case DataTypeId::kDate:
    return compiler_impl::Value(
        convertToCompilerDate(value.GetValue<::neug::Date>()));
  case DataTypeId::kTimestampMs:
    return compiler_impl::Value(
        convertToCompilerTimestamp(value.GetValue<::neug::DateTime>()));
  case DataTypeId::kInterval:
    return compiler_impl::Value(
        convertToCompilerInterval(value.GetValue<::neug::Interval>()));
  case DataTypeId::kArray: {
    std::vector<std::unique_ptr<compiler_impl::Value>> children;
    const auto& childType = ArrayType::GetChildType(type);
    const auto& defaultChildren = ::neug::ArrayValue::GetChildren(value);
    children.reserve(defaultChildren.size());
    for (const auto& child : defaultChildren) {
      children.push_back(std::make_unique<compiler_impl::Value>(
          convertToCompilerValue(child, childType)));
    }
    return compiler_impl::Value(type.copy(), std::move(children));
  }
  case DataTypeId::kList: {
    std::vector<std::unique_ptr<compiler_impl::Value>> children;
    const auto& childType = ListType::GetChildType(type);
    const auto& defaultChildren = ::neug::ListValue::GetChildren(value);
    children.reserve(defaultChildren.size());
    for (const auto& child : defaultChildren) {
      children.push_back(std::make_unique<compiler_impl::Value>(
          convertToCompilerValue(child, childType)));
    }
    return compiler_impl::Value(type.copy(), std::move(children));
  }
  case DataTypeId::kStruct: {
    std::vector<std::unique_ptr<compiler_impl::Value>> children;
    const auto& childTypes = StructType::GetChildTypes(type);
    const auto& defaultChildren = ::neug::StructValue::GetChildren(value);
    children.reserve(defaultChildren.size());
    for (auto i = 0u; i < defaultChildren.size(); ++i) {
      children.push_back(std::make_unique<compiler_impl::Value>(
          convertToCompilerValue(defaultChildren[i], childTypes[i])));
    }
    return compiler_impl::Value(type.copy(), std::move(children));
  }
  default:
    return compiler_impl::Value::createNullValue(type);
  }
}

}  // namespace common
}  // namespace neug
