/** Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * This file is originally from the DuckDB project
 * (https://github.com/duckdb/duckdb) Licensed under the MIT License. Modified
 * by Liu Lexiao in 2026 to support Neug-specific features.
 */

#pragma once

#include <rapidjson/document.h>
#include <charconv>
#include "neug/common/types.h"
#include "neug/execution/common/types/graph_types.h"
#include "neug/execution/utils/numeric_cast.h"

namespace neug {
class Property;
class Encoder;

namespace execution {
using timestamp_ms_t = neug::DateTime;
using interval_t = neug::Interval;
using date_t = neug::Date;
using vertex_t = neug::execution::VertexRecord;
using edge_t = neug::execution::EdgeRecord;
struct ExtraValueInfo;
class Value {
  friend struct StringValue;
  friend struct StructValue;
  friend struct ListValue;
  friend struct PathValue;

 public:
  explicit Value(DataType type);

  Value(const Value& other);
  Value(Value&& other) noexcept;

  Value& operator=(const Value& other);
  Value& operator=(Value&& other) noexcept;

  const DataType& type() const { return type_; }

  static Value BOOLEAN(bool value);

  static Value INT32(int32_t value);

  static Value INT64(int64_t value);

  static Value UINT32(uint32_t value);

  static Value UINT64(uint64_t value);

  static Value DATE(date_t date);

  static Value TIMESTAMPMS(timestamp_ms_t timestamp);

  static Value INTERVAL(interval_t interval);

  static Value FLOAT(float value);

  static Value DOUBLE(double value);

  static Value STRUCT(std::vector<Value>&& values);
  static Value STRUCT(const DataType& type, std::vector<Value>&& struct_values);

  static Value LIST(const DataType& child_type, std::vector<Value>&& values);
  static Value LIST(std::vector<Value>&& values);

  static Value STRING(const std::string& str);

  static Value VERTEX(const vertex_t& vertex);

  static Value EDGE(const edge_t& edge);

  static Value PATH(const Path& path);

  bool operator==(const Value& other) const;

  bool operator<(const Value& other) const;

  bool IsNull() const { return is_null_; }

  bool IsTrue() const {
    return !is_null_ && type_.id() == DataTypeId::kBoolean && value_.boolean;
  }

  template <class T>
  T GetValue() const {
    static_assert(sizeof(T) == 0, "Unsupported type for GetValue");
    return T();
  }

  template <class OP>
  static Value ApplyArithmeticOp(const Value& lhs, const Value& rhs);

  template <class OP>
  static bool ApplyComparisonOp(const Value& lhs, const Value& rhs);

  template <class T>
  static Value CreateValue(T value) {
    static_assert(sizeof(T) == 0, "Unsupported type for CreateValue");
    return Value(DataType::SQLNULL);
  }

  // Arithmetic operators

  Value operator+(const Value& rhs) const;
  Value operator-(const Value& rhs) const;
  Value operator*(const Value& rhs) const;
  Value operator/(const Value& rhs) const;
  Value operator%(const Value& rhs) const;

  std::string to_string() const;
  // Parse from json string, with type info
  static Value FromJson(const std::string& json_str, const DataType& type);
  static Value FromJson(const rapidjson::Value& json_value,
                        const DataType& type);
  static rapidjson::Value ToJson(const Value& value,
                                 rapidjson::Document::AllocatorType& allocator);

 private:
  DataType type_;
  bool is_null_;
  union Val {
    bool boolean;
    int8_t tinyint;
    int16_t smallint;
    int32_t integer;
    int64_t bigint;
    uint8_t utinyint;
    uint16_t usmallint;
    uint32_t uinteger;
    uint64_t ubigint;
    float float_;    // NOLINT
    double double_;  // NOLINT
    date_t date;
    timestamp_ms_t timestamp_ms;
    interval_t interval;
    vertex_t vertex;
    edge_t edge;
  } value_;  // NOLINT

  std::shared_ptr<ExtraValueInfo> value_info_;  // NOLINT
};

struct StringValue {
  static const std::string& Get(const Value& value);
};

struct ListValue {
  static const std::vector<Value>& GetChildren(const Value& value);
};

struct StructValue {
  static const std::vector<Value>& GetChildren(const Value& value);
};

struct PathValue {
  static const Path& Get(const Value& value);
};

template <>
Value Value::CreateValue(bool value);

template <>
Value Value::CreateValue(uint32_t value);
template <>
Value Value::CreateValue(uint64_t value);
template <>
Value Value::CreateValue(int32_t value);
template <>
Value Value::CreateValue(int64_t value);

template <>
Value Value::CreateValue(date_t value);
template <>
Value Value::CreateValue(timestamp_t value);
template <>
Value Value::CreateValue(timestamp_ms_t value);
template <>
Value Value::CreateValue(std::string value);

template <>
Value Value::CreateValue(float value);
template <>
Value Value::CreateValue(double value);
template <>
Value Value::CreateValue(interval_t value);
template <>
Value Value::CreateValue(Value value);

template <>
Value Value::CreateValue(vertex_t value);

template <>
Value Value::CreateValue(edge_t value);

template <>
bool Value::GetValue() const;
template <>
int32_t Value::GetValue() const;
template <>
int64_t Value::GetValue() const;
template <>
uint32_t Value::GetValue() const;
template <>
uint64_t Value::GetValue() const;
template <>
std::string Value::GetValue() const;

template <>
float Value::GetValue() const;
template <>
double Value::GetValue() const;
template <>
date_t Value::GetValue() const;
template <>
timestamp_ms_t Value::GetValue() const;
template <>
interval_t Value::GetValue() const;

template <>
vertex_t Value::GetValue() const;

template <>
edge_t Value::GetValue() const;

template <>
Value Value::GetValue() const;

template <typename T>
struct ValueConverter {
  static DataType type() { return DataType(DataTypeId::kUnknown); }
};

template <>
struct ValueConverter<int32_t> {
  static DataType type() { return DataType(DataTypeId::kInt32); }
  static std::string name() { return "int32"; }
  static int32_t typed_from_string(const std::string& str) {
    return std::stoi(str);
  }
  template <typename T>
  static bool cast(const T& input, int32_t& output) {
    if constexpr (std::is_same_v<T, std::string>) {
      auto [data, len] = neug::execution::removeWhiteSpaces(input);
      auto [ptr, ec] = std::from_chars(data, data + len, output);
      return ec == std::errc() && ptr == data + len;
    } else {
      return neug::execution::TryCastWithOverflowCheck(input, output);
    }
  }
};

template <>
struct ValueConverter<int64_t> {
  static DataType type() { return DataType(DataTypeId::kInt64); }
  static std::string name() { return "int64"; }
  static int64_t typed_from_string(const std::string& str) {
    return std::stoll(str);
  }

  template <typename T>
  static bool cast(const T& input, int64_t& output) {
    if constexpr (std::is_same_v<T, DateTime>) {
      output = input.milli_second;
      return true;
    } else if constexpr (std::is_same_v<T, Date>) {
      output = input.to_timestamp();
      return true;
    } else if constexpr (std::is_same_v<T, Interval>) {
      output = input.to_mill_seconds();
      return true;
    } else if constexpr (std::is_same_v<T, std::string>) {
      auto [data, len] = neug::execution::removeWhiteSpaces(input);
      auto [ptr, ec] = std::from_chars(data, data + len, output);
      return ec == std::errc() && ptr == data + len;
    } else {
      return neug::execution::TryCastWithOverflowCheck(input, output);
    }
  }
};

template <>
struct ValueConverter<uint32_t> {
  static DataType type() { return DataType(DataTypeId::kUInt32); }
  static std::string name() { return "uint32"; }
  static uint32_t typed_from_string(const std::string& str) {
    return std::stoul(str);
  }
  template <typename T>
  static bool cast(const T& input, uint32_t& output) {
    if constexpr (std::is_same_v<T, std::string>) {
      auto [data, len] = neug::execution::removeWhiteSpaces(input);
      auto [ptr, ec] = std::from_chars(data, data + len, output);
      return ec == std::errc() && ptr == data + len;
    } else {
      return neug::execution::TryCastWithOverflowCheck(input, output);
    }
  }
};

template <>
struct ValueConverter<uint64_t> {
  static DataType type() { return DataType(DataTypeId::kUInt64); }
  static std::string name() { return "uint64"; }
  static uint64_t typed_from_string(const std::string& str) {
    return std::stoull(str);
  }
  template <typename T>
  static bool cast(const T& input, uint64_t& output) {
    if constexpr (std::is_same_v<T, std::string>) {
      auto [data, len] = neug::execution::removeWhiteSpaces(input);
      auto [ptr, ec] = std::from_chars(data, data + len, output);
      return ec == std::errc() && ptr == data + len;
    } else {
      return neug::execution::TryCastWithOverflowCheck(input, output);
    }
  }
};

template <>
struct ValueConverter<std::string> {
  static DataType type() { return DataType(DataTypeId::kVarchar); }
  static std::string name() { return "string"; }
  static std::string typed_from_string(const std::string& str) { return str; }
};

template <>
struct ValueConverter<double> {
  static DataType type() { return DataType(DataTypeId::kDouble); }
  static std::string name() { return "double"; }
  static double typed_from_string(const std::string& str) {
    return std::stod(str);
  }

  template <typename T>
  static bool cast(const T& input, double& output) {
    if constexpr (std::is_same_v<T, std::string>) {
      return neug::execution::tryDoubleCast(input, output);
    } else {
      return neug::execution::TryCastWithOverflowCheck(input, output);
    }
  }
};

template <>
struct ValueConverter<float> {
  static DataType type() { return DataType(DataTypeId::kFloat); }
  static std::string name() { return "float"; }
  static float typed_from_string(const std::string& str) {
    return std::stof(str);
  }
  template <typename T>
  static bool cast(const T& input, float& output) {
    if constexpr (std::is_same_v<T, std::string>) {
      return neug::execution::tryDoubleCast(input, output);
    } else {
      return neug::execution::TryCastWithOverflowCheck(input, output);
    }
  }
};

template <>
struct ValueConverter<bool> {
  static DataType type() { return DataType(DataTypeId::kBoolean); }
  static std::string name() { return "bool"; }
  static bool typed_from_string(const std::string& str) {
    if (str == "true" || str == "1") {
      return true;
    } else if (str == "false" || str == "0") {
      return false;
    } else {
      LOG(FATAL) << "Invalid boolean string: " << str;
    }
    return false;  // to suppress compiler warning
  }
};

template <>
struct ValueConverter<date_t> {
  static DataType type() { return DataType(DataTypeId::kDate); }
  static std::string name() { return "date"; }
  static Date typed_from_string(const std::string& str) { return Date(str); }

  template <typename T>
  static bool cast(const T& input, Date& output) {
    if constexpr (std::is_same_v<T, Date>) {
      output = input;
      return true;
    } else if constexpr (std::is_same_v<T, DateTime>) {
      output = Date(input.milli_second);
      return true;
    } else if constexpr (std::is_same_v<T, std::string>) {
      output = Date(std::string(input));
      return true;
    } else {
      return false;
    }
  }
};

template <>
struct ValueConverter<timestamp_ms_t> {
  static DataType type() { return DataType(DataTypeId::kTimestampMs); }
  static std::string name() { return "timestamp_ms"; }
  static DateTime typed_from_string(const std::string& str) {
    return DateTime(str);
  }

  template <typename T>
  static bool cast(const T& input, DateTime& output) {
    if constexpr (std::is_same_v<T, DateTime>) {
      output = input;
      return true;
    } else if constexpr (std::is_same_v<T, Date>) {
      output = DateTime(input.to_timestamp());
      return true;
    } else if constexpr (std::is_same_v<T, std::string>) {
      output = DateTime(input);
      return true;
    } else {
      return false;
    }
  }
};

template <>
struct ValueConverter<interval_t> {
  static DataType type() { return DataType(DataTypeId::kInterval); }
  static std::string name() { return "interval"; }
  static Interval typed_from_string(const std::string& str) {
    return Interval(str);
  }

  template <typename T>
  static bool cast(const T& input, Interval& output) {
    if constexpr (std::is_same_v<T, Interval>) {
      output = input;
      return true;
    } else if constexpr (std::is_same_v<T, std::string>) {
      output = Interval(input);
      return true;
    } else {
      return false;
    }
  }
};

template <>
struct ValueConverter<vertex_t> {
  static DataType type() { return DataType(DataTypeId::kVertex); }
};

template <>
struct ValueConverter<edge_t> {
  static DataType type() { return DataType(DataTypeId::kEdge); }
};

struct AddOp {
  template <typename T>
  static T operation(const T& left, const T& right) {
    return left + right;
  }
};

struct SubOp {
  template <typename T>
  static T operation(const T& left, const T& right) {
    return left - right;
  }
};

struct MulOp {
  template <typename T>
  static T operation(const T& left, const T& right) {
    return left * right;
  }
};

struct DivOp {
  template <typename T>
  static T operation(const T& left, const T& right) {
    return left / right;
  }
};

struct ModOp {
  template <typename T>
  static T operation(const T& left, const T& right) {
    if constexpr (std::is_floating_point<T>::value) {
      return std::fmod(left, right);
    } else {
      return left % right;
    }
  }
};

struct EqOp {
  template <typename T>
  static bool operation(const T& left, const T& right) {
    return left == right;
  }
};

struct NeqOp {
  template <typename T>
  static bool operation(const T& left, const T& right) {
    return !(left == right);
  }
};

struct GtOp {
  template <typename T>
  static bool operation(const T& left, const T& right) {
    return right < left;
  }
};

struct LtOp {
  template <typename T>
  static bool operation(const T& left, const T& right) {
    return left < right;
  }
};

struct GeOp {
  template <typename T>
  static bool operation(const T& left, const T& right) {
    return !(left < right);
  }
};

struct LeOp {
  template <typename T>
  static bool operation(const T& left, const T& right) {
    return !(right < left);
  }
};

template <class OP>
Value Value::ApplyArithmeticOp(const Value& lhs, const Value& rhs) {
  assert(lhs.type() == rhs.type());
  switch (lhs.type().id()) {
#define TYPE_DISPATCHER(data_type_id, cpp_type)                             \
  case DataTypeId::data_type_id: {                                          \
    return Value::CreateValue(                                              \
        OP::operation(lhs.GetValue<cpp_type>(), rhs.GetValue<cpp_type>())); \
  }
    FOR_EACH_NUMERIC_DATA_TYPE(TYPE_DISPATCHER)
#undef TYPE_DISPATCHER
  default: {
    LOG(FATAL) << "Unsupported data type for binary op: "
               << static_cast<int>(lhs.type().id());
    return Value(DataType::SQLNULL);
  }
  }
}

template <class OP>
bool Value::ApplyComparisonOp(const Value& lhs, const Value& rhs) {
  assert(lhs.type() == rhs.type());
  switch (lhs.type().id()) {
#define TYPE_DISPATCHER(data_type_id, cpp_type)                               \
  case DataTypeId::data_type_id: {                                            \
    return OP::operation(lhs.GetValue<cpp_type>(), rhs.GetValue<cpp_type>()); \
  }
    FOR_EACH_DATA_TYPE_NO_STRING(TYPE_DISPATCHER)
#undef TYPE_DISPATCHER
  case DataTypeId::kVarchar: {
    return OP::operation(StringValue::Get(lhs), StringValue::Get(rhs));
  }
  case DataTypeId::kPath: {
    return OP::operation(PathValue::Get(lhs), PathValue::Get(rhs));
  }
  case DataTypeId::kStruct: {
    const auto& lhs_children = StructValue::GetChildren(lhs);
    const auto& rhs_children = StructValue::GetChildren(rhs);
    assert(lhs_children.size() == rhs_children.size());

    for (size_t i = 0; i < lhs_children.size(); ++i) {
      if (!ApplyComparisonOp<OP>(lhs_children[i], rhs_children[i])) {
        return false;
      }
    }
    return true;
  }
  case DataTypeId::kList: {
    const auto& lhs_children = ListValue::GetChildren(lhs);
    const auto& rhs_children = ListValue::GetChildren(rhs);
    size_t size = std::min(lhs_children.size(), rhs_children.size());
    for (size_t i = 0; i < size; ++i) {
      if (!ApplyComparisonOp<OP>(lhs_children[i], rhs_children[i])) {
        return false;
      }
    }
    if (lhs_children.size() != rhs_children.size()) {
      return lhs_children.size() < rhs_children.size();
    }
    return true;
  }
  case DataTypeId::kVertex: {
    return OP::operation(lhs.GetValue<vertex_t>(), rhs.GetValue<vertex_t>());
  }
  case DataTypeId::kEdge: {
    return OP::operation(lhs.GetValue<edge_t>(), rhs.GetValue<edge_t>());
  }

  default: {
    LOG(FATAL) << "Unsupported data type for comparison op: "
               << static_cast<int>(lhs.type().id());
    return false;
  }
  }
}

Property value_to_property(const Value& value);
Value property_to_value(const Property& property);

template <typename T>
Value performCast(const Value& input) {
  T val;
  bool ret = false;
  switch (input.type().id()) {
#define TYPE_DISPATCHER(enum_val, type)                         \
  case DataTypeId::enum_val: {                                  \
    ret = ValueConverter<T>::cast(input.GetValue<type>(), val); \
    break;                                                      \
  }
    FOR_EACH_NUMERIC_DATA_TYPE(TYPE_DISPATCHER)
#undef TYPE_DISPATCHER
  case DataTypeId::kVarchar:
    ret = ValueConverter<T>::cast(StringValue::Get(input), val);
    break;
  default: {
    if constexpr (std::is_same_v<T, int64_t>) {
      if (input.type().id() == DataTypeId::kDate) {
        ret = ValueConverter<T>::cast(input.GetValue<date_t>(), val);
      } else if (input.type().id() == DataTypeId::kTimestampMs) {
        ret = ValueConverter<T>::cast(input.GetValue<timestamp_ms_t>(), val);
      } else if (input.type().id() == DataTypeId::kInterval) {
        ret = ValueConverter<T>::cast(input.GetValue<interval_t>(), val);
      } else {
        THROW_CONVERSION_EXCEPTION("Unsupported type for casting.");
      }
    } else {
      THROW_CONVERSION_EXCEPTION("Unsupported type for casting.");
    }
  } break;
  }

  if (ret) {
    return Value::CreateValue<T>(val);
  } else {
    LOG(ERROR) << "Failed to cast value: " << input.to_string();
    THROW_OVERFLOW_EXCEPTION("Failed to cast value.");
  }
  return Value(ValueConverter<T>::type());
}

template <>
inline Value performCast<timestamp_t>(const Value& input) {
  neug::DateTime val;
  bool ret = false;
  if (input.type().id() == DataTypeId::kVarchar) {
    ret = ValueConverter<timestamp_ms_t>::cast(StringValue::Get(input), val);
  } else if (input.type().id() == DataTypeId::kDate) {
    ret = ValueConverter<timestamp_ms_t>::cast(input.GetValue<date_t>(), val);
  } else {
    THROW_CONVERSION_EXCEPTION(
        "Only string type is supported for casting to DateTime.");
  }
  if (ret) {
    return Value::CreateValue<timestamp_ms_t>(val);
  } else {
    THROW_CONVERSION_EXCEPTION("Failed to cast value to DateTime.");
  }
  return Value(DataType::TIMESTAMP_MS);
}

template <>
inline Value performCast<neug::Date>(const Value& input) {
  neug::Date val;
  bool ret = false;
  if (input.type().id() == DataTypeId::kVarchar) {
    ret = ValueConverter<neug::Date>::cast(StringValue::Get(input), val);
  } else if (input.type().id() == DataTypeId::kTimestampMs) {
    ret =
        ValueConverter<neug::Date>::cast(input.GetValue<timestamp_ms_t>(), val);
  } else {
    THROW_CONVERSION_EXCEPTION(
        "Only string type is supported for casting to Date.");
  }
  if (ret) {
    return Value::CreateValue<neug::Date>(val);
  } else {
    THROW_CONVERSION_EXCEPTION("Failed to cast value to Date.");
  }
  return Value(DataType(DataTypeId::kDate));
}

Value performCastToString(const Value& input);

void encode_value(const Value& val, Encoder& encoder);

}  // namespace execution
}  // namespace neug