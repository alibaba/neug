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

#include "neug/execution/common/types/value.h"
#include "neug/utils/encoder.h"
#include "neug/utils/exception/exception.h"
#include "neug/utils/property/property.h"

namespace neug {
namespace execution {
enum class ExtraValueInfoType : uint8_t {
  INVALID_TYPE_INFO = 0,
  STRING_VALUE_INFO = 1,
  NESTED_VALUE_INFO = 2,
  PATH_VALUE_INFO = 3,
};

struct ExtraValueInfo {
  explicit ExtraValueInfo(ExtraValueInfoType type) : type(type) {}
  virtual ~ExtraValueInfo() {}
  ExtraValueInfoType type;

 public:
  bool Equals(ExtraValueInfo* other_p) const {
    if (!other_p) {
      return false;
    }
    if (type != other_p->type) {
      return false;
    }
    return EqualsInternal(other_p);
  }

  template <class T>
  T& Get() {
    if (type != T::TYPE) {
      throw std::runtime_error("ExtraValueInfo type mismatch");
    }
    return (T&) *this;
  }

 protected:
  virtual bool EqualsInternal(ExtraValueInfo* other_p) const { return true; }
};

struct StringValueInfo : public ExtraValueInfo {
  static constexpr const ExtraValueInfoType TYPE =
      ExtraValueInfoType::STRING_VALUE_INFO;

 public:
  explicit StringValueInfo(const std::string& str_p)
      : ExtraValueInfo(TYPE), str(str_p) {}

  const std::string& GetString() const { return str; }

 protected:
  bool EqualsInternal(ExtraValueInfo* other_p) const override {
    return other_p->Get<StringValueInfo>().str == str;
  }

  std::string str;
};

struct NestedValueInfo : public ExtraValueInfo {
  static constexpr const ExtraValueInfoType TYPE =
      ExtraValueInfoType::NESTED_VALUE_INFO;

 public:
  NestedValueInfo() : ExtraValueInfo(ExtraValueInfoType::NESTED_VALUE_INFO) {}
  explicit NestedValueInfo(std::vector<Value> values_p)
      : ExtraValueInfo(ExtraValueInfoType::NESTED_VALUE_INFO),
        values(std::move(values_p)) {}

  const std::vector<Value>& GetValues() { return values; }

 protected:
  bool EqualsInternal(ExtraValueInfo* other_p) const override {
    return other_p->Get<NestedValueInfo>().values == values;
  }

  std::vector<Value> values;
};

struct PathValueInfo : public ExtraValueInfo {
  static constexpr const ExtraValueInfoType TYPE =
      ExtraValueInfoType::PATH_VALUE_INFO;

 public:
  explicit PathValueInfo(Path path_p)
      : ExtraValueInfo(TYPE), path(std::move(path_p)) {}
  const Path& GetPath() const { return path; }

 protected:
  bool EqualsInternal(ExtraValueInfo* other_p) const override {
    return other_p->Get<PathValueInfo>().path == path;
  }
  Path path;
};

Value::Value(DataType type) : type_(std::move(type)), is_null_(true) {}

Value::Value(const Value& other)
    : type_(other.type_),
      is_null_(other.is_null_),
      value_(other.value_),
      value_info_(other.value_info_) {}

Value::Value(Value&& other) noexcept
    : type_(std::move(other.type_)),
      is_null_(other.is_null_),
      value_(other.value_),
      value_info_(std::move(other.value_info_)) {}

Value& Value::operator=(const Value& other) {
  if (this == &other) {
    return *this;
  }
  type_ = other.type_;
  is_null_ = other.is_null_;
  value_ = other.value_;
  value_info_ = other.value_info_;
  return *this;
}

Value& Value::operator=(Value&& other) noexcept {
  type_ = std::move(other.type_);
  is_null_ = other.is_null_;
  value_ = other.value_;
  value_info_ = std::move(other.value_info_);
  return *this;
}

Value Value::BOOLEAN(bool value) {
  Value result(DataType::BOOLEAN);
  result.value_.boolean = value;
  result.is_null_ = false;
  return result;
}

Value Value::INT32(int32_t value) {
  Value result(DataType::INT32);
  result.value_.integer = value;
  result.is_null_ = false;
  return result;
}

Value Value::INT64(int64_t value) {
  Value result(DataType::INT64);
  result.value_.bigint = value;
  result.is_null_ = false;
  return result;
}

Value Value::UINT32(uint32_t value) {
  Value result(DataType::UINT32);
  result.value_.uinteger = value;
  result.is_null_ = false;
  return result;
}

Value Value::UINT64(uint64_t value) {
  Value result(DataType::UINT64);
  result.value_.ubigint = value;
  result.is_null_ = false;
  return result;
}

Value Value::DATE(date_t date) {
  Value result(DataType::DATE);
  result.value_.date = date;
  result.is_null_ = false;
  return result;
}

Value Value::TIMESTAMPMS(timestamp_ms_t timestamp) {
  Value result(DataType::TIMESTAMP_MS);
  result.value_.timestamp_ms = timestamp;
  result.is_null_ = false;
  return result;
}

Value Value::INTERVAL(interval_t interval) {
  Value result(DataType::INTERVAL);
  result.value_.interval = interval;
  result.is_null_ = false;
  return result;
}

Value Value::FLOAT(float value) {
  Value result(DataType::FLOAT);
  result.value_.float_ = value;
  result.is_null_ = false;
  return result;
}

Value Value::DOUBLE(double value) {
  Value result(DataType::DOUBLE);
  result.value_.double_ = value;
  result.is_null_ = false;
  return result;
}

Value Value::LIST(const DataType& child_type, std::vector<Value>&& values) {
  Value result(DataType::List(child_type));
  result.value_info_ = std::make_shared<NestedValueInfo>(std::move(values));
  result.is_null_ = false;
  return result;
}

Value Value::LIST(std::vector<Value>&& values) {
  if (values.empty()) {
    throw std::runtime_error("Cannot create LIST Value with empty values");
  }
  const auto& type = values[0].type();
  return Value::LIST(type, std::move(values));
}

Value Value::STRUCT(const DataType& type, std::vector<Value>&& struct_values) {
  Value result(type);
  result.value_info_ =
      std::make_shared<NestedValueInfo>(std::move(struct_values));
  result.is_null_ = false;
  return result;
}

Value Value::STRUCT(std::vector<Value>&& values) {
  std::vector<DataType> child_types;
  for (const auto& val : values) {
    child_types.push_back(val.type());
  }
  DataType struct_type = DataType::Struct(child_types);
  return Value::STRUCT(struct_type, std::move(values));
}

Value Value::STRING(const std::string& str) {
  Value result(DataType::VARCHAR);
  result.value_info_ = std::make_shared<StringValueInfo>(str);
  result.is_null_ = false;
  return result;
}

Value Value::VERTEX(const vertex_t& vertex) {
  Value result(DataType::VERTEX);
  result.value_.vertex = vertex;
  result.is_null_ = false;
  return result;
}

Value Value::EDGE(const edge_t& edge) {
  Value result(DataType::EDGE);
  result.value_.edge = edge;
  result.is_null_ = false;
  return result;
}

Value Value::PATH(const Path& path) {
  Value result(DataType::PATH);
  result.value_info_ = std::make_shared<PathValueInfo>(path);
  result.is_null_ = false;
  return result;
}

const std::string& StringValue::Get(const Value& value) {
  if (value.IsNull()) {
    throw std::runtime_error("Cannot get string of null StringValue");
  }
  assert(value.type().id() == DataTypeId::kVarchar);
  return value.value_info_->Get<StringValueInfo>().GetString();
}

const std::vector<Value>& ListValue::GetChildren(const Value& value) {
  if (value.IsNull()) {
    throw std::runtime_error("Cannot get children of null ListValue");
  }
  assert(value.type().id() == DataTypeId::kList);
  return value.value_info_->Get<NestedValueInfo>().GetValues();
}

const std::vector<Value>& StructValue::GetChildren(const Value& value) {
  if (value.IsNull()) {
    throw std::runtime_error("Cannot get children of null StructValue");
  }
  assert(value.type().id() == DataTypeId::kStruct);
  return value.value_info_->Get<NestedValueInfo>().GetValues();
}

const Path& PathValue::Get(const Value& value) {
  if (value.IsNull()) {
    throw std::runtime_error("Cannot get path of null PathValue");
  }
  assert(value.type().id() == DataTypeId::kPath);
  return value.value_info_->Get<PathValueInfo>().GetPath();
}

bool Value::operator==(const Value& other) const {
  if (type_ != other.type_ || is_null_ != other.is_null_) {
    return false;
  }
  if (is_null_ || other.is_null_) {
    return false;
  }
  return ApplyComparisonOp<EqOp>(*this, other);
}

bool Value::operator<(const Value& other) const {
  if (type_ != other.type_) {
    return false;
  }
  if (is_null_ || other.is_null_) {
    return false;
  }
  return ApplyComparisonOp<LtOp>(*this, other);
}
//===--------------------------------------------------------------------===//
// CreateValue
//===--------------------------------------------------------------------===//
template <>
Value Value::CreateValue(bool value) {
  return Value::BOOLEAN(value);
}

template <>
Value Value::CreateValue(int32_t value) {
  return Value::INT32(value);
}

template <>
Value Value::CreateValue(int64_t value) {
  return Value::INT64(value);
}

template <>
Value Value::CreateValue(uint32_t value) {
  return Value::UINT32(value);
}

template <>
Value Value::CreateValue(uint64_t value) {
  return Value::UINT64(value);
}

template <>
Value Value::CreateValue(date_t value) {
  return Value::DATE(value);
}

template <>
Value Value::CreateValue(timestamp_ms_t value) {
  return Value::TIMESTAMPMS(value);
}

template <>
Value Value::CreateValue(const char* value) {
  return Value::STRING(std::string(value));
}

template <>
Value Value::CreateValue(
    std::string value) {  // NOLINT: required for templating
  return Value::STRING(value);
}

template <>
Value Value::CreateValue(float value) {
  return Value::FLOAT(value);
}

template <>
Value Value::CreateValue(double value) {
  return Value::DOUBLE(value);
}

template <>
Value Value::CreateValue(interval_t value) {
  return Value::INTERVAL(value);
}

template <>
Value Value::CreateValue(vertex_t value) {
  return Value::VERTEX(value);
}

template <>
Value Value::CreateValue(edge_t value) {
  return Value::EDGE(value);
}

template <>
Value Value::CreateValue(Value value) {
  return value;
}

template <>
bool Value::GetValue() const {
  return value_.boolean;
}

template <>
int32_t Value::GetValue() const {
  return value_.integer;
}

template <>
int64_t Value::GetValue() const {
  return value_.bigint;
}

template <>
uint32_t Value::GetValue() const {
  return value_.uinteger;
}

template <>
uint64_t Value::GetValue() const {
  return value_.ubigint;
}

template <>
std::string Value::GetValue() const {
  return value_info_->Get<StringValueInfo>().GetString();
}

template <>
float Value::GetValue() const {
  return value_.float_;
}

template <>
double Value::GetValue() const {
  return value_.double_;
}

template <>
date_t Value::GetValue() const {
  return value_.date;
}

template <>
timestamp_ms_t Value::GetValue() const {
  return value_.timestamp_ms;
}

template <>
interval_t Value::GetValue() const {
  return value_.interval;
}

template <>
vertex_t Value::GetValue() const {
  return value_.vertex;
}

template <>
edge_t Value::GetValue() const {
  return value_.edge;
}

template <>
Value Value::GetValue() const {
  return Value(*this);
}

Value Value::operator+(const Value& rhs) const {
  if (IsNull() || rhs.IsNull()) {
    return Value(type_);
  }
  if (rhs.type().id() == DataTypeId::kInterval) {
    if (type_.id() == DataTypeId::kDate) {
      date_t date = this->GetValue<date_t>();
      interval_t interval = rhs.GetValue<interval_t>();
      return Value::DATE(date + interval);
    } else if (type_.id() == DataTypeId::kTimestampMs) {
      timestamp_ms_t timestamp = this->GetValue<timestamp_ms_t>();
      interval_t interval = rhs.GetValue<interval_t>();
      return Value::TIMESTAMPMS(timestamp + interval);
    } else {
      throw std::runtime_error(
          "Invalid types for Date/Timestamp + Interval operation");
    }
  }
  if (type_.id() == DataTypeId::kInterval) {
    if (rhs.type().id() == DataTypeId::kDate) {
      date_t date = rhs.GetValue<date_t>();
      interval_t interval = this->GetValue<interval_t>();
      return Value::DATE(date + interval);
    } else if (rhs.type().id() == DataTypeId::kTimestampMs) {
      timestamp_ms_t timestamp = rhs.GetValue<timestamp_ms_t>();
      interval_t interval = this->GetValue<interval_t>();
      return Value::TIMESTAMPMS(timestamp + interval);
    } else {
      throw std::runtime_error(
          "Invalid types for Interval + Date/Timestamp operation");
    }
  }
  return ApplyArithmeticOp<AddOp>(*this, rhs);
}

Value Value::operator-(const Value& rhs) const {
  if (IsNull() || rhs.IsNull()) {
    return Value(type_);
  }
  if (rhs.type().id() == DataTypeId::kInterval) {
    if (type_.id() == DataTypeId::kDate) {
      date_t date = this->GetValue<date_t>();
      interval_t interval = rhs.GetValue<interval_t>();
      return Value::DATE(date - interval);
    } else if (type_.id() == DataTypeId::kTimestampMs) {
      timestamp_ms_t timestamp = this->GetValue<timestamp_ms_t>();
      interval_t interval = rhs.GetValue<interval_t>();
      return Value::TIMESTAMPMS(timestamp - interval);
    } else {
      throw std::runtime_error(
          "Invalid types for Date/Timestamp - Interval operation");
    }
  }
  if (type_.id() == DataTypeId::kDate && rhs.type().id() == DataTypeId::kDate) {
    date_t left_date = this->GetValue<date_t>();
    date_t right_date = rhs.GetValue<date_t>();
    interval_t interval = left_date - right_date;
    return Value::INTERVAL(interval);
  }

  if (type_.id() == DataTypeId::kTimestampMs &&
      rhs.type().id() == DataTypeId::kTimestampMs) {
    timestamp_ms_t left_timestamp = this->GetValue<timestamp_ms_t>();
    timestamp_ms_t right_timestamp = rhs.GetValue<timestamp_ms_t>();
    interval_t interval = left_timestamp - right_timestamp;
    return Value::INTERVAL(interval);
  }
  return ApplyArithmeticOp<SubOp>(*this, rhs);
}

Value Value::operator*(const Value& rhs) const {
  if (IsNull() || rhs.IsNull()) {
    return Value(type_);
  }
  return ApplyArithmeticOp<MulOp>(*this, rhs);
}

Value Value::operator/(const Value& rhs) const {
  if (IsNull() || rhs.IsNull()) {
    return Value(type_);
  }
  return ApplyArithmeticOp<DivOp>(*this, rhs);
}

Value Value::operator%(const Value& rhs) const {
  if (IsNull() || rhs.IsNull()) {
    return Value(type_);
  }
  return ApplyArithmeticOp<ModOp>(*this, rhs);
}

std::string Value::to_string() const {
  if (IsNull()) {
    return "NULL";
  }
  if (type_.id() == DataTypeId::kList) {
    const auto& children = ListValue::GetChildren(*this);
    std::string result = "[";
    for (size_t i = 0; i < children.size(); ++i) {
      result += children[i].to_string();
      if (i != children.size() - 1) {
        result += ", ";
      }
    }
    return result + "]";
  } else if (type_.id() == DataTypeId::kStruct) {
    const auto& children = StructValue::GetChildren(*this);
    std::string result = "(";
    for (size_t i = 0; i < children.size(); ++i) {
      result += children[i].to_string();
      if (i != children.size() - 1) {
        result += ", ";
      }
    }
    return result + ")";
  } else if (type_.id() == DataTypeId::kVertex) {
    const auto& vertex = this->GetValue<vertex_t>();
    return vertex.to_string();
  } else if (type_.id() == DataTypeId::kEdge) {
    const auto& edge = this->GetValue<edge_t>();
    return edge.to_string();
  }
  return StringValue::Get(performCastToString(*this));
}

Value Value::FromJson(const rapidjson::Value& json_value,
                      const DataType& type) {
  if (json_value.IsNull()) {
    return Value(type);
  }
  switch (type.id()) {
  case DataTypeId::kUnknown: {
    return Value(type);
  }
  case DataTypeId::kBoolean: {
    // If the value is 1/0, treat it as boolean
    if (json_value.IsInt()) {
      return execution::Value::BOOLEAN(json_value.GetInt() != 0);
    }
    return execution::Value::BOOLEAN(json_value.GetBool());
  }
  case DataTypeId::kDate: {
    if (json_value.IsInt64()) {
      return execution::Value::DATE(Date(json_value.GetInt64()));
    } else if (json_value.IsString()) {
      return execution::Value::DATE(Date(json_value.GetString()));
    } else {
      THROW_INVALID_ARGUMENT_EXCEPTION(
          "Expected an (u)int/string for Date type");
    }
  }
  case DataTypeId::kDouble: {
    return execution::Value::DOUBLE(json_value.GetDouble());
  }
  case DataTypeId::kFloat: {
    return execution::Value::FLOAT(json_value.GetFloat());
  }
  case DataTypeId::kInt32: {
    return execution::Value::INT32(json_value.GetInt());
  }
  case DataTypeId::kInt64: {
    return execution::Value::INT64(json_value.GetInt64());
  }
  case DataTypeId::kUInt32: {
    return execution::Value::UINT32(json_value.GetUint());
  }
  case DataTypeId::kUInt64: {
    return execution::Value::UINT64(json_value.GetUint64());
  }
  case DataTypeId::kVarchar: {
    return execution::Value::STRING(json_value.GetString());
  }
  case DataTypeId::kTimestampMs: {
    if (json_value.IsInt64()) {
      return execution::Value::TIMESTAMPMS(
          execution::timestamp_ms_t(json_value.GetInt64()));
    } else if (json_value.IsString()) {
      return execution::Value::TIMESTAMPMS(
          execution::timestamp_ms_t(std::stoll(json_value.GetString())));
    } else {
      THROW_INVALID_ARGUMENT_EXCEPTION(
          "Expected an (u)int64/string for TimestampMs type");
    }
  }
  case DataTypeId::kList: {
    std::vector<execution::Value> values;
    if (!json_value.IsArray()) {
      return execution::Value::LIST(DataType::UNKNOWN, std::move(values));
    }
    const auto list = json_value.GetArray();
    auto child_type = ListType::GetChildType(type);
    for (auto item = list.begin(); item != list.end(); ++item) {
      values.emplace_back(FromJson(*item, child_type));
    }
    return execution::Value::LIST(child_type, std::move(values));
  }
  default:
    THROW_NOT_IMPLEMENTED_EXCEPTION(
        "Deserialization for parameter type " +
        std::to_string(static_cast<int>(type.id())) + " is not supported.");
  }
}

Value Value::FromJson(const std::string& json_str, const DataType& type) {
  rapidjson::Document document;
  document.Parse(json_str.c_str());
  if (document.HasParseError()) {
    THROW_INVALID_ARGUMENT_EXCEPTION("Failed to parse JSON string: " +
                                     json_str);
  }
  return Value::FromJson(document, type);
}

rapidjson::Value Value::ToJson(const Value& value,
                               rapidjson::Document::AllocatorType& allocator) {
  if (value.IsNull()) {
    return rapidjson::Value(rapidjson::kNullType);
  }
  auto type_id = value.type().id();
  switch (type_id) {
  case neug::DataTypeId::kVarchar: {
    return rapidjson::Value(value.GetValue<std::string>().c_str(), allocator);
  }
#define TYPE_DISPATCHER(type_enum, cpp_type)                \
  case neug::DataTypeId::type_enum: {                       \
    return rapidjson::Value(                                \
        static_cast<cpp_type>(value.GetValue<cpp_type>())); \
  }
    FOR_EACH_NUMERIC_DATA_TYPE(TYPE_DISPATCHER)
#undef TYPE_DISPATCHER
  case neug::DataTypeId::kList: {
    rapidjson::Value list_doc(rapidjson::kArrayType);
    const auto& list = execution::ListValue::GetChildren(value);
    for (size_t i = 0; i < list.size(); ++i) {
      list_doc.PushBack(ToJson(list[i], allocator), allocator);
    }
    return list_doc;
  }
  case neug::DataTypeId::kDate: {
    return rapidjson::Value(value.GetValue<date_t>().to_string().c_str(),
                            allocator);
  }
  case neug::DataTypeId::kTimestampMs: {
    return rapidjson::Value(
        value.GetValue<timestamp_ms_t>().to_string().c_str(), allocator);
  }
  default: {
    THROW_NOT_IMPLEMENTED_EXCEPTION("Serialization for parameter type " +
                                    std::to_string(static_cast<int>(type_id)) +
                                    " is not supported.");
    return rapidjson::Value();  // unreachable
  }
  }
  return rapidjson::Value();  // unreachable
}

Property value_to_property(const Value& value) {
  switch (value.type().id()) {
  case DataTypeId::kBoolean:
    return Property::from_bool(value.GetValue<bool>());
  case DataTypeId::kInt64:
    return Property::from_int64(value.GetValue<int64_t>());
  case DataTypeId::kUInt64:
    return Property::from_uint64(value.GetValue<uint64_t>());
  case DataTypeId::kInt32:
    return Property::from_int32(value.GetValue<int32_t>());
  case DataTypeId::kUInt32:
    return Property::from_uint32(value.GetValue<uint32_t>());
  case DataTypeId::kFloat:
    return Property::from_float(value.GetValue<float>());
  case DataTypeId::kDouble:
    return Property::from_double(value.GetValue<double>());
  case DataTypeId::kVarchar:
    return Property::from_string_view(StringValue::Get(value));
  case DataTypeId::kDate:
    return Property::from_date(value.GetValue<date_t>());
  case DataTypeId::kTimestampMs:
    return Property::from_datetime(value.GetValue<timestamp_ms_t>());
  case DataTypeId::kInterval:
    return Property::from_interval(value.GetValue<interval_t>());
  default:
    THROW_NOT_SUPPORTED_EXCEPTION(
        "Unexpected type: " +
        std::to_string(static_cast<int>(value.type().id())));
  }
}

Value property_to_value(const Property& property) {
  switch (property.type()) {
  case DataTypeId::kBoolean:
    return Value::BOOLEAN(property.as_bool());
  case DataTypeId::kInt64:
    return Value::INT64(property.as_int64());
  case DataTypeId::kUInt64:
    return Value::UINT64(property.as_uint64());
  case DataTypeId::kInt32:
    return Value::INT32(property.as_int32());
  case DataTypeId::kUInt32:
    return Value::UINT32(property.as_uint32());
  case DataTypeId::kFloat:
    return Value::FLOAT(property.as_float());
  case DataTypeId::kDouble:
    return Value::DOUBLE(property.as_double());
  case DataTypeId::kVarchar:
    return Value::STRING(std::string(property.as_string_view()));
  case DataTypeId::kDate:
    return Value::DATE(property.as_date());
  case DataTypeId::kTimestampMs:
    return Value::TIMESTAMPMS(property.as_datetime());
  case DataTypeId::kInterval:
    return Value::INTERVAL(property.as_interval());
  default:
    THROW_NOT_SUPPORTED_EXCEPTION(
        "Unexpected property type: " + std::to_string(property.type()) +
        "value: " + property.to_string());
  }
}

void encode_value(const Value& val, Encoder& encoder) {
  const auto& type = val.type();
  if (val.IsNull()) {
    encoder.put_int(-1);
    return;
  }
  if (type.id() == DataTypeId::kInt64) {
    encoder.put_long(val.GetValue<int64_t>());
  } else if (type.id() == DataTypeId::kVarchar) {
    encoder.put_string_view(StringValue::Get(val));
  } else if (type.id() == DataTypeId::kInt32) {
    encoder.put_int(val.GetValue<int32_t>());
  } else if (type.id() == DataTypeId::kUInt32) {
    encoder.put_int(val.GetValue<uint32_t>());
  } else if (type.id() == DataTypeId::kUInt64) {
    encoder.put_long(val.GetValue<uint64_t>());
  } else if (type.id() == DataTypeId::kVertex) {
    const auto& v = val.GetValue<vertex_t>();
    encoder.put_byte(v.label_);
    encoder.put_int(v.vid_);
  } else if (type.id() == DataTypeId::kEdge) {
    const auto& [label, src, dst, prop, dir] = val.GetValue<edge_t>();

    encoder.put_byte(label.src_label);
    encoder.put_byte(label.dst_label);
    encoder.put_byte(label.edge_label);
    encoder.put_int(src);
    encoder.put_int(dst);
    encoder.put_byte(dir == Direction::kOut ? 1 : 0);

  } else if (type.id() == DataTypeId::kBoolean) {
    encoder.put_byte(val.IsTrue() ? 1 : 0);
  } else if (type.id() == DataTypeId::kList) {
    const auto& vals = ListValue::GetChildren(val);
    encoder.put_int(vals.size());

    for (const auto& v : vals) {
      encode_value(v, encoder);
    }
  } else if (type.id() == DataTypeId::kStruct) {
    const auto& vals = StructValue::GetChildren(val);
    encoder.put_int(vals.size());
    for (const auto& v : vals) {
      encode_value(v, encoder);
    }
  } else if (type.id() == DataTypeId::kFloat) {
    encoder.put_float(val.GetValue<float>());
  } else if (type.id() == DataTypeId::kDouble) {
    encoder.put_double(val.GetValue<double>());
  } else if (type.id() == DataTypeId::kPath) {
    Path p = PathValue::Get(val);
    encoder.put_int(p.length() + 1);
    auto nodes = p.nodes();
    for (size_t i = 0; i < nodes.size(); ++i) {
      encoder.put_byte(nodes[i].label_);
      encoder.put_int(nodes[i].vid_);
    }
  } else if (type.id() == DataTypeId::kTimestampMs) {
    encoder.put_long(val.GetValue<timestamp_ms_t>().milli_second);
  } else if (type.id() == DataTypeId::kDate) {
    encoder.put_long(val.GetValue<date_t>().to_timestamp());
  } else if (type.id() == DataTypeId::kInterval) {
    encoder.put_long(val.GetValue<interval_t>().to_mill_seconds());
  } else {
    THROW_RUNTIME_ERROR("RTAny::encode_sig not support for " +
                        std::to_string(static_cast<int>(type.id())));
  }
}

Value performCastToString(const Value& input) {
  std::string ret{};
  switch (input.type().id()) {
#define TYPE_DISPATCHER(enum_val, type)           \
  case DataTypeId::enum_val: {                    \
    ret = std::to_string(input.GetValue<type>()); \
    break;                                        \
  }
    FOR_EACH_NUMERIC_DATA_TYPE(TYPE_DISPATCHER)
#undef TYPE_DISPATCHER
  case DataTypeId::kVarchar: {
    ret = StringValue::Get(input);
    break;
  }
  case DataTypeId::kTimestampMs: {
    ret = input.GetValue<timestamp_ms_t>().to_string();
    break;
  }
  case DataTypeId::kDate: {
    ret = input.GetValue<date_t>().to_string();
    break;
  }
  case DataTypeId::kInterval: {
    ret = input.GetValue<interval_t>().to_string();
    break;
  }
  default: {
    THROW_CONVERSION_EXCEPTION("Unsupported type for casting to string.");
  }
  }
  return Value::STRING(ret);
}

}  // namespace execution
}  // namespace neug