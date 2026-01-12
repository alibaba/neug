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

#include "neug/utils/runtime/rt_any.h"

#include <arrow/type.h>
#include <assert.h>
#include <glog/logging.h>
#include "neug/generated/proto/plan/basic_type.pb.h"
#include "neug/generated/proto/plan/results.pb.h"
#include "neug/generated/proto/plan/type.pb.h"

#include <cstdint>
#include <string>

#include "neug/utils/app_utils.h"
#include "neug/utils/exception/exception.h"

namespace gs {

namespace runtime {

DataType parse_from_data_type(const ::common::DataType& ddt) {
  switch (ddt.item_case()) {
  case ::common::DataType::kPrimitiveType: {
    const ::common::PrimitiveType pt = ddt.primitive_type();
    switch (pt) {
    case ::common::PrimitiveType::DT_SIGNED_INT32:
      return DataType(DataTypeId::INTEGER);
    case ::common::PrimitiveType::DT_UNSIGNED_INT32:
      return DataType(DataTypeId::UINTEGER);
    case ::common::PrimitiveType::DT_UNSIGNED_INT64:
      return DataType(DataTypeId::UBIGINT);
    case ::common::PrimitiveType::DT_SIGNED_INT64:
      return DataType(DataTypeId::BIGINT);
    case ::common::PrimitiveType::DT_FLOAT:
      return DataType(DataTypeId::FLOAT);
    case ::common::PrimitiveType::DT_DOUBLE:
      return DataType(DataTypeId::DOUBLE);
    case ::common::PrimitiveType::DT_BOOL:
      return DataType(DataTypeId::BOOLEAN);
    case ::common::PrimitiveType::DT_ANY:
      return DataType(DataTypeId::UNKNOWN);
    default:
      THROW_NOT_SUPPORTED_EXCEPTION("unrecognized primitive type - " +
                                    std::to_string(pt));
      break;
    }
  }
  case ::common::DataType::kString:
    return DataType(DataTypeId::VARCHAR);
  case ::common::DataType::kTemporal: {
    if (ddt.temporal().item_case() == ::common::Temporal::kDate32) {
      return DataType(DataTypeId::DATE);
    } else if (ddt.temporal().item_case() == ::common::Temporal::kDateTime) {
      return DataType(DataTypeId::TIMESTAMP_MS);
    } else if (ddt.temporal().item_case() == ::common::Temporal::kDate) {
      return DataType(DataTypeId::DATE);
    } else if (ddt.temporal().item_case() == ::common::Temporal::kInterval) {
      return DataType(DataTypeId::INTERVAL);
    } else if (ddt.temporal().item_case() == ::common::Temporal::kTimestamp) {
      return DataType(DataTypeId::TIMESTAMP_MS);
    } else {
      THROW_NOT_SUPPORTED_EXCEPTION("unrecognized temporal type - " +
                                    ddt.temporal().DebugString());
    }
  }
  case ::common::DataType::kArray: {
    const auto& element_type = ddt.array().component_type();
    auto data_type = parse_from_data_type(element_type);
    return DataType(DataTypeId::LIST,
                    std::make_shared<ListTypeInfo>(data_type));
  }
  case ::common::DataType::kTuple: {
    const auto& component_types = ddt.tuple().component_types();
    std::vector<DataType> data_types;
    for (int i = 0; i < component_types.size(); ++i) {
      data_types.push_back(parse_from_data_type(component_types.Get(i)));
    }
    std::shared_ptr<ExtraTypeInfo> type_info =
        std::make_shared<StructTypeInfo>(data_types);
    return DataType(DataTypeId::STRUCT, type_info);
  }
  default:
    THROW_NOT_SUPPORTED_EXCEPTION("unrecognized data type - " +
                                  ddt.DebugString());
    break;
  }

  return DataType(DataTypeId::UNKNOWN);
}

DataType parse_from_ir_data_type(const ::common::IrDataType& dt) {
  switch (dt.type_case()) {
  case ::common::IrDataType::TypeCase::kDataType: {
    const ::common::DataType ddt = dt.data_type();
    return parse_from_data_type(ddt);
  }
  case ::common::IrDataType::TypeCase::kGraphType: {
    const ::common::GraphDataType gdt = dt.graph_type();
    switch (gdt.element_opt()) {
    case ::common::GraphDataType_GraphElementOpt::
        GraphDataType_GraphElementOpt_VERTEX:
      return DataType(DataTypeId::VERTEX);
    case ::common::GraphDataType_GraphElementOpt::
        GraphDataType_GraphElementOpt_EDGE:
      return DataType(DataTypeId::EDGE);
    case ::common::GraphDataType_GraphElementOpt::
        GraphDataType_GraphElementOpt_PATH:
      return DataType(DataTypeId::PATH);
    default:
      THROW_NOT_SUPPORTED_EXCEPTION("unrecognized graph data type - " +
                                    gdt.DebugString());
      break;
    }
  } break;
  default:
    break;
  }
  return DataType(DataTypeId::UNKNOWN);
}

RTAny List::get(size_t idx) const { return impl_->get(idx); }

DataType Set::elem_type() const { return impl_->type(); }

void Set::insert(const RTAny& val) { impl_->insert(val); }
bool Set::operator<(const Set& p) const { return *impl_ < *(p.impl_); }
bool Set::operator==(const Set& p) const { return *(impl_) == *(p.impl_); }
bool Set::exists(const RTAny& val) const { return impl_->exists(val); }
std::vector<RTAny> Set::values() const { return impl_->values(); }
size_t Set::size() const { return impl_->size(); }
std::string Set::to_string() const {
  auto vals = impl_->values();
  std::stringstream ss;
  ss << "{";
  for (size_t i = 0; i < vals.size(); i++) {
    if (i != 0) {
      ss << ", ";
    }
    ss << vals[i].to_string();
  }
  ss << "}";
  return ss.str();
}

DataType List::elem_type() const { return impl_->type(); }
std::string List::to_string() const {
  std::stringstream ss;
  ss << "[";
  for (size_t i = 0; i < size(); i++) {
    if (i != 0) {
      ss << ", ";
    }
    ss << get(i).to_string();
  }
  ss << "]";
  return ss.str();
}

RTAny Tuple::get(size_t idx) const { return impl_->get(idx); }
std::string Tuple::to_string() const {
  std::stringstream ss;
  ss << "(";
  for (size_t i = 0; i < size(); i++) {
    if (i != 0) {
      ss << ", ";
    }
    ss << get(i).to_string();
  }
  ss << ")";
  return ss.str();
}

DataType arrow_type_to_rt_type(const std::shared_ptr<arrow::DataType>& type) {
  if (type->Equals(arrow::int64())) {
    return DataType(DataTypeId::BIGINT);
  } else if (type->Equals(arrow::int32())) {
    return DataType(DataTypeId::INTEGER);
  } else if (type->Equals(arrow::uint32())) {
    return DataType(DataTypeId::UINTEGER);
  } else if (type->Equals(arrow::uint64())) {
    return DataType(DataTypeId::UBIGINT);
  } else if (type->Equals(arrow::float32())) {
    return DataType(DataTypeId::FLOAT);
  } else if (type->Equals(arrow::float64())) {
    return DataType(DataTypeId::DOUBLE);
  } else if (type->Equals(arrow::boolean())) {
    return DataType(DataTypeId::BOOLEAN);
  } else if (type->Equals(arrow::utf8()) || type->Equals(arrow::large_utf8())) {
    return DataType(DataTypeId::VARCHAR);
  } else if (type->Equals(arrow::date32())) {
    return DataType(DataTypeId::DATE);
  } else if (type->Equals(arrow::timestamp(arrow::TimeUnit::SECOND))) {
    return DataType(DataTypeId::TIMESTAMP_MS);
  } else if (type->Equals(arrow::timestamp(arrow::TimeUnit::MILLI))) {
    return DataType(DataTypeId::TIMESTAMP_MS);
  } else if (type->Equals(arrow::timestamp(arrow::TimeUnit::MICRO))) {
    return DataType(DataTypeId::TIMESTAMP_MS);
  } else if (type->Equals(arrow::timestamp(arrow::TimeUnit::NANO))) {
    return DataType(DataTypeId::TIMESTAMP_MS);
  } else {
    THROW_NOT_SUPPORTED_EXCEPTION("Unexpected arrow type: " + type->ToString());
  }
}

RTAny::RTAny() : type_(DataType(DataTypeId::UNKNOWN)) {}
RTAny::RTAny(DataType type) : type_(type) {}

RTAny::RTAny(const Property& val) {
  type_ = DataType(val.type());
  if (val.type() == DataTypeId::BOOLEAN) {
    value_.b_val = val.as_bool();
  } else if (val.type() == DataTypeId::BIGINT) {
    value_.i64_val = val.as_int64();
  } else if (val.type() == DataTypeId::INTEGER) {
    value_.i32_val = val.as_int32();
  } else if (val.type() == DataTypeId::UINTEGER) {
    value_.u32_val = val.as_uint32();
  } else if (val.type() == DataTypeId::UBIGINT) {
    value_.u64_val = val.as_uint64();
  } else if (val.type() == DataTypeId::FLOAT) {
    value_.f32_val = val.as_float();
  } else if (val.type() == DataTypeId::DOUBLE) {
    value_.f64_val = val.as_double();
  } else if (val.type() == DataTypeId::VARCHAR) {
    value_.str_val = val.as_string_view();
  } else if (val.type() == DataTypeId::EMPTY) {
  } else if (val.type() == DataTypeId::DATE) {
    value_.date_val = val.as_date();
  } else if (val.type() == DataTypeId::INTERVAL) {
    value_.interval_val = val.as_interval();
  } else if (val.type() == DataTypeId::TIMESTAMP_MS) {
    value_.dt_val = val.as_datetime();
  } else {
    THROW_NOT_SUPPORTED_EXCEPTION(
        "Unexpected property type: " + std::to_string(val.type()) +
        "value: " + val.to_string());
  }
}

RTAny::RTAny(const Path& p) {
  type_ = DataType(DataTypeId::PATH);
  value_.p = p;
}

RTAny::RTAny(const RTAny& rhs) : type_(rhs.type_) {
  if (type_.id() == DataTypeId::BOOLEAN) {
    value_.b_val = rhs.value_.b_val;
  } else if (type_.id() == DataTypeId::BIGINT) {
    value_.i64_val = rhs.value_.i64_val;
  } else if (type_.id() == DataTypeId::UBIGINT) {
    value_.u64_val = rhs.value_.u64_val;
  } else if (type_.id() == DataTypeId::INTEGER) {
    value_.i32_val = rhs.value_.i32_val;
  } else if (type_.id() == DataTypeId::UINTEGER) {
    value_.u32_val = rhs.value_.u32_val;
  } else if (type_.id() == DataTypeId::VERTEX) {
    value_.vertex = rhs.value_.vertex;
  } else if (type_.id() == DataTypeId::EDGE) {
    value_.edge = rhs.value_.edge;
  } else if (type_.id() == DataTypeId::VARCHAR) {
    value_.str_val = rhs.value_.str_val;
  } else if (type_.id() == DataTypeId::SQLNULL) {
    // do nothing
  } else if (type_.id() == DataTypeId::STRUCT) {
    value_.t = rhs.value_.t;
  } else if (type_.id() == DataTypeId::LIST) {
    value_.list = rhs.value_.list;
  } else if (type_.id() == DataTypeId::FLOAT) {
    value_.f32_val = rhs.value_.f32_val;
  } else if (type_.id() == DataTypeId::DOUBLE) {
    value_.f64_val = rhs.value_.f64_val;
  } else if (type_.id() == DataTypeId::DATE) {
    value_.date_val = rhs.value_.date_val;
  } else if (type_.id() == DataTypeId::TIMESTAMP_MS) {
    value_.dt_val = rhs.value_.dt_val;
  } else if (type_.id() == DataTypeId::INTERVAL) {
    value_.interval_val = rhs.value_.interval_val;
  } else if (type_.id() == DataTypeId::PATH) {
    value_.p = rhs.value_.p;
  } else {
    THROW_NOT_SUPPORTED_EXCEPTION("Unexpected RTAny type: " +
                                  std::to_string(static_cast<int>(type_.id())));
  }
}

RTAny& RTAny::operator=(const RTAny& rhs) {
  type_ = rhs.type_;
  if (type_.id() == DataTypeId::BOOLEAN) {
    value_.b_val = rhs.value_.b_val;
  } else if (type_.id() == DataTypeId::BIGINT) {
    value_.i64_val = rhs.value_.i64_val;
  } else if (type_.id() == DataTypeId::INTEGER) {
    value_.i32_val = rhs.value_.i32_val;
  } else if (type_.id() == DataTypeId::UINTEGER) {
    value_.u32_val = rhs.value_.u32_val;
  } else if (type_.id() == DataTypeId::VERTEX) {
    value_.vertex = rhs.value_.vertex;
  } else if (type_.id() == DataTypeId::EDGE) {
    value_.edge = rhs.value_.edge;
  } else if (type_.id() == DataTypeId::VARCHAR) {
    value_.str_val = rhs.value_.str_val;
  } else if (type_.id() == DataTypeId::STRUCT) {
    value_.t = rhs.value_.t;
  } else if (type_.id() == DataTypeId::LIST) {
    value_.list = rhs.value_.list;
  } else if (type_.id() == DataTypeId::FLOAT) {
    value_.f32_val = rhs.value_.f32_val;
  } else if (type_.id() == DataTypeId::DOUBLE) {
    value_.f64_val = rhs.value_.f64_val;
  } else if (type_.id() == DataTypeId::PATH) {
    value_.p = rhs.value_.p;
  } else if (type_.id() == DataTypeId::DATE) {
    value_.date_val = rhs.value_.date_val;
  } else if (type_.id() == DataTypeId::TIMESTAMP_MS) {
    value_.dt_val = rhs.value_.dt_val;
  } else {
    THROW_NOT_SUPPORTED_EXCEPTION("Unexpected RTAny type: " +
                                  std::to_string(static_cast<int>(type_.id())));
  }
  return *this;
}

DataType RTAny::type() const { return type_; }

Property RTAny::to_any() const {
  switch (type_.id()) {
  case DataTypeId::BOOLEAN:
    return Property::from_bool(value_.b_val);
  case DataTypeId::BIGINT:
    return Property::from_int64(value_.i64_val);
  case DataTypeId::UBIGINT:
    return Property::from_uint64(value_.u64_val);
  case DataTypeId::INTEGER:
    return Property::from_int32(value_.i32_val);
  case DataTypeId::UINTEGER:
    return Property::from_uint32(value_.u32_val);
  case DataTypeId::FLOAT:
    return Property::from_float(value_.f32_val);
  case DataTypeId::DOUBLE:
    return Property::from_double(value_.f64_val);
  case DataTypeId::VARCHAR:
    return Property::from_string_view(value_.str_val);
  case DataTypeId::DATE:
    return Property::from_date(value_.date_val);
  case DataTypeId::TIMESTAMP_MS:
    return Property::from_datetime(value_.dt_val);
  case DataTypeId::INTERVAL:
    return Property::from_interval(value_.interval_val);
  default:
    THROW_NOT_SUPPORTED_EXCEPTION("Unexpected RTAny type: " +
                                  std::to_string(static_cast<int>(type_.id())));
  }
}

RTAny RTAny::from_vertex(label_t l, vid_t v) {
  RTAny ret;
  ret.type_ = DataType(DataTypeId::VERTEX);
  ret.value_.vertex.label_ = l;
  ret.value_.vertex.vid_ = v;
  return ret;
}

RTAny RTAny::from_vertex(VertexRecord v) {
  RTAny ret;
  ret.type_ = DataType(DataTypeId::VERTEX);
  ret.value_.vertex = v;
  return ret;
}

RTAny RTAny::from_edge(const EdgeRecord& v) {
  RTAny ret;
  ret.type_ = DataType(DataTypeId::EDGE);
  ret.value_.edge = v;
  return ret;
}

RTAny RTAny::from_path(const Path& v) {
  RTAny ret;
  ret.type_ = DataType(DataTypeId::PATH);
  ret.value_.p = v;
  return ret;
}

RTAny RTAny::from_bool(bool v) {
  RTAny ret;
  ret.type_ = DataType(DataTypeId::BOOLEAN);
  ret.value_.b_val = v;
  return ret;
}

RTAny RTAny::from_int64(int64_t v) {
  RTAny ret;
  ret.type_ = DataType(DataTypeId::BIGINT);
  ret.value_.i64_val = v;
  return ret;
}

RTAny RTAny::from_uint64(uint64_t v) {
  RTAny ret;
  ret.type_ = DataType(DataTypeId::UBIGINT);
  ret.value_.u64_val = v;
  return ret;
}

RTAny RTAny::from_int32(int v) {
  RTAny ret;
  ret.type_ = DataType(DataTypeId::INTEGER);
  ret.value_.i32_val = v;
  return ret;
}

RTAny RTAny::from_uint32(uint32_t v) {
  RTAny ret;
  ret.type_ = DataType(DataTypeId::UINTEGER);
  ret.value_.u32_val = v;
  return ret;
}

RTAny RTAny::from_string(const std::string& str) {
  RTAny ret;
  ret.type_ = DataType(DataTypeId::VARCHAR);
  ret.value_.str_val = std::string_view(str);
  return ret;
}

RTAny RTAny::from_string(const std::string_view& str) {
  RTAny ret;
  ret.type_ = DataType(DataTypeId::VARCHAR);
  ret.value_.str_val = str;
  return ret;
}

RTAny RTAny::from_date(Date v) {
  RTAny ret;
  ret.type_ = DataType(DataTypeId::DATE);
  ret.value_.date_val = v;
  return ret;
}

RTAny RTAny::from_datetime(DateTime v) {
  RTAny ret;
  ret.type_ = DataType(DataTypeId::TIMESTAMP_MS);
  ret.value_.dt_val = v;
  return ret;
}

RTAny RTAny::from_tuple(const Tuple& t) {
  RTAny ret;
  std::vector<DataType> data_types;
  for (int i = 0; i < t.size(); ++i) {
    data_types.push_back(t.get(i).type());
  }
  ret.type_ = DataType(DataTypeId::STRUCT,
                       std::make_shared<StructTypeInfo>(data_types));
  ret.value_.t = t;
  return ret;
}

RTAny RTAny::from_list(const List& l) {
  RTAny ret;
  const auto& element_type = l.elem_type();
  ret.type_ =
      DataType(DataTypeId::LIST, std::make_shared<ListTypeInfo>(element_type));
  ret.value_.list = std::move(l);
  return ret;
}

RTAny RTAny::from_float(float v) {
  RTAny ret;
  ret.type_ = DataType(DataTypeId::FLOAT);
  ret.value_.f32_val = v;
  return ret;
}

RTAny RTAny::from_double(double v) {
  RTAny ret;
  ret.type_ = DataType(DataTypeId::DOUBLE);
  ret.value_.f64_val = v;
  return ret;
}

RTAny RTAny::from_set(const Set& s) {
  RTAny ret;
  ret.type_ = DataType(DataTypeId::SET);
  ret.value_.set = s;
  return ret;
}

RTAny RTAny::from_interval(const Interval& i) {
  RTAny ret;
  ret.type_ = DataType(DataTypeId::INTERVAL);
  ret.value_.interval_val = i;
  return ret;
}

bool RTAny::as_bool() const {
  if (type_.id() == DataTypeId::SQLNULL) {
    return false;
  }
  assert(type_.id() == DataTypeId::BOOLEAN);
  return value_.b_val;
}
int RTAny::as_int32() const {
  assert(type_.id() == DataTypeId::INTEGER);
  return value_.i32_val;
}

uint32_t RTAny::as_uint32() const {
  assert(type_.id() == DataTypeId::UINTEGER);
  return value_.u32_val;
}

int64_t RTAny::as_int64() const {
  assert(type_.id() == DataTypeId::BIGINT);
  return value_.i64_val;
}
uint64_t RTAny::as_uint64() const {
  assert(type_.id() == DataTypeId::UBIGINT);
  return value_.u64_val;
}
Date RTAny::as_date() const {
  assert(type_.id() == DataTypeId::DATE);
  return value_.date_val;
}

DateTime RTAny::as_datetime() const {
  assert(type_.id() == DataTypeId::TIMESTAMP_MS);
  return value_.dt_val;
}

Interval RTAny::as_interval() const {
  assert(type_.id() == DataTypeId::INTERVAL);
  return value_.interval_val;
}

float RTAny::as_float() const {
  assert(type_.id() == DataTypeId::FLOAT);
  return value_.f32_val;
}

double RTAny::as_double() const {
  assert(type_.id() == DataTypeId::DOUBLE);
  return value_.f64_val;
}

VertexRecord RTAny::as_vertex() const {
  assert(type_.id() == DataTypeId::VERTEX);
  return value_.vertex;
}

const EdgeRecord& RTAny::as_edge() const {
  assert(type_.id() == DataTypeId::EDGE);
  return value_.edge;
}

Set RTAny::as_set() const {
  assert(type_.id() == DataTypeId::SET);
  return value_.set;
}

std::string_view RTAny::as_string() const {
  if (type_.id() == DataTypeId::VARCHAR) {
    return value_.str_val;
  } else if (type_.id() == DataTypeId::UNKNOWN) {
    return std::string_view();
  } else {
    THROW_NOT_SUPPORTED_EXCEPTION(
        "Unexpected RTAny type for string conversion: " +
        std::to_string(static_cast<int>(type_.id())));
    return std::string_view();
  }
}

List RTAny::as_list() const {
  assert(type_.id() == DataTypeId::LIST);
  return value_.list;
}

Path RTAny::as_path() const {
  assert(type_.id() == DataTypeId::PATH);
  return value_.p;
}

Tuple RTAny::as_tuple() const {
  assert(type_.id() == DataTypeId::STRUCT);
  return value_.t;
}

RTAny TupleImpl<RTAny>::get(size_t idx) const {
  CHECK(idx < values.size());
  return values[idx];
}

TupleImpl<RTAny>::TupleImpl(std::vector<RTAny>&& val)
    : values(std::move(val)) {}

TupleImpl<RTAny>::~TupleImpl() {}

bool TupleImpl<RTAny>::operator<(const TupleImplBase& p) const {
  // return values < dynamic_cast<const TupleImpl<RTAny>&>(p).values;
  THROW_NOT_SUPPORTED_EXCEPTION(
      "TupleImpl<RTAny> should not be compared directly.");
  return false;  // This line is unreachable but avoids compiler warning.
}
bool TupleImpl<RTAny>::operator==(const TupleImplBase& p) const {
  return values == dynamic_cast<const TupleImpl<RTAny>&>(p).values;
}
size_t TupleImpl<RTAny>::size() const { return values.size(); }

std::string TupleImpl<RTAny>::to_string() const {
  std::stringstream ss;
  ss << "(";
  for (size_t i = 0; i < size(); i++) {
    if (i != 0) {
      ss << ", ";
    }
    ss << get(i).to_string();
  }
  ss << ")";
  return ss.str();
}

int RTAny::numerical_cmp(const RTAny& other) const {
  switch (type_.id()) {
  case DataTypeId::BIGINT:
    switch (other.type_.id()) {
    case DataTypeId::INTEGER: {
      auto ret = value_.i64_val - other.value_.i32_val;
      return ret > 0 ? 1 : (ret < 0 ? -1 : 0);
    }
    case DataTypeId::UINTEGER: {
      auto ret = value_.i64_val - other.value_.u32_val;
      return ret > 0 ? 1 : (ret < 0 ? -1 : 0);
    }
    case DataTypeId::FLOAT: {
      auto ret = value_.i64_val - other.value_.f32_val;
      return ret > 0 ? 1 : (ret < 0 ? -1 : 0);
    }
    case DataTypeId::DOUBLE: {
      auto ret = value_.i64_val - other.value_.f64_val;
      return ret > 0 ? 1 : (ret < 0 ? -1 : 0);
    }
    default:
      THROW_NOT_SUPPORTED_EXCEPTION(
          "not support for " +
          std::to_string(static_cast<int>(other.type_.id())));
    }
    break;
  case DataTypeId::INTEGER:
    switch (other.type_.id()) {
    case DataTypeId::BIGINT: {
      auto ret = value_.i32_val - other.value_.i64_val;
      return ret > 0 ? 1 : (ret < 0 ? -1 : 0);
    }
    case DataTypeId::FLOAT: {
      auto ret = value_.i32_val - other.value_.f32_val;
      return ret > 0 ? 1 : (ret < 0 ? -1 : 0);
    }
    case DataTypeId::DOUBLE: {
      auto ret = value_.i32_val - other.value_.f64_val;
      return ret > 0 ? 1 : (ret < 0 ? -1 : 0);
    }
    case DataTypeId::UINTEGER: {
      auto ret = value_.i32_val - other.value_.u32_val;
      return ret > 0 ? 1 : (ret < 0 ? -1 : 0);
    }

    default:
      THROW_NOT_SUPPORTED_EXCEPTION(
          "not support for " +
          std::to_string(static_cast<int>(other.type_.id())));
    }
    break;
  case DataTypeId::DOUBLE:
    switch (other.type_.id()) {
    case DataTypeId::BIGINT: {
      auto ret = value_.f64_val - other.value_.i64_val;
      return ret > 0 ? 1 : (ret < 0 ? -1 : 0);
    }
    case DataTypeId::INTEGER: {
      auto ret = value_.f64_val - other.value_.i32_val;
      return ret > 0 ? 1 : (ret < 0 ? -1 : 0);
    }
    case DataTypeId::UINTEGER: {
      auto ret = value_.f64_val - other.value_.u32_val;
      return ret > 0 ? 1 : (ret < 0 ? -1 : 0);
    }
    case DataTypeId::FLOAT: {
      auto ret = value_.f64_val - other.value_.f32_val;
      return ret > 0 ? 1 : (ret < 0 ? -1 : 0);
    }
    default:
      THROW_NOT_SUPPORTED_EXCEPTION(
          "not support for " +
          std::to_string(static_cast<int>(other.type_.id())));
    }
    break;
  case DataTypeId::UINTEGER:
    switch (other.type_.id()) {
    case DataTypeId::BIGINT: {
      auto ret = value_.u32_val - other.value_.i64_val;
      return ret > 0 ? 1 : (ret < 0 ? -1 : 0);
    }
    case DataTypeId::INTEGER: {
      auto ret = value_.u32_val - other.value_.i32_val;
      return ret > 0 ? 1 : (ret < 0 ? -1 : 0);
    }
    case DataTypeId::FLOAT: {
      auto ret = value_.u32_val - other.value_.f32_val;
      return ret > 0 ? 1 : (ret < 0 ? -1 : 0);
    }
    case DataTypeId::DOUBLE: {
      auto ret = value_.u32_val - other.value_.f64_val;
      return ret > 0 ? 1 : (ret < 0 ? -1 : 0);
    }
    case DataTypeId::UBIGINT: {
      auto ret = value_.u32_val - other.value_.u64_val;
      return ret > 0 ? 1 : (ret < 0 ? -1 : 0);
    }
    default:
      THROW_NOT_SUPPORTED_EXCEPTION(
          "not support for " +
          std::to_string(static_cast<int>(other.type_.id())));
    }
    break;
  case DataTypeId::UBIGINT:
    switch (other.type_.id()) {
    case DataTypeId::BIGINT: {
      auto ret = value_.u64_val - other.value_.i64_val;
      return ret > 0 ? 1 : (ret < 0 ? -1 : 0);
    }
    case DataTypeId::INTEGER: {
      auto ret = value_.u64_val - other.value_.i32_val;
      return ret > 0 ? 1 : (ret < 0 ? -1 : 0);
    }
    case DataTypeId::FLOAT: {
      auto ret = value_.u64_val - other.value_.f32_val;
      return ret > 0 ? 1 : (ret < 0 ? -1 : 0);
    }
    case DataTypeId::DOUBLE: {
      auto ret = value_.u64_val - other.value_.f64_val;
      return ret > 0 ? 1 : (ret < 0 ? -1 : 0);
    }
    case DataTypeId::UINTEGER: {
      auto ret = value_.u64_val - other.value_.u32_val;
      return ret > 0 ? 1 : (ret < 0 ? -1 : 0);
    }
    default:
      THROW_NOT_SUPPORTED_EXCEPTION(
          "not support for " +
          std::to_string(static_cast<int>(other.type_.id())));
    }
    break;
  case DataTypeId::FLOAT:
    switch (other.type_.id()) {
    case DataTypeId::BIGINT: {
      auto ret = value_.f32_val - other.value_.i64_val;
      return ret > 0 ? 1 : (ret < 0 ? -1 : 0);
    }
    case DataTypeId::INTEGER: {
      auto ret = value_.f32_val - other.value_.i32_val;
      return ret > 0 ? 1 : (ret < 0 ? -1 : 0);
    }
    case DataTypeId::UINTEGER: {
      auto ret = value_.f32_val - other.value_.u32_val;
      return ret > 0 ? 1 : (ret < 0 ? -1 : 0);
    }
    case DataTypeId::DOUBLE: {
      auto ret = value_.f32_val - other.value_.f64_val;
      return ret > 0 ? 1 : (ret < 0 ? -1 : 0);
    }
    case DataTypeId::UBIGINT: {
      auto ret = value_.f32_val - other.value_.u64_val;
      return ret > 0 ? 1 : (ret < 0 ? -1 : 0);
    }
    default:
      LOG(FATAL) << "not support for " << static_cast<int>(type_.id());
    }
    break;
  default:
    THROW_NOT_SUPPORTED_EXCEPTION("not support for " +
                                  std::to_string(static_cast<int>(type_.id())));
  }
  return 0;  // This line is unreachable but avoids compiler warning.
}
inline static bool is_numerical_type(const DataType& type) {
  return type.id() == DataTypeId::BIGINT || type.id() == DataTypeId::INTEGER ||
         type.id() == DataTypeId::DOUBLE || type.id() == DataTypeId::UINTEGER ||
         type.id() == DataTypeId::UBIGINT || type.id() == DataTypeId::FLOAT;
}

bool RTAny::operator<(const RTAny& other) const {
  if (type_ != other.type_) {
    if (is_numerical_type(type_) && is_numerical_type(other.type_)) {
      return numerical_cmp(other) < 0;
    } else {
      return false;
    }
  }
  if (type_.id() == DataTypeId::BIGINT) {
    return value_.i64_val < other.value_.i64_val;
  } else if (type_.id() == DataTypeId::INTEGER) {
    return value_.i32_val < other.value_.i32_val;
  } else if (type_.id() == DataTypeId::UINTEGER) {
    return value_.u32_val < other.value_.u32_val;
  } else if (type_.id() == DataTypeId::VARCHAR) {
    return value_.str_val < other.value_.str_val;
  } else if (type_.id() == DataTypeId::DATE) {
    return value_.date_val < other.value_.date_val;
  } else if (type_.id() == DataTypeId::TIMESTAMP_MS) {
    return value_.dt_val < other.value_.dt_val;
  } else if (type_.id() == DataTypeId::INTERVAL) {
    return value_.interval_val < other.value_.interval_val;
  } else if (type_.id() == DataTypeId::FLOAT) {
    return value_.f32_val < other.value_.f32_val;
  } else if (type_.id() == DataTypeId::DOUBLE) {
    return value_.f64_val < other.value_.f64_val;
  } else if (type_.id() == DataTypeId::EDGE) {
    return value_.edge < other.value_.edge;
  } else if (type_.id() == DataTypeId::VERTEX) {
    return value_.vertex < other.value_.vertex;
  } else if (type_.id() == DataTypeId::UBIGINT) {
    return value_.u64_val < other.value_.u64_val;
  } else if (type_.id() == DataTypeId::BOOLEAN) {
    return value_.b_val < other.value_.b_val;
  } else if (type_.id() == DataTypeId::PATH) {
    return value_.p < other.value_.p;
  }

  THROW_NOT_SUPPORTED_EXCEPTION("not support for " +
                                std::to_string(static_cast<int>(type_.id())));
  return true;
}

bool RTAny::operator==(const RTAny& other) const {
  if (type_.id() != other.type_.id()) {
    if (is_numerical_type(type_) && is_numerical_type(other.type_)) {
      return numerical_cmp(other) == 0;
    } else {
      return false;
    }
  }

  if (type_.id() == DataTypeId::BIGINT) {
    return value_.i64_val == other.value_.i64_val;
  } else if (type_.id() == DataTypeId::UBIGINT) {
    return value_.u64_val == other.value_.u64_val;
  } else if (type_.id() == DataTypeId::INTEGER) {
    return value_.i32_val == other.value_.i32_val;
  } else if (type_.id() == DataTypeId::UINTEGER) {
    return value_.u32_val == other.value_.u32_val;
  } else if (type_.id() == DataTypeId::FLOAT) {
    return value_.f32_val == other.value_.f32_val;
  } else if (type_.id() == DataTypeId::DOUBLE) {
    return value_.f64_val == other.value_.f64_val;
  } else if (type_.id() == DataTypeId::BOOLEAN) {
    return value_.b_val == other.value_.b_val;
  } else if (type_.id() == DataTypeId::VARCHAR) {
    return value_.str_val == other.value_.str_val;
  } else if (type_.id() == DataTypeId::VERTEX) {
    return value_.vertex == other.value_.vertex;
  } else if (type_.id() == DataTypeId::EDGE) {
    return value_.edge == other.value_.edge;
  } else if (type_.id() == DataTypeId::DATE) {
    return value_.date_val == other.value_.date_val;
  } else if (type_.id() == DataTypeId::TIMESTAMP_MS) {
    return value_.dt_val == other.value_.dt_val;
  } else if (type_.id() == DataTypeId::INTERVAL) {
    return value_.interval_val == other.value_.interval_val;
  }

  THROW_NOT_SUPPORTED_EXCEPTION("not support for " +
                                std::to_string(static_cast<int>(type_.id())));
  return true;
}

RTAny RTAny::operator+(const RTAny& other) const {
  if (type_.id() == DataTypeId::DATE) {
    if (other.type().id() == DataTypeId::INTERVAL) {
      return RTAny::from_date(value_.date_val + other.value_.interval_val);
    } else {
      THROW_NOT_SUPPORTED_EXCEPTION(
          "not support for " +
          std::to_string(static_cast<int>(other.type_.id())));
    }
  } else if (type_.id() == DataTypeId::TIMESTAMP_MS) {
    if (other.type() == DataTypeId::INTERVAL) {
      return RTAny::from_datetime(value_.dt_val + other.value_.interval_val);
    } else {
      THROW_NOT_SUPPORTED_EXCEPTION(
          "not support for " +
          std::to_string(static_cast<int>(other.type_.id())));
    }
  } else if (type_.id() == DataTypeId::INTERVAL) {
    if (other.type() == DataTypeId::DATE) {
      return RTAny::from_date(other.value_.date_val + value_.interval_val);
    } else if (other.type() == DataTypeId::TIMESTAMP_MS) {
      return RTAny::from_datetime(other.value_.dt_val + value_.interval_val);
    } else {
      THROW_NOT_SUPPORTED_EXCEPTION(
          "RTAny::operator+ not support for " +
          std::to_string(static_cast<int>(type_.id())) + " and " +
          std::to_string(static_cast<int>(other.type_.id())));
    }
  }

  assert(type_ == other.type_);
  switch (type_.id()) {
  case DataTypeId::INTEGER:
    return RTAny::from_int32(value_.i32_val + other.value_.i32_val);
  case DataTypeId::UINTEGER:
    return RTAny::from_uint32(value_.u32_val + other.value_.u32_val);
  case DataTypeId::BIGINT:
    return RTAny::from_int64(value_.i64_val + other.value_.i64_val);
  case DataTypeId::UBIGINT:
    return RTAny::from_uint64(value_.u64_val + other.value_.u64_val);
  case DataTypeId::FLOAT:
    return RTAny::from_float(value_.f32_val + other.value_.f32_val);
  case DataTypeId::DOUBLE:
    return RTAny::from_double(value_.f64_val + other.value_.f64_val);
  default:
    LOG(FATAL) << "RTAny::operator+ not support for " +
                      std::to_string(static_cast<int>(type_.id())) + " and " +
                      std::to_string(static_cast<int>(other.type_.id()));
  }
}

// TODO(zhanglei): Support generate RTAny substraction.
RTAny RTAny::operator-(const RTAny& other) const {
  if (type_.id() == DataTypeId::DATE) {
    if (other.type_.id() == DataTypeId::DATE) {
      return RTAny::from_interval(value_.date_val - other.value_.date_val);
    } else if (other.type_.id() == DataTypeId::INTERVAL) {
      return RTAny::from_date(value_.date_val - other.value_.interval_val);
    } else {
      THROW_RUNTIME_ERROR("RTAny::operator- not support for " +
                          std::to_string(static_cast<int>(type_.id())) +
                          " and " +
                          std::to_string(static_cast<int>(other.type_.id())));
    }
  } else if (type_.id() == DataTypeId::TIMESTAMP_MS) {
    if (other.type_.id() == DataTypeId::TIMESTAMP_MS) {
      return RTAny::from_interval(value_.dt_val - other.value_.dt_val);
    } else if (other.type_.id() == DataTypeId::INTERVAL) {
      return RTAny::from_datetime(value_.dt_val - other.value_.interval_val);
    } else {
      THROW_RUNTIME_ERROR("RTAny::operator- not support for " +
                          std::to_string(static_cast<int>(type_.id())) +
                          " and " +
                          std::to_string(static_cast<int>(other.type_.id())));
    }
  } else if (type_.id() == DataTypeId::INTERVAL) {
    THROW_RUNTIME_ERROR("RTAny::operator- not support for " +
                        std::to_string(static_cast<int>(type_.id())) + " and " +
                        std::to_string(static_cast<int>(other.type_.id())));
  }
  assert(type_ == other.type_);
  switch (type_.id()) {
  case DataTypeId::INTEGER:
    return RTAny::from_int32(value_.i32_val - other.value_.i32_val);
  case DataTypeId::UINTEGER:
    return RTAny::from_uint32(value_.u32_val - other.value_.u32_val);
  case DataTypeId::BIGINT:
    return RTAny::from_int64(value_.i64_val - other.value_.i64_val);
  case DataTypeId::UBIGINT:
    return RTAny::from_uint64(value_.u64_val - other.value_.u64_val);
  case DataTypeId::FLOAT:
    return RTAny::from_float(value_.f32_val - other.value_.f32_val);
  case DataTypeId::DOUBLE:
    return RTAny::from_double(value_.f64_val - other.value_.f64_val);
  default:
    LOG(FATAL) << "RTAny::operator- not support for " +
                      std::to_string(static_cast<int>(type_.id())) + " and " +
                      std::to_string(static_cast<int>(other.type_.id()));
  }
}

RTAny RTAny::operator*(const RTAny& other) const {
  assert(type_ == other.type_);
  switch (type_.id()) {
  case DataTypeId::INTEGER:
    return RTAny::from_int32(value_.i32_val * other.value_.i32_val);
  case DataTypeId::UINTEGER:
    return RTAny::from_uint32(value_.u32_val * other.value_.u32_val);
  case DataTypeId::BIGINT:
    return RTAny::from_int64(value_.i64_val * other.value_.i64_val);
  case DataTypeId::UBIGINT:
    return RTAny::from_uint64(value_.u64_val * other.value_.u64_val);
  case DataTypeId::FLOAT:
    return RTAny::from_float(value_.f32_val * other.value_.f32_val);
  case DataTypeId::DOUBLE:
    return RTAny::from_double(value_.f64_val * other.value_.f64_val);
  default:
    LOG(FATAL) << "RTAny::operator* not support for " +
                      std::to_string(static_cast<int>(type_.id())) + " and " +
                      std::to_string(static_cast<int>(other.type_.id()));
  }
}

RTAny RTAny::operator/(const RTAny& other) const {
  assert(type_ == other.type_);
  switch (type_.id()) {
  case DataTypeId::INTEGER:
    return RTAny::from_int32(value_.i32_val / other.value_.i32_val);
  case DataTypeId::UINTEGER:
    return RTAny::from_uint32(value_.u32_val / other.value_.u32_val);
  case DataTypeId::BIGINT:
    return RTAny::from_int64(value_.i64_val / other.value_.i64_val);
  case DataTypeId::UBIGINT:
    return RTAny::from_uint64(value_.u64_val / other.value_.u64_val);
  case DataTypeId::FLOAT:
    return RTAny::from_float(value_.f32_val / other.value_.f32_val);
  case DataTypeId::DOUBLE:
    return RTAny::from_double(value_.f64_val / other.value_.f64_val);
  default:
    LOG(FATAL) << "RTAny::operator/ not support for " +
                      std::to_string(static_cast<int>(type_.id())) + " and " +
                      std::to_string(static_cast<int>(other.type_.id()));
  }
}

RTAny RTAny::operator%(const RTAny& other) const {
  assert(type_ == other.type_);
  switch (type_.id()) {
  case DataTypeId::INTEGER:
    return RTAny::from_int32(value_.i32_val % other.value_.i32_val);
  case DataTypeId::UINTEGER:
    return RTAny::from_uint32(value_.u32_val % other.value_.u32_val);
  case DataTypeId::BIGINT:
    return RTAny::from_int64(value_.i64_val % other.value_.i64_val);
  case DataTypeId::UBIGINT:
    return RTAny::from_uint64(value_.u64_val % other.value_.u64_val);
  default:
    LOG(FATAL) << "RTAny::operator% not support for " +
                      std::to_string(static_cast<int>(type_.id())) + " and " +
                      std::to_string(static_cast<int>(other.type_.id()));
  }
}

void RTAny::sink_impl(common::Value* value) const {
  if (type_.id() == DataTypeId::BOOLEAN) {
    value->set_boolean(value_.b_val);
  } else if (type_.id() == DataTypeId::BIGINT) {
    value->set_i64(value_.i64_val);
  } else if (type_.id() == DataTypeId::VARCHAR) {
    value->set_str(value_.str_val.data(), value_.str_val.size());
  } else if (type_.id() == DataTypeId::INTEGER) {
    value->set_i32(value_.i32_val);
  } else if (type_.id() == DataTypeId::DATE) {
    auto date_str = value_.date_val.to_string();
    value->set_str(date_str.data(), date_str.size());
  } else if (type_.id() == DataTypeId::TIMESTAMP_MS) {
    auto date_time_str = value_.dt_val.to_string();
    value->set_str(date_time_str.data(), date_time_str.size());
  } else if (type_.id() == DataTypeId::INTERVAL) {
    auto interval_str = value_.interval_val.to_string();
    value->set_str(interval_str.data(), interval_str.size());
  } else if (type_.id() == DataTypeId::BOOLEAN) {
    value->set_boolean(value_.b_val);
  } else if (type_.id() == DataTypeId::FLOAT) {
    value->set_f32(value_.f32_val);
  } else if (type_.id() == DataTypeId::DOUBLE) {
    value->set_f64(value_.f64_val);
  } else if (type_.id() == DataTypeId::LIST) {
    THROW_NOT_SUPPORTED_EXCEPTION("RTAny::sink_impl not support for list type");
  } else if (type_.id() == DataTypeId::STRUCT) {
    auto tup = value_.t;
    for (size_t i = 0; i < tup.size(); ++i) {
      std::string s = tup.get(i).to_string();
      value->mutable_str_array()->add_item(s.data(), s.size());
    }
  } else if (type_.id() == DataTypeId::UBIGINT) {
    value->set_u64(value_.u64_val);
  } else if (type_.id() == DataTypeId::UINTEGER) {
    value->set_u32(value_.u32_val);
  } else if (type_.id() == DataTypeId::SQLNULL) {
    value->mutable_none();
  } else {
    THROW_NOT_SUPPORTED_EXCEPTION("RTAny::sink_impl not support for " +
                                  std::to_string(static_cast<int>(type_.id())));
  }
}

// just for ldbc snb interactive queries
void RTAny::encode_sig(DataType type, Encoder& encoder) const {
  if (type.id() == DataTypeId::BIGINT) {
    encoder.put_long(this->as_int64());
  } else if (type.id() == DataTypeId::VARCHAR) {
    encoder.put_string_view(this->as_string());
  } else if (type.id() == DataTypeId::INTEGER) {
    encoder.put_int(this->as_int32());
  } else if (type.id() == DataTypeId::UINTEGER) {
    encoder.put_int(this->as_uint32());
  } else if (type.id() == DataTypeId::UBIGINT) {
    encoder.put_long(this->as_uint64());
  } else if (type.id() == DataTypeId::VERTEX) {
    const auto& v = this->value_.vertex;
    encoder.put_byte(v.label_);
    encoder.put_int(v.vid_);
  } else if (type.id() == DataTypeId::EDGE) {
    const auto& [label, src, dst, prop, dir] = this->as_edge();

    encoder.put_byte(label.src_label);
    encoder.put_byte(label.dst_label);
    encoder.put_byte(label.edge_label);
    encoder.put_int(src);
    encoder.put_int(dst);
    encoder.put_byte(dir == Direction::kOut ? 1 : 0);

  } else if (type.id() == DataTypeId::BOOLEAN) {
    encoder.put_byte(this->as_bool() ? 1 : 0);
  } else if (type.id() == DataTypeId::LIST) {
    encoder.put_int(this->as_list().size());
    List list = this->as_list();
    for (size_t i = 0; i < list.size(); ++i) {
      list.get(i).encode_sig(list.get(i).type(), encoder);
    }
  } else if (type.id() == DataTypeId::STRUCT) {
    Tuple tuple = this->as_tuple();
    encoder.put_int(tuple.size());
    for (size_t i = 0; i < tuple.size(); ++i) {
      tuple.get(i).encode_sig(tuple.get(i).type(), encoder);
    }
  } else if (type.id() == DataTypeId::SQLNULL) {
    encoder.put_int(-1);
  } else if (type.id() == DataTypeId::FLOAT) {
    encoder.put_float(this->as_float());
  } else if (type.id() == DataTypeId::DOUBLE) {
    encoder.put_double(this->as_double());
  } else if (type.id() == DataTypeId::PATH) {
    Path p = this->as_path();
    encoder.put_int(p.len() + 1);
    auto nodes = p.nodes();
    for (size_t i = 0; i < nodes.size(); ++i) {
      encoder.put_byte(nodes[i].label_);
      encoder.put_int(nodes[i].vid_);
    }
  } else if (type.id() == DataTypeId::TIMESTAMP_MS) {
    encoder.put_long(value_.dt_val.milli_second);
  } else {
    THROW_RUNTIME_ERROR("RTAny::encode_sig not support for " +
                        std::to_string(static_cast<int>(type.id())));
  }
}

std::string RTAny::to_string() const {
  if (type_.id() == DataTypeId::BOOLEAN) {
    return value_.b_val ? "true" : "false";
  } else if (type_.id() == DataTypeId::BIGINT) {
    return std::to_string(value_.i64_val);
  } else if (type_.id() == DataTypeId::INTEGER) {
    return std::to_string(value_.i32_val);
  } else if (type_.id() == DataTypeId::UINTEGER) {
    return std::to_string(value_.u32_val);
  } else if (type_.id() == DataTypeId::VERTEX) {
#if 0
      return std::string("v") +
             std::to_string(static_cast<int>(value_.vertex.label_)) + "-" +
             std::to_string(value_.vertex.vid_);
#else
    return std::to_string(value_.vertex.vid_);
#endif
  } else if (type_.id() == DataTypeId::SET) {
    std::string ret = "{";
    for (auto& val : value_.set.values()) {
      ret += val.to_string();
      ret += ", ";
    }
    ret += "}";
    return ret;
  } else if (type_.id() == DataTypeId::EDGE) {
    auto [label, src, dst, prop, dir] = value_.edge;
    return std::to_string(src) + " -> " + std::to_string(dst);
  } else if (type_.id() == DataTypeId::PATH) {
    return value_.p.to_string();
  } else if (type_.id() == DataTypeId::VARCHAR) {
    return std::string(value_.str_val);
  } else if (type_.id() == DataTypeId::LIST) {
    std::string ret = "[";
    for (size_t i = 0; i < value_.list.size(); ++i) {
      ret += value_.list.get(i).to_string();
      if (i != value_.list.size() - 1) {
        ret += ", ";
      }
    }
    ret += "]";
    return ret;
  } else if (type_.id() == DataTypeId::STRUCT) {
    std::string ret = "(";
    for (size_t i = 0; i < value_.t.size(); ++i) {
      ret += value_.t.get(i).to_string();
      if (i != value_.t.size() - 1) {
        ret += ", ";
      }
    }
    ret += ")";
    return ret;
  } else if (type_.id() == DataTypeId::SQLNULL) {
    return "null";
  } else if (type_.id() == DataTypeId::FLOAT) {
    return std::to_string(value_.f32_val);
  } else if (type_.id() == DataTypeId::DOUBLE) {
    return std::to_string(value_.f64_val);
  } else if (type_.id() == DataTypeId::TIMESTAMP_MS) {
    return value_.dt_val.to_string();
  } else if (type_.id() == DataTypeId::DATE) {
    return value_.date_val.to_string();
  } else if (type_.id() == DataTypeId::INTERVAL) {
    return value_.interval_val.to_string();
  } else {
    LOG(FATAL) << "not implemented for " << static_cast<int>(type_.id());
    return "";
  }
}

}  // namespace runtime

}  // namespace gs
