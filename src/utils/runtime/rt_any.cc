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

RTAnyType parse_from_data_type(const ::common::DataType& ddt) {
  switch (ddt.item_case()) {
  case ::common::DataType::kPrimitiveType: {
    const ::common::PrimitiveType pt = ddt.primitive_type();
    switch (pt) {
    case ::common::PrimitiveType::DT_SIGNED_INT32:
      return RTAnyType::kI32Value;
    case ::common::PrimitiveType::DT_UNSIGNED_INT32:
      return RTAnyType::kU32Value;
    case ::common::PrimitiveType::DT_UNSIGNED_INT64:
      return RTAnyType::kU64Value;
    case ::common::PrimitiveType::DT_SIGNED_INT64:
      return RTAnyType::kI64Value;
    case ::common::PrimitiveType::DT_FLOAT:
      return RTAnyType::kF32Value;
    case ::common::PrimitiveType::DT_DOUBLE:
      return RTAnyType::kF64Value;
    case ::common::PrimitiveType::DT_BOOL:
      return RTAnyType::kBoolValue;
    case ::common::PrimitiveType::DT_ANY:
      return RTAnyType::kUnknown;
    default:
      THROW_NOT_SUPPORTED_EXCEPTION("unrecognized primitive type - " +
                                    std::to_string(pt));
      break;
    }
  }
  case ::common::DataType::kString:
    return RTAnyType::kStringValue;
  case ::common::DataType::kTemporal: {
    if (ddt.temporal().item_case() == ::common::Temporal::kDate32) {
      return RTAnyType::kDate;
    } else if (ddt.temporal().item_case() == ::common::Temporal::kDateTime) {
      return RTAnyType::kDateTime;
    } else if (ddt.temporal().item_case() == ::common::Temporal::kDate) {
      return RTAnyType::kDate;
    } else if (ddt.temporal().item_case() == ::common::Temporal::kInterval) {
      return RTAnyType::kInterval;
    } else if (ddt.temporal().item_case() == ::common::Temporal::kTimestamp) {
      return RTAnyType::kDateTime;
    } else {
      THROW_NOT_SUPPORTED_EXCEPTION("unrecognized temporal type - " +
                                    ddt.temporal().DebugString());
    }
  }
  case ::common::DataType::kArray:
    return RTAnyType::kList;
  case ::common::DataType::kTuple:
    return RTAnyType::kTuple;
  default:
    THROW_NOT_SUPPORTED_EXCEPTION("unrecognized data type - " +
                                  ddt.DebugString());
    break;
  }

  return RTAnyType::kUnknown;
}

RTAnyType parse_from_ir_data_type(const ::common::IrDataType& dt) {
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
      return RTAnyType::kVertex;
    case ::common::GraphDataType_GraphElementOpt::
        GraphDataType_GraphElementOpt_EDGE:
      return RTAnyType::kEdge;
    case ::common::GraphDataType_GraphElementOpt::
        GraphDataType_GraphElementOpt_PATH:
      return RTAnyType::kPath;
    default:
      THROW_NOT_SUPPORTED_EXCEPTION("unrecognized graph data type - " +
                                    gdt.DebugString());
      break;
    }
  } break;
  default:
    break;
  }
  return RTAnyType::kUnknown;
}

RTAny List::get(size_t idx) const { return impl_->get(idx); }

RTAnyType Set::elem_type() const { return impl_->type(); }

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

RTAnyType List::elem_type() const { return impl_->type(); }
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

RTAnyType arrow_type_to_rt_type(const std::shared_ptr<arrow::DataType>& type) {
  if (type->Equals(arrow::int64())) {
    return RTAnyType::kI64Value;
  } else if (type->Equals(arrow::int32())) {
    return RTAnyType::kI32Value;
  } else if (type->Equals(arrow::uint32())) {
    return RTAnyType::kU32Value;
  } else if (type->Equals(arrow::uint64())) {
    return RTAnyType::kU64Value;
  } else if (type->Equals(arrow::float32())) {
    return RTAnyType::kF32Value;
  } else if (type->Equals(arrow::float64())) {
    return RTAnyType::kF64Value;
  } else if (type->Equals(arrow::boolean())) {
    return RTAnyType::kBoolValue;
  } else if (type->Equals(arrow::utf8()) || type->Equals(arrow::large_utf8())) {
    return RTAnyType::kStringValue;
  } else if (type->Equals(arrow::date32())) {
    return RTAnyType::kDate;
  } else if (type->Equals(arrow::timestamp(arrow::TimeUnit::SECOND))) {
    return RTAnyType::kDateTime;
  } else if (type->Equals(arrow::timestamp(arrow::TimeUnit::MILLI))) {
    return RTAnyType::kDateTime;
  } else if (type->Equals(arrow::timestamp(arrow::TimeUnit::MICRO))) {
    return RTAnyType::kDateTime;
  } else if (type->Equals(arrow::timestamp(arrow::TimeUnit::NANO))) {
    return RTAnyType::kDateTime;
  } else {
    THROW_NOT_SUPPORTED_EXCEPTION("Unexpected arrow type: " + type->ToString());
  }
}

RTAny::RTAny() : type_(RTAnyType::kUnknown) {}
RTAny::RTAny(RTAnyType type) : type_(type) {}

RTAny::RTAny(const Property& val) {
  if (val.type() == DataTypeId::kBool) {
    type_ = RTAnyType::kBoolValue;
    value_.b_val = val.as_bool();
  } else if (val.type() == DataTypeId::kInt64) {
    type_ = RTAnyType::kI64Value;
    value_.i64_val = val.as_int64();
  } else if (val.type() == DataTypeId::kInt32) {
    type_ = RTAnyType::kI32Value;
    value_.i32_val = val.as_int32();
  } else if (val.type() == DataTypeId::kUInt32) {
    type_ = RTAnyType::kU32Value;
    value_.i32_val = val.as_uint32();
  } else if (val.type() == DataTypeId::kUInt64) {
    type_ = RTAnyType::kU64Value;
    value_.u64_val = val.as_uint64();
  } else if (val.type() == DataTypeId::kFloat) {
    type_ = RTAnyType::kF32Value;
    value_.f32_val = val.as_float();
  } else if (val.type() == DataTypeId::kDouble) {
    type_ = RTAnyType::kF64Value;
    value_.f64_val = val.as_double();
  } else if (val.type() == DataTypeId::kStringView) {
    type_ = RTAnyType::kStringValue;
    value_.str_val = val.as_string_view();
  } else if (val.type() == DataTypeId::kEmpty) {
    type_ = RTAnyType::kNull;
  } else if (val.type() == DataTypeId::kDate) {
    type_ = RTAnyType::kDate;
    value_.date_val = val.as_date();
  } else if (val.type() == DataTypeId::kInterval) {
    type_ = RTAnyType::kInterval;
    value_.interval_val = val.as_interval();
  } else if (val.type() == DataTypeId::kDateTime) {
    type_ = RTAnyType::kDateTime;
    value_.dt_val = val.as_datetime();
  } else {
    THROW_NOT_SUPPORTED_EXCEPTION(
        "Unexpected property type: " + std::to_string(val.type()) +
        "value: " + val.to_string());
  }
}

RTAny::RTAny(const Path& p) {
  type_ = RTAnyType::kPath;
  value_.p = p;
}

RTAny::RTAny(const RTAny& rhs) : type_(rhs.type_) {
  if (type_ == RTAnyType::kBoolValue) {
    value_.b_val = rhs.value_.b_val;
  } else if (type_ == RTAnyType::kI64Value) {
    value_.i64_val = rhs.value_.i64_val;
  } else if (type_ == RTAnyType::kU64Value) {
    value_.u64_val = rhs.value_.u64_val;
  } else if (type_ == RTAnyType::kI32Value) {
    value_.i32_val = rhs.value_.i32_val;
  } else if (type_ == RTAnyType::kU32Value) {
    value_.u32_val = rhs.value_.u32_val;
  } else if (type_ == RTAnyType::kVertex) {
    value_.vertex = rhs.value_.vertex;
  } else if (type_ == RTAnyType::kEdge) {
    value_.edge = rhs.value_.edge;
  } else if (type_ == RTAnyType::kStringValue) {
    value_.str_val = rhs.value_.str_val;
  } else if (type_ == RTAnyType::kNull) {
    // do nothing
  } else if (type_ == RTAnyType::kTuple) {
    value_.t = rhs.value_.t;
  } else if (type_ == RTAnyType::kList) {
    value_.list = rhs.value_.list;
  } else if (type_ == RTAnyType::kF32Value) {
    value_.f32_val = rhs.value_.f32_val;
  } else if (type_ == RTAnyType::kF64Value) {
    value_.f64_val = rhs.value_.f64_val;
  } else if (type_ == RTAnyType::kDate) {
    value_.date_val = rhs.value_.date_val;
  } else if (type_ == RTAnyType::kDateTime) {
    value_.dt_val = rhs.value_.dt_val;
  } else if (type_ == RTAnyType::kInterval) {
    value_.interval_val = rhs.value_.interval_val;
  } else if (type_ == RTAnyType::kPath) {
    value_.p = rhs.value_.p;
  } else {
    THROW_NOT_SUPPORTED_EXCEPTION("Unexpected RTAny type: " +
                                  std::to_string(static_cast<int>(type_)));
  }
}

RTAny& RTAny::operator=(const RTAny& rhs) {
  type_ = rhs.type_;
  if (type_ == RTAnyType::kBoolValue) {
    value_.b_val = rhs.value_.b_val;
  } else if (type_ == RTAnyType::kI64Value) {
    value_.i64_val = rhs.value_.i64_val;
  } else if (type_ == RTAnyType::kU64Value) {
    value_.u64_val = rhs.value_.u64_val;
  } else if (type_ == RTAnyType::kI32Value) {
    value_.i32_val = rhs.value_.i32_val;
  } else if (type_ == RTAnyType::kU32Value) {
    value_.u32_val = rhs.value_.u32_val;
  } else if (type_ == RTAnyType::kVertex) {
    value_.vertex = rhs.value_.vertex;
  } else if (type_ == RTAnyType::kEdge) {
    value_.edge = rhs.value_.edge;
  } else if (type_ == RTAnyType::kStringValue) {
    value_.str_val = rhs.value_.str_val;
  } else if (type_ == RTAnyType::kTuple) {
    value_.t = rhs.value_.t;
  } else if (type_ == RTAnyType::kList) {
    value_.list = rhs.value_.list;
  } else if (type_ == RTAnyType::kF32Value) {
    value_.f32_val = rhs.value_.f32_val;
  } else if (type_ == RTAnyType::kF64Value) {
    value_.f64_val = rhs.value_.f64_val;
  } else if (type_ == RTAnyType::kPath) {
    value_.p = rhs.value_.p;
  } else if (type_ == RTAnyType::kDate) {
    value_.date_val = rhs.value_.date_val;
  } else if (type_ == RTAnyType::kDateTime) {
    value_.dt_val = rhs.value_.dt_val;
  } else {
    THROW_NOT_SUPPORTED_EXCEPTION("Unexpected RTAny type: " +
                                  std::to_string(static_cast<int>(type_)));
  }
  return *this;
}

RTAnyType RTAny::type() const { return type_; }

Property RTAny::to_any() const {
  switch (type_) {
  case RTAnyType::kBoolValue:
    return Property::from_bool(value_.b_val);
  case RTAnyType::kI64Value:
    return Property::from_int64(value_.i64_val);
  case RTAnyType::kU64Value:
    return Property::from_uint64(value_.u64_val);
  case RTAnyType::kI32Value:
    return Property::from_int32(value_.i32_val);
  case RTAnyType::kU32Value:
    return Property::from_uint32(value_.u32_val);
  case RTAnyType::kF32Value:
    return Property::from_float(value_.f32_val);
  case RTAnyType::kF64Value:
    return Property::from_double(value_.f64_val);
  case RTAnyType::kStringValue:
    return Property::from_string_view(value_.str_val);
  case RTAnyType::kDate:
    return Property::from_date(value_.date_val);
  case RTAnyType::kDateTime:
    return Property::from_datetime(value_.dt_val);
  case RTAnyType::kInterval:
    return Property::from_interval(value_.interval_val);
  default:
    THROW_NOT_SUPPORTED_EXCEPTION("Unexpected RTAny type: " +
                                  std::to_string(static_cast<int>(type_)));
  }
}

RTAny RTAny::from_vertex(label_t l, vid_t v) {
  RTAny ret;
  ret.type_ = RTAnyType::kVertex;
  ret.value_.vertex.label_ = l;
  ret.value_.vertex.vid_ = v;
  return ret;
}

RTAny RTAny::from_vertex(VertexRecord v) {
  RTAny ret;
  ret.type_ = RTAnyType::kVertex;
  ret.value_.vertex = v;
  return ret;
}

RTAny RTAny::from_edge(const EdgeRecord& v) {
  RTAny ret;
  ret.type_ = RTAnyType::kEdge;
  ret.value_.edge = v;
  return ret;
}

RTAny RTAny::from_path(const Path& v) {
  RTAny ret;
  ret.type_ = RTAnyType::kPath;
  ret.value_.p = v;
  return ret;
}

RTAny RTAny::from_bool(bool v) {
  RTAny ret;
  ret.type_ = RTAnyType::kBoolValue;
  ret.value_.b_val = v;
  return ret;
}

RTAny RTAny::from_int64(int64_t v) {
  RTAny ret;
  ret.type_ = RTAnyType::kI64Value;
  ret.value_.i64_val = v;
  return ret;
}

RTAny RTAny::from_uint64(uint64_t v) {
  RTAny ret;
  ret.type_ = RTAnyType::kU64Value;
  ret.value_.u64_val = v;
  return ret;
}

RTAny RTAny::from_int32(int v) {
  RTAny ret;
  ret.type_ = RTAnyType::kI32Value;
  ret.value_.i32_val = v;
  return ret;
}

RTAny RTAny::from_uint32(uint32_t v) {
  RTAny ret;
  ret.type_ = RTAnyType::kU32Value;
  ret.value_.u32_val = v;
  return ret;
}

RTAny RTAny::from_string(const std::string& str) {
  RTAny ret;
  ret.type_ = RTAnyType::kStringValue;
  ret.value_.str_val = std::string_view(str);
  return ret;
}

RTAny RTAny::from_string(const std::string_view& str) {
  RTAny ret;
  ret.type_ = RTAnyType::kStringValue;
  ret.value_.str_val = str;
  return ret;
}

RTAny RTAny::from_date(Date v) {
  RTAny ret;
  ret.type_ = RTAnyType::kDate;
  ret.value_.date_val = v;
  return ret;
}

RTAny RTAny::from_datetime(DateTime v) {
  RTAny ret;
  ret.type_ = RTAnyType::kDateTime;
  ret.value_.dt_val = v;
  return ret;
}

RTAny RTAny::from_tuple(const Tuple& t) {
  RTAny ret;
  ret.type_ = RTAnyType::kTuple;
  ret.value_.t = t;
  return ret;
}

RTAny RTAny::from_list(const List& l) {
  RTAny ret;
  ret.type_ = RTAnyType::kList;
  ret.value_.list = std::move(l);
  return ret;
}

RTAny RTAny::from_float(float v) {
  RTAny ret;
  ret.type_ = RTAnyType::kF32Value;
  ret.value_.f32_val = v;
  return ret;
}

RTAny RTAny::from_double(double v) {
  RTAny ret;
  ret.type_ = RTAnyType::kF64Value;
  ret.value_.f64_val = v;
  return ret;
}

RTAny RTAny::from_set(const Set& s) {
  RTAny ret;
  ret.type_ = RTAnyType::kSet;
  ret.value_.set = s;
  return ret;
}

RTAny RTAny::from_interval(const Interval& i) {
  RTAny ret;
  ret.type_ = RTAnyType::kInterval;
  ret.value_.interval_val = i;
  return ret;
}

bool RTAny::as_bool() const {
  if (type_ == RTAnyType::kNull) {
    return false;
  }
  assert(type_ == RTAnyType::kBoolValue);
  return value_.b_val;
}
int RTAny::as_int32() const {
  assert(type_ == RTAnyType::kI32Value);
  return value_.i32_val;
}

uint32_t RTAny::as_uint32() const {
  assert(type_ == RTAnyType::kU32Value);
  return value_.u32_val;
}

int64_t RTAny::as_int64() const {
  assert(type_ == RTAnyType::kI64Value);
  return value_.i64_val;
}
uint64_t RTAny::as_uint64() const {
  assert(type_ == RTAnyType::kU64Value);
  return value_.u64_val;
}
Date RTAny::as_date() const {
  assert(type_ == RTAnyType::kDate);
  return value_.date_val;
}

DateTime RTAny::as_datetime() const {
  assert(type_ == RTAnyType::kDateTime);
  return value_.dt_val;
}

Interval RTAny::as_interval() const {
  assert(type_ == RTAnyType::kInterval);
  return value_.interval_val;
}

float RTAny::as_float() const {
  assert(type_ == RTAnyType::kF32Value);
  return value_.f32_val;
}

double RTAny::as_double() const {
  assert(type_ == RTAnyType::kF64Value);
  return value_.f64_val;
}

VertexRecord RTAny::as_vertex() const {
  assert(type_ == RTAnyType::kVertex);
  return value_.vertex;
}

const EdgeRecord& RTAny::as_edge() const {
  assert(type_ == RTAnyType::kEdge);
  return value_.edge;
}

Set RTAny::as_set() const {
  assert(type_ == RTAnyType::kSet);
  return value_.set;
}

std::string_view RTAny::as_string() const {
  if (type_ == RTAnyType::kStringValue) {
    return value_.str_val;
  } else if (type_ == RTAnyType::kUnknown) {
    return std::string_view();
  } else {
    THROW_NOT_SUPPORTED_EXCEPTION(
        "Unexpected RTAny type for string conversion: " +
        std::to_string(static_cast<int>(type_)));
    return std::string_view();
  }
}

List RTAny::as_list() const {
  assert(type_ == RTAnyType::kList);
  return value_.list;
}

Path RTAny::as_path() const {
  assert(type_ == RTAnyType::kPath);
  return value_.p;
}

Tuple RTAny::as_tuple() const {
  assert(type_ == RTAnyType::kTuple);
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
  switch (type_) {
  case RTAnyType::kI64Value:
    switch (other.type_) {
    case RTAnyType::kI32Value: {
      auto ret = value_.i64_val - other.value_.i32_val;
      return ret > 0 ? 1 : (ret < 0 ? -1 : 0);
    }
    case RTAnyType::kU32Value: {
      auto ret = value_.i64_val - other.value_.u32_val;
      return ret > 0 ? 1 : (ret < 0 ? -1 : 0);
    }
    case RTAnyType::kF32Value: {
      auto ret = value_.i64_val - other.value_.f32_val;
      return ret > 0 ? 1 : (ret < 0 ? -1 : 0);
    }
    case RTAnyType::kF64Value: {
      auto ret = value_.i64_val - other.value_.f64_val;
      return ret > 0 ? 1 : (ret < 0 ? -1 : 0);
    }
    default:
      THROW_NOT_SUPPORTED_EXCEPTION(
          "not support for " + std::to_string(static_cast<int>(other.type_)));
    }
    break;
  case RTAnyType::kI32Value:
    switch (other.type_) {
    case RTAnyType::kI64Value: {
      auto ret = value_.i32_val - other.value_.i64_val;
      return ret > 0 ? 1 : (ret < 0 ? -1 : 0);
    }
    case RTAnyType::kF32Value: {
      auto ret = value_.i32_val - other.value_.f32_val;
      return ret > 0 ? 1 : (ret < 0 ? -1 : 0);
    }
    case RTAnyType::kF64Value: {
      auto ret = value_.i32_val - other.value_.f64_val;
      return ret > 0 ? 1 : (ret < 0 ? -1 : 0);
    }
    case RTAnyType::kU32Value: {
      auto ret = value_.i32_val - other.value_.u32_val;
      return ret > 0 ? 1 : (ret < 0 ? -1 : 0);
    }

    default:
      THROW_NOT_SUPPORTED_EXCEPTION(
          "not support for " + std::to_string(static_cast<int>(other.type_)));
    }
    break;
  case RTAnyType::kF64Value:
    switch (other.type_) {
    case RTAnyType::kI64Value: {
      auto ret = value_.f64_val - other.value_.i64_val;
      return ret > 0 ? 1 : (ret < 0 ? -1 : 0);
    }
    case RTAnyType::kI32Value: {
      auto ret = value_.f64_val - other.value_.i32_val;
      return ret > 0 ? 1 : (ret < 0 ? -1 : 0);
    }
    case RTAnyType::kU32Value: {
      auto ret = value_.f64_val - other.value_.u32_val;
      return ret > 0 ? 1 : (ret < 0 ? -1 : 0);
    }
    case RTAnyType::kF32Value: {
      auto ret = value_.f64_val - other.value_.f32_val;
      return ret > 0 ? 1 : (ret < 0 ? -1 : 0);
    }
    default:
      THROW_NOT_SUPPORTED_EXCEPTION(
          "not support for " + std::to_string(static_cast<int>(other.type_)));
    }
    break;
  case RTAnyType::kU32Value:
    switch (other.type_) {
    case RTAnyType::kI64Value: {
      auto ret = value_.u32_val - other.value_.i64_val;
      return ret > 0 ? 1 : (ret < 0 ? -1 : 0);
    }
    case RTAnyType::kI32Value: {
      auto ret = value_.u32_val - other.value_.i32_val;
      return ret > 0 ? 1 : (ret < 0 ? -1 : 0);
    }
    case RTAnyType::kF32Value: {
      auto ret = value_.u32_val - other.value_.f32_val;
      return ret > 0 ? 1 : (ret < 0 ? -1 : 0);
    }
    case RTAnyType::kF64Value: {
      auto ret = value_.u32_val - other.value_.f64_val;
      return ret > 0 ? 1 : (ret < 0 ? -1 : 0);
    }
    case RTAnyType::kU64Value: {
      auto ret = value_.u32_val - other.value_.u64_val;
      return ret > 0 ? 1 : (ret < 0 ? -1 : 0);
    }
    default:
      THROW_NOT_SUPPORTED_EXCEPTION(
          "not support for " + std::to_string(static_cast<int>(other.type_)));
    }
    break;
  case RTAnyType::kU64Value:
    switch (other.type_) {
    case RTAnyType::kI64Value: {
      auto ret = value_.u64_val - other.value_.i64_val;
      return ret > 0 ? 1 : (ret < 0 ? -1 : 0);
    }
    case RTAnyType::kI32Value: {
      auto ret = value_.u64_val - other.value_.i32_val;
      return ret > 0 ? 1 : (ret < 0 ? -1 : 0);
    }
    case RTAnyType::kF32Value: {
      auto ret = value_.u64_val - other.value_.f32_val;
      return ret > 0 ? 1 : (ret < 0 ? -1 : 0);
    }
    case RTAnyType::kF64Value: {
      auto ret = value_.u64_val - other.value_.f64_val;
      return ret > 0 ? 1 : (ret < 0 ? -1 : 0);
    }
    case RTAnyType::kU32Value: {
      auto ret = value_.u64_val - other.value_.u32_val;
      return ret > 0 ? 1 : (ret < 0 ? -1 : 0);
    }
    default:
      THROW_NOT_SUPPORTED_EXCEPTION(
          "not support for " + std::to_string(static_cast<int>(other.type_)));
    }
    break;
  case RTAnyType::kF32Value:
    switch (other.type_) {
    case RTAnyType::kI64Value: {
      auto ret = value_.f32_val - other.value_.i64_val;
      return ret > 0 ? 1 : (ret < 0 ? -1 : 0);
    }
    case RTAnyType::kI32Value: {
      auto ret = value_.f32_val - other.value_.i32_val;
      return ret > 0 ? 1 : (ret < 0 ? -1 : 0);
    }
    case RTAnyType::kU32Value: {
      auto ret = value_.f32_val - other.value_.u32_val;
      return ret > 0 ? 1 : (ret < 0 ? -1 : 0);
    }
    case RTAnyType::kF64Value: {
      auto ret = value_.f32_val - other.value_.f64_val;
      return ret > 0 ? 1 : (ret < 0 ? -1 : 0);
    }
    case RTAnyType::kU64Value: {
      auto ret = value_.f32_val - other.value_.u64_val;
      return ret > 0 ? 1 : (ret < 0 ? -1 : 0);
    }
    default:
      LOG(FATAL) << "not support for " << static_cast<int>(type_);
    }
    break;
  default:
    THROW_NOT_SUPPORTED_EXCEPTION("not support for " +
                                  std::to_string(static_cast<int>(type_)));
  }
  return 0;  // This line is unreachable but avoids compiler warning.
}
inline static bool is_numerical_type(const RTAnyType& type) {
  return type == RTAnyType::kI64Value || type == RTAnyType::kI32Value ||
         type == RTAnyType::kF64Value || type == RTAnyType::kU32Value ||
         type == RTAnyType::kU64Value || type == RTAnyType::kF32Value;
}

bool RTAny::operator<(const RTAny& other) const {
  if (type_ != other.type_) {
    if (is_numerical_type(type_) && is_numerical_type(other.type_)) {
      return numerical_cmp(other) < 0;
    } else {
      return false;
    }
  }
  if (type_ == RTAnyType::kI64Value) {
    return value_.i64_val < other.value_.i64_val;

  } else if (type_ == RTAnyType::kI32Value) {
    return value_.i32_val < other.value_.i32_val;
  } else if (type_ == RTAnyType::kU32Value) {
    return value_.u32_val < other.value_.u32_val;
  } else if (type_ == RTAnyType::kStringValue) {
    return value_.str_val < other.value_.str_val;
  } else if (type_ == RTAnyType::kDate) {
    return value_.date_val < other.value_.date_val;
  } else if (type_ == RTAnyType::kDateTime) {
    return value_.dt_val < other.value_.dt_val;
  } else if (type_ == RTAnyType::kInterval) {
    return value_.interval_val < other.value_.interval_val;
  } else if (type_ == RTAnyType::kF32Value) {
    return value_.f32_val < other.value_.f32_val;
  } else if (type_ == RTAnyType::kF64Value) {
    return value_.f64_val < other.value_.f64_val;
  } else if (type_ == RTAnyType::kEdge) {
    return value_.edge < other.value_.edge;
  } else if (type_ == RTAnyType::kVertex) {
    return value_.vertex < other.value_.vertex;
  } else if (type_ == RTAnyType::kU64Value) {
    return value_.u64_val < other.value_.u64_val;
  } else if (type_ == RTAnyType::kBoolValue) {
    return value_.b_val < other.value_.b_val;
  } else if (type_ == RTAnyType::kPath) {
    return value_.p < other.value_.p;
  }

  THROW_NOT_SUPPORTED_EXCEPTION("not support for " +
                                std::to_string(static_cast<int>(type_)));
  return true;
}

bool RTAny::operator==(const RTAny& other) const {
  if (type_ != other.type_) {
    if (is_numerical_type(type_) && is_numerical_type(other.type_)) {
      return numerical_cmp(other) == 0;
    } else {
      return false;
    }
  }

  if (type_ == RTAnyType::kI64Value) {
    return value_.i64_val == other.value_.i64_val;
  } else if (type_ == RTAnyType::kU64Value) {
    return value_.u64_val == other.value_.u64_val;
  } else if (type_ == RTAnyType::kI32Value) {
    return value_.i32_val == other.value_.i32_val;
  } else if (type_ == RTAnyType::kU32Value) {
    return value_.u32_val == other.value_.u32_val;
  } else if (type_ == RTAnyType::kF32Value) {
    return value_.f32_val == other.value_.f32_val;
  } else if (type_ == RTAnyType::kF64Value) {
    return value_.f64_val == other.value_.f64_val;
  } else if (type_ == RTAnyType::kBoolValue) {
    return value_.b_val == other.value_.b_val;
  } else if (type_ == RTAnyType::kStringValue) {
    return value_.str_val == other.value_.str_val;
  } else if (type_ == RTAnyType::kVertex) {
    return value_.vertex == other.value_.vertex;
  } else if (type_ == RTAnyType::kEdge) {
    return value_.edge == other.value_.edge;
  } else if (type_ == RTAnyType::kDate) {
    return value_.date_val == other.value_.date_val;
  } else if (type_ == RTAnyType::kDateTime) {
    return value_.dt_val == other.value_.dt_val;
  } else if (type_ == RTAnyType::kInterval) {
    return value_.interval_val == other.value_.interval_val;
  }

  THROW_NOT_SUPPORTED_EXCEPTION("not support for " +
                                std::to_string(static_cast<int>(type_)));
  return true;
}

RTAny RTAny::operator+(const RTAny& other) const {
  if (type_ == RTAnyType::kDate) {
    if (other.type() == RTAnyType::kInterval) {
      return RTAny::from_date(value_.date_val + other.value_.interval_val);
    } else {
      THROW_NOT_SUPPORTED_EXCEPTION(
          "not support for " + std::to_string(static_cast<int>(other.type_)));
    }
  } else if (type_ == RTAnyType::kDateTime) {
    if (other.type() == RTAnyType::kInterval) {
      return RTAny::from_datetime(value_.dt_val + other.value_.interval_val);
    } else {
      THROW_NOT_SUPPORTED_EXCEPTION(
          "not support for " + std::to_string(static_cast<int>(other.type_)));
    }
  } else if (type_ == RTAnyType::kInterval) {
    if (other.type() == RTAnyType::kDate) {
      return RTAny::from_date(other.value_.date_val + value_.interval_val);
    } else if (other.type() == RTAnyType::kDateTime) {
      return RTAny::from_datetime(other.value_.dt_val + value_.interval_val);
    } else {
      THROW_NOT_SUPPORTED_EXCEPTION(
          "RTAny::operator+ not support for " +
          std::to_string(static_cast<int>(type_)) + " and " +
          std::to_string(static_cast<int>(other.type_)));
    }
  }

  assert(type_ == other.type_);
  switch (type_) {
  case RTAnyType::kI32Value:
    return RTAny::from_int32(value_.i32_val + other.value_.i32_val);
  case RTAnyType::kU32Value:
    return RTAny::from_uint32(value_.u32_val + other.value_.u32_val);
  case RTAnyType::kI64Value:
    return RTAny::from_int64(value_.i64_val + other.value_.i64_val);
  case RTAnyType::kU64Value:
    return RTAny::from_uint64(value_.u64_val + other.value_.u64_val);
  case RTAnyType::kF32Value:
    return RTAny::from_float(value_.f32_val + other.value_.f32_val);
  case RTAnyType::kF64Value:
    return RTAny::from_double(value_.f64_val + other.value_.f64_val);
  default:
    LOG(FATAL) << "RTAny::operator+ not support for " +
                      std::to_string(static_cast<int>(type_)) + " and " +
                      std::to_string(static_cast<int>(other.type_));
  }
}

// TODO(zhanglei): Support generate RTAny substraction.
RTAny RTAny::operator-(const RTAny& other) const {
  if (type_ == RTAnyType::kDate) {
    if (other.type_ == RTAnyType::kDate) {
      return RTAny::from_interval(value_.date_val - other.value_.date_val);
    } else if (other.type_ == RTAnyType::kInterval) {
      return RTAny::from_date(value_.date_val - other.value_.interval_val);
    } else {
      THROW_RUNTIME_ERROR("RTAny::operator- not support for " +
                          std::to_string(static_cast<int>(type_)) + " and " +
                          std::to_string(static_cast<int>(other.type_)));
    }
  } else if (type_ == RTAnyType::kDateTime) {
    if (other.type_ == RTAnyType::kDateTime) {
      return RTAny::from_interval(value_.dt_val - other.value_.dt_val);
    } else if (other.type_ == RTAnyType::kInterval) {
      return RTAny::from_datetime(value_.dt_val - other.value_.interval_val);
    } else {
      THROW_RUNTIME_ERROR("RTAny::operator- not support for " +
                          std::to_string(static_cast<int>(type_)) + " and " +
                          std::to_string(static_cast<int>(other.type_)));
    }
  } else if (type_ == RTAnyType::kInterval) {
    THROW_RUNTIME_ERROR("RTAny::operator- not support for " +
                        std::to_string(static_cast<int>(type_)) + " and " +
                        std::to_string(static_cast<int>(other.type_)));
  }
  assert(type_ == other.type_);
  switch (type_) {
  case RTAnyType::kI32Value:
    return RTAny::from_int32(value_.i32_val - other.value_.i32_val);
  case RTAnyType::kU32Value:
    return RTAny::from_uint32(value_.u32_val - other.value_.u32_val);
  case RTAnyType::kI64Value:
    return RTAny::from_int64(value_.i64_val - other.value_.i64_val);
  case RTAnyType::kU64Value:
    return RTAny::from_uint64(value_.u64_val - other.value_.u64_val);
  case RTAnyType::kF32Value:
    return RTAny::from_float(value_.f32_val - other.value_.f32_val);
  case RTAnyType::kF64Value:
    return RTAny::from_double(value_.f64_val - other.value_.f64_val);
  default:
    LOG(FATAL) << "RTAny::operator- not support for " +
                      std::to_string(static_cast<int>(type_)) + " and " +
                      std::to_string(static_cast<int>(other.type_));
  }
}

RTAny RTAny::operator*(const RTAny& other) const {
  assert(type_ == other.type_);
  switch (type_) {
  case RTAnyType::kI32Value:
    return RTAny::from_int32(value_.i32_val * other.value_.i32_val);
  case RTAnyType::kU32Value:
    return RTAny::from_uint32(value_.u32_val * other.value_.u32_val);
  case RTAnyType::kI64Value:
    return RTAny::from_int64(value_.i64_val * other.value_.i64_val);
  case RTAnyType::kU64Value:
    return RTAny::from_uint64(value_.u64_val * other.value_.u64_val);
  case RTAnyType::kF32Value:
    return RTAny::from_float(value_.f32_val * other.value_.f32_val);
  case RTAnyType::kF64Value:
    return RTAny::from_double(value_.f64_val * other.value_.f64_val);
  default:
    LOG(FATAL) << "RTAny::operator* not support for " +
                      std::to_string(static_cast<int>(type_)) + " and " +
                      std::to_string(static_cast<int>(other.type_));
  }
}

RTAny RTAny::operator/(const RTAny& other) const {
  assert(type_ == other.type_);
  switch (type_) {
  case RTAnyType::kI32Value:
    return RTAny::from_int32(value_.i32_val / other.value_.i32_val);
  case RTAnyType::kU32Value:
    return RTAny::from_uint32(value_.u32_val / other.value_.u32_val);
  case RTAnyType::kI64Value:
    return RTAny::from_int64(value_.i64_val / other.value_.i64_val);
  case RTAnyType::kU64Value:
    return RTAny::from_uint64(value_.u64_val / other.value_.u64_val);
  case RTAnyType::kF32Value:
    return RTAny::from_float(value_.f32_val / other.value_.f32_val);
  case RTAnyType::kF64Value:
    return RTAny::from_double(value_.f64_val / other.value_.f64_val);
  default:
    LOG(FATAL) << "RTAny::operator/ not support for " +
                      std::to_string(static_cast<int>(type_)) + " and " +
                      std::to_string(static_cast<int>(other.type_));
  }
}

RTAny RTAny::operator%(const RTAny& other) const {
  assert(type_ == other.type_);
  switch (type_) {
  case RTAnyType::kI32Value:
    return RTAny::from_int32(value_.i32_val % other.value_.i32_val);
  case RTAnyType::kU32Value:
    return RTAny::from_uint32(value_.u32_val % other.value_.u32_val);
  case RTAnyType::kI64Value:
    return RTAny::from_int64(value_.i64_val % other.value_.i64_val);
  case RTAnyType::kU64Value:
    return RTAny::from_uint64(value_.u64_val % other.value_.u64_val);
  default:
    LOG(FATAL) << "RTAny::operator% not support for " +
                      std::to_string(static_cast<int>(type_)) + " and " +
                      std::to_string(static_cast<int>(other.type_));
  }
}

void RTAny::sink_impl(common::Value* value) const {
  if (type_ == RTAnyType::kBoolValue) {
    value->set_boolean(value_.b_val);
  } else if (type_ == RTAnyType::kI64Value) {
    value->set_i64(value_.i64_val);
  } else if (type_ == RTAnyType::kStringValue) {
    value->set_str(value_.str_val.data(), value_.str_val.size());
  } else if (type_ == RTAnyType::kI32Value) {
    value->set_i32(value_.i32_val);
  } else if (type_ == RTAnyType::kDate) {
    auto date_str = value_.date_val.to_string();
    value->set_str(date_str.data(), date_str.size());
  } else if (type_ == RTAnyType::kDateTime) {
    auto date_time_str = value_.dt_val.to_string();
    value->set_str(date_time_str.data(), date_time_str.size());
  } else if (type_ == RTAnyType::kInterval) {
    auto interval_str = value_.interval_val.to_string();
    value->set_str(interval_str.data(), interval_str.size());
  } else if (type_ == RTAnyType::kBoolValue) {
    value->set_boolean(value_.b_val);
  } else if (type_ == RTAnyType::kF32Value) {
    value->set_f32(value_.f32_val);
  } else if (type_ == RTAnyType::kF64Value) {
    value->set_f64(value_.f64_val);
  } else if (type_ == RTAnyType::kList) {
    THROW_NOT_SUPPORTED_EXCEPTION("RTAny::sink_impl not support for list type");
  } else if (type_ == RTAnyType::kTuple) {
    auto tup = value_.t;
    for (size_t i = 0; i < tup.size(); ++i) {
      std::string s = tup.get(i).to_string();
      value->mutable_str_array()->add_item(s.data(), s.size());
    }
  } else if (type_ == RTAnyType::kU64Value) {
    value->set_u64(value_.u64_val);
  } else if (type_ == RTAnyType::kU32Value) {
    value->set_u32(value_.u32_val);
  } else if (type_ == RTAnyType::kNull) {
    value->mutable_none();
  } else {
    THROW_NOT_SUPPORTED_EXCEPTION("RTAny::sink_impl not support for " +
                                  std::to_string(static_cast<int>(type_)));
  }
}

// just for ldbc snb interactive queries
void RTAny::encode_sig(RTAnyType type, Encoder& encoder) const {
  if (type == RTAnyType::kI64Value) {
    encoder.put_long(this->as_int64());
  } else if (type == RTAnyType::kStringValue) {
    encoder.put_string_view(this->as_string());
  } else if (type == RTAnyType::kI32Value) {
    encoder.put_int(this->as_int32());
  } else if (type == RTAnyType::kU32Value) {
    encoder.put_int(this->as_uint32());
  } else if (type == RTAnyType::kU64Value) {
    encoder.put_long(this->as_uint64());
  } else if (type == RTAnyType::kVertex) {
    const auto& v = this->value_.vertex;
    encoder.put_byte(v.label_);
    encoder.put_int(v.vid_);
  } else if (type == RTAnyType::kEdge) {
    const auto& [label, src, dst, prop, dir] = this->as_edge();

    encoder.put_byte(label.src_label);
    encoder.put_byte(label.dst_label);
    encoder.put_byte(label.edge_label);
    encoder.put_int(src);
    encoder.put_int(dst);
    encoder.put_byte(dir == Direction::kOut ? 1 : 0);

  } else if (type == RTAnyType::kBoolValue) {
    encoder.put_byte(this->as_bool() ? 1 : 0);
  } else if (type == RTAnyType::kList) {
    encoder.put_int(this->as_list().size());
    List list = this->as_list();
    for (size_t i = 0; i < list.size(); ++i) {
      list.get(i).encode_sig(list.get(i).type(), encoder);
    }
  } else if (type == RTAnyType::kTuple) {
    Tuple tuple = this->as_tuple();
    encoder.put_int(tuple.size());
    for (size_t i = 0; i < tuple.size(); ++i) {
      tuple.get(i).encode_sig(tuple.get(i).type(), encoder);
    }
  } else if (type == RTAnyType::kNull) {
    encoder.put_int(-1);
  } else if (type == RTAnyType::kF32Value) {
    encoder.put_float(this->as_float());
  } else if (type == RTAnyType::kF64Value) {
    encoder.put_double(this->as_double());
  } else if (type == RTAnyType::kPath) {
    Path p = this->as_path();
    encoder.put_int(p.len() + 1);
    auto nodes = p.nodes();
    for (size_t i = 0; i < nodes.size(); ++i) {
      encoder.put_byte(nodes[i].label_);
      encoder.put_int(nodes[i].vid_);
    }
  } else if (type == RTAnyType::kDateTime) {
    encoder.put_long(value_.dt_val.milli_second);
  } else {
    THROW_RUNTIME_ERROR("RTAny::encode_sig not support for " +
                        std::to_string(static_cast<int>(type)));
  }
}

std::string RTAny::to_string() const {
  if (type_ == RTAnyType::kBoolValue) {
    return value_.b_val ? "true" : "false";
  } else if (type_ == RTAnyType::kI64Value) {
    return std::to_string(value_.i64_val);
  } else if (type_ == RTAnyType::kI32Value) {
    return std::to_string(value_.i32_val);
  } else if (type_ == RTAnyType::kU32Value) {
    return std::to_string(value_.u32_val);
  } else if (type_ == RTAnyType::kU64Value) {
    return std::to_string(value_.u64_val);
  } else if (type_ == RTAnyType::kVertex) {
#if 0
      return std::string("v") +
             std::to_string(static_cast<int>(value_.vertex.label_)) + "-" +
             std::to_string(value_.vertex.vid_);
#else
    return std::to_string(value_.vertex.vid_);
#endif
  } else if (type_ == RTAnyType::kSet) {
    std::string ret = "{";
    for (auto& val : value_.set.values()) {
      ret += val.to_string();
      ret += ", ";
    }
    ret += "}";
    return ret;
  } else if (type_ == RTAnyType::kEdge) {
    auto [label, src, dst, prop, dir] = value_.edge;
    return std::to_string(src) + " -> " + std::to_string(dst);
  } else if (type_ == RTAnyType::kPath) {
    return value_.p.to_string();
  } else if (type_ == RTAnyType::kStringValue) {
    return std::string(value_.str_val);
  } else if (type_ == RTAnyType::kList) {
    std::string ret = "[";
    for (size_t i = 0; i < value_.list.size(); ++i) {
      ret += value_.list.get(i).to_string();
      if (i != value_.list.size() - 1) {
        ret += ", ";
      }
    }
    ret += "]";
    return ret;
  } else if (type_ == RTAnyType::kTuple) {
    std::string ret = "(";
    for (size_t i = 0; i < value_.t.size(); ++i) {
      ret += value_.t.get(i).to_string();
      if (i != value_.t.size() - 1) {
        ret += ", ";
      }
    }
    ret += ")";
    return ret;
  } else if (type_ == RTAnyType::kNull) {
    return "null";
  } else if (type_ == RTAnyType::kF32Value) {
    return std::to_string(value_.f32_val);
  } else if (type_ == RTAnyType::kF64Value) {
    return std::to_string(value_.f64_val);
  } else if (type_ == RTAnyType::kDateTime) {
    return value_.dt_val.to_string();
  } else if (type_ == RTAnyType::kDate) {
    return value_.date_val.to_string();
  } else if (type_ == RTAnyType::kInterval) {
    return value_.interval_val.to_string();
  } else {
    LOG(FATAL) << "not implemented for " << static_cast<int>(type_);
    return "";
  }
}

}  // namespace runtime

}  // namespace gs
