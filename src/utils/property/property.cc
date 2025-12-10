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

#include "neug/utils/property/property.h"

namespace gs {

grape::InArchive& operator<<(grape::InArchive& in_archive,
                             const Property& value) {
  if (value.type() == PropertyType::Empty()) {
    in_archive << value.type();
  } else if (value.type() == PropertyType::Bool()) {
    in_archive << value.type() << value.as_bool();
  } else if (value.type() == PropertyType::Int32()) {
    in_archive << value.type() << value.as_int32();
  } else if (value.type() == PropertyType::UInt32()) {
    in_archive << value.type() << value.as_uint32();
  } else if (value.type() == PropertyType::Int64()) {
    in_archive << value.type() << value.as_int64();
  } else if (value.type() == PropertyType::UInt64()) {
    in_archive << value.type() << value.as_uint64();
  } else if (value.type() == PropertyType::Float()) {
    in_archive << value.type() << value.as_float();
  } else if (value.type() == PropertyType::Double()) {
    in_archive << value.type() << value.as_double();
  } else if (value.type().type_enum == impl::PropertyTypeImpl::kString) {
    // serialize as string_view
    auto s = value.as_string();
    auto type = PropertyType::StringView();
    in_archive << type << s;
  } else if (value.type().type_enum == impl::PropertyTypeImpl::kStringView) {
    in_archive << value.type() << value.as_string_view();
  } else if (value.type() == PropertyType::Date()) {
    in_archive << value.type() << value.as_date().to_u32();
  } else if (value.type() == PropertyType::DateTime()) {
    in_archive << value.type() << value.as_datetime().milli_second;
  } else if (value.type() == PropertyType::Interval()) {
    in_archive << value.type() << value.as_interval().months
               << value.as_interval().days << value.as_interval().micros;
  } else {
    THROW_NOT_SUPPORTED_EXCEPTION(std::string("Not supported: ") +
                                  value.type().ToString());
  }

  return in_archive;
}

grape::OutArchive& operator>>(grape::OutArchive& out_archive, Property& value) {
  PropertyType pt;
  out_archive >> pt;
  if (pt == PropertyType::Empty()) {
  } else if (pt == PropertyType::Bool()) {
    bool tmp;
    out_archive >> tmp;
    value.set_bool(tmp);
  } else if (pt == PropertyType::Int32()) {
    int32_t tmp;
    out_archive >> tmp;
    value.set_int32(tmp);
  } else if (pt == PropertyType::UInt32()) {
    uint32_t tmp;
    out_archive >> tmp;
    value.set_uint32(tmp);
  } else if (pt == PropertyType::Float()) {
    float tmp;
    out_archive >> tmp;
    value.set_float(tmp);
  } else if (pt == PropertyType::Int64()) {
    int64_t tmp;
    out_archive >> tmp;
    value.set_int64(tmp);
  } else if (pt == PropertyType::UInt64()) {
    uint64_t tmp;
    out_archive >> tmp;
    value.set_uint64(tmp);
  } else if (pt == PropertyType::Double()) {
    double tmp;
    out_archive >> tmp;
    value.set_double(tmp);
  } else if (pt.type_enum == impl::PropertyTypeImpl::kString) {
    THROW_NOT_SUPPORTED_EXCEPTION(
        "Deserialization of String type is not supported, use StringView "
        "instead.");
  } else if (pt.type_enum == impl::PropertyTypeImpl::kStringView) {
    std::string_view tmp;
    out_archive >> tmp;
    value.set_string_view(tmp);
  } else if (pt == PropertyType::Date()) {
    uint32_t date_val;
    out_archive >> date_val;
    value.set_date(date_val);
  } else if (pt == PropertyType::DateTime()) {
    int64_t date_time_val;
    out_archive >> date_time_val;
    value.set_datetime(date_time_val);
  } else if (pt == PropertyType::Interval()) {
    Interval interval_val;
    out_archive >> interval_val.months >> interval_val.days >>
        interval_val.micros;
    value.set_interval(interval_val);
  } else {
    THROW_NOT_SUPPORTED_EXCEPTION("Not supported: " + value.type().ToString());
  }

  return out_archive;
}

Property& Property::operator=(const Property& other) {
  if (this != &other) {
    // clean up existing string ptr if needed
    if (type_.type_enum == impl::PropertyTypeImpl::kString) {
      value_.sp.~StringPtr();
    }

    type_ = other.type_;
    if (type_.type_enum == impl::PropertyTypeImpl::kString) {
      new (&value_.sp) StringPtr(*other.value_.sp.ptr);
    } else {
      memcpy(&value_, &other.value_, sizeof(PropValue));
    }
  }
  return *this;
}

bool Property::operator==(const Property& other) const {
  if (type_ != other.type_) {
    return false;
  } else {
    if (type() == PropertyType::kInt32) {
      return value_.i == other.value_.i;
    } else if (type() == PropertyType::kUInt32) {
      return value_.ui == other.value_.ui;
    } else if (type().type_enum == impl::PropertyTypeImpl::kString) {
      return *value_.sp == other.as_string();
    } else if (type().type_enum == impl::PropertyTypeImpl::kStringView) {
      return value_.s == other.as_string_view();
    } else if (type().type_enum == impl::PropertyTypeImpl::kString) {
      return this->as_string() == other.as_string();
    } else if (type() == PropertyType::kEmpty) {
      return true;
    } else if (type() == PropertyType::kDouble) {
      return value_.db == other.value_.db;
    } else if (type() == PropertyType::kInt64) {
      return value_.l == other.value_.l;
    } else if (type() == PropertyType::kUInt64) {
      return value_.ul == other.value_.ul;
    } else if (type() == PropertyType::kBool) {
      return value_.b == other.value_.b;
    } else if (type() == PropertyType::kFloat) {
      return value_.f == other.value_.f;
    } else {
      return false;
    }
  }
}

bool Property::operator<(const Property& other) const {
  if (type_ == other.type_) {
    if (type() == PropertyType::kInt32) {
      return value_.i < other.value_.i;
    } else if (type() == PropertyType::kInt64) {
      return value_.l < other.value_.l;
    } else if (type().type_enum == impl::PropertyTypeImpl::kString) {
      return *value_.sp < other.as_string();
    } else if (type().type_enum == impl::PropertyTypeImpl::kStringView) {
      return value_.s < other.value_.s;
    } else if (type() == PropertyType::kEmpty) {
      return false;
    } else if (type() == PropertyType::kDouble) {
      return value_.db < other.value_.db;
    } else if (type() == PropertyType::kUInt32) {
      return value_.ui < other.value_.ui;
    } else if (type() == PropertyType::kUInt64) {
      return value_.ul < other.value_.ul;
    } else if (type() == PropertyType::kBool) {
      return value_.b < other.value_.b;
    } else if (type() == PropertyType::kFloat) {
      return value_.f < other.value_.f;
    } else if (type() == PropertyType::kDate) {
      return value_.d.to_u32() < other.value_.d.to_u32();
    } else if (type() == PropertyType::kDateTime) {
      return value_.dt.milli_second < other.value_.dt.milli_second;
    } else if (type() == PropertyType::kInterval) {
      return value_.itv < other.value_.itv;
    } else {
      THROW_NOT_SUPPORTED_EXCEPTION(
          "Unsupported property type for comparison: " + type().ToString());
    }
  } else {
    return type_.type_enum < other.type_.type_enum;
  }
}
}  // namespace gs
