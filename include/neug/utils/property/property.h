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

#ifndef INCLUDE_NEUG_UTILS_PROPERTY_PROPERTY_H_
#define INCLUDE_NEUG_UTILS_PROPERTY_PROPERTY_H_

#include <cassert>
#include <cstdint>

#include <string>
#include <string_view>
#include <variant>

#include "libgrape-lite/grape/serialization/in_archive.h"
#include "libgrape-lite/grape/serialization/out_archive.h"
#include "neug/utils/property/types.h"

namespace gs {

union PropValue {
  PropValue() {}
  ~PropValue() {}

  grape::EmptyType empty;
  bool b;
  int32_t i;
  uint32_t ui;
  int64_t l;
  uint64_t ul;
  std::string_view s;
  StringPtr sp;
  float f;
  double db;
  struct TimeStamp ts;
  struct Date d;
  struct DateTime dt;
  struct Interval itv;
};

template <typename T>
struct PropUtils;

class Property {
 public:
  Property() = default;
  ~Property() = default;

  template <typename T>
  explicit Property(const T& val) {
    Property a = Property::From(val);
    type_ = a.type_;
    if (type_.type_enum == impl::PropertyTypeImpl::kString) {
      new (&value_.sp) StringPtr(a.value_.sp);
    } else {
      memcpy(static_cast<void*>(&value_), static_cast<const void*>(&a.value_),
             sizeof(AnyValue));
    }
  }

  Property(const Property& other) {
    type_ = other.type_;
    if (type_.type_enum == impl::PropertyTypeImpl::kString) {
      new (&value_.sp) StringPtr(other.value_.sp);
    } else {
      memcpy(&value_, &other.value_, sizeof(PropValue));
    }
  }

  Property(Property&& other) noexcept {
    type_ = other.type_;
    if (type_.type_enum == impl::PropertyTypeImpl::kString) {
      new (&value_.sp) StringPtr(std::move(other.value_.sp));
    } else {
      memcpy(&value_, &other.value_, sizeof(PropValue));
    }
  }

  void set_bool(bool v) {
    type_ = PropertyType::kBool;
    value_.b = v;
  }

  void set_int32(int32_t v) {
    type_ = PropertyType::kInt32;
    value_.i = v;
  }

  void set_uint32(uint32_t v) {
    type_ = PropertyType::kUInt32;
    value_.ui = v;
  }

  void set_int64(int64_t v) {
    type_ = PropertyType::kInt64;
    value_.l = v;
  }

  void set_uint64(uint64_t v) {
    type_ = PropertyType::kUInt64;
    value_.ul = v;
  }

  void set_string_view(const std::string_view& v) {
    type_ = PropertyType::kStringView;
    value_.s = v;
  }

  void set_string(const std::string& v) {
    type_ = PropertyType::kString;
    new (&value_.sp) StringPtr(v);
  }

  void set_float(float v) {
    type_ = PropertyType::kFloat;
    value_.f = v;
  }

  void set_double(double v) {
    type_ = PropertyType::kDouble;
    value_.db = v;
  }

  void set_timestamp(const TimeStamp& v) {
    type_ = PropertyType::kTimestamp;
    value_.ts = v;
  }

  void set_date(const Date& v) {
    type_ = PropertyType::kDate;
    value_.d = v;
  }

  void set_date(uint32_t val) {
    type_ = PropertyType::kDate;
    value_.d.from_u32(val);
  }

  void set_date_time(const DateTime& v) {
    type_ = PropertyType::kDateTime;
    value_.dt = v;
  }

  void set_date_time(int64_t mill_seconds) {
    type_ = PropertyType::kDateTime;
    value_.dt.milli_second = mill_seconds;
  }

  void set_interval(const Interval& v) {
    type_ = PropertyType::kInterval;
    value_.itv = v;
  }

  void set_interval(uint64_t mill_seconds) {
    type_ = PropertyType::kInterval;
    value_.itv.from_mill_seconds(mill_seconds);
  }

  bool as_bool() const {
    assert(type() == PropertyType::kBool);
    return value_.b;
  }

  int as_int32() const {
    assert(type() == PropertyType::kInt32);
    return value_.i;
  }

  uint32_t as_uint32() const {
    assert(type() == PropertyType::kUInt32);
    return value_.ui;
  }

  int64_t as_int64() const {
    assert(type() == PropertyType::kInt64);
    return value_.l;
  }

  uint64_t as_uint64() const {
    assert(type() == PropertyType::kUInt64);
    return value_.ul;
  }

  std::string_view as_string_view() const {
    assert(type().type_enum == impl::PropertyTypeImpl::kStringView ||
           type().type_enum == impl::PropertyTypeImpl::kString);
    if (type().type_enum == impl::PropertyTypeImpl::kStringView) {
      return value_.s;
    } else {
      return *value_.sp;
    }
  }

  const std::string& as_string() const {
    assert(type().type_enum == impl::PropertyTypeImpl::kString);
    return *value_.sp.ptr;
  }

  float as_float() const {
    assert(type() == PropertyType::kFloat);
    return value_.f;
  }

  double as_double() const {
    assert(type() == PropertyType::kDouble);
    return value_.db;
  }

  TimeStamp as_timestamp() const {
    assert(type() == PropertyType::kTimestamp);
    return value_.ts;
  }

  Date as_date() const {
    assert(type() == PropertyType::kDate);
    return value_.d;
  }

  DateTime as_date_time() const {
    assert(type() == PropertyType::kDateTime);
    return value_.dt;
  }

  Interval as_interval() const {
    assert(type() == PropertyType::kInterval);
    return value_.itv;
  }

  std::string to_string() const {
    auto type = this->type();
    if (type == PropertyType::kInt32) {
      return std::to_string(as_int32());
    } else if (type == PropertyType::kUInt32) {
      return std::to_string(as_uint32());
    } else if (type == PropertyType::kInt64) {
      return std::to_string(as_int64());
    } else if (type == PropertyType::kUInt64) {
      return std::to_string(as_uint64());
    } else if (type.type_enum == impl::PropertyTypeImpl::kStringView) {
      return std::string(as_string_view());
    } else if (type.type_enum == impl::PropertyTypeImpl::kString) {
      return as_string();
    } else if (type == PropertyType::kFloat) {
      return std::to_string(as_float());
    } else if (type == PropertyType::kDouble) {
      return std::to_string(as_double());
    } else if (type == PropertyType::kTimestamp) {
      return as_timestamp().to_string();
    } else if (type == PropertyType::kDate) {
      return as_date().to_string();
    } else if (type == PropertyType::kDateTime) {
      return as_date_time().to_string();
    } else if (type == PropertyType::kInterval) {
      return as_interval().to_string();
    } else if (type == PropertyType::kBool) {
      return as_bool() ? "true" : "false";
    } else if (type == PropertyType::kEmpty) {
      return "EMPTY";
    } else {
      return "UNKNOWN";
    }
  }

  static Property empty() { return Property(); }

  static Property from_bool(bool v) {
    Property ret;
    ret.set_bool(v);
    return ret;
  }

  static Property from_int32(int32_t v) {
    Property ret;
    ret.set_int32(v);
    return ret;
  }

  static Property from_uint32(uint32_t v) {
    Property ret;
    ret.set_uint32(v);
    return ret;
  }

  static Property from_int64(int64_t v) {
    Property ret;
    ret.set_int64(v);
    return ret;
  }

  static Property from_uint64(uint64_t v) {
    Property ret;
    ret.set_uint64(v);
    return ret;
  }

  static Property from_string_view(const std::string_view& v) {
    Property ret;
    ret.set_string_view(v);
    return ret;
  }

  static Property from_string(const std::string& v) {
    Property ret;
    ret.set_string(v);
    return ret;
  }

  static Property from_float(float v) {
    Property ret;
    ret.set_float(v);
    return ret;
  }

  static Property from_double(double v) {
    Property ret;
    ret.set_double(v);
    return ret;
  }

  static Property from_timestamp(const TimeStamp& v) {
    Property ret;
    ret.set_timestamp(v);
    return ret;
  }

  static Property from_date(const Date& v) {
    Property ret;
    ret.set_date(v);
    return ret;
  }

  static Property from_date_time(const DateTime& v) {
    Property ret;
    ret.set_date_time(v);
    return ret;
  }

  static Property from_interval(const Interval& v) {
    Property ret;
    ret.set_interval(v);
    return ret;
  }

  PropertyType type() const { return type_; }

  Property& operator=(const Property& other);

  bool operator==(const Property& other) const;

  bool operator<(const Property& other) const;

  template <typename T>
  static Property From(const T& v) {
    return PropUtils<T>::to_prop(v);
  }

 private:
  PropertyType type_;
  PropValue value_;
};

inline Property parse_property_from_string(PropertyType pt,
                                           const std::string& str) {
  if (pt == PropertyType::kEmpty) {
    return Property::empty();
  } else if (pt == PropertyType::kBool) {
    return Property::from_bool(str == "true" || str == "1" || str == "TRUE");
  } else if (pt == PropertyType::kInt32) {
    return Property::from_int32(std::stoi(str));
  } else if (pt == PropertyType::kUInt32) {
    return Property::from_uint32(static_cast<uint32_t>(std::stoul(str)));
  } else if (pt == PropertyType::kInt64) {
    return Property::from_int64(std::stoll(str));
  } else if (pt == PropertyType::kUInt64) {
    return Property::from_uint64(static_cast<uint64_t>(std::stoull(str)));
  } else if (pt.type_enum == impl::PropertyTypeImpl::kStringView) {
    return Property::from_string_view(str);
  } else if (pt == PropertyType::kFloat) {
    return Property::from_float(std::stof(str));
  } else if (pt == PropertyType::kDouble) {
    return Property::from_double(std::stod(str));
  } else if (pt == PropertyType::kTimestamp) {
    return Property::from_timestamp(TimeStamp(std::stoll(str)));
  } else if (pt == PropertyType::kDate) {
    return Property::from_date(Date(str));
  } else if (pt == PropertyType::kDateTime) {
    return Property::from_date_time(DateTime(str));
  } else if (pt == PropertyType::kInterval) {
    return Property::from_interval(Interval(str));
  } else {
    LOG(FATAL) << "Unsupported property type: " << pt.ToString();
    return Property::empty();
  }
}
inline void serialize_property(grape::InArchive& arc, const Property& prop) {
  auto type = prop.type();
  if (type == PropertyType::kBool) {
    arc << prop.as_bool();
  } else if (type == PropertyType::kInt32) {
    arc << prop.as_int32();
  } else if (type == PropertyType::kUInt32) {
    arc << prop.as_uint32();
  } else if (type == PropertyType::kInt64) {
    arc << prop.as_int64();
  } else if (type == PropertyType::kUInt64) {
    arc << prop.as_uint64();
  } else if (type.type_enum == impl::PropertyTypeImpl::kString) {
    arc << prop.as_string();
  } else if (type.type_enum == impl::PropertyTypeImpl::kStringView) {
    arc << prop.as_string_view();
  } else if (type == PropertyType::kFloat) {
    arc << prop.as_float();
  } else if (type == PropertyType::kDouble) {
    arc << prop.as_double();
  } else if (type == PropertyType::kTimestamp) {
    arc << prop.as_timestamp().milli_second;
  } else if (type == PropertyType::kDate) {
    arc << prop.as_date().to_u32();
  } else if (type == PropertyType::kDateTime) {
    arc << prop.as_date_time().milli_second;
  } else if (type == PropertyType::kInterval) {
    arc << prop.as_interval().to_mill_seconds();
  } else if (type == PropertyType::kEmpty) {
  } else {
    LOG(FATAL) << "Unexpected property type" << type.ToString();
  }
}

inline void deserialize_property(grape::OutArchive& arc, PropertyType pt,
                                 Property& prop) {
  if (pt == PropertyType::kBool) {
    bool v;
    arc >> v;
    prop.set_bool(v);
  } else if (pt == PropertyType::kInt32) {
    int32_t v;
    arc >> v;
    prop.set_int32(v);
  } else if (pt == PropertyType::kUInt32) {
    uint32_t v;
    arc >> v;
    prop.set_uint32(v);
  } else if (pt == PropertyType::kInt64) {
    int64_t v;
    arc >> v;
    prop.set_int64(v);
  } else if (pt == PropertyType::kUInt64) {
    uint64_t v;
    arc >> v;
    prop.set_uint64(v);
  } else if (pt.type_enum == impl::PropertyTypeImpl::kStringView) {
    std::string_view v;
    arc >> v;
    prop.set_string_view(v);
  } else if (pt.type_enum == impl::PropertyTypeImpl::kString) {
    std::string v;
    arc >> v;
    prop.set_string(v);
  } else if (pt == PropertyType::kFloat) {
    float v;
    arc >> v;
    prop.set_float(v);
  } else if (pt == PropertyType::kDouble) {
    double v;
    arc >> v;
    prop.set_double(v);
  } else if (pt == PropertyType::kTimestamp) {
    int64_t ts_val;
    arc >> ts_val;
    prop.set_timestamp(TimeStamp(ts_val));
  } else if (pt == PropertyType::kDate) {
    uint32_t date_val;
    arc >> date_val;
    Date d;
    d.from_u32(date_val);
    prop.set_date(d);
  } else if (pt == PropertyType::kDateTime) {
    int64_t dt_val;
    arc >> dt_val;
    prop.set_date_time(DateTime(dt_val));
  } else if (pt == PropertyType::kInterval) {
    int64_t iv_val;
    arc >> iv_val;
    Interval iv;
    iv.from_mill_seconds(iv_val);
    prop.set_interval(iv);
  } else if (pt == PropertyType::kEmpty) {
    prop = Property::empty();
  } else {
    LOG(FATAL) << "Unexpected property type" << pt.ToString();
  }
}

template <typename T>
struct PropUtils {
  static Property to_prop(const T& v) {
    LOG(FATAL) << "Not implemented";
    return Property::empty();
  }

  static T to_typed(const Property& prop) {
    LOG(FATAL) << "Not implemented";
    return T();
  }
};

template <>
struct PropUtils<bool> {
  static PropertyType prop_type() { return PropertyType::kBool; }
  static bool to_typed(const Property& prop) { return prop.as_bool(); }
  static Property to_prop(bool v) { return Property::from_bool(v); }
};

template <>
struct PropUtils<int32_t> {
  static PropertyType prop_type() { return PropertyType::kInt32; }
  static int32_t to_typed(const Property& prop) { return prop.as_int32(); }
  static Property to_prop(int32_t v) { return Property::from_int32(v); }
};

template <>
struct PropUtils<uint32_t> {
  static PropertyType prop_type() { return PropertyType::kUInt32; }
  static uint32_t to_typed(const Property& prop) { return prop.as_uint32(); }
  static Property to_prop(uint32_t v) { return Property::from_uint32(v); }
};

template <>
struct PropUtils<int64_t> {
  static PropertyType prop_type() { return PropertyType::kInt64; }
  static int64_t to_typed(const Property& prop) { return prop.as_int64(); }
  static Property to_prop(int64_t v) { return Property::from_int64(v); }
};

template <>
struct PropUtils<uint64_t> {
  static PropertyType prop_type() { return PropertyType::kUInt64; }
  static uint64_t to_typed(const Property& prop) { return prop.as_uint64(); }
  static Property to_prop(uint64_t v) { return Property::from_uint64(v); }
};

template <>
struct PropUtils<std::string_view> {
  static PropertyType prop_type() { return PropertyType::kStringView; }
  static std::string_view to_typed(const Property& prop) {
    return prop.as_string_view();
  }
  static Property to_prop(const std::string_view& v) {
    return Property::from_string_view(v);
  }
};

template <>
struct PropUtils<std::string> {
  static PropertyType prop_type() { return PropertyType::kString; }
  static std::string to_typed(const Property& prop) { return prop.as_string(); }
  static Property to_prop(const std::string& v) {
    return Property::from_string(v);
  }
};

template <>
struct PropUtils<float> {
  static PropertyType prop_type() { return PropertyType::kFloat; }
  static float to_typed(const Property& prop) { return prop.as_float(); }
  static Property to_prop(float v) { return Property::from_float(v); }
};

template <>
struct PropUtils<double> {
  static PropertyType prop_type() { return PropertyType::kDouble; }
  static double to_typed(const Property& prop) { return prop.as_double(); }
  static Property to_prop(double v) { return Property::from_double(v); }
};

template <>
struct PropUtils<TimeStamp> {
  static PropertyType prop_type() { return PropertyType::kTimestamp; }
  static TimeStamp to_typed(const Property& prop) {
    return prop.as_timestamp();
  }
  static Property to_prop(const TimeStamp& v) {
    return Property::from_timestamp(v);
  }
  static Property to_prop(int64_t mill_seconds) {
    return Property::from_timestamp(TimeStamp(mill_seconds));
  }
};

template <>
struct PropUtils<Date> {
  static PropertyType prop_type() { return PropertyType::kDate; }
  static Date to_typed(const Property& prop) { return prop.as_date(); }
  static Property to_prop(const Date& v) { return Property::from_date(v); }
  static Property to_prop(int32_t num_days) {
    return Property::from_date(Date(num_days));
  }
};

template <>
struct PropUtils<DateTime> {
  static PropertyType prop_type() { return PropertyType::kDateTime; }
  static DateTime to_typed(const Property& prop) { return prop.as_date_time(); }
  static Property to_prop(const DateTime& v) {
    return Property::from_date_time(v);
  }
  static Property to_prop(int64_t mill_seconds) {
    return Property::from_date_time(DateTime(mill_seconds));
  }
};

template <>
struct PropUtils<grape::EmptyType> {
  static PropertyType prop_type() { return PropertyType::kEmpty; }
  static grape::EmptyType to_typed(const Property& prop) {
    return grape::EmptyType();
  }
  static Property to_prop(const grape::EmptyType& v) {
    return Property::empty();
  }
};

template <>
struct PropUtils<Interval> {
  static PropertyType prop_type() { return PropertyType::kInterval; }
  static Interval to_typed(const Property& prop) { return prop.as_interval(); }
  static Property to_prop(const Interval& v) {
    return Property::from_interval(v);
  }
  static Property to_prop(const std::string_view& str) {
    return Property::from_interval(Interval(str));
  }
};

grape::InArchive& operator<<(grape::InArchive& in_archive,
                             const Property& value);
grape::OutArchive& operator>>(grape::OutArchive& out_archive, Property& value);

}  // namespace gs

#endif  // INCLUDE_NEUG_UTILS_PROPERTY_PROPERTY_H_
