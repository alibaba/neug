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

enum class PropType {
  kEmpty,
  kBool,
  kInt32,
  kUInt32,
  kInt64,
  kUInt64,
  kString,
  kFloat,
  kDouble,
  kTimestamp,
  kDate,
  kDateTime,
  kInterval,
};

using PropValue =
    std::variant<grape::EmptyType, bool, int32_t, uint32_t, int64_t, uint64_t,
                 std::string_view, float, double, struct TimeStamp, struct Date,
                 struct DateTime, struct Interval>;

class Prop {
 public:
  Prop() = default;
  ~Prop() = default;

  PropType type() const { return static_cast<PropType>(value_.index()); }

  void set_bool(bool v) { value_ = v; }

  void set_int32(int32_t v) { value_ = v; }

  void set_uint32(uint32_t v) { value_ = v; }

  void set_int64(int64_t v) { value_ = v; }

  void set_uint64(uint64_t v) { value_ = v; }

  void set_string(const std::string_view& v) { value_ = v; }

  void set_float(float v) { value_ = v; }

  void set_double(double v) { value_ = v; }

  void set_timestamp(const TimeStamp& v) { value_ = v; }

  void set_date(const Date& v) { value_ = v; }

  void set_date_time(const DateTime& v) { value_ = v; }

  void set_interval(const Interval& v) { value_ = v; }

  bool as_bool() const {
    assert(type() == PropType::kBool);
    return std::get<1>(value_);
  }

  int as_int32() const {
    assert(type() == PropType::kInt32);
    return std::get<2>(value_);
  }

  uint32_t as_uint32() const {
    assert(type() == PropType::kUInt32);
    return std::get<3>(value_);
  }

  int64_t as_int64() const {
    assert(type() == PropType::kInt64);
    return std::get<4>(value_);
  }

  uint64_t as_uint64() const {
    assert(type() == PropType::kUInt64);
    return std::get<5>(value_);
  }

  std::string_view as_string() const {
    assert(type() == PropType::kString);
    return std::get<6>(value_);
  }

  float as_float() const {
    assert(type() == PropType::kFloat);
    return std::get<7>(value_);
  }

  double as_double() const {
    assert(type() == PropType::kDouble);
    return std::get<8>(value_);
  }

  TimeStamp as_timestamp() const {
    assert(type() == PropType::kTimestamp);
    return std::get<9>(value_);
  }

  Date as_date() const {
    assert(type() == PropType::kDate);
    return std::get<10>(value_);
  }

  DateTime as_date_time() const {
    assert(type() == PropType::kDateTime);
    return std::get<11>(value_);
  }

  Interval as_interval() const {
    assert(type() == PropType::kInterval);
    return std::get<12>(value_);
  }

  std::string to_string() const {
    switch (type()) {
    case PropType::kEmpty:
      return "EMPTY";
    case PropType::kInt32:
      return std::to_string(as_int32());
    case PropType::kUInt32:
      return std::to_string(as_uint32());
    case PropType::kInt64:
      return std::to_string(as_int64());
    case PropType::kUInt64:
      return std::to_string(as_uint64());
    case PropType::kString:
      return std::string(as_string());
    case PropType::kFloat:
      return std::to_string(as_float());
    case PropType::kDouble:
      return std::to_string(as_double());
    case PropType::kTimestamp:
      return as_timestamp().to_string();
    case PropType::kDate:
      return as_date().to_string();
    case PropType::kDateTime:
      return as_date_time().to_string();
    case PropType::kInterval:
      return as_interval().to_string();
    default:
      return "UNKNOWN";
    }
  }

  static Prop empty() { return Prop(); }

  static Prop from_bool(bool v) {
    Prop ret;
    ret.set_bool(v);
    return ret;
  }

  static Prop from_int32(int32_t v) {
    Prop ret;
    ret.set_int32(v);
    return ret;
  }

  static Prop from_uint32(uint32_t v) {
    Prop ret;
    ret.set_uint32(v);
    return ret;
  }

  static Prop from_int64(int64_t v) {
    Prop ret;
    ret.set_int64(v);
    return ret;
  }

  static Prop from_uint64(uint64_t v) {
    Prop ret;
    ret.set_uint64(v);
    return ret;
  }

  static Prop from_string(const std::string_view& v) {
    Prop ret;
    ret.set_string(v);
    return ret;
  }

  static Prop from_float(float v) {
    Prop ret;
    ret.set_float(v);
    return ret;
  }

  static Prop from_double(double v) {
    Prop ret;
    ret.set_double(v);
    return ret;
  }

  static Prop from_timestamp(const TimeStamp& v) {
    Prop ret;
    ret.set_timestamp(v);
    return ret;
  }

  static Prop from_date(const Date& v) {
    Prop ret;
    ret.set_date(v);
    return ret;
  }

  static Prop from_date_time(const DateTime& v) {
    Prop ret;
    ret.set_date_time(v);
    return ret;
  }

  static Prop from_interval(const Interval& v) {
    Prop ret;
    ret.set_interval(v);
    return ret;
  }

 private:
  PropValue value_;
};

inline PropType to_prop_type(const PropertyType& pt) {
  if (pt == PropertyType::Bool()) {
    return PropType::kBool;
  } else if (pt == PropertyType::Int32()) {
    return PropType::kInt32;
  } else if (pt == PropertyType::UInt32()) {
    return PropType::kUInt32;
  } else if (pt == PropertyType::Int64()) {
    return PropType::kInt64;
  } else if (pt == PropertyType::UInt64()) {
    return PropType::kUInt64;
  } else if (pt == PropertyType::String() || pt == PropertyType::StringView()) {
    return PropType::kString;
  } else if (pt == PropertyType::Float()) {
    return PropType::kFloat;
  } else if (pt == PropertyType::Double()) {
    return PropType::kDouble;
  } else if (pt == PropertyType::Timestamp()) {
    return PropType::kTimestamp;
  } else if (pt == PropertyType::Date()) {
    return PropType::kDate;
  } else if (pt == PropertyType::Empty()) {
    return PropType::kEmpty;
  } else if (pt == PropertyType::DateTime()) {
    return PropType::kDateTime;
  } else if (pt == PropertyType::Interval()) {
    return PropType::kInterval;
  } else {
    LOG(FATAL) << "Unsupported property type: " << pt;
    return PropType::kEmpty;
  }
}

inline Prop parse_property_from_string(PropType pt, const std::string& str) {
  switch (pt) {
  case PropType::kEmpty:
    return Prop::empty();
  case PropType::kBool:
    return Prop::from_bool(str == "true" || str == "1" || str == "TRUE");
  case PropType::kInt32:
    return Prop::from_int32(std::stoi(str));
  case PropType::kUInt32:
    return Prop::from_uint32(static_cast<uint32_t>(std::stoul(str)));
  case PropType::kInt64:
    return Prop::from_int64(std::stoll(str));
  case PropType::kUInt64:
    return Prop::from_uint64(static_cast<uint64_t>(std::stoull(str)));
  case PropType::kString:
    return Prop::from_string(str);
  case PropType::kFloat:
    return Prop::from_float(std::stof(str));
  case PropType::kDouble:
    return Prop::from_double(std::stod(str));
  case PropType::kTimestamp:
    return Prop::from_timestamp(TimeStamp(std::stoll(str)));
  case PropType::kDate:
    return Prop::from_date(Date(str));
  case PropType::kDateTime:
    return Prop::from_date_time(DateTime(str));
  case PropType::kInterval:
    return Prop::from_interval(Interval(str));
  default:
    LOG(FATAL) << "Unsupported property type: " << static_cast<int>(pt);
    return Prop::empty();
  }
}

inline void serialize_property(grape::InArchive& arc, const Prop& prop) {
  auto type = prop.type();
  if (type == PropType::kBool) {
    arc << prop.as_bool();
  } else if (type == PropType::kInt32) {
    arc << prop.as_int32();
  } else if (type == PropType::kUInt32) {
    arc << prop.as_uint32();
  } else if (type == PropType::kInt64) {
    arc << prop.as_int64();
  } else if (type == PropType::kUInt64) {
    arc << prop.as_uint64();
  } else if (type == PropType::kString) {
    arc << prop.as_string();
  } else if (type == PropType::kFloat) {
    arc << prop.as_float();
  } else if (type == PropType::kDouble) {
    arc << prop.as_double();
  } else if (type == PropType::kTimestamp) {
    arc << prop.as_timestamp().milli_second;
  } else if (type == PropType::kDate) {
    arc << prop.as_date().to_u32();
  } else if (type == PropType::kDateTime) {
    arc << prop.as_date_time().milli_second;
  } else if (type == PropType::kInterval) {
    arc << prop.as_interval().to_mill_seconds();
  } else if (type == PropType::kEmpty) {
  } else {
    LOG(FATAL) << "Unexpected property type" << int(type);
  }
}

inline void deserialize_property(grape::OutArchive& arc, PropType pt,
                                 Prop& prop) {
  if (pt == PropType::kBool) {
    bool v;
    arc >> v;
    prop.set_bool(v);
  } else if (pt == PropType::kInt32) {
    int32_t v;
    arc >> v;
    prop.set_int32(v);
  } else if (pt == PropType::kUInt32) {
    uint32_t v;
    arc >> v;
    prop.set_uint32(v);
  } else if (pt == PropType::kInt64) {
    int64_t v;
    arc >> v;
    prop.set_int64(v);
  } else if (pt == PropType::kUInt64) {
    uint64_t v;
    arc >> v;
    prop.set_uint64(v);
  } else if (pt == PropType::kString) {
    std::string_view v;
    arc >> v;
    prop.set_string(v);
  } else if (pt == PropType::kFloat) {
    float v;
    arc >> v;
    prop.set_float(v);
  } else if (pt == PropType::kDouble) {
    double v;
    arc >> v;
    prop.set_double(v);
  } else if (pt == PropType::kTimestamp) {
    int64_t ts_val;
    arc >> ts_val;
    prop.set_timestamp(TimeStamp(ts_val));
  } else if (pt == PropType::kDate) {
    uint32_t date_val;
    arc >> date_val;
    Date d;
    d.from_u32(date_val);
    prop.set_date(d);
  } else if (pt == PropType::kDateTime) {
    int64_t dt_val;
    arc >> dt_val;
    prop.set_date_time(DateTime(dt_val));
  } else if (pt == PropType::kInterval) {
    int64_t iv_val;
    arc >> iv_val;
    Interval iv;
    iv.from_mill_seconds(iv_val);
    prop.set_interval(iv);
  } else if (pt == PropType::kEmpty) {
    prop = Prop::empty();
  } else {
    LOG(FATAL) << "Unexpected property type" << int(pt);
  }
}

template <typename T>
struct PropUtils {
  static Prop to_prop(const T& v) {
    LOG(FATAL) << "Not implemented";
    return Prop::empty();
  }

  static T to_typed(const Prop& prop) {
    LOG(FATAL) << "Not implemented";
    return T();
  }
};

template <>
struct PropUtils<bool> {
  static PropType prop_type() { return PropType::kBool; }
  static bool to_typed(const Prop& prop) { return prop.as_bool(); }
  static Prop to_prop(bool v) { return Prop::from_bool(v); }
};

template <>
struct PropUtils<int32_t> {
  static PropType prop_type() { return PropType::kInt32; }
  static int32_t to_typed(const Prop& prop) { return prop.as_int32(); }
  static Prop to_prop(int32_t v) { return Prop::from_int32(v); }
};

template <>
struct PropUtils<uint32_t> {
  static PropType prop_type() { return PropType::kUInt32; }
  static uint32_t to_typed(const Prop& prop) { return prop.as_uint32(); }
  static Prop to_prop(uint32_t v) { return Prop::from_uint32(v); }
};

template <>
struct PropUtils<int64_t> {
  static PropType prop_type() { return PropType::kInt64; }
  static int64_t to_typed(const Prop& prop) { return prop.as_int64(); }
  static Prop to_prop(int64_t v) { return Prop::from_int64(v); }
};

template <>
struct PropUtils<uint64_t> {
  static PropType prop_type() { return PropType::kUInt64; }
  static uint64_t to_typed(const Prop& prop) { return prop.as_uint64(); }
  static Prop to_prop(uint64_t v) { return Prop::from_uint64(v); }
};

template <>
struct PropUtils<std::string_view> {
  static PropType prop_type() { return PropType::kString; }
  static std::string_view to_typed(const Prop& prop) {
    return prop.as_string();
  }
  static Prop to_prop(const std::string_view& v) {
    return Prop::from_string(v);
  }
};

template <>
struct PropUtils<float> {
  static PropType prop_type() { return PropType::kFloat; }
  static float to_typed(const Prop& prop) { return prop.as_float(); }
  static Prop to_prop(float v) { return Prop::from_float(v); }
};

template <>
struct PropUtils<double> {
  static PropType prop_type() { return PropType::kDouble; }
  static double to_typed(const Prop& prop) { return prop.as_double(); }
  static Prop to_prop(double v) { return Prop::from_double(v); }
};

template <>
struct PropUtils<TimeStamp> {
  static PropType prop_type() { return PropType::kTimestamp; }
  static TimeStamp to_typed(const Prop& prop) { return prop.as_timestamp(); }
  static Prop to_prop(const TimeStamp& v) { return Prop::from_timestamp(v); }
};

template <>
struct PropUtils<Date> {
  static PropType prop_type() { return PropType::kDate; }
  static Date to_typed(const Prop& prop) { return prop.as_date(); }
  static Prop to_prop(const Date& v) { return Prop::from_date(v); }
};

template <>
struct PropUtils<DateTime> {
  static PropType prop_type() { return PropType::kDateTime; }
  static DateTime to_typed(const Prop& prop) { return prop.as_date_time(); }
  static Prop to_prop(const DateTime& v) { return Prop::from_date_time(v); }
};

template <>
struct PropUtils<grape::EmptyType> {
  static PropType prop_type() { return PropType::kEmpty; }
  static grape::EmptyType to_typed(const Prop& prop) {
    return grape::EmptyType();
  }
  static Prop to_prop(const grape::EmptyType& v) { return Prop::empty(); }
};

template <>
struct PropUtils<Interval> {
  static PropType prop_type() { return PropType::kInterval; }
  static Interval to_typed(const Prop& prop) { return prop.as_interval(); }
  static Prop to_prop(const Interval& v) { return Prop::from_interval(v); }
};

}  // namespace gs

#endif  // INCLUDE_NEUG_UTILS_PROPERTY_PROPERTY_H_
