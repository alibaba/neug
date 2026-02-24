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
#pragma once

#include <cassert>
#include <cstdint>

#include <string>
#include <string_view>
#include <variant>

#include "neug/utils/property/types.h"
#include "neug/utils/serialization/in_archive.h"
#include "neug/utils/serialization/out_archive.h"

namespace neug {

/**
 * @brief Internal union for storing property values of different types.
 * @note This is an implementation detail. Use Property class for API access.
 */
union PropValue {
  PropValue() {}
  ~PropValue() {}

  EmptyType empty;
  bool b;
  int32_t i;
  uint32_t ui;
  int64_t l;
  uint64_t ul;
  std::string_view s;
  float f;
  double db;
  struct Date d;
  struct DateTime dt;
  struct Interval itv;
};

template <typename T>
struct PropUtils;

/**
 * @brief Generic property value container supporting multiple data types.
 *
 * Property is a variant-like container that can hold values of different
 * types (int, float, string, date, etc.). It is used throughout NeuG to
 * represent vertex/edge property values and query parameters.
 *
 * **Supported Types:**
 * - `bool` - Boolean values
 * - `int32_t`, `uint32_t`, `int64_t`, `uint64_t` - Integer types
 * - `float`, `double` - Floating point types
 * - `std::string_view` - String values (VARCHAR)
 * - `Date`, `DateTime`, `Interval` - Temporal types
 *
 * **Usage Example:**
 * @code{.cpp}
 * // Create properties
 * neug::Property age;
 * age.set_int32(30);
 *
 * neug::Property name;
 * name.set_string_view("Alice");
 *
 * // Read properties
 * if (age.type() == neug::DataTypeId::kInt32) {
 *     int32_t val = age.as_int32();
 * }
 *
 * // Use convenience constructors
 * neug::Property score(95.5);  // double
 * neug::Property id(12345L);   // int64
 *
 * // Convert to string for display
 * std::string str = prop.to_string();
 * @endcode
 *
 * **Primary Key Usage:**
 * @code{.cpp}
 * // Look up vertex by primary key
 * neug::Property pk;
 * pk.set_int64(12345);
 * vid_t vid;
 * if (graph.get_lid(person_label, pk, vid, timestamp)) {
 *     // Found vertex with ID vid
 * }
 * @endcode
 *
 * @note String views must remain valid for the lifetime of the Property.
 * @note Use appropriate type-checking before accessing values.
 *
 * @see DataTypeId For the enumeration of supported data types
 *
 * @since v0.1.0
 */
class Property {
 public:
  /** @brief Default constructor creating an empty property. */
  Property() : type_(DataTypeId::kEmpty) {}
  ~Property() = default;

  void set_bool(bool v) {
    type_ = DataTypeId::kBoolean;
    value_.b = v;
  }

  void set_int32(int32_t v) {
    type_ = DataTypeId::kInt32;
    value_.i = v;
  }

  void set_uint32(uint32_t v) {
    type_ = DataTypeId::kUInt32;
    value_.ui = v;
  }

  void set_int64(int64_t v) {
    type_ = DataTypeId::kInt64;
    value_.l = v;
  }

  void set_uint64(uint64_t v) {
    type_ = DataTypeId::kUInt64;
    value_.ul = v;
  }

  void set_string_view(const std::string_view& v) {
    type_ = DataTypeId::kVarchar;
    value_.s = v;
  }

  void set_float(float v) {
    type_ = DataTypeId::kFloat;
    value_.f = v;
  }

  void set_double(double v) {
    type_ = DataTypeId::kDouble;
    value_.db = v;
  }

  void set_date(const Date& v) {
    type_ = DataTypeId::kDate;
    value_.d = v;
  }

  void set_date(uint32_t val) {
    type_ = DataTypeId::kDate;
    value_.d.from_u32(val);
  }

  void set_datetime(const DateTime& v) {
    type_ = DataTypeId::kTimestampMs;
    value_.dt = v;
  }

  void set_datetime(int64_t mill_seconds) {
    type_ = DataTypeId::kTimestampMs;
    value_.dt.milli_second = mill_seconds;
  }

  void set_interval(const Interval& v) {
    type_ = DataTypeId::kInterval;
    value_.itv = v;
  }

  bool as_bool() const {
    assert(type() == DataTypeId::kBoolean);
    return value_.b;
  }

  int as_int32() const {
    assert(type() == DataTypeId::kInt32);
    return value_.i;
  }

  uint32_t as_uint32() const {
    assert(type() == DataTypeId::kUInt32);
    return value_.ui;
  }

  int64_t as_int64() const {
    assert(type() == DataTypeId::kInt64);
    return value_.l;
  }

  uint64_t as_uint64() const {
    assert(type() == DataTypeId::kUInt64);
    return value_.ul;
  }

  std::string_view as_string_view() const {
    assert(type() == DataTypeId::kVarchar);
    return value_.s;
  }

  float as_float() const {
    assert(type() == DataTypeId::kFloat);
    return value_.f;
  }

  double as_double() const {
    assert(type() == DataTypeId::kDouble);
    return value_.db;
  }

  Date as_date() const {
    assert(type() == DataTypeId::kDate);
    return value_.d;
  }

  DateTime as_datetime() const {
    assert(type() == DataTypeId::kTimestampMs);
    return value_.dt;
  }

  Interval as_interval() const {
    assert(type() == DataTypeId::kInterval);
    return value_.itv;
  }

  std::string to_string() const {
    auto type = this->type();
    if (type == DataTypeId::kInt32) {
      return std::to_string(as_int32());
    } else if (type == DataTypeId::kUInt32) {
      return std::to_string(as_uint32());
    } else if (type == DataTypeId::kInt64) {
      return std::to_string(as_int64());
    } else if (type == DataTypeId::kUInt64) {
      return std::to_string(as_uint64());
    } else if (type == DataTypeId::kVarchar) {
      return std::string(as_string_view());
    } else if (type == DataTypeId::kFloat) {
      return std::to_string(as_float());
    } else if (type == DataTypeId::kDouble) {
      return std::to_string(as_double());
    } else if (type == DataTypeId::kDate) {
      return as_date().to_string();
    } else if (type == DataTypeId::kTimestampMs) {
      return as_datetime().to_string();
    } else if (type == DataTypeId::kInterval) {
      return as_interval().to_string();
    } else if (type == DataTypeId::kBoolean) {
      return as_bool() ? "true" : "false";
    } else if (type == DataTypeId::kEmpty) {
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

  static Property from_date(const Date& v) {
    Property ret;
    ret.set_date(v);
    return ret;
  }

  static Property from_datetime(const DateTime& v) {
    Property ret;
    ret.set_datetime(v);
    return ret;
  }

  static Property from_interval(const Interval& v) {
    Property ret;
    ret.set_interval(v);
    return ret;
  }

  DataTypeId type() const { return type_; }

  Property& operator=(const Property& other);

  bool operator==(const Property& other) const;

  bool operator<(const Property& other) const;

  template <typename T>
  static Property From(const T& v) {
    return PropUtils<T>::to_prop(v);
  }

 private:
  DataTypeId type_;
  PropValue value_;
};

inline Property parse_property_from_string(DataTypeId pt,
                                           const std::string& str) {
  if (pt == DataTypeId::kEmpty) {
    return Property::empty();
  } else if (pt == DataTypeId::kBoolean) {
    return Property::from_bool(str == "true" || str == "1" || str == "TRUE");
  } else if (pt == DataTypeId::kInt32) {
    return Property::from_int32(std::stoi(str));
  } else if (pt == DataTypeId::kUInt32) {
    return Property::from_uint32(static_cast<uint32_t>(std::stoul(str)));
  } else if (pt == DataTypeId::kInt64) {
    return Property::from_int64(std::stoll(str));
  } else if (pt == DataTypeId::kUInt64) {
    return Property::from_uint64(static_cast<uint64_t>(std::stoull(str)));
  } else if (pt == DataTypeId::kVarchar) {
    return Property::from_string_view(str);
  } else if (pt == DataTypeId::kFloat) {
    return Property::from_float(std::stof(str));
  } else if (pt == DataTypeId::kDouble) {
    return Property::from_double(std::stod(str));
  } else if (pt == DataTypeId::kDate) {
    return Property::from_date(Date(str));
  } else if (pt == DataTypeId::kTimestampMs) {
    return Property::from_datetime(DateTime(str));
  } else if (pt == DataTypeId::kInterval) {
    return Property::from_interval(Interval(str));
  } else {
    LOG(FATAL) << "Unsupported property type: " << std::to_string(pt);
    return Property::empty();
  }
}
inline void serialize_property(InArchive& arc, const Property& prop) {
  auto type = prop.type();
  if (type == DataTypeId::kBoolean) {
    arc << prop.as_bool();
  } else if (type == DataTypeId::kInt32) {
    arc << prop.as_int32();
  } else if (type == DataTypeId::kUInt32) {
    arc << prop.as_uint32();
  } else if (type == DataTypeId::kInt64) {
    arc << prop.as_int64();
  } else if (type == DataTypeId::kUInt64) {
    arc << prop.as_uint64();
  } else if (type == DataTypeId::kVarchar) {
    arc << prop.as_string_view();
  } else if (type == DataTypeId::kFloat) {
    arc << prop.as_float();
  } else if (type == DataTypeId::kDouble) {
    arc << prop.as_double();
  } else if (type == DataTypeId::kDate) {
    arc << prop.as_date().to_u32();
  } else if (type == DataTypeId::kTimestampMs) {
    arc << prop.as_datetime().milli_second;
  } else if (type == DataTypeId::kInterval) {
    arc << prop.as_interval().to_mill_seconds();
  } else if (type == DataTypeId::kEmpty) {
  } else {
    LOG(FATAL) << "Unexpected property type" << std::to_string(type);
  }
}

inline void deserialize_property(OutArchive& arc, DataTypeId pt,
                                 Property& prop) {
  if (pt == DataTypeId::kBoolean) {
    bool v;
    arc >> v;
    prop.set_bool(v);
  } else if (pt == DataTypeId::kInt32) {
    int32_t v;
    arc >> v;
    prop.set_int32(v);
  } else if (pt == DataTypeId::kUInt32) {
    uint32_t v;
    arc >> v;
    prop.set_uint32(v);
  } else if (pt == DataTypeId::kInt64) {
    int64_t v;
    arc >> v;
    prop.set_int64(v);
  } else if (pt == DataTypeId::kUInt64) {
    uint64_t v;
    arc >> v;
    prop.set_uint64(v);
  } else if (pt == DataTypeId::kVarchar) {
    std::string_view v;
    arc >> v;
    prop.set_string_view(v);
  } else if (pt == DataTypeId::kFloat) {
    float v;
    arc >> v;
    prop.set_float(v);
  } else if (pt == DataTypeId::kDouble) {
    double v;
    arc >> v;
    prop.set_double(v);
  } else if (pt == DataTypeId::kDate) {
    uint32_t date_val;
    arc >> date_val;
    Date d;
    d.from_u32(date_val);
    prop.set_date(d);
  } else if (pt == DataTypeId::kTimestampMs) {
    int64_t dt_val;
    arc >> dt_val;
    prop.set_datetime(DateTime(dt_val));
  } else if (pt == DataTypeId::kInterval) {
    int64_t iv_val;
    arc >> iv_val;
    Interval iv;
    iv.from_mill_seconds(iv_val);
    prop.set_interval(iv);
  } else if (pt == DataTypeId::kEmpty) {
    prop = Property::empty();
  } else {
    LOG(FATAL) << "Unexpected property type" << std::to_string(pt);
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
  static DataTypeId prop_type() { return DataTypeId::kBoolean; }
  static bool to_typed(const Property& prop) { return prop.as_bool(); }
  static Property to_prop(bool v) { return Property::from_bool(v); }
};

template <>
struct PropUtils<int32_t> {
  static DataTypeId prop_type() { return DataTypeId::kInt32; }
  static int32_t to_typed(const Property& prop) { return prop.as_int32(); }
  static Property to_prop(int32_t v) { return Property::from_int32(v); }
};

template <>
struct PropUtils<uint32_t> {
  static DataTypeId prop_type() { return DataTypeId::kUInt32; }
  static uint32_t to_typed(const Property& prop) { return prop.as_uint32(); }
  static Property to_prop(uint32_t v) { return Property::from_uint32(v); }
};

template <>
struct PropUtils<int64_t> {
  static DataTypeId prop_type() { return DataTypeId::kInt64; }
  static int64_t to_typed(const Property& prop) { return prop.as_int64(); }
  static Property to_prop(int64_t v) { return Property::from_int64(v); }
};

template <>
struct PropUtils<uint64_t> {
  static DataTypeId prop_type() { return DataTypeId::kUInt64; }
  static uint64_t to_typed(const Property& prop) { return prop.as_uint64(); }
  static Property to_prop(uint64_t v) { return Property::from_uint64(v); }
};

template <>
struct PropUtils<std::string_view> {
  static DataTypeId prop_type() { return DataTypeId::kVarchar; }
  static std::string_view to_typed(const Property& prop) {
    return prop.as_string_view();
  }
  static Property to_prop(const std::string_view& v) {
    return Property::from_string_view(v);
  }
};

// Required by Schema's vlabel_indexer and elabel_indexer
template <>
struct PropUtils<std::string> {
  static DataTypeId prop_type() { return DataTypeId::kVarchar; }
  static std::string to_typed(const Property& prop) {
    return std::string(prop.as_string_view());
  }
  static Property to_prop(const std::string& v) {
    return Property::from_string_view(v);
  }
};

template <>
struct PropUtils<float> {
  static DataTypeId prop_type() { return DataTypeId::kFloat; }
  static float to_typed(const Property& prop) { return prop.as_float(); }
  static Property to_prop(float v) { return Property::from_float(v); }
};

template <>
struct PropUtils<double> {
  static DataTypeId prop_type() { return DataTypeId::kDouble; }
  static double to_typed(const Property& prop) { return prop.as_double(); }
  static Property to_prop(double v) { return Property::from_double(v); }
};

template <>
struct PropUtils<Date> {
  static DataTypeId prop_type() { return DataTypeId::kDate; }
  static Date to_typed(const Property& prop) { return prop.as_date(); }
  static Property to_prop(const Date& v) { return Property::from_date(v); }
  static Property to_prop(int32_t num_days) {
    return Property::from_date(Date(num_days));
  }
};

template <>
struct PropUtils<DateTime> {
  static DataTypeId prop_type() { return DataTypeId::kTimestampMs; }
  static DateTime to_typed(const Property& prop) { return prop.as_datetime(); }
  static Property to_prop(const DateTime& v) {
    return Property::from_datetime(v);
  }
  static Property to_prop(int64_t mill_seconds) {
    return Property::from_datetime(DateTime(mill_seconds));
  }
};

template <>
struct PropUtils<EmptyType> {
  static DataTypeId prop_type() { return DataTypeId::kEmpty; }
  static EmptyType to_typed(const Property& prop) { return EmptyType(); }
  static Property to_prop(const EmptyType& v) { return Property::empty(); }
};

template <>
struct PropUtils<Interval> {
  static DataTypeId prop_type() { return DataTypeId::kInterval; }
  static Interval to_typed(const Property& prop) { return prop.as_interval(); }
  static Property to_prop(const Interval& v) {
    return Property::from_interval(v);
  }
  static Property to_prop(const std::string_view& str) {
    return Property::from_interval(Interval(str));
  }
};

Property get_default_value(const DataTypeId& type);

InArchive& operator<<(InArchive& in_archive, const Property& value);
OutArchive& operator>>(OutArchive& out_archive, Property& value);

}  // namespace neug
