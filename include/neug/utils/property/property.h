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

namespace gs {

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

class Property {
 public:
  Property() : type_(DataTypeId::EMPTY) {}
  ~Property() = default;

  void set_bool(bool v) {
    type_ = DataTypeId::BOOLEAN;
    value_.b = v;
  }

  void set_int32(int32_t v) {
    type_ = DataTypeId::INTEGER;
    value_.i = v;
  }

  void set_uint32(uint32_t v) {
    type_ = DataTypeId::UINTEGER;
    value_.ui = v;
  }

  void set_int64(int64_t v) {
    type_ = DataTypeId::BIGINT;
    value_.l = v;
  }

  void set_uint64(uint64_t v) {
    type_ = DataTypeId::UBIGINT;
    value_.ul = v;
  }

  void set_string_view(const std::string_view& v) {
    type_ = DataTypeId::VARCHAR;
    value_.s = v;
  }

  void set_float(float v) {
    type_ = DataTypeId::FLOAT;
    value_.f = v;
  }

  void set_double(double v) {
    type_ = DataTypeId::DOUBLE;
    value_.db = v;
  }

  void set_date(const Date& v) {
    type_ = DataTypeId::DATE;
    value_.d = v;
  }

  void set_date(uint32_t val) {
    type_ = DataTypeId::DATE;
    value_.d.from_u32(val);
  }

  void set_datetime(const DateTime& v) {
    type_ = DataTypeId::TIMESTAMP_MS;
    value_.dt = v;
  }

  void set_datetime(int64_t mill_seconds) {
    type_ = DataTypeId::TIMESTAMP_MS;
    value_.dt.milli_second = mill_seconds;
  }

  void set_interval(const Interval& v) {
    type_ = DataTypeId::INTERVAL;
    value_.itv = v;
  }

  bool as_bool() const {
    assert(type() == DataTypeId::BOOLEAN);
    return value_.b;
  }

  int as_int32() const {
    assert(type() == DataTypeId::INTEGER);
    return value_.i;
  }

  uint32_t as_uint32() const {
    assert(type() == DataTypeId::UINTEGER);
    return value_.ui;
  }

  int64_t as_int64() const {
    assert(type() == DataTypeId::BIGINT);
    return value_.l;
  }

  uint64_t as_uint64() const {
    assert(type() == DataTypeId::UBIGINT);
    return value_.ul;
  }

  std::string_view as_string_view() const {
    assert(type() == DataTypeId::VARCHAR);
    return value_.s;
  }

  float as_float() const {
    assert(type() == DataTypeId::FLOAT);
    return value_.f;
  }

  double as_double() const {
    assert(type() == DataTypeId::DOUBLE);
    return value_.db;
  }

  Date as_date() const {
    assert(type() == DataTypeId::DATE);
    return value_.d;
  }

  DateTime as_datetime() const {
    assert(type() == DataTypeId::TIMESTAMP_MS);
    return value_.dt;
  }

  Interval as_interval() const {
    assert(type() == DataTypeId::INTERVAL);
    return value_.itv;
  }

  std::string to_string() const {
    auto type = this->type();
    if (type == DataTypeId::INTEGER) {
      return std::to_string(as_int32());
    } else if (type == DataTypeId::UINTEGER) {
      return std::to_string(as_uint32());
    } else if (type == DataTypeId::BIGINT) {
      return std::to_string(as_int64());
    } else if (type == DataTypeId::UBIGINT) {
      return std::to_string(as_uint64());
    } else if (type == DataTypeId::VARCHAR) {
      return std::string(as_string_view());
    } else if (type == DataTypeId::FLOAT) {
      return std::to_string(as_float());
    } else if (type == DataTypeId::DOUBLE) {
      return std::to_string(as_double());
    } else if (type == DataTypeId::DATE) {
      return as_date().to_string();
    } else if (type == DataTypeId::TIMESTAMP_MS) {
      return as_datetime().to_string();
    } else if (type == DataTypeId::INTERVAL) {
      return as_interval().to_string();
    } else if (type == DataTypeId::BOOLEAN) {
      return as_bool() ? "true" : "false";
    } else if (type == DataTypeId::EMPTY) {
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
  if (pt == DataTypeId::EMPTY) {
    return Property::empty();
  } else if (pt == DataTypeId::BOOLEAN) {
    return Property::from_bool(str == "true" || str == "1" || str == "TRUE");
  } else if (pt == DataTypeId::INTEGER) {
    return Property::from_int32(std::stoi(str));
  } else if (pt == DataTypeId::UINTEGER) {
    return Property::from_uint32(static_cast<uint32_t>(std::stoul(str)));
  } else if (pt == DataTypeId::BIGINT) {
    return Property::from_int64(std::stoll(str));
  } else if (pt == DataTypeId::UBIGINT) {
    return Property::from_uint64(static_cast<uint64_t>(std::stoull(str)));
  } else if (pt == DataTypeId::VARCHAR) {
    return Property::from_string_view(str);
  } else if (pt == DataTypeId::FLOAT) {
    return Property::from_float(std::stof(str));
  } else if (pt == DataTypeId::DOUBLE) {
    return Property::from_double(std::stod(str));
  } else if (pt == DataTypeId::DATE) {
    return Property::from_date(Date(str));
  } else if (pt == DataTypeId::TIMESTAMP_MS) {
    return Property::from_datetime(DateTime(str));
  } else if (pt == DataTypeId::INTERVAL) {
    return Property::from_interval(Interval(str));
  } else {
    LOG(FATAL) << "Unsupported property type: " << std::to_string(pt);
    return Property::empty();
  }
}
inline void serialize_property(InArchive& arc, const Property& prop) {
  auto type = prop.type();
  if (type == DataTypeId::BOOLEAN) {
    arc << prop.as_bool();
  } else if (type == DataTypeId::INTEGER) {
    arc << prop.as_int32();
  } else if (type == DataTypeId::UINTEGER) {
    arc << prop.as_uint32();
  } else if (type == DataTypeId::BIGINT) {
    arc << prop.as_int64();
  } else if (type == DataTypeId::UBIGINT) {
    arc << prop.as_uint64();
  } else if (type == DataTypeId::VARCHAR) {
    arc << prop.as_string_view();
  } else if (type == DataTypeId::FLOAT) {
    arc << prop.as_float();
  } else if (type == DataTypeId::DOUBLE) {
    arc << prop.as_double();
  } else if (type == DataTypeId::DATE) {
    arc << prop.as_date().to_u32();
  } else if (type == DataTypeId::TIMESTAMP_MS) {
    arc << prop.as_datetime().milli_second;
  } else if (type == DataTypeId::INTERVAL) {
    arc << prop.as_interval().to_mill_seconds();
  } else if (type == DataTypeId::EMPTY) {
  } else {
    LOG(FATAL) << "Unexpected property type" << std::to_string(type);
  }
}

inline void deserialize_property(OutArchive& arc, DataTypeId pt,
                                 Property& prop) {
  if (pt == DataTypeId::BOOLEAN) {
    bool v;
    arc >> v;
    prop.set_bool(v);
  } else if (pt == DataTypeId::INTEGER) {
    int32_t v;
    arc >> v;
    prop.set_int32(v);
  } else if (pt == DataTypeId::UINTEGER) {
    uint32_t v;
    arc >> v;
    prop.set_uint32(v);
  } else if (pt == DataTypeId::BIGINT) {
    int64_t v;
    arc >> v;
    prop.set_int64(v);
  } else if (pt == DataTypeId::UBIGINT) {
    uint64_t v;
    arc >> v;
    prop.set_uint64(v);
  } else if (pt == DataTypeId::VARCHAR) {
    std::string_view v;
    arc >> v;
    prop.set_string_view(v);
  } else if (pt == DataTypeId::FLOAT) {
    float v;
    arc >> v;
    prop.set_float(v);
  } else if (pt == DataTypeId::DOUBLE) {
    double v;
    arc >> v;
    prop.set_double(v);
  } else if (pt == DataTypeId::DATE) {
    uint32_t date_val;
    arc >> date_val;
    Date d;
    d.from_u32(date_val);
    prop.set_date(d);
  } else if (pt == DataTypeId::TIMESTAMP_MS) {
    int64_t dt_val;
    arc >> dt_val;
    prop.set_datetime(DateTime(dt_val));
  } else if (pt == DataTypeId::INTERVAL) {
    int64_t iv_val;
    arc >> iv_val;
    Interval iv;
    iv.from_mill_seconds(iv_val);
    prop.set_interval(iv);
  } else if (pt == DataTypeId::EMPTY) {
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
  static DataTypeId prop_type() { return DataTypeId::BOOLEAN; }
  static bool to_typed(const Property& prop) { return prop.as_bool(); }
  static Property to_prop(bool v) { return Property::from_bool(v); }
};

template <>
struct PropUtils<int32_t> {
  static DataTypeId prop_type() { return DataTypeId::INTEGER; }
  static int32_t to_typed(const Property& prop) { return prop.as_int32(); }
  static Property to_prop(int32_t v) { return Property::from_int32(v); }
};

template <>
struct PropUtils<uint32_t> {
  static DataTypeId prop_type() { return DataTypeId::UINTEGER; }
  static uint32_t to_typed(const Property& prop) { return prop.as_uint32(); }
  static Property to_prop(uint32_t v) { return Property::from_uint32(v); }
};

template <>
struct PropUtils<int64_t> {
  static DataTypeId prop_type() { return DataTypeId::BIGINT; }
  static int64_t to_typed(const Property& prop) { return prop.as_int64(); }
  static Property to_prop(int64_t v) { return Property::from_int64(v); }
};

template <>
struct PropUtils<uint64_t> {
  static DataTypeId prop_type() { return DataTypeId::UBIGINT; }
  static uint64_t to_typed(const Property& prop) { return prop.as_uint64(); }
  static Property to_prop(uint64_t v) { return Property::from_uint64(v); }
};

template <>
struct PropUtils<std::string_view> {
  static DataTypeId prop_type() { return DataTypeId::VARCHAR; }
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
  static DataTypeId prop_type() { return DataTypeId::VARCHAR; }
  static std::string to_typed(const Property& prop) {
    return std::string(prop.as_string_view());
  }
  static Property to_prop(const std::string& v) {
    return Property::from_string_view(v);
  }
};

template <>
struct PropUtils<float> {
  static DataTypeId prop_type() { return DataTypeId::FLOAT; }
  static float to_typed(const Property& prop) { return prop.as_float(); }
  static Property to_prop(float v) { return Property::from_float(v); }
};

template <>
struct PropUtils<double> {
  static DataTypeId prop_type() { return DataTypeId::DOUBLE; }
  static double to_typed(const Property& prop) { return prop.as_double(); }
  static Property to_prop(double v) { return Property::from_double(v); }
};

template <>
struct PropUtils<Date> {
  static DataTypeId prop_type() { return DataTypeId::DATE; }
  static Date to_typed(const Property& prop) { return prop.as_date(); }
  static Property to_prop(const Date& v) { return Property::from_date(v); }
  static Property to_prop(int32_t num_days) {
    return Property::from_date(Date(num_days));
  }
};

template <>
struct PropUtils<DateTime> {
  static DataTypeId prop_type() { return DataTypeId::TIMESTAMP_MS; }
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
  static DataTypeId prop_type() { return DataTypeId::EMPTY; }
  static EmptyType to_typed(const Property& prop) { return EmptyType(); }
  static Property to_prop(const EmptyType& v) { return Property::empty(); }
};

template <>
struct PropUtils<Interval> {
  static DataTypeId prop_type() { return DataTypeId::INTERVAL; }
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

}  // namespace gs
