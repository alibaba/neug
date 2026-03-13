/** Copyright 2020 Alibaba Group Holding Limited.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
#pragma once

#include <assert.h>
#include <glog/logging.h>
#include <yaml-cpp/node/convert.h>
#include <yaml-cpp/node/emit.h>
#include <yaml-cpp/node/impl.h>
#include <yaml-cpp/node/node.h>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <functional>
#include <initializer_list>
#include <new>
#include <ostream>
#include <string>
#include <string_view>
#include <typeinfo>
#include <utility>
#include <vector>

#include "neug/common/extra_type_info.h"
#include "neug/common/types.h"
#include "neug/config.h"
#include "neug/utils/exception/exception.h"

namespace YAML {
template <typename T>
struct convert;
}  // namespace YAML

namespace neug {

struct EmptyType {
  inline bool operator==(const EmptyType& other) const { return true; }
  inline bool operator!=(const EmptyType& other) const { return false; }
  inline bool operator<(const EmptyType& other) const { return false; }
};

using timestamp_t = uint32_t;
using vid_t = uint32_t;
using label_t = uint8_t;
static constexpr int32_t MAX_PLUGIN_NUM = 256;  // 2^(sizeof(uint8_t)*8)
static constexpr const timestamp_t MAX_TIMESTAMP = 0xFFFFFFFE;
static constexpr const timestamp_t INVALID_TIMESTAMP = 0xFFFFFFFF;

enum class InputFormat : uint8_t {
  kCppEncoder = 0,
  kCypherString = 1,
};

// primitive types
static constexpr const char* DT_UNSIGNED_INT8 = "DT_UNSIGNED_INT8";
static constexpr const char* DT_UNSIGNED_INT16 = "DT_UNSIGNED_INT16";
static constexpr const char* DT_SIGNED_INT32 = "DT_SIGNED_INT32";
static constexpr const char* DT_UNSIGNED_INT32 = "DT_UNSIGNED_INT32";
static constexpr const char* DT_SIGNED_INT64 = "DT_SIGNED_INT64";
static constexpr const char* DT_UNSIGNED_INT64 = "DT_UNSIGNED_INT64";
static constexpr const char* DT_BOOL = "DT_BOOL";
static constexpr const char* DT_FLOAT = "DT_FLOAT";
static constexpr const char* DT_DOUBLE = "DT_DOUBLE";
static constexpr const char* DT_STRING = "DT_STRING";
static constexpr const char* DT_STRINGMAP = "DT_STRINGMAP";
// temporal types
static constexpr const char* DT_DATE = "DT_DATE";  // YYYY-MM-DD
// static constexpr const char* DT_DAY = "DT_DAY32";
// static constexpr const char* DT_DATE32 = "DT_DATE32";
static constexpr const char* DT_DATETIME =
    "DT_DATETIME";  // YYYY-MM-DD HH:MM:SS.zzz
static constexpr const char* DT_INTERVAL =
    "DT_INTERVAL";  // Y Year, M Month, D Day, H Hour, M Minute, S Second
static constexpr const char* DT_TIMESTAMP =
    "DT_TIMESTAMP";  // millisecond timestamp
static constexpr const uint16_t STRING_DEFAULT_MAX_LENGTH = 256;

enum class EdgeStrategy {
  kNone,
  kSingle,
  kMultiple,
};

namespace config_parsing {
std::string PrimitivePropertyTypeToString(DataTypeId type);
DataTypeId StringToPrimitivePropertyType(const std::string& str);

YAML::Node TemporalTypeToYAML(DataTypeId type);

}  // namespace config_parsing

// encoded with label_id and vid_t.
struct GlobalId {
  using label_id_t = uint8_t;
  using vid_t = uint32_t;
  using gid_t = uint64_t;
  static constexpr int32_t label_id_offset = 64 - sizeof(label_id_t) * 8;
  static constexpr uint64_t vid_mask = (1ULL << label_id_offset) - 1;

  static label_id_t get_label_id(gid_t gid);
  static vid_t get_vid(gid_t gid);

  uint64_t global_id;

  GlobalId();
  GlobalId(label_id_t label_id, vid_t vid);
  explicit GlobalId(gid_t gid);

  label_id_t label_id() const;
  vid_t vid() const;

  std::string to_string() const;
};

inline bool operator==(const GlobalId& lhs, const GlobalId& rhs) {
  return lhs.global_id == rhs.global_id;
}

inline bool operator!=(const GlobalId& lhs, const GlobalId& rhs) {
  return lhs.global_id != rhs.global_id;
}
inline bool operator<(const GlobalId& lhs, const GlobalId& rhs) {
  return lhs.global_id < rhs.global_id;
}

inline bool operator>(const GlobalId& lhs, const GlobalId& rhs) {
  return lhs.global_id > rhs.global_id;
}

struct DayValue {
  uint32_t hour : 5;
  uint32_t day : 5;
  uint32_t month : 4;
  uint32_t year : 18;
};

struct Interval {
  static constexpr const int32_t MONTHS_PER_YEAR = 12;
  static constexpr const int64_t DAYS_PER_MONTH = 30;
  static constexpr const int64_t DAYS_PER_YEAR = 365;
  static constexpr const int64_t MSECS_PER_SEC = 1000;
  static constexpr const int32_t SECS_PER_MINUTE = 60;
  static constexpr const int32_t MINS_PER_HOUR = 60;
  static constexpr const int32_t HOURS_PER_DAY = 24;
  static constexpr const int32_t SECS_PER_HOUR =
      SECS_PER_MINUTE * MINS_PER_HOUR;
  static constexpr const int32_t SECS_PER_DAY = SECS_PER_HOUR * HOURS_PER_DAY;

  static constexpr const int64_t MSEC_PER_DAY = SECS_PER_DAY * 1000;

  static constexpr const int64_t MICROS_PER_MSEC = 1000;
  static constexpr const int64_t MICROS_PER_SEC =
      MICROS_PER_MSEC * MSECS_PER_SEC;
  static constexpr const int64_t MICROS_PER_MINUTE =
      MICROS_PER_SEC * SECS_PER_MINUTE;
  static constexpr const int64_t MICROS_PER_HOUR =
      MICROS_PER_MINUTE * MINS_PER_HOUR;
  static constexpr const int64_t MICROS_PER_DAY =
      MICROS_PER_HOUR * HOURS_PER_DAY;
  static constexpr const int64_t MICROS_PER_MONTH =
      MICROS_PER_DAY * DAYS_PER_MONTH;

  int32_t months;
  int32_t days;
  int64_t micros;

  Interval() = default;
  ~Interval() = default;
  explicit Interval(std::string str);
  explicit Interval(std::string_view str_view)
      : Interval(std::string(str_view)) {}

  void normalize(int64_t& months, int64_t& days, int64_t& micros) const;

  // Normalize to interval bounds.
  inline static void borrow(const int64_t msf, int64_t& lsf, int32_t& f,
                            const int64_t scale);
  inline Interval normalize() const;

  inline Interval& operator=(const Interval& rhs) = default;

  void from_mill_seconds(int64_t mill_seconds);

  void invert();

  int64_t to_mill_seconds() const;

  std::string to_string() const;

  inline int32_t year() const { return months / 12; }
  inline int32_t month() const { return months % 12; }
  inline int32_t day() const { return days; }
  inline int32_t hour() const { return micros / MICROS_PER_HOUR; };
  inline int32_t minute() const { return micros / MICROS_PER_MINUTE % 60; };
  inline int32_t second() const { return micros / MICROS_PER_SEC % 60; };
  inline int32_t millisecond() const {
    return micros / MICROS_PER_MSEC % 1000;
  };

  inline bool operator==(const Interval& rhs) const {
    const auto& lhs = *this;
    if (lhs.months == rhs.months && lhs.days == rhs.days &&
        lhs.micros == rhs.micros) {
      return true;
    }

    int64_t lmonths, ldays, lmicros;
    int64_t rmonths, rdays, rmicros;
    lhs.normalize(lmonths, ldays, lmicros);
    rhs.normalize(rmonths, rdays, rmicros);

    return lmonths == rmonths && ldays == rdays && lmicros == rmicros;
  }

  inline bool operator>(const Interval& rhs) const {
    const auto& lhs = *this;
    int64_t lmonths, ldays, lmicros;
    int64_t rmonths, rdays, rmicros;
    lhs.normalize(lmonths, ldays, lmicros);
    rhs.normalize(rmonths, rdays, rmicros);

    if (lmonths > rmonths) {
      return true;
    } else if (lmonths < rmonths) {
      return false;
    }
    if (ldays > rdays) {
      return true;
    } else if (ldays < rdays) {
      return false;
    }
    return lmicros > rmicros;
  }

  inline bool operator<(const Interval& rhs) const { return rhs > *this; }

  inline bool operator<=(const Interval& rhs) const { return !(*this > rhs); }

  inline bool operator>=(const Interval& rhs) const { return !(*this < rhs); }
};

struct Date {
  inline static const int32_t NORMAL_DAYS[13] = {0,  31, 28, 31, 30, 31, 30,
                                                 31, 31, 30, 31, 30, 31};
  inline static const int32_t CUMULATIVE_DAYS[13] = {
      0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334, 365};
  inline static const int32_t LEAP_DAYS[13] = {0,  31, 29, 31, 30, 31, 30,
                                               31, 31, 30, 31, 30, 31};
  inline static const int32_t CUMULATIVE_LEAP_DAYS[13] = {
      0, 31, 60, 91, 121, 152, 182, 213, 244, 274, 305, 335, 366};

  Date() = default;
  ~Date() = default;

  explicit Date(int64_t ts);

  explicit Date(int32_t num_days) { from_num_days(num_days); }

  Date(const std::string& date_str);

  static bool is_leap_year(int32_t year);

  static int32_t month_day(int32_t year, int32_t month);

  std::string to_string() const;

  uint32_t to_u32() const;

  int32_t to_num_days() const;

  void from_num_days(int32_t num_days);

  void from_u32(uint32_t val);

  int64_t to_timestamp() const;

  inline void from_timestamp(int64_t ts) {
#ifdef ENABLE_CHRONO_YMD
    auto tp = std::chrono::system_clock::from_time_t(ts / 1000);

    auto days_since_epoch = floor<std::chrono::days>(tp);
    std::chrono::year_month_day ymd(days_since_epoch);

    auto tod = std::chrono::hh_mm_ss(tp - days_since_epoch);

    value.internal.year = int(ymd.year());
    value.internal.month = unsigned(ymd.month());
    value.internal.day = unsigned(ymd.day());
    value.internal.hour = tod.hours().count();
#else
    ts /= 1000;
    int64_t days_since_epoch = ts / 86400;
    int64_t sec_of_day = ts % 86400;
    if (sec_of_day < 0) {
      sec_of_day += 86400;
      days_since_epoch--;
    }

    value.internal.hour = static_cast<int>(sec_of_day / 3600);

    // copy from
    // https://github.com/gcc-mirror/gcc/blob/80e82de4b802aa2e4c7cfad6e3288d99a7cb16ac/libstdc%2B%2B-v3/include/std/chrono#L1733
    constexpr auto __z2 = static_cast<uint32_t>(-1468000);
    constexpr auto __r2_e3 = static_cast<uint32_t>(536895458);

    const auto __r0 = days_since_epoch + __r2_e3;

    const auto __n1 = 4 * __r0 + 3;
    const auto __q1 = __n1 / 146097;
    const auto __r1 = __n1 % 146097 / 4;

    constexpr auto __p32 = static_cast<uint64_t>(1) << 32;
    const auto __n2 = 4 * __r1 + 3;
    const auto __u2 = static_cast<uint64_t>(2939745) * __n2;
    const auto __q2 = static_cast<uint32_t>(__u2 / __p32);
    const auto __r2 = static_cast<uint32_t>(__u2 % __p32) / 2939745 / 4;

    constexpr auto __p16 = static_cast<uint32_t>(1) << 16;
    const auto __n3 = 2141 * __r2 + 197913;
    const auto __q3 = __n3 / __p16;
    const auto __r3 = __n3 % __p16 / 2141;

    const auto __y0 = 100 * __q1 + __q2;
    const auto __m0 = __q3;
    const auto __d0 = __r3;

    const auto __j = __r2 >= 306;
    const auto __y1 = __y0 + __j;
    const auto __m1 = __j ? __m0 - 12 : __m0;
    const auto __d1 = __d0 + 1;

    value.internal.year = static_cast<int>(__y1 + __z2);
    value.internal.month = static_cast<int>(__m1);
    value.internal.day = static_cast<int>(__d1);
#endif
  }

  bool operator<(const Date& rhs) const;
  bool operator>(const Date& rhs) const;
  bool operator==(const Date& rhs) const;

  Date operator+(const Interval& interval) const;

  Date& operator+=(const Interval& interval);

  Date operator-(const Interval& interval) const;

  Date& operator-=(const Interval& interval);

  Interval operator-(const Date& rhs) const;

  int year() const;
  int month() const;
  int day() const;
  int hour() const;

  union {
    DayValue internal;
    uint32_t integer;
  } value;
};

struct DateTime {
  DateTime() = default;
  ~DateTime() = default;
  explicit DateTime(int64_t x) : milli_second(x) {}
  explicit DateTime(const std::string& date_time_str);

  std::string to_string() const;

  inline bool operator>(const DateTime& rhs) const {
    return milli_second > rhs.milli_second;
  }

  inline bool operator<(const DateTime& rhs) const {
    return milli_second < rhs.milli_second;
  }

  inline bool operator==(const DateTime& rhs) const {
    return milli_second == rhs.milli_second;
  }

  DateTime& operator+=(const Interval& interval);

  DateTime operator+(const Interval& interval) const;

  DateTime& operator-=(const Interval& interval);

  DateTime operator-(const Interval& interval) const;

  inline Interval operator-(const DateTime& rhs) const {
    Interval interval;
    interval.from_mill_seconds(this->milli_second - rhs.milli_second);
    return interval;
  }

  int64_t milli_second;
};

class Table;

class InArchive;
class OutArchive;

InArchive& operator<<(InArchive& arc,
                      const std::shared_ptr<const ExtraTypeInfo>& type_info);
OutArchive& operator>>(OutArchive& arc,
                       std::shared_ptr<ExtraTypeInfo>& type_info);

InArchive& operator<<(InArchive& in_archive, const DataTypeId& value);
OutArchive& operator>>(OutArchive& out_archive, DataTypeId& value);

InArchive& operator<<(InArchive& in_archive, const GlobalId& value);
OutArchive& operator>>(OutArchive& out_archive, GlobalId& value);

InArchive& operator<<(InArchive& in_archive, const Interval& value);
OutArchive& operator>>(OutArchive& out_archive, Interval& value);

// Init value of types
static const bool DEFAULT_BOOL_VALUE = false;
static const uint8_t DEFAULT_UNSIGNED_INT8_VALUE = 0;
static const uint16_t DEFAULT_UNSIGNED_INT16_VALUE = 0;
static const int32_t DEFAULT_INT32_VALUE = 0;
static const uint32_t DEFAULT_UNSIGNED_INT32_VALUE = 0;
static const int64_t DEFAULT_INT64_VALUE = 0;
static const uint64_t DEFAULT_UNSIGNED_INT64_VALUE = 0;
static const double DEFAULT_DOUBLE_VALUE = 0;
static const float DEFAULT_FLOAT_VALUE = 0;
static const Date DEFAULT_DATE_VALUE = Date(0);
static const DateTime DEFAULT_DATE_TIME_VALUE = DateTime(0);

}  // namespace neug

namespace std {

std::string to_string(neug::DataTypeId type);
std::string to_string(neug::MemoryLevel level);

inline ostream& operator<<(ostream& os, const neug::EdgeStrategy& strategy) {
  switch (strategy) {
  case neug::EdgeStrategy::kNone:
    os << "None";
    break;
  case neug::EdgeStrategy::kSingle:
    os << "Single";
    break;
  case neug::EdgeStrategy::kMultiple:
    os << "Multiple";
    break;
  default:
    os << "Unknown";
    break;
  }
  return os;
}
inline ostream& operator<<(ostream& os, const neug::Date& dt) {
  os << dt.to_string();
  return os;
}

inline ostream& operator<<(ostream& os, const neug::DateTime& dt) {
  os << dt.to_string();
  return os;
}

inline ostream& operator<<(ostream& os, const neug::Interval& interval) {
  os << interval.to_string();
  return os;
}

inline ostream& operator<<(ostream& os, neug::DataTypeId pt) {
  os << std::to_string(pt);
  return os;
}

inline ostream& operator<<(ostream& os, const neug::DBMode& mode) {
  switch (mode) {
  case neug::DBMode::READ_ONLY:
    os << "READ_ONLY";
    break;
  case neug::DBMode::READ_WRITE:
    os << "READ_WRITE";
    break;
  default:
    os << "UNKNOWN";
    break;
  }
  return os;
}

template <>
struct hash<neug::GlobalId> {
  size_t operator()(const neug::GlobalId& value) const {
    return std::hash<uint64_t>()(value.global_id);
  }
};

}  // namespace std

namespace YAML {

template <>
struct convert<std::shared_ptr<neug::ExtraTypeInfo>> {
  static Node encode(const std::shared_ptr<neug::ExtraTypeInfo>& type) {
    YAML::Node node;
    if (type->type == neug::ExtraTypeInfoType::STRING_TYPE_INFO) {
      auto string_type_info =
          std::dynamic_pointer_cast<neug::StringTypeInfo>(type);
      if (!string_type_info) {
        THROW_INTERNAL_EXCEPTION("Failed to cast to StringTypeInfo");
      }
      node["string"]["varchar"]["max_length"] = string_type_info->max_length;
    }
    return node;
  }

  static bool decode(const Node& node,
                     std::shared_ptr<neug::ExtraTypeInfo>& rhs) {
    if (node["string"]) {
      if (node["string"]["varchar"] &&
          node["string"]["varchar"]["max_length"]) {
        size_t max_length =
            node["string"]["varchar"]["max_length"].as<size_t>();
        rhs = std::make_shared<neug::StringTypeInfo>(max_length);
      } else if (node["string"]["var_char"] &&
                 node["string"]["var_char"]["max_length"]) {
        LOG(WARNING) << "var_char is deprecated, use varchar instead.";
        size_t max_length =
            node["string"]["var_char"]["max_length"].as<size_t>();
        rhs = std::make_shared<neug::StringTypeInfo>(max_length);
      }
    }
    return true;
  }
};

template <>
struct convert<neug::DataType> {
  // concurrently preserve backwards compatibility with old config files
  static bool decode(const Node& config, neug::DataType& property_type) {
    if (config["primitive_type"]) {
      property_type = neug::config_parsing::StringToPrimitivePropertyType(
          config["primitive_type"].as<std::string>());
    } else if (config["string"]) {
      if (config["string"].IsMap()) {
        if (config["string"]["var_char"]) {
          LOG(WARNING) << "var_char is deprecated, use long_text instead.";
          property_type = neug::DataType(
              neug::DataTypeId::kVarchar,
              std::make_shared<neug::StringTypeInfo>(
                  config["string"]["var_char"]["max_length"].as<size_t>()));
        } else if (config["string"]["long_text"]) {
          property_type = neug::DataType(neug::DataTypeId::kVarchar,
                                         std::make_shared<neug::StringTypeInfo>(
                                             neug::STRING_DEFAULT_MAX_LENGTH));
        } else {
          LOG(ERROR) << "Unrecognized string type";
        }
      } else {
        LOG(ERROR) << "string should be a map";
      }
    } else if (config["temporal"]) {
      auto temporal = config["temporal"];
      if (temporal["date"]) {
        property_type = neug::DataTypeId::kDate;
      } else if (temporal["datetime"]) {
        property_type = neug::DataTypeId::kTimestampMs;
      } else if (temporal["interval"]) {
        property_type = neug::DataTypeId::kInterval;
      } else if (temporal["timestamp"]) {
        property_type = neug::DataTypeId::kTimestampMs;
      } else {
        THROW_NOT_SUPPORTED_EXCEPTION("Unrecognized temporal type: " +
                                      temporal.as<std::string>());
      }
    } else if (config["date"]) {
      property_type = neug::DataTypeId::kDate;
    } else {
      LOG(ERROR) << "Unrecognized property type: " << config;
      return false;
    }
    return true;
  }

  static Node encode(const neug::DataType& type) {
    YAML::Node node;
    if (type == neug::DataTypeId::kBoolean ||
        type == neug::DataTypeId::kInt32 || type == neug::DataTypeId::kUInt32 ||
        type == neug::DataTypeId::kFloat || type == neug::DataTypeId::kInt64 ||
        type == neug::DataTypeId::kUInt64 ||
        type == neug::DataTypeId::kDouble) {
      node["primitive_type"] =
          neug::config_parsing::PrimitivePropertyTypeToString(type.id());
    } else if (type == neug::DataTypeId::kVarchar) {
      const auto* extra_type_info = type.RawExtraTypeInfo();
      const auto* string_type_info =
          dynamic_cast<const neug::StringTypeInfo*>(extra_type_info);
      node["string"]["varchar"]["max_length"] =
          string_type_info ? string_type_info->max_length
                           : neug::STRING_DEFAULT_MAX_LENGTH;
    } else if (type == neug::DataTypeId::kDate) {
      node["temporal"]["datetime"] = "";
    } else {
      LOG(ERROR) << "Unrecognized property type: " << type.ToString();
    }
    return node;
  }
};
}  // namespace YAML

namespace hash_tuple {

template <typename T, typename Enable = void>
struct hash_combine;

// Helper function to combine hashes
template <typename T>
struct is_tuple : std::false_type {};

template <typename... Args>
struct is_tuple<std::tuple<Args...>> : std::true_type {};

template <typename T>
struct hash_combine<T, std::enable_if_t<!is_tuple<T>::value>> {
  hash_combine(const T& val) : value(val) {}
  T value;
  void operator()(std::size_t& seed) const {
    seed ^= std::hash<T>{}(value) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
  }
};

template <typename T>
struct hash_combine<T, std::enable_if_t<is_tuple<T>::value>> {
  hash_combine(const T& val) : value(val) {}
  T value;
  void operator()(std::size_t& seed) const {
    std::apply(
        [&seed](const auto&... args) {
          ((seed ^= std::hash<std::decay_t<decltype(args)>>{}(args) +
                    0x9e3779b9 + (seed << 6) + (seed >> 2)),
           ...);
        },
        value);
  }
};

// Hash struct for tuples
template <typename Tuple, std::size_t Index = std::tuple_size<Tuple>::value - 1>
struct TupleHash {
  static void apply(std::size_t& seed, const Tuple& tuple) {
    TupleHash<Tuple, Index - 1>::apply(seed, tuple);
    hash_combine<typename std::tuple_element_t<Index, Tuple>>(
        std::get<Index>(tuple))(seed);
  }
};

template <typename Tuple>
struct TupleHash<Tuple, 0> {
  static void apply(std::size_t& seed, const Tuple& tuple) {
    hash_combine<typename std::tuple_element_t<0, Tuple>> combiner(
        std::get<0>(tuple));
    combiner(seed);
  }
};

template <typename... Args>
struct hash {
  std::size_t operator()(const std::tuple<Args...>& tuple) const {
    std::size_t seed = 0;
    TupleHash<std::tuple<Args...>>::apply(seed, tuple);
    return seed;
  }
};

}  // namespace hash_tuple
