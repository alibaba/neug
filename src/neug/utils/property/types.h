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

#ifndef UTILS_PROPERTY_TYPES_H_
#define UTILS_PROPERTY_TYPES_H_

#include <assert.h>
#include <glog/logging.h>
#include <yaml-cpp/node/emit.h>
#include <yaml-cpp/node/impl.h>
#include <yaml-cpp/node/node.h>
#include <yaml-cpp/yaml.h>
#include <boost/cstdint.hpp>
#include <boost/date_time/date.hpp>
#include <boost/date_time/gregorian/greg_date.hpp>
#include <boost/date_time/gregorian_calendar.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/date_time/posix_time/posix_time_config.hpp>
#include <boost/date_time/posix_time/posix_time_duration.hpp>
#include <boost/date_time/posix_time/posix_time_io.hpp>
#include <boost/date_time/posix_time/ptime.hpp>
#include <boost/date_time/time.hpp>
#include <boost/date_time/time_system_counted.hpp>
#include <boost/exception/exception.hpp>
#include <boost/iterator/iterator_facade.hpp>
#include <boost/lexical_cast/bad_lexical_cast.hpp>
#include <chrono>
#include <compare>
#include <cstdint>
#include <cstring>
#include <exception>
#include <functional>
#include <initializer_list>
#include <istream>
#include <new>
#include <ostream>
#include <regex>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "libgrape-lite/grape/serialization/in_archive.h"
#include "libgrape-lite/grape/serialization/out_archive.h"
#include "libgrape-lite/grape/types.h"

namespace YAML {
template <typename T>
struct convert;
}  // namespace YAML

namespace grape {
class InArchive;
class OutArchive;

inline bool operator<(const EmptyType& lhs, const EmptyType& rhs) {
  return false;
}

}  // namespace grape

namespace gs {

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

enum class StorageStrategy {
  kNone,
  kMem,
  kDisk,
};

namespace impl {

enum class PropertyTypeImpl {
  // Numeric types
  kEmpty,
  kBool,
  kUInt8,
  kUInt16,
  kInt32,
  kUInt32,
  kInt64,
  kUInt64,
  // floating point types
  kFloat,
  kDouble,
  // string types
  kStringView,
  kStringMap,
  kVarChar,
  kString,  // holding a string
  // graph types
  kVertexGlobalId,
  kLabel,

  // Stores multiple properties
  kRecordView,
  kRecord,

  // temporal types
  kDate,
  kDateTime,
  // kDay,
  kInterval,
  kTimestamp,  // millisecond timestamp
};

// Stores additional type information for PropertyTypeImpl
union AdditionalTypeInfo {
  uint16_t max_length;  // for varchar
};
}  // namespace impl

struct PropertyType {
 private:
  static constexpr const uint16_t STRING_DEFAULT_MAX_LENGTH = 256;

 public:
  static uint16_t GetStringDefaultMaxLength();
  impl::PropertyTypeImpl type_enum;
  impl::AdditionalTypeInfo additional_type_info;

  constexpr PropertyType()
      : type_enum(impl::PropertyTypeImpl::kEmpty), additional_type_info() {}
  constexpr PropertyType(impl::PropertyTypeImpl type)
      : type_enum(type), additional_type_info() {}
  constexpr PropertyType(impl::PropertyTypeImpl type, uint16_t max_length)
      : type_enum(type) {
    assert(type == impl::PropertyTypeImpl::kVarChar);
    additional_type_info.max_length = max_length;
  }

  bool IsVarchar() const;
  std::string ToString() const;

  static PropertyType Empty();
  static PropertyType Bool();
  static PropertyType UInt8();
  static PropertyType UInt16();
  static PropertyType Int32();
  static PropertyType UInt32();
  static PropertyType Int64();
  static PropertyType UInt64();
  static PropertyType Float();
  static PropertyType Double();

  static PropertyType StringView();
  static PropertyType StringMap();
  static PropertyType String();
  static PropertyType Varchar(uint16_t max_length);
  static PropertyType VertexGlobalId();
  static PropertyType Label();
  static PropertyType RecordView();
  static PropertyType Record();

  static PropertyType Date();
  static PropertyType DateTime();
  static PropertyType Interval();
  static PropertyType Timestamp();

  static const PropertyType kEmpty;
  static const PropertyType kBool;
  static const PropertyType kUInt8;
  static const PropertyType kUInt16;
  static const PropertyType kInt32;
  static const PropertyType kUInt32;
  static const PropertyType kInt64;
  static const PropertyType kUInt64;
  static const PropertyType kFloat;
  static const PropertyType kDouble;

  static const PropertyType kStringView;
  static const PropertyType kStringMap;
  static const PropertyType kString;
  static const PropertyType kVertexGlobalId;
  static const PropertyType kLabel;
  static const PropertyType kRecordView;
  static const PropertyType kRecord;

  static const PropertyType kDate;
  static const PropertyType kDateTime;
  static const PropertyType kInterval;
  static const PropertyType kTimestamp;  // millisecond timestamp

  bool operator==(const PropertyType& other) const;
  bool operator!=(const PropertyType& other) const;
};

namespace config_parsing {
std::string PrimitivePropertyTypeToString(PropertyType type);
PropertyType StringToPrimitivePropertyType(const std::string& str);

YAML::Node TemporalTypeToYAML(PropertyType type);

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
  GlobalId(gid_t gid);

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
  uint32_t year : 18;
  uint32_t month : 4;
  uint32_t day : 5;
  uint32_t hour : 5;
};

struct __attribute__((packed)) IntervalValue {
  uint64_t year : 18;         // 0~8192
  uint64_t month : 4;         // 0~12
  uint64_t day : 5;           // 0~31
  uint64_t hour : 5;          // 0~23
  uint64_t minute : 6;        // 0~59
  uint64_t second : 6;        // 0~59
  uint64_t millisecond : 10;  // 0~999
  uint64_t microsecond : 10;  // 0~999, microseconds current stored but not
                              // used. 14+4+5+5+6+6+10+10 = 60

  bool operator==(const IntervalValue& rhs) const;

  bool operator<(const IntervalValue& rhs) const;

  inline bool operator>(const IntervalValue& rhs) const {
    return !(*this < rhs) && !(*this == rhs);
  }

  IntervalValue& operator=(const IntervalValue& rhs) = default;
};

struct __attribute__((packed)) Interval {
  static constexpr int64_t SECOND_PER_YEAR = 365 * 24 * 3600;
  static constexpr int64_t SECOND_PER_MONTH = 30 * 24 * 3600;
  static constexpr int64_t SECOND_PER_DAY = 24 * 3600;
  static constexpr int64_t SECOND_PER_HOUR = 3600;
  static constexpr int64_t SECOND_PER_MINUTE = 60;

  Interval() = default;
  ~Interval() = default;
  explicit Interval(std::string str);
  explicit Interval(std::string_view str_view)
      : Interval(std::string(str_view)) {}

  Interval& operator=(const Interval& rhs) = default;

  void from_mill_seconds(int64_t mill_seconds);

  int64_t to_mill_seconds() const;

  std::string to_string() const;

  inline int32_t year() const { return static_cast<int32_t>(internal.year); }
  int32_t month() const { return static_cast<int32_t>(internal.month); }
  int32_t day() const { return static_cast<int32_t>(internal.day); }
  int32_t hour() const { return static_cast<int32_t>(internal.hour); }
  int32_t minute() const { return static_cast<int32_t>(internal.minute); }
  int32_t second() const { return static_cast<int32_t>(internal.second); }
  int32_t millisecond() const {
    return static_cast<int32_t>(internal.millisecond);
  }

  inline Interval& operator=(uint64_t ts) {
    LOG(INFO) << "Set interval from mill seconds: " << ts;
    from_mill_seconds(ts);
    return *this;
  }

  inline bool operator==(const Interval& rhs) const {
    return internal == rhs.internal;
  }

  inline bool operator<(const Interval& rhs) const {
    return internal < internal;
  }

  inline bool operator>(const Interval& rhs) const {
    return internal > rhs.internal;
  }

  inline bool operator<=(const Interval& rhs) const {
    return !(internal > rhs.internal);
  }

  inline bool operator>=(const Interval& rhs) const {
    return !(internal < rhs.internal);
  }

  Interval& operator+(const Interval& rhs);

  Interval& operator+=(const Interval& rhs);

  Interval& operator-(const Interval& rhs);

  Interval& operator-=(const Interval& rhs);

  bool negative;
  IntervalValue internal;

 private:
  static void normalize(Interval& interval);

  static void adjustNegative(Interval& interval);

  // TODO(zhanglei): This is a simplified logic for month/year overflow and
  // underflow.
  //  It does not consider the actual number of days in each month.
  //  A more robust implementation would require a mapping of month to days and
  //  handling leap years for February.
  static void adjustMonthYearOverflow(Interval& interval);

  // TODO(zhanglei): This is a simplified logic for month/year underflow.
  //   It does not consider the actual number of days in each month.
  //   A more robust implementation would require a mapping of month to days and
  //   handling leap years for February.
  static void adjustMonthYearUnderflow(Interval& interval);
};

struct __attribute__((packed)) Date {
  Date() = default;
  ~Date() = default;

  Date(int64_t ts);

  Date(int32_t num_days) { from_num_days(num_days); }

  Date(const std::string& date_str);

  std::string to_string() const;

  uint32_t to_u32() const;

  uint32_t to_num_days() const;

  void from_num_days(int32_t num_days);

  void from_u32(uint32_t val);

  int64_t to_timestamp() const;

  void from_timestamp(int64_t ts);

  bool operator<(const Date& rhs) const;
  bool operator==(const Date& rhs) const;

  Date& operator+(const Interval& interval);

  Date& operator+=(const Interval& interval);

  Date& operator-(const Interval& interval);

  Date& operator-=(const Interval& interval);

  Interval operator-(const Date& rhs) const;

  Interval operator-=(const Date& rhs) const;

  int year() const;
  int month() const;
  int day() const;
  int hour() const;

  union {
    DayValue internal;
    uint32_t integer;
  } value;
};

struct __attribute__((packed)) DateTime {
  DateTime() = default;
  ~DateTime() = default;
  DateTime(int64_t x) : milli_second(x) {}
  DateTime(const std::string& date_time_str);

  std::string to_string() const;

  inline bool operator<(const DateTime& rhs) const {
    return milli_second < rhs.milli_second;
  }

  inline bool operator==(const DateTime& rhs) const {
    return milli_second == rhs.milli_second;
  }

  inline DateTime& operator+=(const Interval& interval) {
    milli_second += interval.to_mill_seconds();
    return *this;
  }

  inline DateTime& operator+(const Interval& interval) {
    milli_second += interval.to_mill_seconds();
    return *this;
  }

  inline DateTime& operator-=(const Interval& interval) {
    milli_second -= interval.to_mill_seconds();
    return *this;
  }

  inline DateTime& operator-(const Interval& interval) {
    milli_second -= interval.to_mill_seconds();
    return *this;
  }

  inline Interval operator-(const DateTime& rhs) const {
    Interval interval;
    interval.from_mill_seconds(this->milli_second - rhs.milli_second);
    return interval;
  }

  inline Interval operator-=(const DateTime& rhs) {
    return this->operator-(rhs);
  }

  int64_t milli_second;
};

// Although DateTime and TimeStamp are similar, they are different types, parsed
// from different arrow types, and has different usages.
struct __attribute__((packed)) TimeStamp {
  TimeStamp() = default;
  ~TimeStamp() = default;

  TimeStamp(int64_t x) : milli_second(x) {}

  std::string to_string() const;

  inline bool operator<(const TimeStamp& rhs) const {
    return milli_second < rhs.milli_second;
  }

  inline bool operator==(const TimeStamp& rhs) const {
    return milli_second == rhs.milli_second;
  }

  TimeStamp& operator+(const Interval& interval) {
    milli_second += interval.to_mill_seconds();
    return *this;
  }

  TimeStamp& operator+=(const Interval& interval) {
    milli_second += interval.to_mill_seconds();
    return *this;
  }

  TimeStamp& operator-(const Interval& interval) {
    milli_second -= interval.to_mill_seconds();
    return *this;
  }

  inline TimeStamp& operator-=(const Interval& interval) {
    milli_second -= interval.to_mill_seconds();
    return *this;
  }

  Interval operator-(const TimeStamp& rhs) const {
    Interval interval;
    interval.from_mill_seconds(this->milli_second - rhs.milli_second);
    return interval;
  }

  Interval operator-=(const TimeStamp& rhs) { return this->operator-(rhs); }

  int64_t milli_second;
};

Date operator+(const Date& date, const Interval& interval);

Date operator-(const Date& date, const Interval& interval);

DateTime operator+(const DateTime& dt, const Interval& interval);

DateTime operator-(const DateTime& dt, const Interval& interval);

TimeStamp operator+(const TimeStamp& ts, const Interval& interval);

TimeStamp operator-(const TimeStamp& ts, const Interval& interval);

Interval operator+(const Interval& lhs, const Interval& rhs);

Interval operator-(const Interval& lhs, const Interval& rhs);
struct LabelKey {
  using label_data_type = uint8_t;
  int32_t label_id;
  LabelKey() = default;
  LabelKey(label_data_type id) : label_id(id) {}
};

class Table;
struct Any;

struct RecordView {
  RecordView() = default;
  RecordView(size_t offset, const Table* table)
      : offset(offset), table(table) {}
  size_t size() const;
  Any operator[](size_t idx) const;

  template <typename T>
  T get_field(int col_id) const;

  inline bool operator==(const RecordView& other) const {
    if (size() != other.size()) {
      return false;
    }
    return table == other.table && offset == other.offset;
  }

  std::string to_string() const;

  size_t offset;
  const Table* table;
};

struct Any;

struct Record {
  Record() : len(0), props(nullptr) {}
  Record(size_t len);
  Record(const Record& other);
  Record(Record&& other);
  Record& operator=(const Record& other);
  Record(const std::vector<Any>& vec);
  Record(const std::initializer_list<Any>& list);
  ~Record();
  size_t size() const { return len; }
  Any operator[](size_t idx) const;
  Any* begin() const;
  Any* end() const;

  size_t len;
  Any* props;
};

struct StringPtr {
  StringPtr() : ptr(nullptr) {}
  StringPtr(const std::string& str) : ptr(new std::string(str)) {}
  StringPtr(const StringPtr& other) {
    if (other.ptr) {
      ptr = new std::string(*other.ptr);
    } else {
      ptr = nullptr;
    }
  }
  StringPtr(StringPtr&& other) : ptr(other.ptr) { other.ptr = nullptr; }
  StringPtr& operator=(const StringPtr& other) {
    if (this == &other) {
      return *this;
    }
    if (ptr) {
      delete ptr;
    }
    if (other.ptr) {
      ptr = new std::string(*other.ptr);
    } else {
      ptr = nullptr;
    }
    return *this;
  }
  ~StringPtr() {
    if (ptr) {
      delete ptr;
    }
  }
  // return string_view
  std::string_view operator*() const {
    return std::string_view((*ptr).data(), (*ptr).size());
  }
  std::string* ptr;
};
union AnyValue {
  AnyValue() {}
  ~AnyValue() {}

  bool b;
  int32_t i;
  uint32_t ui;
  float f;
  int64_t l;
  uint64_t ul;
  GlobalId vertex_gid;
  LabelKey label_key;

  std::string_view s;
  double db;
  uint8_t u8;
  uint16_t u16;
  RecordView record_view;

  // temporal types
  Date d;
  DateTime dt;
  Interval interval;
  TimeStamp ts;

  // Non-trivial types
  Record record;
  StringPtr s_ptr;
};

template <typename T>
struct AnyConverter;

struct Any {
  Any() : type(PropertyType::kEmpty) {}

  Any(const Any& other) : type(other.type) {
    if (type == PropertyType::kRecord) {
      new (&value.record) Record(other.value.record);
    } else if (type.type_enum == impl::PropertyTypeImpl::kString) {
      new (&value.s_ptr) StringPtr(other.value.s_ptr);
    } else {
      memcpy(static_cast<void*>(&value), static_cast<const void*>(&other.value),
             sizeof(AnyValue));
    }
  }

  Any(Any&& other) : type(other.type) {
    if (type == PropertyType::kRecord) {
      new (&value.record) Record(std::move(other.value.record));
    } else if (type.type_enum == impl::PropertyTypeImpl::kString) {
      new (&value.s_ptr) StringPtr(std::move(other.value.s_ptr));
    } else {
      memcpy(static_cast<void*>(&value), static_cast<const void*>(&other.value),
             sizeof(AnyValue));
    }
  }

  Any(const std::initializer_list<Any>& list) {
    type = PropertyType::kRecord;
    new (&value.record) Record(list);
  }
  Any(const std::vector<Any>& vec) {
    type = PropertyType::kRecord;
    new (&value.record) Record(vec);
  }

  Any(const std::string& str) {
    type = PropertyType::kString;
    new (&value.s_ptr) StringPtr(str);
  }

  template <typename T>
  Any(const T& val) {
    Any a = Any::From(val);
    type = a.type;
    if (type == PropertyType::kRecord) {
      new (&value.record) Record(a.value.record);
    } else if (type.type_enum == impl::PropertyTypeImpl::kString) {
      new (&value.s_ptr) StringPtr(a.value.s_ptr);
    } else {
      memcpy(static_cast<void*>(&value), static_cast<const void*>(&a.value),
             sizeof(AnyValue));
    }
  }

  Any& operator=(const Any& other) {
    if (this == &other) {
      return *this;
    }
    if (type == PropertyType::kRecord) {
      value.record.~Record();
    }
    type = other.type;
    if (type == PropertyType::kRecord) {
      new (&value.record) Record(other.value.record);
    } else if (type.type_enum == impl::PropertyTypeImpl::kString) {
      new (&value.s_ptr) StringPtr(other.value.s_ptr);
    } else {
      memcpy(static_cast<void*>(&value), static_cast<const void*>(&other.value),
             sizeof(AnyValue));
    }
    return *this;
  }

  ~Any() {
    if (type == PropertyType::kRecord) {
      value.record.~Record();
    } else if (type.type_enum == impl::PropertyTypeImpl::kString) {
      value.s_ptr.~StringPtr();
    }
  }

  int64_t get_long() const {
    assert(type == PropertyType::kInt64);
    return value.l;
  }
  void set_bool(bool v) {
    type = PropertyType::kBool;
    value.b = v;
  }

  void set_i32(int32_t v) {
    type = PropertyType::kInt32;
    value.i = v;
  }

  void set_u32(uint32_t v) {
    type = PropertyType::kUInt32;
    value.ui = v;
  }

  void set_i64(int64_t v) {
    type = PropertyType::kInt64;
    value.l = v;
  }

  void set_u64(uint64_t v) {
    type = PropertyType::kUInt64;
    value.ul = v;
  }

  void set_vertex_gid(GlobalId v) {
    type = PropertyType::kVertexGlobalId;
    value.vertex_gid = v;
  }

  void set_label_key(LabelKey v) {
    type = PropertyType::kLabel;
    value.label_key = v;
  }

  void set_string_view(std::string_view v) {
    type = PropertyType::kStringView;
    value.s = v;
  }

  void set_string(const std::string& v) {
    type = PropertyType::kString;
    new (&value.s_ptr) StringPtr(v);
  }

  void set_float(float v) {
    type = PropertyType::kFloat;
    value.f = v;
  }

  void set_double(double db) {
    type = PropertyType::kDouble;
    value.db = db;
  }

  void set_u8(uint8_t v) {
    type = PropertyType::kUInt8;
    value.u8 = v;
  }

  void set_u16(uint16_t v) {
    type = PropertyType::kUInt16;
    value.u16 = v;
  }

  void set_record_view(RecordView v) {
    type = PropertyType::kRecordView;
    value.record_view = v;
  }

  void set_record(Record v) {
    if (type == PropertyType::kRecord) {
      value.record.~Record();
    }
    type = PropertyType::kRecord;
    new (&(value.record)) Record(v);
  }

  void set_date(int32_t days) {
    type = PropertyType::kDate;
    value.d.from_num_days(days);
  }

  void set_date(Date v) {
    type = PropertyType::kDate;
    value.d = v;
  }

  void set_datetime(DateTime v) {
    type = PropertyType::kDateTime;
    value.dt = v;
  }

  void set_datetime(int64_t v) {
    type = PropertyType::kDateTime;
    value.dt.milli_second = v;
  }

  void set_interval(Interval v) {
    type = PropertyType::kInterval;
    value.interval = v;
  }

  void set_interval(uint64_t v) {
    type = PropertyType::kInterval;
    value.interval.from_mill_seconds(v);
  }

  void set_timestamp(int64_t v) {
    type = PropertyType::kTimestamp;
    value.ts.milli_second = v;
  }

  std::string to_string() const;

  bool AsBool() const {
    assert(type == PropertyType::kBool);
    return value.b;
  }

  int64_t AsInt64() const {
    assert(type == PropertyType::kInt64);
    return value.l;
  }

  uint64_t AsUInt64() const {
    assert(type == PropertyType::kUInt64);
    return value.ul;
  }

  int32_t AsInt32() const {
    assert(type == PropertyType::kInt32);
    return value.i;
  }

  uint32_t AsUInt32() const {
    assert(type == PropertyType::kUInt32);
    return value.ui;
  }

  double AsDouble() const {
    assert(type == PropertyType::kDouble);
    return value.db;
  }

  float AsFloat() const {
    assert(type == PropertyType::kFloat);
    return value.f;
  }

  const std::string& AsString() const {
    assert(type.type_enum == impl::PropertyTypeImpl::kString);
    return *value.s_ptr.ptr;
  }

  std::string_view AsStringView() const {
    assert(type == PropertyType::kStringView);
    if (type.type_enum != impl::PropertyTypeImpl::kString) {
      return value.s;
    } else {
      return *value.s_ptr.ptr;
    }
  }

  const GlobalId& AsGlobalId() const {
    assert(type == PropertyType::kVertexGlobalId);
    return value.vertex_gid;
  }

  const LabelKey& AsLabelKey() const {
    assert(type == PropertyType::kLabel);
    return value.label_key;
  }

  const RecordView& AsRecordView() const {
    assert(type == PropertyType::kRecordView);
    return value.record_view;
  }

  const Record& AsRecord() const {
    assert(type == PropertyType::kRecord);
    return value.record;
  }

  const Date& AsDate() const {
    assert(type == PropertyType::kDate);
    return value.d;
  }

  const DateTime& AsDateTime() const {
    assert(type == PropertyType::kDateTime);
    return value.dt;
  }

  const Interval& AsInterval() const {
    assert(type == PropertyType::kInterval);
    return value.interval;
  }

  const TimeStamp& AsTimeStamp() const {
    assert(type == PropertyType::kTimestamp);
    return value.ts;
  }

  template <typename T>
  static Any From(const T& value) {
    return AnyConverter<T>::to_any(value);
  }

  bool operator==(const Any& other) const;

  bool operator<(const Any& other) const;

  PropertyType type;
  AnyValue value;
};

template <typename T>
struct ConvertAny {
  static void to(const Any& value, T& out) {
    LOG(FATAL) << "Unexpected convert type...";
  }
};

template <>
struct ConvertAny<bool> {
  static void to(const Any& value, bool& out) {
    assert(value.type == PropertyType::kBool);
    out = value.value.b;
  }
};

template <>
struct ConvertAny<int32_t> {
  static void to(const Any& value, int32_t& out) {
    assert(value.type == PropertyType::kInt32);
    out = value.value.i;
  }
};

template <>
struct ConvertAny<uint32_t> {
  static void to(const Any& value, uint32_t& out) {
    assert(value.type == PropertyType::kUInt32);
    out = value.value.ui;
  }
};

template <>
struct ConvertAny<int64_t> {
  static void to(const Any& value, int64_t& out) {
    assert(value.type == PropertyType::kInt64);
    out = value.value.l;
  }
};

template <>
struct ConvertAny<uint64_t> {
  static void to(const Any& value, uint64_t& out) {
    assert(value.type == PropertyType::kUInt64);
    out = value.value.ul;
  }
};

template <>
struct ConvertAny<GlobalId> {
  static void to(const Any& value, GlobalId& out) {
    assert(value.type == PropertyType::kVertexGlobalId);
    out = value.value.vertex_gid;
  }
};

template <>
struct ConvertAny<LabelKey> {
  static void to(const Any& value, LabelKey& out) {
    assert(value.type == PropertyType::kLabel);
    out = value.value.label_key;
  }
};

template <>
struct ConvertAny<Date> {
  static void to(const Any& value, Date& out) {
    assert(value.type == PropertyType::kDate);
    out = value.value.d;
  }
};

template <>
struct ConvertAny<grape::EmptyType> {
  static void to(const Any& value, grape::EmptyType& out) {
    assert(value.type == PropertyType::kEmpty);
  }
};

template <>
struct ConvertAny<std::string> {
  static void to(const Any& value, std::string& out) {
    assert(value.type.type_enum == impl::PropertyTypeImpl::kString);
    out = *value.value.s_ptr.ptr;
  }
};

template <>
struct ConvertAny<std::string_view> {
  static void to(const Any& value, std::string_view& out) {
    assert(value.type == PropertyType::kStringView);
    out = value.AsStringView();
  }
};

template <>
struct ConvertAny<float> {
  static void to(const Any& value, float& out) {
    assert(value.type == PropertyType::kFloat);
    out = value.value.f;
  }
};

template <>
struct ConvertAny<double> {
  static void to(const Any& value, double& out) {
    assert(value.type == PropertyType::kDouble);
    out = value.value.db;
  }
};

template <>
struct ConvertAny<RecordView> {
  static void to(const Any& value, RecordView& out) {
    assert(value.type == PropertyType::kRecordView);
    out.offset = value.value.record_view.offset;
    out.table = value.value.record_view.table;
  }
};

template <>
struct ConvertAny<Record> {
  static void to(const Any& value, Record& out) {
    assert(value.type == PropertyType::kRecord);
    out = value.value.record;
  }
};

template <typename T>
struct AnyConverter {};

// specialization for bool
template <>
struct AnyConverter<bool> {
  static PropertyType type() { return PropertyType::kBool; }

  static Any to_any(const bool& value) {
    Any ret;
    ret.set_bool(value);
    return ret;
  }
  static const bool& from_any(const Any& value) {
    assert(value.type == PropertyType::kBool);
    return value.value.b;
  }

  static std::string type_name() { return "bool"; }

  static const bool& from_any_value(const AnyValue& value) { return value.b; }
};

template <>
struct AnyConverter<uint8_t> {
  static PropertyType type() { return PropertyType::kUInt8; }
  static Any to_any(const uint8_t& value) {
    Any ret;
    ret.set_u8(value);
    return ret;
  }
  static const uint8_t& from_any(const Any& value) {
    assert(value.type == PropertyType::kUInt8);
    return value.value.u8;
  }

  static std::string type_name() { return "uint8_t"; }
};

template <>
struct AnyConverter<uint16_t> {
  static PropertyType type() { return PropertyType::kUInt16; }
  static Any to_any(const uint16_t& value) {
    Any ret;
    ret.set_u16(value);
    return ret;
  }
  static const uint16_t& from_any(const Any& value) {
    assert(value.type == PropertyType::kUInt8);
    return value.value.u16;
  }

  static std::string type_name() { return "uint16_t"; }
};

template <>
struct AnyConverter<int32_t> {
  static PropertyType type() { return PropertyType::kInt32; }

  static Any to_any(const int32_t& value) {
    Any ret;
    ret.set_i32(value);
    return ret;
  }

  static const int32_t& from_any(const Any& value) {
    assert(value.type == PropertyType::kInt32);
    return value.value.i;
  }

  static const int32_t& from_any_value(const AnyValue& value) {
    return value.i;
  }

  static std::string type_name() { return "int32_t"; }
};

template <>
struct AnyConverter<uint32_t> {
  static PropertyType type() { return PropertyType::kUInt32; }

  static Any to_any(const uint32_t& value) {
    Any ret;
    ret.set_u32(value);
    return ret;
  }

  static const uint32_t& from_any(const Any& value) {
    assert(value.type == PropertyType::kUInt32);
    return value.value.ui;
  }

  static const uint32_t& from_any_value(const AnyValue& value) {
    return value.ui;
  }

  static std::string type_name() { return "uint32_t"; }
};
template <>
struct AnyConverter<int64_t> {
  static PropertyType type() { return PropertyType::kInt64; }

  static Any to_any(const int64_t& value) {
    Any ret;
    ret.set_i64(value);
    return ret;
  }

  static const int64_t& from_any(const Any& value) {
    assert(value.type == PropertyType::kInt64);
    return value.value.l;
  }

  static const int64_t& from_any_value(const AnyValue& value) {
    return value.l;
  }

  static std::string type_name() { return "int64_t"; }
};

template <>
struct AnyConverter<uint64_t> {
  static PropertyType type() { return PropertyType::kUInt64; }

  static Any to_any(const uint64_t& value) {
    Any ret;
    ret.set_u64(value);
    return ret;
  }

  static const uint64_t& from_any(const Any& value) {
    assert(value.type == PropertyType::kUInt64);
    return value.value.ul;
  }

  static const uint64_t& from_any_value(const AnyValue& value) {
    return value.ul;
  }

  static std::string type_name() { return "uint64_t"; }
};

#ifdef __APPLE__
template <>
struct AnyConverter<size_t> {
  static PropertyType type() { return PropertyType::kUInt64; }

  static Any to_any(const uint64_t& value) {
    Any ret;
    ret.set_u64(value);
    return ret;
  }

  static const uint64_t& from_any(const Any& value) {
    assert(value.type == PropertyType::kUInt64);
    return value.value.ul;
  }

  static const uint64_t& from_any_value(const AnyValue& value) {
    return value.ul;
  }

  static std::string type_name() { return "size_t"; }
};
#endif

template <>
struct AnyConverter<GlobalId> {
  static PropertyType type() { return PropertyType::kVertexGlobalId; }

  static Any to_any(const GlobalId& value) {
    Any ret;
    ret.set_vertex_gid(value);
    return ret;
  }

  static const GlobalId& from_any(const Any& value) {
    assert(value.type == PropertyType::kVertexGlobalId);
    return value.value.vertex_gid;
  }

  static const GlobalId& from_any_value(const AnyValue& value) {
    return value.vertex_gid;
  }

  static std::string type_name() { return "GlobalId"; }
};

template <>
struct AnyConverter<std::string_view> {
  static PropertyType type() { return PropertyType::kStringView; }

  static Any to_any(const std::string_view& value) {
    Any ret;
    ret.set_string_view(value);
    return ret;
  }

  static const std::string_view& from_any(const Any& value) {
    assert(value.type == PropertyType::kStringView &&
           value.type.type_enum != impl::PropertyTypeImpl::kString);
    return value.value.s;
  }

  static const std::string_view& from_any_value(const AnyValue& value) {
    return value.s;
  }

  static std::string type_name() { return "std::string_view"; }
};

template <>
struct AnyConverter<std::string> {
  static PropertyType type() { return PropertyType::kString; }

  static Any to_any(const std::string& value) {
    Any ret;
    ret.set_string(value);
    return ret;
  }

  static std::string& from_any(const Any& value) {
    assert(value.type.type_enum == impl::PropertyTypeImpl::kString);
    return *value.value.s_ptr.ptr;
  }

  static std::string& from_any_value(const AnyValue& value) {
    return *value.s_ptr.ptr;
  }

  static std::string type_name() { return "std::string"; }
};

template <>
struct AnyConverter<grape::EmptyType> {
  static PropertyType type() { return PropertyType::kEmpty; }

  static Any to_any(const grape::EmptyType& value) {
    Any ret;
    return ret;
  }

  static grape::EmptyType from_any(const Any& value) {
    assert(value.type == PropertyType::kEmpty);
    return grape::EmptyType();
  }

  static grape::EmptyType from_any_value(const AnyValue& value) {
    return grape::EmptyType();
  }

  static std::string type_name() { return "grape::EmptyType"; }
};

template <>
struct AnyConverter<double> {
  static PropertyType type() { return PropertyType::kDouble; }

  static Any to_any(const double& value) {
    Any ret;
    ret.set_double(value);
    return ret;
  }

  static const double& from_any(const Any& value) {
    assert(value.type == PropertyType::kDouble);
    return value.value.db;
  }

  static const double& from_any_value(const AnyValue& value) {
    return value.db;
  }

  static std::string type_name() { return "double"; }
};

// specialization for float
template <>
struct AnyConverter<float> {
  static PropertyType type() { return PropertyType::kFloat; }

  static Any to_any(const float& value) {
    Any ret;
    ret.set_float(value);
    return ret;
  }

  static const float& from_any(const Any& value) {
    assert(value.type == PropertyType::kFloat);
    return value.value.f;
  }

  static const float& from_any_value(const AnyValue& value) { return value.f; }

  static std::string type_name() { return "float"; }
};

template <>
struct AnyConverter<LabelKey> {
  static PropertyType type() { return PropertyType::kLabel; }

  static Any to_any(const LabelKey& value) {
    Any ret;
    ret.set_label_key(value);
    return ret;
  }

  static const LabelKey& from_any(const Any& value) {
    assert(value.type == PropertyType::kLabel);
    return value.value.label_key;
  }

  static const LabelKey& from_any_value(const AnyValue& value) {
    return value.label_key;
  }

  static std::string type_name() { return "LabelKey"; }
};
Any ConvertStringToAny(const std::string& value, const gs::PropertyType& type);

template <>
struct AnyConverter<RecordView> {
  static PropertyType type() { return PropertyType::kRecordView; }

  static Any to_any(const RecordView& value) {
    Any ret;
    ret.set_record_view(value);
    return ret;
  }

  static const RecordView& from_any(const Any& value) {
    assert(value.type == PropertyType::kRecordView);
    return value.value.record_view;
  }

  static const RecordView& from_any_value(const AnyValue& value) {
    return value.record_view;
  }

  static std::string type_name() { return "RecordView"; }
};

template <>
struct AnyConverter<Record> {
  static PropertyType type() { return PropertyType::kRecord; }

  static Any to_any(const Record& value) {
    Any ret;
    ret.set_record(value);
    return ret;
  }

  static const Record& from_any(const Any& value) {
    assert(value.type == PropertyType::kRecord);
    return value.value.record;
  }

  static const Record& from_any_value(const AnyValue& value) {
    return value.record;
  }

  static std::string type_name() { return "Record"; }
};

template <>
struct AnyConverter<Date> {
  static PropertyType type() { return PropertyType::kDate; }

  static Any to_any(const Date& value) {
    Any ret;
    ret.set_date(value);
    return ret;
  }

  static Any to_any(int32_t value) {
    Any ret;
    ret.set_date(value);
    return ret;
  }

  static const Date& from_any(const Any& value) {
    assert(value.type == PropertyType::kDate);
    return value.value.d;
  }

  static const Date& from_any_value(const AnyValue& value) { return value.d; }

  static std::string type_name() { return "Date"; }
};

template <>
struct AnyConverter<DateTime> {
  static PropertyType type() { return PropertyType::kDateTime; }

  static Any to_any(const DateTime& value) {
    Any ret;
    ret.set_datetime(value);
    return ret;
  }

  static Any to_any(int64_t value) {
    Any ret;
    ret.set_datetime(value);
    return ret;
  }

  static const DateTime& from_any(const Any& value) {
    assert(value.type == PropertyType::kDateTime);
    return value.value.dt;
  }

  static const DateTime& from_any_value(const AnyValue& value) {
    return value.dt;
  }

  static std::string type_name() { return "DateTime"; }
};

template <>
struct AnyConverter<Interval> {
  static PropertyType type() { return PropertyType::kInterval; }

  static Any to_any(const Interval& value) {
    Any ret;
    ret.set_interval(value);
    return ret;
  }

  static Any to_any(uint64_t value) {
    Any ret;
    ret.set_interval(value);
    return ret;
  }

  /**
   * Parse the interval from a string. Expect the string in specific format,
   * such as "1 day 2 hours 3 minutes 4 seconds" or "3 years 2 days 13 hours 2
   * minutes" or "18 minutes 24 milliseconds". If the string is not in the
   * expected format, it will throw an exception.
   * @param value The string to parse.
   * @return An Any object containing the parsed Interval.
   * TODO(zhanglei): optimize the performance.
   */
  static Any to_any(std::string_view value);

  static const Interval& from_any(const Any& value) {
    assert(value.type == PropertyType::kInterval);
    return value.value.interval;
  }

  static const Interval& from_any_value(const AnyValue& value) {
    return value.interval;
  }

  static std::string type_name() { return "Interval"; }
};

template <>
struct AnyConverter<TimeStamp> {
  static PropertyType type() { return PropertyType::kTimestamp; }

  static Any to_any(const TimeStamp& value) {
    Any ret;
    ret.set_timestamp(value.milli_second);
    return ret;
  }

  static const TimeStamp& from_any(const Any& value) {
    assert(value.type == PropertyType::kTimestamp);
    return value.value.ts;
  }

  static const TimeStamp& from_any_value(const AnyValue& value) {
    return value.ts;
  }

  static std::string type_name() { return "TimeStamp"; }
};

template <typename T>
T RecordView::get_field(int col_id) const {
  auto val = operator[](col_id);
  T ret{};
  ConvertAny<T>::to(val, ret);
  return ret;
}

grape::InArchive& operator<<(grape::InArchive& in_archive,
                             const PropertyType& value);
grape::OutArchive& operator>>(grape::OutArchive& out_archive,
                              PropertyType& value);

grape::InArchive& operator<<(grape::InArchive& in_archive, const Any& value);
grape::OutArchive& operator>>(grape::OutArchive& out_archive, Any& value);

grape::InArchive& operator<<(grape::InArchive& in_archive,
                             const std::string_view& value);
grape::OutArchive& operator>>(grape::OutArchive& out_archive,
                              std::string_view& value);

grape::InArchive& operator<<(grape::InArchive& in_archive,
                             const GlobalId& value);
grape::OutArchive& operator>>(grape::OutArchive& out_archive, GlobalId& value);

grape::InArchive& operator<<(grape::InArchive& in_archive,
                             const Interval& value);
grape::OutArchive& operator>>(grape::OutArchive& out_archive, Interval& value);

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
static const TimeStamp DEFAULT_TIME_STAMP_VALUE = TimeStamp(0);

}  // namespace gs

namespace boost {
// override boost hash function for EmptyType
inline std::size_t hash_value(const grape::EmptyType& value) { return 0; }
inline std::size_t hash_value(const gs::GlobalId& value) {
  return std::hash<uint64_t>()(value.global_id);
}
// overload hash_value for LabelKey
inline std::size_t hash_value(const gs::LabelKey& key) {
  return std::hash<int32_t>()(key.label_id);
}

}  // namespace boost

namespace std {

inline ostream& operator<<(ostream& os, const gs::Date& dt) {
  os << dt.to_string();
  return os;
}

inline ostream& operator<<(ostream& os, gs::PropertyType pt) {
  if (pt == gs::PropertyType::Bool()) {
    os << "bool";
  } else if (pt == gs::PropertyType::Empty()) {
    os << "empty";
  } else if (pt == gs::PropertyType::UInt8()) {
    os << "uint8";
  } else if (pt == gs::PropertyType::UInt16()) {
    os << "uint16";
  } else if (pt == gs::PropertyType::Int32()) {
    os << "int32";
  } else if (pt == gs::PropertyType::UInt32()) {
    os << "uint32";
  } else if (pt == gs::PropertyType::Float()) {
    os << "float";
  } else if (pt == gs::PropertyType::Int64()) {
    os << "int64";
  } else if (pt == gs::PropertyType::UInt64()) {
    os << "uint64";
  } else if (pt == gs::PropertyType::Double()) {
    os << "double";
  } else if (pt == gs::PropertyType::StringView()) {
    os << "string";
  } else if (pt == gs::PropertyType::StringMap()) {
    os << "string_map";
  } else if (pt.type_enum == gs::impl::PropertyTypeImpl::kVarChar) {
    os << "varchar(" << pt.additional_type_info.max_length << ")";
  } else if (pt == gs::PropertyType::VertexGlobalId()) {
    os << "vertex_global_id";
  } else if (pt == gs::PropertyType::Label()) {
    os << "label";
  } else if (pt == gs::PropertyType::RecordView()) {
    os << "record_view";
  } else if (pt == gs::PropertyType::Record()) {
    os << "record";
  } else if (pt == gs::PropertyType::Date()) {
    os << "date";
  } else if (pt == gs::PropertyType::DateTime()) {
    os << "datetime";
  } else if (pt == gs::PropertyType::Interval()) {
    os << "interval";
  } else if (pt == gs::PropertyType::Timestamp()) {
    os << "timestamp";
  } else {
    os << "unknown";
  }
  return os;
}

template <>
struct hash<gs::GlobalId> {
  size_t operator()(const gs::GlobalId& value) const {
    return std::hash<uint64_t>()(value.global_id);
  }
};

}  // namespace std

namespace grape {
inline bool operator==(const EmptyType& a, const EmptyType& b) { return true; }

inline bool operator!=(const EmptyType& a, const EmptyType& b) { return false; }
}  // namespace grape

namespace YAML {
template <>
struct convert<gs::PropertyType> {
  // concurrently preserve backwards compatibility with old config files
  static bool decode(const Node& config, gs::PropertyType& property_type) {
    if (config["primitive_type"]) {
      property_type = gs::config_parsing::StringToPrimitivePropertyType(
          config["primitive_type"].as<std::string>());
    } else if (config["string"]) {
      if (config["string"].IsMap()) {
        if (config["string"]["long_text"]) {
          property_type = gs::PropertyType::StringView();
        } else if (config["string"]["var_char"]) {
          if (config["string"]["var_char"]["max_length"]) {
            property_type = gs::PropertyType::Varchar(
                config["string"]["var_char"]["max_length"].as<int32_t>());
          } else {
            property_type = gs::PropertyType::Varchar(
                gs::PropertyType::GetStringDefaultMaxLength());
          }
        } else {
          LOG(ERROR) << "Unrecognized string type";
        }
      } else {
        LOG(ERROR) << "string should be a map";
      }
    } else if (config["temporal"]) {
      auto temporal = config["temporal"];
      if (temporal["date"]) {
        property_type = gs::PropertyType::Date();
      } else if (temporal["datetime"]) {
        property_type = gs::PropertyType::DateTime();
      } else if (temporal["interval"]) {
        property_type = gs::PropertyType::Interval();
      } else if (temporal["timestamp"]) {
        property_type = gs::PropertyType::Timestamp();
      } else {
        throw std::runtime_error("Unrecognized temporal type: " +
                                 temporal.as<std::string>());
      }
    }
    // compatibility with old config files
    else if (config["varchar"]) {
      if (config["varchar"]["max_length"]) {
        property_type = gs::PropertyType::Varchar(
            config["varchar"]["max_length"].as<int32_t>());
      } else {
        property_type = gs::PropertyType::Varchar(
            gs::PropertyType::GetStringDefaultMaxLength());
      }
    } else if (config["date"]) {
      property_type = gs::PropertyType::Date();
    } else {
      LOG(ERROR) << "Unrecognized property type: " << config;
      return false;
    }
    return true;
  }

  static Node encode(const gs::PropertyType& type) {
    YAML::Node node;
    if (type == gs::PropertyType::Bool() || type == gs::PropertyType::Int32() ||
        type == gs::PropertyType::UInt32() ||
        type == gs::PropertyType::Float() ||
        type == gs::PropertyType::Int64() ||
        type == gs::PropertyType::UInt64() ||
        type == gs::PropertyType::Double()) {
      node["primitive_type"] =
          gs::config_parsing::PrimitivePropertyTypeToString(type);
    } else if (type == gs::PropertyType::StringView() ||
               type == gs::PropertyType::StringMap()) {
      node["string"]["long_text"] = "";
    } else if (type.IsVarchar()) {
      node["string"]["var_char"]["max_length"] =
          type.additional_type_info.max_length;
    } else if (type == gs::PropertyType::Date()) {
      node["temporal"]["timestamp"] = "";
    } else {
      LOG(ERROR) << "Unrecognized property type: " << type;
    }
    return node;
  }
};
}  // namespace YAML

#endif  // UTILS_PROPERTY_TYPES_H_
