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
#include <cmath>

#include "src/utils/property/types.h"

#include <algorithm>
#include <cctype>
#include <cstdlib>
#include <iomanip>
#include <istream>
#include <memory>
#include <ostream>

#include "src/utils/property/column.h"
#include "src/utils/property/table.h"
#include "src/utils/property/types.h"
#include "third_party/libgrape-lite/grape/serialization/in_archive.h"
#include "third_party/libgrape-lite/grape/serialization/out_archive.h"

namespace gs {

namespace config_parsing {

std::string PrimitivePropertyTypeToString(PropertyType type) {
  if (type == PropertyType::kEmpty) {
    return "Empty";
  } else if (type == PropertyType::kBool) {
    return DT_BOOL;
  } else if (type == PropertyType::kUInt8) {
    return DT_UNSIGNED_INT8;
  } else if (type == PropertyType::kUInt16) {
    return DT_UNSIGNED_INT16;
  } else if (type == PropertyType::kInt32) {
    return DT_SIGNED_INT32;
  } else if (type == PropertyType::kUInt32) {
    return DT_UNSIGNED_INT32;
  } else if (type == PropertyType::kInt64) {
    return DT_SIGNED_INT64;
  } else if (type == PropertyType::kUInt64) {
    return DT_UNSIGNED_INT64;
  } else if (type == PropertyType::kFloat) {
    return DT_FLOAT;
  } else if (type == PropertyType::kDouble) {
    return DT_DOUBLE;
  } else if (type == PropertyType::kStringView) {
    return DT_STRING;
  } else if (type == PropertyType::kStringMap) {
    return DT_STRINGMAP;
  } else if (type == PropertyType::kVertexGlobalId) {
    return "VertexGlobalId";
  } else if (type == PropertyType::kLabel) {
    return "Label";
  } else if (type == PropertyType::kRecord) {
    return "Record";
  } else if (type == PropertyType::kRecordView) {
    return "RecordView";
  } else if (type == PropertyType::kDate) {
    return DT_DATE;
  } else if (type == PropertyType::kDateTime) {
    return DT_DATETIME;
  } else if (type == PropertyType::kInterval) {
    return DT_INTERVAL;
  } else if (type == PropertyType::kTimestamp) {
    return DT_TIMESTAMP;
  } else {
    LOG(FATAL) << "Unknown property type: " << type;
  }
}

PropertyType StringToPrimitivePropertyType(const std::string& str) {
  if (str == "int32" || str == "INT" || str == DT_SIGNED_INT32) {
    return PropertyType::kInt32;
  } else if (str == "uint32" || str == DT_UNSIGNED_INT32) {
    return PropertyType::kUInt32;
  } else if (str == "bool" || str == "BOOL" || str == DT_BOOL) {
    return PropertyType::kBool;
  } else if (str == "Date" || str == DT_DATE) {
    return PropertyType::kDate;
  } else if (str == "DateTime" || str == DT_DATETIME) {
    return PropertyType::kDateTime;
  } else if (str == "Interval" || str == DT_INTERVAL) {
    return PropertyType::kInterval;
  } else if (str == "Timestamp" || str == DT_TIMESTAMP) {
    return PropertyType::kTimestamp;
  } else if (str == "String" || str == "STRING" || str == DT_STRING) {
    // DT_STRING is a alias for VARCHAR(GetStringDefaultMaxLength());
    return PropertyType::Varchar(PropertyType::GetStringDefaultMaxLength());
  } else if (str == DT_STRINGMAP) {
    return PropertyType::kStringMap;
  } else if (str == "Empty") {
    return PropertyType::kEmpty;
  } else if (str == "int64" || str == "LONG" || str == DT_SIGNED_INT64) {
    return PropertyType::kInt64;
  } else if (str == "uint64" || str == DT_UNSIGNED_INT64) {
    return PropertyType::kUInt64;
  } else if (str == "float" || str == "FLOAT" || str == DT_FLOAT) {
    return PropertyType::kFloat;
  } else if (str == "double" || str == "DOUBLE" || str == DT_DOUBLE) {
    return PropertyType::kDouble;
  } else {
    return PropertyType::kEmpty;
  }
}

YAML::Node TemporalTypeToYAML(PropertyType type) {
  YAML::Node node;
  if (type == PropertyType::kDate) {
    node["date"] = "";
  } else if (type == PropertyType::kDateTime) {
    node["datetime"] = "";
  } else if (type == PropertyType::kTimestamp) {
    node["timestamp"] = "";
  } else if (type == PropertyType::kInterval) {
    node["interval"] = "";
  } else {
    LOG(FATAL) << "Unsupported property type: " << type.type_enum;
  }
  return node;
}

}  // namespace config_parsing

static uint16_t get_string_default_max_length_env() {
  static uint16_t max_length = 0;
  if (max_length == 0) {
    char* env = std::getenv("FLEX_STRING_DEFAULT_MAX_LENGTH");
    if (env) {
      try {
        max_length = static_cast<uint16_t>(std::stoi(env));
        LOG(INFO) << "FLEX_STRING_DEFAULT_MAX_LENGTH: " << max_length;
      } catch (std::exception& e) {
        LOG(ERROR) << "Invalid FLEX_STRING_DEFAULT_MAX_LENGTH: " << env;
      }
    }
  }
  return max_length;
}

uint16_t PropertyType::GetStringDefaultMaxLength() {
  return get_string_default_max_length_env() > 0
             ? get_string_default_max_length_env()
             : PropertyType::STRING_DEFAULT_MAX_LENGTH;
}

size_t RecordView::size() const { return table->col_num(); }

Any RecordView::operator[](size_t col_id) const {
  return table->get_column_by_id(col_id)->get(offset);
}

std::string RecordView::to_string() const {
  std::string ret = "RecordView{";
  for (size_t i = 0; i < table->col_num(); ++i) {
    if (i > 0) {
      ret += ", ";
    }
    ret += table->get_column_by_id(i)->get(offset).to_string();
  }
  ret += "}";
  return ret;
}

Record::Record(size_t len) : len(len) { props = new Any[len]; }

Record::Record(const Record& record) {
  len = record.len;
  props = new Any[len];
  for (size_t i = 0; i < len; ++i) {
    props[i] = record.props[i];
  }
}

Record::Record(Record&& record) {
  len = record.len;
  props = record.props;
  record.len = 0;
  record.props = nullptr;
}

Record& Record::operator=(const Record& record) {
  if (this == &record) {
    return *this;
  }
  if (props) {
    delete[] props;
  }
  len = record.len;
  props = new Any[len];
  for (size_t i = 0; i < len; ++i) {
    props[i] = record.props[i];
  }
  return *this;
}

Record::Record(const std::initializer_list<Any>& list) {
  len = list.size();
  props = new Any[len];
  size_t i = 0;
  for (auto& item : list) {
    props[i++] = item;
  }
}

Record::Record(const std::vector<Any>& vec) {
  len = vec.size();
  props = new Any[len];
  for (size_t i = 0; i < len; ++i) {
    props[i] = vec[i];
  }
}

Any Record::operator[](size_t idx) const {
  if (idx >= len) {
    return Any();
  }
  return props[idx];
}

Any* Record::begin() const { return props; }
Any* Record::end() const { return props + len; }

Record::~Record() {
  if (props) {
    delete[] props;
  }
  props = nullptr;
}

const PropertyType PropertyType::kEmpty =
    PropertyType(impl::PropertyTypeImpl::kEmpty);
const PropertyType PropertyType::kBool =
    PropertyType(impl::PropertyTypeImpl::kBool);
const PropertyType PropertyType::kUInt8 =
    PropertyType(impl::PropertyTypeImpl::kUInt8);
const PropertyType PropertyType::kUInt16 =
    PropertyType(impl::PropertyTypeImpl::kUInt16);
const PropertyType PropertyType::kInt32 =
    PropertyType(impl::PropertyTypeImpl::kInt32);
const PropertyType PropertyType::kUInt32 =
    PropertyType(impl::PropertyTypeImpl::kUInt32);
const PropertyType PropertyType::kInt64 =
    PropertyType(impl::PropertyTypeImpl::kInt64);
const PropertyType PropertyType::kUInt64 =
    PropertyType(impl::PropertyTypeImpl::kUInt64);
const PropertyType PropertyType::kFloat =
    PropertyType(impl::PropertyTypeImpl::kFloat);
const PropertyType PropertyType::kDouble =
    PropertyType(impl::PropertyTypeImpl::kDouble);
const PropertyType PropertyType::kStringView =
    PropertyType(impl::PropertyTypeImpl::kStringView);
const PropertyType PropertyType::kStringMap =
    PropertyType(impl::PropertyTypeImpl::kStringMap);
const PropertyType PropertyType::kString =
    PropertyType(impl::PropertyTypeImpl::kString);
const PropertyType PropertyType::kVertexGlobalId =
    PropertyType(impl::PropertyTypeImpl::kVertexGlobalId);
const PropertyType PropertyType::kLabel =
    PropertyType(impl::PropertyTypeImpl::kLabel);
const PropertyType PropertyType::kRecordView =
    PropertyType(impl::PropertyTypeImpl::kRecordView);
const PropertyType PropertyType::kRecord =
    PropertyType(impl::PropertyTypeImpl::kRecord);

const PropertyType PropertyType::kDate =
    PropertyType(impl::PropertyTypeImpl::kDate);
const PropertyType PropertyType::kDateTime =
    PropertyType(impl::PropertyTypeImpl::kDateTime);
const PropertyType PropertyType::kInterval =
    PropertyType(impl::PropertyTypeImpl::kInterval);
const PropertyType PropertyType::kTimestamp =
    PropertyType(impl::PropertyTypeImpl::kTimestamp);

bool PropertyType::operator==(const PropertyType& other) const {
  if (type_enum == impl::PropertyTypeImpl::kVarChar &&
      other.type_enum == impl::PropertyTypeImpl::kVarChar) {
    return additional_type_info.max_length ==
           other.additional_type_info.max_length;
  }
  if ((type_enum == impl::PropertyTypeImpl::kStringView &&
       other.type_enum == impl::PropertyTypeImpl::kVarChar) ||
      (type_enum == impl::PropertyTypeImpl::kVarChar &&
       other.type_enum == impl::PropertyTypeImpl::kStringView)) {
    return true;
  }
  if ((type_enum == impl::PropertyTypeImpl::kString &&
       (other.type_enum == impl::PropertyTypeImpl::kStringView ||
        other.type_enum == impl::PropertyTypeImpl::kVarChar)) ||
      (other.type_enum == impl::PropertyTypeImpl::kString &&
       (type_enum == impl::PropertyTypeImpl::kStringView ||
        type_enum == impl::PropertyTypeImpl::kVarChar))) {
    return true;
  }
  return type_enum == other.type_enum;
}

bool PropertyType::operator!=(const PropertyType& other) const {
  return !(*this == other);
}

bool PropertyType::IsVarchar() const {
  return type_enum == impl::PropertyTypeImpl::kVarChar;
}

std::string PropertyType::ToString() const {
  switch (type_enum) {
  case impl::PropertyTypeImpl::kEmpty:
    return "Empty";
  case impl::PropertyTypeImpl::kBool:
    return "Bool";
  case impl::PropertyTypeImpl::kUInt8:
    return "UInt8";
  case impl::PropertyTypeImpl::kUInt16:
    return "UInt16";
  case impl::PropertyTypeImpl::kInt32:
    return "Int32";
  case impl::PropertyTypeImpl::kUInt32:
    return "UInt32";
  case impl::PropertyTypeImpl::kInt64:
    return "Int64";
  case impl::PropertyTypeImpl::kUInt64:
    return "UInt64";
  case impl::PropertyTypeImpl::kFloat:
    return "Float";
  case impl::PropertyTypeImpl::kDouble:
    return "Double";
  case impl::PropertyTypeImpl::kVarChar:
    return "VarChar";
  case impl::PropertyTypeImpl::kString:
    return "String";
  case impl::PropertyTypeImpl::kStringView:
    return "StringView";
  case impl::PropertyTypeImpl::kStringMap:
    return "StringMap";
  case impl::PropertyTypeImpl::kVertexGlobalId:
    return "VertexGlobalId";
  case impl::PropertyTypeImpl::kLabel:
    return "Label";
  case impl::PropertyTypeImpl::kRecordView:
    return "RecordView";
  case impl::PropertyTypeImpl::kRecord:
    return "Record";
  case impl::PropertyTypeImpl::kDate:
    return "Date";
  case impl::PropertyTypeImpl::kDateTime:
    return "DateTime";
  case impl::PropertyTypeImpl::kInterval:
    return "Interval";
  case impl::PropertyTypeImpl::kTimestamp:
    return "Timestamp";
  default:
    return "Unknown";
  }
}

/////////////////////////////// Get Type Instance
//////////////////////////////////
PropertyType PropertyType::Empty() {
  return PropertyType(impl::PropertyTypeImpl::kEmpty);
}
PropertyType PropertyType::Bool() {
  return PropertyType(impl::PropertyTypeImpl::kBool);
}
PropertyType PropertyType::UInt8() {
  return PropertyType(impl::PropertyTypeImpl::kUInt8);
}
PropertyType PropertyType::UInt16() {
  return PropertyType(impl::PropertyTypeImpl::kUInt16);
}
PropertyType PropertyType::Int32() {
  return PropertyType(impl::PropertyTypeImpl::kInt32);
}
PropertyType PropertyType::UInt32() {
  return PropertyType(impl::PropertyTypeImpl::kUInt32);
}
PropertyType PropertyType::Int64() {
  return PropertyType(impl::PropertyTypeImpl::kInt64);
}
PropertyType PropertyType::UInt64() {
  return PropertyType(impl::PropertyTypeImpl::kUInt64);
}
PropertyType PropertyType::Float() {
  return PropertyType(impl::PropertyTypeImpl::kFloat);
}
PropertyType PropertyType::Double() {
  return PropertyType(impl::PropertyTypeImpl::kDouble);
}
PropertyType PropertyType::String() {
  return PropertyType(impl::PropertyTypeImpl::kString);
}
PropertyType PropertyType::StringView() {
  return PropertyType(impl::PropertyTypeImpl::kStringView);
}
PropertyType PropertyType::StringMap() {
  return PropertyType(impl::PropertyTypeImpl::kStringMap);
}
PropertyType PropertyType::Varchar(uint16_t max_length) {
  return PropertyType(impl::PropertyTypeImpl::kVarChar, max_length);
}

PropertyType PropertyType::VertexGlobalId() {
  return PropertyType(impl::PropertyTypeImpl::kVertexGlobalId);
}

PropertyType PropertyType::Label() {
  return PropertyType(impl::PropertyTypeImpl::kLabel);
}

PropertyType PropertyType::RecordView() {
  return PropertyType(impl::PropertyTypeImpl::kRecordView);
}

PropertyType PropertyType::Record() {
  return PropertyType(impl::PropertyTypeImpl::kRecord);
}

PropertyType PropertyType::Date() {
  return PropertyType(impl::PropertyTypeImpl::kDate);
}

PropertyType PropertyType::DateTime() {
  return PropertyType(impl::PropertyTypeImpl::kDateTime);
}

PropertyType PropertyType::Interval() {
  return PropertyType(impl::PropertyTypeImpl::kInterval);
}

PropertyType PropertyType::Timestamp() {
  return PropertyType(impl::PropertyTypeImpl::kTimestamp);
}

grape::InArchive& operator<<(grape::InArchive& in_archive,
                             const PropertyType& value) {
  in_archive << value.type_enum;
  if (value.type_enum == impl::PropertyTypeImpl::kVarChar) {
    in_archive << value.additional_type_info.max_length;
  }
  return in_archive;
}
grape::OutArchive& operator>>(grape::OutArchive& out_archive,
                              PropertyType& value) {
  out_archive >> value.type_enum;
  if (value.type_enum == impl::PropertyTypeImpl::kVarChar) {
    out_archive >> value.additional_type_info.max_length;
  }
  return out_archive;
}

grape::InArchive& operator<<(grape::InArchive& in_archive, const Any& value) {
  if (value.type == PropertyType::Empty()) {
    in_archive << value.type;
  } else if (value.type == PropertyType::Bool()) {
    in_archive << value.type << value.value.b;
  } else if (value.type == PropertyType::UInt8()) {
    in_archive << value.type << value.value.u8;
  } else if (value.type == PropertyType::UInt16()) {
    in_archive << value.type << value.value.u16;
  } else if (value.type == PropertyType::Int32()) {
    in_archive << value.type << value.value.i;
  } else if (value.type == PropertyType::UInt32()) {
    in_archive << value.type << value.value.ui;
  } else if (value.type == PropertyType::Int64()) {
    in_archive << value.type << value.value.l;
  } else if (value.type == PropertyType::UInt64()) {
    in_archive << value.type << value.value.ul;
  } else if (value.type == PropertyType::Float()) {
    in_archive << value.type << value.value.f;
  } else if (value.type == PropertyType::Double()) {
    in_archive << value.type << value.value.db;
  } else if (value.type == impl::PropertyTypeImpl::kString) {
    // serialize as string_view
    auto s = *value.value.s_ptr;
    auto type = PropertyType::StringView();
    in_archive << type << s;
  } else if (value.type == PropertyType::StringView()) {
    in_archive << value.type << value.value.s;
  } else if (value.type == PropertyType::VertexGlobalId()) {
    in_archive << value.type << value.value.vertex_gid;
  } else if (value.type == PropertyType::Label()) {
    in_archive << value.type << value.value.label_key;
  } else if (value.type == PropertyType::RecordView()) {
    LOG(FATAL) << "Not supported";
  } else if (value.type == PropertyType::Record()) {
    in_archive << value.type << value.value.record.len;
    for (size_t i = 0; i < value.value.record.len; ++i) {
      in_archive << value.value.record.props[i];
    }
  } else if (value.type == PropertyType::Date()) {
    in_archive << value.type << value.value.d.to_u32();
  } else if (value.type == PropertyType::DateTime()) {
    in_archive << value.type << value.value.dt.milli_second;
  } else if (value.type == PropertyType::Interval()) {
    in_archive << value.type << value.value.interval.to_mill_seconds();
  } else {
    LOG(FATAL) << "Not supported";
  }

  return in_archive;
}

grape::OutArchive& operator>>(grape::OutArchive& out_archive, Any& value) {
  out_archive >> value.type;
  if (value.type == PropertyType::Empty()) {
  } else if (value.type == PropertyType::Bool()) {
    out_archive >> value.value.b;
  } else if (value.type == PropertyType::UInt8()) {
    out_archive >> value.value.u8;
  } else if (value.type == PropertyType::UInt16()) {
    out_archive >> value.value.u16;
  } else if (value.type == PropertyType::Int32()) {
    out_archive >> value.value.i;
  } else if (value.type == PropertyType::UInt32()) {
    out_archive >> value.value.ui;
  } else if (value.type == PropertyType::Float()) {
    out_archive >> value.value.f;
  } else if (value.type == PropertyType::Int64()) {
    out_archive >> value.value.l;
  } else if (value.type == PropertyType::UInt64()) {
    out_archive >> value.value.ul;
  } else if (value.type == PropertyType::Double()) {
    out_archive >> value.value.db;
  } else if (value.type.type_enum == impl::PropertyTypeImpl::kString) {
    LOG(FATAL) << "Not supported";
  } else if (value.type == PropertyType::StringView()) {
    out_archive >> value.value.s;
  } else if (value.type == PropertyType::VertexGlobalId()) {
    out_archive >> value.value.vertex_gid;
  } else if (value.type == PropertyType::Label()) {
    out_archive >> value.value.label_key;
  } else if (value.type == PropertyType::RecordView()) {
    LOG(FATAL) << "Not supported";
  } else if (value.type == PropertyType::Record()) {
    size_t len;
    out_archive >> len;
    Record r;
    r.props = new Any[len];
    for (size_t i = 0; i < len; ++i) {
      out_archive >> r.props[i];
    }
    value.set_record(r);
  } else if (value.type == PropertyType::Date()) {
    uint32_t date_val;
    out_archive >> date_val;
    value.value.d.from_u32(date_val);
  } else if (value.type == PropertyType::DateTime()) {
    int64_t date_time_val;
    out_archive >> date_time_val;
    value.value.dt.milli_second = date_time_val;
  } else if (value.type == PropertyType::Interval()) {
    uint64_t interval_val;
    out_archive >> interval_val;
    value.value.interval.from_mill_seconds(interval_val);
  } else {
    LOG(FATAL) << "Not supported";
  }

  return out_archive;
}

grape::InArchive& operator<<(grape::InArchive& in_archive,
                             const std::string_view& str) {
  in_archive << str.length();
  in_archive.AddBytes(str.data(), str.length());
  return in_archive;
}

grape::OutArchive& operator>>(grape::OutArchive& out_archive,
                              std::string_view& str) {
  size_t size;
  out_archive >> size;
  str = std::string_view(reinterpret_cast<char*>(out_archive.GetBytes(size)),
                         size);
  return out_archive;
}

grape::InArchive& operator<<(grape::InArchive& in_archive,
                             const GlobalId& value) {
  in_archive << value.global_id;
  return in_archive;
}
grape::OutArchive& operator>>(grape::OutArchive& out_archive, GlobalId& value) {
  out_archive >> value.global_id;
  return out_archive;
}

grape::InArchive& operator<<(grape::InArchive& in_archive,
                             const LabelKey& value) {
  in_archive << value.label_id;
  return in_archive;
}
grape::OutArchive& operator>>(grape::OutArchive& out_archive, LabelKey& value) {
  out_archive >> value.label_id;
  return out_archive;
}

grape::InArchive& operator<<(grape::InArchive& in_archive,
                             const Interval& value) {
  in_archive << value.to_mill_seconds();
  return in_archive;
}

grape::OutArchive& operator>>(grape::OutArchive& out_archive, Interval& value) {
  uint64_t interval_val;
  out_archive >> interval_val;
  value.from_mill_seconds(interval_val);
  return out_archive;
}

GlobalId::label_id_t GlobalId::get_label_id(gid_t gid) {
  return static_cast<label_id_t>(gid >> label_id_offset);
}

GlobalId::vid_t GlobalId::get_vid(gid_t gid) {
  return static_cast<vid_t>(gid & vid_mask);
}

GlobalId::GlobalId() : global_id(0) {}

GlobalId::GlobalId(label_id_t label_id, vid_t vid) {
  global_id = (static_cast<uint64_t>(label_id) << label_id_offset) | vid;
}

GlobalId::GlobalId(gid_t gid) : global_id(gid) {}

GlobalId::label_id_t GlobalId::label_id() const {
  return static_cast<label_id_t>(global_id >> label_id_offset);
}

GlobalId::vid_t GlobalId::vid() const {
  return static_cast<vid_t>(global_id & vid_mask);
}

std::string GlobalId::to_string() const { return std::to_string(global_id); }

bool IntervalValue::operator==(const IntervalValue& rhs) const {
  return year == rhs.year && month == rhs.month && day == rhs.day &&
         hour == rhs.hour && minute == rhs.minute && second == rhs.second &&
         millisecond == rhs.millisecond && microsecond == rhs.microsecond;
}

bool IntervalValue::operator<(const IntervalValue& rhs) const {
  if (year != rhs.year)
    return year < rhs.year;
  if (month != rhs.month)
    return month < rhs.month;
  if (day != rhs.day)
    return day < rhs.day;
  if (hour != rhs.hour)
    return hour < rhs.hour;
  if (minute != rhs.minute)
    return minute < rhs.minute;
  if (second != rhs.second)
    return second < rhs.second;
  if (millisecond != rhs.millisecond)
    return millisecond < rhs.millisecond;
  return microsecond < rhs.microsecond;
}

Date::Date(int64_t x) { from_timestamp(x); }

Date::Date(const std::string& date_str) {
  // Parse date string in format YYYY-MM-DD
  std::istringstream ss(date_str);
  // Extract year, month, and day
  int year, month, day;
  char dash1, dash2;
  ss >> year >> dash1 >> month >> dash2 >> day;
  if (ss.fail() || dash1 != '-' || dash2 != '-' || month < 1 || month > 12 ||
      day < 1 || day > 31) {
    throw std::invalid_argument("Invalid date string format");
  }
  value.internal.year = year;
  value.internal.month = month;
  value.internal.day = day;
  value.internal.hour = 0;  // Default hour to 0
  LOG(INFO) << "Set date from string: " << date_str
            << ", year: " << value.internal.year
            << ", month: " << value.internal.month
            << ", day: " << value.internal.day
            << ", hour: " << value.internal.hour;
}

int64_t Date::to_timestamp() const {
  const boost::posix_time::ptime epoch(boost::gregorian::date(1970, 1, 1));

  boost::gregorian::date new_date(year(), month(), day());
  boost::posix_time::ptime new_time_point(
      new_date, boost::posix_time::time_duration(hour(), 0, 0));
  boost::posix_time::time_duration diff = new_time_point - epoch;
  uint64_t new_timestamp_sec = diff.total_seconds();

  return new_timestamp_sec * 1000;
}

void Date::from_timestamp(int64_t ts) {
  const boost::posix_time::ptime epoch(boost::gregorian::date(1970, 1, 1));
  int64_t ts_sec = ts / 1000;
  boost::posix_time::ptime time_point =
      epoch + boost::posix_time::seconds(ts_sec);
  boost::posix_time::ptime::date_type date = time_point.date();
  boost::posix_time::time_duration td = time_point.time_of_day();
  this->value.internal.year = date.year();
  this->value.internal.month = date.month().as_number();
  this->value.internal.day = date.day();
  this->value.internal.hour = td.hours();
  // We could not assert here because we may lose precision when converting
  //  from timestamp to date.
  //  assert(ts == to_timestamp());
}

bool Date::operator<(const Date& rhs) const {
  return this->to_u32() < rhs.to_u32();
}
bool Date::operator==(const Date& rhs) const {
  return this->to_u32() == rhs.to_u32();
}

Date& Date::operator+(const Interval& interval) {
  auto ts = this->to_timestamp();
  ts += interval.to_mill_seconds();
  this->from_timestamp(ts);
  return *this;
}

Date& Date::operator+=(const Interval& interval) {
  return this->operator+(interval);
}

Date& Date::operator-(const Interval& interval) {
  auto ts = this->to_timestamp();
  LOG(INFO) << "old timestamp: " << ts
            << ", interval: " << interval.to_mill_seconds();
  ts -= interval.to_mill_seconds();
  this->from_timestamp(ts);
  return *this;
}

Date& Date::operator-=(const Interval& interval) {
  return this->operator-(interval);
}

std::string Date::to_string() const {
  // Expect string like "YYYY-MM-DD"
  std::ostringstream oss;
  oss << year() << "-" << std::setw(2) << std::setfill('0') << month() << "-"
      << std::setw(2) << std::setfill('0') << day();
  return oss.str();
}

uint32_t Date::to_u32() const { return value.integer; }

uint32_t Date::to_num_days() const {
  // Convert to a number of days since epoch (1970-01-01)
  int64_t epoch_millis = to_timestamp();
  int64_t days_since_epoch = epoch_millis / (24 * 60 * 60 * 1000);
  return static_cast<uint32_t>(days_since_epoch);
}

void Date::from_num_days(int32_t num_days) {
  // Convert from number of days since epoch (1970-01-01)
  int64_t epoch_millis = static_cast<int64_t>(num_days) * 24 * 60 * 60 * 1000;
  from_timestamp(epoch_millis);
}

void Date::from_u32(uint32_t val) { value.integer = val; }

int Date::year() const { return value.internal.year; }

int Date::month() const { return value.internal.month; }

int Date::day() const { return value.internal.day; }

int Date::hour() const { return value.internal.hour; }

Interval Date::operator-(const Date& rhs) const {
  auto lhs_ts = this->to_timestamp();
  auto rhs_ts = rhs.to_timestamp();
  Interval interval;
  interval.from_mill_seconds(lhs_ts - rhs_ts);
  return interval;
}

Interval Date::operator-=(const Date& interval) const {
  return *this - interval;
}

DateTime::DateTime(const std::string& date_time_str) {
  // For input string like "YYYY-MM-DD HH:MM:SS.zzz". the .zzz part is
  // optional.
  boost::posix_time::ptime pt;
  std::istringstream ss(date_time_str);
  try {
    ss >> pt;
    if (ss.fail()) {
      throw std::invalid_argument("Invalid date time string format");
    }
    if (pt.time_of_day().total_milliseconds() < 0 || pt.date().year() < 1970) {
      throw std::invalid_argument("Date time string is before epoch");
    }
    // Convert to milliseconds since epoch
    const boost::posix_time::ptime epoch(boost::gregorian::date(1970, 1, 1));
    boost::posix_time::time_duration diff = pt - epoch;
    milli_second = diff.total_milliseconds();
    LOG(INFO) << "Set DateTime from string: " << date_time_str
              << ", milliseconds since epoch: " << milli_second;
  } catch (const std::exception& e) {
    LOG(ERROR) << "Failed to parse DateTime from string: " << date_time_str
               << ", error: " << e.what();
    throw std::invalid_argument("Invalid date time string format");
  }
}

std::string DateTime::to_string() const {
  // Convert to a string representation, YYYY-MM-DD HH:MM:SS.zzz
  boost::posix_time::ptime epoch(boost::gregorian::date(1970, 1, 1));
  boost::posix_time::ptime time_point =
      epoch + boost::posix_time::milliseconds(milli_second);
  boost::posix_time::time_duration td = time_point.time_of_day();
  boost::gregorian::date date = time_point.date();
  std::ostringstream oss;
  oss << date.year() << "-" << std::setw(2) << std::setfill('0')
      << date.month().as_number() << "-" << std::setw(2) << std::setfill('0')
      << date.day() << " " << std::setw(2) << std::setfill('0') << td.hours()
      << ":" << std::setw(2) << std::setfill('0') << td.minutes() << ":"
      << std::setw(2) << std::setfill('0') << td.seconds() << "."
      << std::setw(3) << std::setfill('0') << milli_second % 1000;
  return oss.str();
}

// Interval

std::string Interval::to_string() const {
  // Convert to a string representation, YYYY-MM-DD HH:MM:SS.zzz
  std::ostringstream oss;

  auto lambda_func = [&](int64_t value, const std::string& unit,
                         const std::string& plural_unit) {
    if (value > 0) {
      if (!oss.str().empty()) {
        oss << " ";
      }
      oss << value << " " << (value == 1 ? unit : plural_unit);
    }
  };
  lambda_func(year(), "year", "years");
  lambda_func(month(), "month", "months");
  lambda_func(day(), "day", "days");
  lambda_func(hour(), "hour", "hours");
  lambda_func(minute(), "minute", "minutes");
  lambda_func(second(), "second", "seconds");
  lambda_func(millisecond(), "millisecond", "milliseconds");
  if (negative) {
    // prepend
    auto neg_str = oss.str();
    neg_str.insert(0, "-");
    return neg_str;
  }
  return oss.str();
}

void Interval::from_mill_seconds(int64_t mill_seconds) {
  if (mill_seconds < 0) {
    negative = true;
    mill_seconds = -mill_seconds;
  } else {
    negative = false;
  }
  int64_t total_seconds = mill_seconds / 1000;
  // internal.year = std::floor(total_seconds / SECOND_PER_YEAR);
  // Get the floor division for years, months, days, hours, minutes, and seconds
  internal.year = std::floor((double) total_seconds / SECOND_PER_YEAR);
  total_seconds -= internal.year * SECOND_PER_YEAR;
  internal.month = std::floor((double) total_seconds / SECOND_PER_MONTH);
  total_seconds -= internal.month * SECOND_PER_MONTH;
  internal.day = std::floor((double) total_seconds / SECOND_PER_DAY);
  total_seconds -= internal.day * SECOND_PER_DAY;
  internal.hour = std::floor((double) total_seconds / SECOND_PER_HOUR);
  total_seconds -= internal.hour * SECOND_PER_HOUR;
  internal.minute = std::floor((double) total_seconds / SECOND_PER_MINUTE);
  total_seconds -= internal.minute * SECOND_PER_MINUTE;
  internal.second = total_seconds;
  internal.millisecond = mill_seconds % 1000;
  internal.microsecond = 0;  // Not used in this implementation

  assert(negative ? (-mill_seconds == to_mill_seconds())
                  : (mill_seconds == to_mill_seconds()));
}

int64_t Interval::to_mill_seconds() const {
  int64_t total_seconds = internal.second;
  total_seconds += internal.minute * SECOND_PER_MINUTE;
  total_seconds += internal.hour * SECOND_PER_HOUR;
  total_seconds += internal.day * SECOND_PER_DAY;
  total_seconds += internal.month * SECOND_PER_MONTH;
  total_seconds += internal.year * SECOND_PER_YEAR;
  int64_t total_milliseconds =
      total_seconds * 1000 + static_cast<int64_t>(internal.millisecond);
  return (negative ? -1 : 1) * total_milliseconds;
}

Interval& Interval::operator+(const Interval& rhs) {
  Interval result;
  if (!negative && !rhs.negative) {
    result.negative = false;
  } else if (negative && rhs.negative) {
    result.negative = true;
  } else if (negative && !rhs.negative) {
    result.negative = internal > rhs.internal;
  } else {
    result.negative = rhs.internal > internal;
  }
  result.internal.year = this->internal.year + rhs.internal.year;
  result.internal.month = this->internal.month + rhs.internal.month;
  result.internal.day = this->internal.day + rhs.internal.day;
  result.internal.hour = this->internal.hour + rhs.internal.hour;
  result.internal.minute = this->internal.minute + rhs.internal.minute;
  result.internal.second = this->internal.second + rhs.internal.second;
  result.internal.millisecond =
      this->internal.millisecond + rhs.internal.millisecond;
  result.internal.microsecond =
      this->internal.microsecond + rhs.internal.microsecond;

  normalize(result);
  adjustMonthYearOverflow(result);
  return *this = result;
}

Interval& Interval::operator+=(const Interval& rhs) { return *this + rhs; }

Interval& Interval::operator-(const Interval& rhs) {
  Interval result;
  if (!negative && !rhs.negative) {
    result.negative = internal < rhs.internal;
  } else if (negative && rhs.negative) {
    result.negative = internal > rhs.internal;
  } else if (negative && !rhs.negative) {
    result.negative = true;
  } else {
    result.negative = false;
  }

  result.internal.year = internal.year - rhs.internal.year;
  result.internal.month = internal.month - rhs.internal.month;
  result.internal.day = internal.day - rhs.internal.day;
  result.internal.hour = internal.hour - rhs.internal.hour;
  result.internal.minute = internal.minute - rhs.internal.minute;
  result.internal.second = internal.second - rhs.internal.second;
  result.internal.millisecond = internal.millisecond - rhs.internal.millisecond;

  normalize(result);
  adjustNegative(result);
  return *this = result;
}

Interval& Interval::operator-=(const Interval& rhs) { return *this - rhs; }

void Interval::normalize(Interval& interval) {
  if (interval.internal.millisecond >= 1000) {
    interval.internal.second += interval.internal.millisecond / 1000;
    interval.internal.millisecond %= 1000;
  }
  if (interval.internal.second >= 60) {
    interval.internal.minute += interval.internal.second / 60;
    interval.internal.second %= 60;
  }
  if (interval.internal.minute >= 60) {
    interval.internal.hour += interval.internal.minute / 60;
    interval.internal.minute %= 60;
  }
  if (interval.internal.hour >= 24) {
    interval.internal.day += interval.internal.hour / 24;
    interval.internal.hour %= 24;
  }
  // Handle month overflow considering days in the month
  adjustMonthYearOverflow(interval);
}

void Interval::adjustNegative(Interval& interval) {
  if (interval.internal.millisecond < 0) {
    interval.internal.second -= (interval.internal.millisecond / 1000) - 1;
    interval.internal.millisecond =
        1000 + (interval.internal.millisecond % 1000);
  }
  if (interval.internal.second < 0) {
    interval.internal.minute -= (interval.internal.second / 60) - 1;
    interval.internal.second = 60 + (interval.internal.second % 60);
  }
  if (interval.internal.minute < 0) {
    interval.internal.hour -= (interval.internal.minute / 60) - 1;
    interval.internal.minute = 60 + (interval.internal.minute % 60);
  }
  if (interval.internal.hour < 0) {
    interval.internal.day -= (interval.internal.hour / 24) - 1;
    interval.internal.hour = 24 + (interval.internal.hour % 24);
  }
  // Handle month underflow considering days in the month
  adjustMonthYearUnderflow(interval);
}

void Interval::adjustMonthYearOverflow(Interval& interval) {
  while (interval.internal.month >= 12) {
    interval.internal.year += interval.internal.month / 12;
    interval.internal.month = interval.internal.month % 12;
  }
  while (interval.internal.day >= 31) {
    interval.internal.month += interval.internal.day / 31;
    interval.internal.day = interval.internal.day % 31;
  }
}

void Interval::adjustMonthYearUnderflow(Interval& interval) {
  while (interval.internal.month < 0) {
    interval.internal.year += (interval.internal.month - 11) / 12;
    interval.internal.month = 12 + (interval.internal.month % 12);
  }
  // Consider day underflow (simplified logic, needs month-length handling)
  while (interval.internal.day < 0) {
    interval.internal.month += (interval.internal.day - 1) / 31;
    interval.internal.day = 31 + (interval.internal.day % 31);
  }
}

std::string TimeStamp::to_string() const {
  return std::to_string(milli_second);
}

Date operator+(const Date& date, const Interval& interval) {
  Date new_date = date;
  new_date += interval;
  return new_date;
}

Date operator-(const Date& date, const Interval& interval) {
  Date new_date = date;
  new_date -= interval;
  return new_date;
}

DateTime operator+(const DateTime& dt, const Interval& interval) {
  DateTime new_dt = dt;
  new_dt += interval;
  return new_dt;
}

DateTime operator-(const DateTime& dt, const Interval& interval) {
  DateTime new_dt = dt;
  new_dt -= interval;
  return new_dt;
}

TimeStamp operator+(const TimeStamp& ts, const Interval& interval) {
  TimeStamp new_ts = ts;
  new_ts += interval;
  return new_ts;
}

TimeStamp operator-(const TimeStamp& ts, const Interval& interval) {
  TimeStamp new_ts = ts;
  new_ts -= interval;
  return new_ts;
}

Interval operator+(const Interval& lhs, const Interval& rhs) {
  Interval result = lhs;
  result += rhs;
  return result;
}

Interval operator-(const Interval& lhs, const Interval& rhs) {
  Interval result = lhs;
  result -= rhs;
  return result;
}

std::string Any::to_string() const {
  if (type == PropertyType::kInt32) {
    return std::to_string(value.i);
  } else if (type == PropertyType::kInt64) {
    return std::to_string(value.l);
  } else if (type.type_enum == impl::PropertyTypeImpl::kString) {
    return *value.s_ptr.ptr;
  } else if (type == PropertyType::kStringView) {
    return std::string(value.s.data(), value.s.size());
    //      return value.s.to_string();
  } else if (type == PropertyType::kEmpty) {
    return "NULL";
  } else if (type == PropertyType::kDouble) {
    return std::to_string(value.db);
  } else if (type == PropertyType::kUInt8) {
    return std::to_string(value.u8);
  } else if (type == PropertyType::kUInt16) {
    return std::to_string(value.u16);
  } else if (type == PropertyType::kUInt32) {
    return std::to_string(value.ui);
  } else if (type == PropertyType::kUInt64) {
    return std::to_string(value.ul);
  } else if (type == PropertyType::kBool) {
    return value.b ? "true" : "false";
  } else if (type == PropertyType::kFloat) {
    return std::to_string(value.f);
  } else if (type == PropertyType::kVertexGlobalId) {
    return value.vertex_gid.to_string();
  } else if (type == PropertyType::kLabel) {
    return std::to_string(value.label_key.label_id);
  } else if (type == PropertyType::kDate) {
    return value.d.to_string();
  } else if (type == PropertyType::kDateTime) {
    return value.dt.to_string();
  } else if (type == PropertyType::kInterval) {
    return value.interval.to_string();
  } else if (type == PropertyType::kTimestamp) {
    return value.ts.to_string();
  } else {
    LOG(FATAL) << "Unexpected property type: "
               << static_cast<int>(type.type_enum);
    return "";
  }
}

bool Any::operator==(const Any& other) const {
  if (type == other.type) {
    if (type == PropertyType::kInt32) {
      return value.i == other.value.i;
    } else if (type == PropertyType::kInt64) {
      return value.l == other.value.l;
    } else if (type.type_enum == impl::PropertyTypeImpl::kString) {
      return *value.s_ptr == other.AsStringView();
    } else if (type == PropertyType::kStringView) {
      return value.s == other.AsStringView();
    } else if (type == PropertyType::kEmpty) {
      return true;
    } else if (type == PropertyType::kDouble) {
      return value.db == other.value.db;
    } else if (type == PropertyType::kUInt32) {
      return value.ui == other.value.ui;
    } else if (type == PropertyType::kUInt64) {
      return value.ul == other.value.ul;
    } else if (type == PropertyType::kBool) {
      return value.b == other.value.b;
    } else if (type == PropertyType::kFloat) {
      return value.f == other.value.f;
    } else if (type == PropertyType::kVertexGlobalId) {
      return value.vertex_gid == other.value.vertex_gid;
    } else if (type == PropertyType::kLabel) {
      return value.label_key.label_id == other.value.label_key.label_id;
    } else if (type.type_enum == impl::PropertyTypeImpl::kVarChar) {
      if (other.type.type_enum != impl::PropertyTypeImpl::kVarChar) {
        return false;
      }
      return value.s == other.value.s;
    } else if (type == PropertyType::kDate) {
      return value.d.to_u32() == other.value.d.to_u32();
    } else if (type == PropertyType::kDateTime) {
      return value.dt.milli_second == other.value.dt.milli_second;
    } else if (type == PropertyType::kTimestamp) {
      return value.ts.milli_second == other.value.ts.milli_second;
    } else if (type == PropertyType::kInterval) {
      return value.interval.internal == other.value.interval.internal;
    } else {
      return false;
    }
  } else if (type == PropertyType::kRecordView) {
    return value.record_view.offset == other.value.record_view.offset &&
           value.record_view.table == other.value.record_view.table;
  } else if (type == PropertyType::kRecord) {
    if (value.record.len != other.value.record.len) {
      return false;
    }
    for (size_t i = 0; i < value.record.len; ++i) {
      if (!(value.record.props[i] == other.value.record.props[i])) {
        return false;
      }
    }
    return true;
  } else {
    return false;
  }
}

bool Any::operator<(const Any& other) const {
  if (type == other.type) {
    if (type == PropertyType::kInt32) {
      return value.i < other.value.i;
    } else if (type == PropertyType::kInt64) {
      return value.l < other.value.l;
    } else if (type.type_enum == impl::PropertyTypeImpl::kString) {
      return *value.s_ptr < other.AsStringView();
    } else if (type == PropertyType::kStringView) {
      return value.s < other.AsStringView();
    } else if (type == PropertyType::kEmpty) {
      return false;
    } else if (type == PropertyType::kDouble) {
      return value.db < other.value.db;
    } else if (type == PropertyType::kUInt32) {
      return value.ui < other.value.ui;
    } else if (type == PropertyType::kUInt64) {
      return value.ul < other.value.ul;
    } else if (type == PropertyType::kBool) {
      return value.b < other.value.b;
    } else if (type == PropertyType::kFloat) {
      return value.f < other.value.f;
    } else if (type == PropertyType::kVertexGlobalId) {
      return value.vertex_gid < other.value.vertex_gid;
    } else if (type == PropertyType::kLabel) {
      return value.label_key.label_id < other.value.label_key.label_id;
    } else if (type == PropertyType::kRecord) {
      for (size_t i = 0; i < value.record.len; ++i) {
        if (i >= other.value.record.len) {
          return false;
        }
        if (value.record.props[i] < other.value.record.props[i]) {
          return true;
        } else if (other.value.record.props[i] < value.record.props[i]) {
          return false;
        }
      }
      return false;
    } else if (type == PropertyType::kDate) {
      return value.d.to_u32() < other.value.d.to_u32();
    } else if (type == PropertyType::kDateTime) {
      return value.dt.milli_second < other.value.dt.milli_second;
    } else if (type == PropertyType::kTimestamp) {
      return value.ts.milli_second < other.value.ts.milli_second;
    } else if (type == PropertyType::kInterval) {
      return value.interval < other.value.interval;
    } else {
      return false;
    }
  } else {
    LOG(FATAL) << "Type [" << static_cast<int>(type.type_enum) << "] and ["
               << static_cast<int>(other.type.type_enum)
               << "] cannot be compared..";
  }
}

Any ConvertStringToAny(const std::string& value, const gs::PropertyType& type) {
  if (type == gs::PropertyType::Int32()) {
    return gs::Any(static_cast<int32_t>(std::stoi(value)));
  } else if (type == gs::PropertyType::String() ||
             type == gs::PropertyType::StringMap()) {
    return gs::Any(std::string(value));
  } else if (type == gs::PropertyType::Int64()) {
    return gs::Any(static_cast<int64_t>(std::stoll(value)));
  } else if (type == gs::PropertyType::Double()) {
    return gs::Any(std::stod(value));
  } else if (type == gs::PropertyType::UInt32()) {
    return gs::Any(static_cast<uint32_t>(std::stoul(value)));
  } else if (type == gs::PropertyType::UInt64()) {
    return gs::Any(static_cast<uint64_t>(std::stoull(value)));
  } else if (type == gs::PropertyType::Bool()) {
    auto lower = value;
    std::transform(lower.begin(), lower.end(), lower.begin(),
                   [](unsigned char c) { return std::tolower(c); });
    if (lower == "true") {
      return gs::Any(true);
    } else if (lower == "false") {
      return gs::Any(false);
    } else {
      LOG(FATAL) << "Invalid bool value: " << value;
    }
  } else if (type == gs::PropertyType::Float()) {
    return gs::Any(std::stof(value));
  } else if (type == gs::PropertyType::UInt8()) {
    return gs::Any(static_cast<uint8_t>(std::stoul(value)));
  } else if (type == gs::PropertyType::UInt16()) {
    return gs::Any(static_cast<uint16_t>(std::stoul(value)));
  } else if (type == gs::PropertyType::VertexGlobalId()) {
    return gs::Any(gs::GlobalId(static_cast<uint64_t>(std::stoull(value))));
  } else if (type == gs::PropertyType::Label()) {
    return gs::Any(gs::LabelKey(static_cast<uint8_t>(std::stoul(value))));
  } else if (type == gs::PropertyType::Empty()) {
    return gs::Any();
  } else if (type == gs::PropertyType::StringView()) {
    return gs::Any(std::string_view(value));
  } else if (type == gs::PropertyType::Date()) {
    return gs::Any(gs::Date(static_cast<int64_t>(std::stoll(value))));
  } else {
    LOG(ERROR) << "Unsupported type: " << type.ToString();
    return gs::Any();
  }
}

Any AnyConverter<Interval>::to_any(std::string_view value) {
  static const std::regex interval_regex(
      R"((\d+)\s*(years?|days?|hours?|minutes?|seconds?|milliseconds?|us?))");
  std::smatch match;
  Interval interval;
  std::string value_str(value);
  int32_t years, months, days, hours, minutes, seconds, milliseconds,
      microseconds;
  years = months = days = hours = minutes = seconds = milliseconds =
      microseconds = 0;
  while (std::regex_search(value_str, match, interval_regex)) {
    int64_t num = std::stoll(match[1].str());
    std::string unit = match[2].str();
    if (unit == "year" || unit == "years") {
      years = num;
    } else if (unit == "month" || unit == "months") {
      months = num;
    } else if (unit == "day" || unit == "days") {
      days = num;
    } else if (unit == "hour" || unit == "hours") {
      hours = num;
    } else if (unit == "minute" || unit == "minutes") {
      minutes = num;
    } else if (unit == "second" || unit == "seconds") {
      seconds = num;
    } else if (unit == "millisecond" || unit == "milliseconds") {
      milliseconds = num;
    } else if (unit == "us" || unit == "microsecond" ||
               unit == "microseconds") {
      microseconds = num;
    } else {
      throw std::invalid_argument("Invalid interval unit: " + unit);
    }
    LOG(INFO) << "Parsed interval part: " << num << " " << unit;
    value_str = match.suffix().str();
    // trim leading and trailing spaces
    value_str.erase(0, value_str.find_first_not_of(' '));
    value_str.erase(value_str.find_last_not_of(' ') + 1);
  }
  if (!value_str.empty()) {
    throw std::invalid_argument("Invalid interval format: " + value_str +
                                ",size: " + std::to_string(value_str.size()));
  }
  Any ret;
  interval.negative = false;
  interval.internal.year = years;
  interval.internal.month = months;
  interval.internal.day = days;
  interval.internal.hour = hours;
  interval.internal.minute = minutes;
  interval.internal.second = seconds;
  interval.internal.millisecond = milliseconds;
  interval.internal.microsecond = microseconds;
  ret.set_interval(interval);
  return ret;
}

}  // namespace gs
