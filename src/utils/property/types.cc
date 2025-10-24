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

#include "neug/utils/property/types.h"

#include <algorithm>
#include <cctype>
#include <chrono>
#include <cmath>
#include <compare>
#include <cstdlib>
#include <exception>
#include <iomanip>
#include <istream>
#include <memory>
#include <ostream>
#include <ratio>
#include <regex>

#include "date/date.h"
#include "libgrape-lite/grape/serialization/in_archive.h"
#include "libgrape-lite/grape/serialization/out_archive.h"
#include "neug/utils/property/column.h"
#include "neug/utils/property/table.h"

namespace gs {

namespace config_parsing {

std::string PrimitivePropertyTypeToString(PropertyType type) {
  if (type == PropertyType::kEmpty) {
    return "Empty";
  } else if (type == PropertyType::kBool) {
    return DT_BOOL;
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
  } else if (type.type_enum == impl::PropertyTypeImpl::kStringView) {
    return DT_STRING;
  } else if (type == PropertyType::kDate) {
    return DT_DATE;
  } else if (type == PropertyType::kDateTime) {
    return DT_DATETIME;
  } else if (type == PropertyType::kInterval) {
    return DT_INTERVAL;
  } else if (type == PropertyType::kTimestamp) {
    return DT_TIMESTAMP;
  } else {
    THROW_INVALID_ARGUMENT_EXCEPTION("Unknown property type: " +
                                     type.ToString());
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
    THROW_INVALID_ARGUMENT_EXCEPTION(
        "Unsupported temporal type for YAML encoding: " + type.ToString());
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

const PropertyType PropertyType::kEmpty =
    PropertyType(impl::PropertyTypeImpl::kEmpty);
const PropertyType PropertyType::kBool =
    PropertyType(impl::PropertyTypeImpl::kBool);
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
const PropertyType PropertyType::kString =
    PropertyType(impl::PropertyTypeImpl::kString);
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

PropertyType PropertyType::Varchar(uint16_t max_length) {
  return PropertyType(impl::PropertyTypeImpl::kVarChar, max_length);
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
    THROW_INVALID_ARGUMENT_EXCEPTION("Invalid date string format");
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
  date::year_month_day ymd = date::year_month_day(
      date::year(year()), date::month(month()), date::day(day()));
  date::sys_days time_point_days = date::sys_days(ymd);
  std::chrono::system_clock::time_point date_time =
      time_point_days + std::chrono::hours(hour());
  // Calculate the difference in seconds from epoch
  auto timestamp = std::chrono::duration_cast<std::chrono::seconds>(
                       date_time.time_since_epoch())
                       .count();

  // Convert seconds to milliseconds
  int64_t timestamp_millis = timestamp * 1000;
  return timestamp_millis;
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
  std::istringstream ss(date_time_str);
  date::sys_time<std::chrono::milliseconds> sys_time;
  // Try multiple formats
  try {
    date::from_stream(ss, "%Y-%m-%d %H:%M:%S.%f", sys_time);
    if (ss.fail()) {
      // If it fails, try with milliseconds
      ss.clear();
      ss.str(date_time_str);  // Reset the stream
      date::from_stream(ss, "%Y-%m-%d %H:%M:%S", sys_time);
      if (ss.fail()) {
        THROW_INVALID_ARGUMENT_EXCEPTION("Invalid date time string format");
      }
    }
  } catch (const std::exception& e) {
    THROW_INVALID_ARGUMENT_EXCEPTION("Invalid date time string format: " +
                                     std::string(e.what()));
  }

  milli_second = std::chrono::duration_cast<std::chrono::milliseconds>(
                     sys_time.time_since_epoch())
                     .count();
}

std::string DateTime::to_string() const {
  // Convert to a string representation, YYYY-MM-DD HH:MM:SS.zzz, using date.h
  std::ostringstream oss;
  auto time_point = std::chrono::system_clock::from_time_t(
      milli_second / 1000);  // Convert milliseconds to seconds
  auto ymd = std::chrono::floor<std::chrono::days>(time_point);
  auto time_of_day = time_point - std::chrono::system_clock::time_point(
                                      ymd);  // Get the time of day part
  auto hours = std::chrono::duration_cast<std::chrono::hours>(time_of_day);
  auto minutes =
      std::chrono::duration_cast<std::chrono::minutes>(time_of_day - hours);
  auto seconds = std::chrono::duration_cast<std::chrono::seconds>(
      time_of_day - hours - minutes);
  auto milliseconds = std::chrono::milliseconds(milli_second % 1000);
  oss << int(date::year_month_day(ymd).year()) << "-" << std::setw(2)
      << std::setfill('0')
      << static_cast<unsigned>(date::year_month_day(ymd).month()) << "-"
      << std::setw(2) << std::setfill('0')
      << static_cast<unsigned>(date::year_month_day(ymd).day()) << " "
      << std::setw(2) << std::setfill('0') << hours.count() << ":"
      << std::setw(2) << std::setfill('0') << minutes.count() << ":"
      << std::setw(2) << std::setfill('0') << seconds.count() << "."
      << std::setw(3) << std::setfill('0')
      << milliseconds.count() % 1000;  // Milliseconds
  return oss.str();
}

// Interval
Interval::Interval(std::string str) {
  for (size_t i = 0; i < str.length(); ++i) {
    str[i] = std::tolower(str[i]);
  }
  static const std::regex interval_regex(
      R"((\d+)\s*(years?|year?|days?|day?|hours?|hour?|minutes?|minute?|seconds?|second?|milliseconds?|millisecond?|us?|microsecond?|microseconds?))");
  std::smatch match;
  negative = false;
  internal.year = internal.month = internal.day = internal.hour =
      internal.minute = internal.second = internal.millisecond =
          internal.microsecond = 0;
  while (std::regex_search(str, match, interval_regex)) {
    int64_t num = std::stoll(match[1].str());
    std::string unit = match[2].str();
    if (unit == "year" || unit == "years") {
      internal.year = num;
    } else if (unit == "month" || unit == "months") {
      internal.month = num;
    } else if (unit == "day" || unit == "days") {
      internal.day = num;
    } else if (unit == "hour" || unit == "hours") {
      internal.hour = num;
    } else if (unit == "minute" || unit == "minutes") {
      internal.minute = num;
    } else if (unit == "second" || unit == "seconds") {
      internal.second = num;
    } else if (unit == "millisecond" || unit == "milliseconds") {
      internal.millisecond = num;
    } else if (unit == "us" || unit == "microsecond" ||
               unit == "microseconds") {
      internal.microsecond = num;
    } else {
      THROW_INVALID_ARGUMENT_EXCEPTION("Invalid interval unit: " + unit);
    }
    str = match.suffix().str();
    // trim leading and trailing spaces
    str.erase(0, str.find_first_not_of(' '));
    str.erase(str.find_last_not_of(' ') + 1);
  }
  if (!str.empty()) {
    THROW_INVALID_ARGUMENT_EXCEPTION("Invalid interval format: " + str +
                                     ",size: " + std::to_string(str.size()));
  }
}

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
  internal.year =
      std::floor(static_cast<double>(total_seconds) / SECOND_PER_YEAR);
  total_seconds -= internal.year * SECOND_PER_YEAR;
  internal.month =
      std::floor(static_cast<double>(total_seconds) / SECOND_PER_MONTH);
  total_seconds -= internal.month * SECOND_PER_MONTH;
  internal.day =
      std::floor(static_cast<double>(total_seconds) / SECOND_PER_DAY);
  total_seconds -= internal.day * SECOND_PER_DAY;
  internal.hour =
      std::floor(static_cast<double>(total_seconds) / SECOND_PER_HOUR);
  total_seconds -= internal.hour * SECOND_PER_HOUR;
  internal.minute =
      std::floor(static_cast<double>(total_seconds) / SECOND_PER_MINUTE);
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

}  // namespace gs
