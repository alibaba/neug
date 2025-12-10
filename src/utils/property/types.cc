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
#include "neug/utils/property/column.h"
#include "neug/utils/property/table.h"
#include "neug/utils/serialization/in_archive.h"
#include "neug/utils/serialization/out_archive.h"

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
    return PropertyType::kDateTime;
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

InArchive& operator<<(InArchive& in_archive, const PropertyType& value) {
  in_archive << value.type_enum;
  if (value.type_enum == impl::PropertyTypeImpl::kVarChar) {
    in_archive << value.additional_type_info.max_length;
  }
  return in_archive;
}
OutArchive& operator>>(OutArchive& out_archive, PropertyType& value) {
  out_archive >> value.type_enum;
  if (value.type_enum == impl::PropertyTypeImpl::kVarChar) {
    out_archive >> value.additional_type_info.max_length;
  }
  return out_archive;
}

InArchive& operator<<(InArchive& in_archive, const GlobalId& value) {
  in_archive << value.global_id;
  return in_archive;
}
OutArchive& operator>>(OutArchive& out_archive, GlobalId& value) {
  out_archive >> value.global_id;
  return out_archive;
}

InArchive& operator<<(InArchive& in_archive, const LabelKey& value) {
  in_archive << value.label_id;
  return in_archive;
}
OutArchive& operator>>(OutArchive& out_archive, LabelKey& value) {
  out_archive >> value.label_id;
  return out_archive;
}

InArchive& operator<<(InArchive& in_archive, const Interval& value) {
  in_archive << value.months << value.days << value.micros;
  return in_archive;
}

OutArchive& operator>>(OutArchive& out_archive, Interval& value) {
  out_archive >> value.months >> value.days >> value.micros;
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

Date::Date(int64_t x) { from_timestamp(x); }

Date::Date(const std::string& date_str) {
  if (std::all_of(date_str.begin(), date_str.end(), ::isdigit)) {
    from_timestamp(std::stoll(date_str));
  } else {
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
    value.internal.hour = 0;
  }
}

bool Date::is_leap_year(int32_t year) {
  return year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
}

int32_t Date::month_day(int32_t year, int32_t month) {
  return is_leap_year(year) ? LEAP_DAYS[month] : NORMAL_DAYS[month];
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
bool Date::operator>(const Date& rhs) const {
  return this->to_u32() > rhs.to_u32();
}
bool Date::operator==(const Date& rhs) const {
  return this->to_u32() == rhs.to_u32();
}

Date Date::operator+(const Interval& interval) const {
  Date new_date;
  int32_t year, month, day;
  year = this->year();
  month = this->month();
  day = this->day();
  if (interval.months != 0) {
    int32_t year_diff = interval.months / Interval::MONTHS_PER_YEAR;
    year += year_diff;
    month += interval.months - year_diff * Interval::MONTHS_PER_YEAR;
    if (month > Interval::MONTHS_PER_YEAR) {
      year++;
      month -= Interval::MONTHS_PER_YEAR;
    } else if (month <= 0) {
      year--;
      month += Interval::MONTHS_PER_YEAR;
    }
    if (day > Date::month_day(year, month)) {
      day = Date::month_day(year, month);
    }
  }
  new_date.value.internal.year = year;
  new_date.value.internal.month = month;
  new_date.value.internal.day = day;
  new_date.value.internal.hour = 0;
  int64_t days = new_date.to_num_days();
  if (interval.days != 0) {
    days += interval.days;
  }
  if (interval.micros != 0) {
    days += int32_t(interval.micros / Interval::MICROS_PER_DAY);
  }
  if (interval.micros < 0 && interval.micros % Interval::MICROS_PER_DAY != 0) {
    days--;
  }
  if (days > std::numeric_limits<int32_t>::max()) {
    THROW_INVALID_ARGUMENT_EXCEPTION(
        "Date arithmetic resulted in an out-of-range date.");
  }
  new_date.from_num_days(days);
  return new_date;
}

Date& Date::operator+=(const Interval& interval) {
  *this = *this + interval;
  return *this;
}

Date Date::operator-(const Interval& interval) const {
  Interval invert_interval = interval;
  invert_interval.invert();
  return *this + invert_interval;
}

Date& Date::operator-=(const Interval& interval) {
  *this = *this - interval;
  return *this;
}

std::string Date::to_string() const {
  // Expect string like "YYYY-MM-DD"
  std::ostringstream oss;
  oss << year() << "-" << std::setw(2) << std::setfill('0') << month() << "-"
      << std::setw(2) << std::setfill('0') << day();
  return oss.str();
}

uint32_t Date::to_u32() const { return value.integer; }

int32_t Date::to_num_days() const {
  // Convert to a number of days since epoch (1970-01-01)
  int64_t epoch_millis = to_timestamp();
  int64_t days_since_epoch = epoch_millis / (24 * 60 * 60 * 1000);
  return static_cast<int32_t>(days_since_epoch);
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

DateTime::DateTime(const std::string& date_time_str) {
  if (std::all_of(date_time_str.begin(), date_time_str.end(), ::isdigit)) {
    milli_second = std::stoll(date_time_str);
  } else {
    std::istringstream ss(date_time_str);
    do {
      if (date_time_str.size() == 10) {
        date::sys_time<std::chrono::milliseconds> sys_time;
        // Try multiple formats
        date::from_stream(ss, "%Y-%m-%d", sys_time);
        if (!ss.fail()) {
          milli_second = std::chrono::duration_cast<std::chrono::milliseconds>(
                             sys_time.time_since_epoch())
                             .count();
          break;
        } else {
          ss.clear();
          ss.str(date_time_str);
        }
      }
      std::istringstream ss(date_time_str);
      date::sys_time<std::chrono::milliseconds> sys_time;

      date::from_stream(ss, "%Y-%m-%dT%H:%M:%S%z", sys_time);
      if (!ss.fail()) {
        milli_second = std::chrono::duration_cast<std::chrono::milliseconds>(
                           sys_time.time_since_epoch())
                           .count();
        break;
      } else {
        ss.clear();
        ss.str(date_time_str);
      }
      date::from_stream(ss, "%Y-%m-%d %H:%M:%S.%f", sys_time);
      if (!ss.fail()) {
        milli_second = std::chrono::duration_cast<std::chrono::milliseconds>(
                           sys_time.time_since_epoch())
                           .count();
        break;
      } else {
        ss.clear();
        ss.str(date_time_str);
      }
      date::from_stream(ss, "%Y-%m-%d %H:%M:%S", sys_time);
      if (!ss.fail()) {
        milli_second = std::chrono::duration_cast<std::chrono::milliseconds>(
                           sys_time.time_since_epoch())
                           .count();
        break;
      } else {
        THROW_INVALID_ARGUMENT_EXCEPTION("Invalid date time string format");
      }

    } while (0);
  }
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

DateTime& DateTime::operator+=(const Interval& interval) {
  *this = *this + interval;
  return *this;
}

DateTime DateTime::operator+(const Interval& interval) const {
  int64_t delta_milli = 0;
  if (interval.months != 0) {
    int32_t days = this->milli_second / Interval::MSEC_PER_DAY;
    Interval month_interval;
    month_interval.months = interval.months;
    month_interval.days = 0;
    month_interval.micros = 0;
    Date date;
    date.from_num_days(days);
    date = date + month_interval;
    delta_milli += (static_cast<int32_t>(date.to_num_days()) - days) *
                   Interval::MSEC_PER_DAY;
  }

  if (interval.days != 0) {
    delta_milli += interval.days * Interval::MSEC_PER_DAY;
  }

  if (interval.micros != 0) {
    delta_milli += interval.micros / Interval::MICROS_PER_MSEC;
  }
  return DateTime(this->milli_second + delta_milli);
}

DateTime& DateTime::operator-=(const Interval& interval) {
  *this = *this - interval;
  return *this;
}

DateTime DateTime::operator-(const Interval& interval) const {
  Interval invert_interval = interval;
  invert_interval.invert();
  return *this + invert_interval;
}

// Interval
void Interval::invert() {
  micros = -micros;
  days = -days;
  months = -months;
}

void Interval::normalize(int64_t& months, int64_t& days,
                         int64_t& micros) const {
  auto& input = *this;

  //  Carry left
  micros = input.micros;
  int64_t carry_days = micros / Interval::MICROS_PER_DAY;
  micros -= carry_days * Interval::MICROS_PER_DAY;

  days = input.days;
  days += carry_days;
  int64_t carry_months = days / Interval::DAYS_PER_MONTH;
  days -= carry_months * Interval::DAYS_PER_MONTH;

  months = input.months;
  months += carry_months;
}

void Interval::borrow(const int64_t msf, int64_t& lsf, int32_t& f,
                      const int64_t scale) {
  if (msf > std::numeric_limits<int32_t>::max()) {
    f = std::numeric_limits<int32_t>::max();
    lsf += (msf - f) * scale;
  } else if (msf < std::numeric_limits<int32_t>::lowest()) {
    f = std::numeric_limits<int32_t>::lowest();
    lsf += (msf - f) * scale;
  } else {
    f = static_cast<int32_t>(msf);
  }
}

Interval Interval::normalize() const {
  Interval result;

  int64_t mm;
  int64_t dd;
  normalize(mm, dd, result.micros);

  //  Borrow right on overflow
  borrow(mm, dd, result.months, Interval::DAYS_PER_MONTH);
  borrow(dd, result.micros, result.days, Interval::MICROS_PER_DAY);

  return result;
}

Interval::Interval(std::string str) {
  for (size_t i = 0; i < str.length(); ++i) {
    str[i] = std::tolower(str[i]);
  }
  static const std::regex interval_regex(
      R"((\d+)\s*(years?|year?|months?|month?|days?|day?|hours?|hour?|minutes?|minute?|seconds?|second?|milliseconds?|millisecond?|us?|microsecond?|microseconds?))");
  std::smatch match;
  months = days = micros = 0;
  while (std::regex_search(str, match, interval_regex)) {
    int64_t num = std::stoll(match[1].str());
    std::string unit = match[2].str();
    if (unit == "year" || unit == "years") {
      months += num * Interval::MONTHS_PER_YEAR;
    } else if (unit == "month" || unit == "months") {
      months += num;
    } else if (unit == "day" || unit == "days") {
      days = num;
    } else if (unit == "hour" || unit == "hours") {
      micros += num * Interval::MICROS_PER_HOUR;
    } else if (unit == "minute" || unit == "minutes") {
      micros += num * Interval::MICROS_PER_MINUTE;
    } else if (unit == "second" || unit == "seconds") {
      micros += num * Interval::MICROS_PER_SEC;
    } else if (unit == "millisecond" || unit == "milliseconds") {
      micros += num * Interval::MICROS_PER_MSEC;
    } else if (unit == "us" || unit == "microsecond" ||
               unit == "microseconds") {
      micros += num;
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
  if (months != 0) {
    // format the years and months
    int32_t format_years = months / 12;
    int32_t format_months = months - format_years * 12;
    lambda_func(format_years, "year", "years");
    lambda_func(format_months, "month", "months");
  }
  if (days != 0) {
    // format the days
    lambda_func(days, "day", "days");
  }
  if (micros != 0) {
    int64_t format_micros = -micros;
    int64_t hour = -(format_micros / Interval::MICROS_PER_HOUR);
    format_micros += hour * Interval::MICROS_PER_HOUR;
    lambda_func(hour, "hour", "hours");
    int64_t min = -(format_micros / Interval::MICROS_PER_MINUTE);
    format_micros += min * Interval::MICROS_PER_MINUTE;
    lambda_func(min, "minute", "minutes");
    int64_t sec = -(format_micros / Interval::MICROS_PER_SEC);
    format_micros += sec * Interval::MICROS_PER_SEC;
    lambda_func(sec, "second", "seconds");
    int64_t mill_sec = -(format_micros / Interval::MICROS_PER_MSEC);
    format_micros += mill_sec * Interval::MICROS_PER_MSEC;
    lambda_func(mill_sec, "millisecond", "milliseconds");
    lambda_func(-format_micros, "microsecond", "microseconds");
  }
  return oss.str();
}

void Interval::from_mill_seconds(int64_t mill_seconds) {
  months = 0;
  days = 0;
  micros = mill_seconds * MICROS_PER_MSEC;
}

int64_t Interval::to_mill_seconds() const {
  int64_t total_mill_seconds = micros / MICROS_PER_MSEC;
  total_mill_seconds += days * (MICROS_PER_DAY / MICROS_PER_MSEC);
  return total_mill_seconds;
}

}  // namespace gs
