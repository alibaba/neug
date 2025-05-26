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

#include "src/utils/property/types.h"
#include "src/utils/property/table.h"
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
    node["temporal"]["date"] = "";
  } else if (type == PropertyType::kDateTime) {
    node["temporal"]["datetime"] = "";
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
    in_archive << value.type << value.value.interval.to_u64();
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
    value.value.interval.from_u64(interval_val);
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
  in_archive << value.to_u64();
  return in_archive;
}

grape::OutArchive& operator>>(grape::OutArchive& out_archive, Interval& value) {
  uint64_t interval_val;
  out_archive >> interval_val;
  value.from_u64(interval_val);
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

std::string Date::to_string() const {
  return std::to_string(static_cast<int>(year())) + "-" +
         std::to_string(static_cast<int>(month())) + "-" +
         std::to_string(static_cast<int>(day()));
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

// int32_t Date::year() const {
//   boost::posix_time::ptime epoch(boost::gregorian::date(1970, 1, 1));
//   boost::posix_time::ptime time_point =
//       epoch + boost::posix_time::milliseconds(milli_second);
//   boost::gregorian::date date = time_point.date();
//   return date.year();
// }

// int32_t Date::month() const {
//   boost::posix_time::ptime epoch(boost::gregorian::date(1970, 1, 1));
//   boost::posix_time::ptime time_point =
//       epoch + boost::posix_time::milliseconds(milli_second);
//   boost::gregorian::date date = time_point.date();
//   return date.month().as_number();
// }

// int32_t Date::day() const {
//   boost::posix_time::ptime epoch(boost::gregorian::date(1970, 1, 1));
//   boost::posix_time::ptime time_point =
//       epoch + boost::posix_time::milliseconds(milli_second);
//   boost::gregorian::date date = time_point.date();
//   return date.day();
// }

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
  boost::posix_time::ptime epoch(boost::gregorian::date(1970, 1, 1));
  boost::posix_time::ptime time_point =
      epoch + boost::posix_time::milliseconds(value.integer);
  boost::posix_time::time_duration td = time_point.time_of_day();
  boost::gregorian::date date = time_point.date();
  std::ostringstream oss;
  oss << date.year() << "-" << std::setw(2) << std::setfill('0')
      << date.month().as_number() << "-" << std::setw(2) << std::setfill('0')
      << date.day() << " " << std::setw(2) << std::setfill('0') << td.hours()
      << ":" << std::setw(2) << std::setfill('0') << td.minutes() << ":"
      << std::setw(2) << std::setfill('0') << td.seconds() << "."
      << std::setw(3) << std::setfill('0') << value.integer % 1000;
  return oss.str();
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

}  // namespace gs
