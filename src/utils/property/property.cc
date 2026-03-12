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

#include "neug/utils/property/property.h"
#include "neug/utils/serialization/in_archive.h"
#include "neug/utils/serialization/out_archive.h"

namespace neug {

Property get_default_value(const DataTypeId& type) {
  Property default_value;
  switch (type) {
  case DataTypeId::kEmpty:
    break;
  case DataTypeId::kBoolean:
    default_value.set_bool(false);
    break;
  case DataTypeId::kInt32:
    default_value.set_int32(0);
    break;
  case DataTypeId::kUInt32:
    default_value.set_uint32(0);
    break;
  case DataTypeId::kInt64:
    default_value.set_int64(0);
    break;
  case DataTypeId::kUInt64:
    default_value.set_uint64(0);
    break;
  case DataTypeId::kFloat:
    default_value.set_float(0.0f);
    break;
  case DataTypeId::kDouble:
    default_value.set_double(0.0);
    break;
  case DataTypeId::kVarchar:
    default_value.set_string_view("");
    break;
  case DataTypeId::kDate:
    default_value.set_date(Date((int32_t) 0));
    break;
  case DataTypeId::kTimestampMs:
    default_value.set_datetime(DateTime((int64_t) 0));
    break;
  case DataTypeId::kInterval:
    default_value.set_interval(Interval());
    break;
  default:
    THROW_NOT_SUPPORTED_EXCEPTION(
        "Unsupported property type for default value: " + std::to_string(type) +
        ", " + std::to_string((int) type));
  }
  return default_value;
}

InArchive& operator<<(InArchive& in_archive, const Property& value) {
  if (value.type() == DataTypeId::kEmpty) {
    in_archive << value.type();
  } else if (value.type() == DataTypeId::kBoolean) {
    in_archive << value.type() << value.as_bool();
  } else if (value.type() == DataTypeId::kInt32) {
    in_archive << value.type() << value.as_int32();
  } else if (value.type() == DataTypeId::kUInt32) {
    in_archive << value.type() << value.as_uint32();
  } else if (value.type() == DataTypeId::kInt64) {
    in_archive << value.type() << value.as_int64();
  } else if (value.type() == DataTypeId::kUInt64) {
    in_archive << value.type() << value.as_uint64();
  } else if (value.type() == DataTypeId::kFloat) {
    in_archive << value.type() << value.as_float();
  } else if (value.type() == DataTypeId::kDouble) {
    in_archive << value.type() << value.as_double();
  } else if (value.type() == DataTypeId::kVarchar) {
    in_archive << value.type() << value.as_string_view();
  } else if (value.type() == DataTypeId::kDate) {
    in_archive << value.type() << value.as_date().to_u32();
  } else if (value.type() == DataTypeId::kTimestampMs) {
    in_archive << value.type() << value.as_datetime().milli_second;
  } else if (value.type() == DataTypeId::kInterval) {
    in_archive << value.type() << value.as_interval().months
               << value.as_interval().days << value.as_interval().micros;
  } else {
    THROW_NOT_SUPPORTED_EXCEPTION(std::string("Not supported: ") +
                                  std::to_string(value.type()));
  }

  return in_archive;
}

OutArchive& operator>>(OutArchive& out_archive, Property& value) {
  DataTypeId pt;
  out_archive >> pt;
  if (pt == DataTypeId::kEmpty) {
  } else if (pt == DataTypeId::kBoolean) {
    bool tmp;
    out_archive >> tmp;
    value.set_bool(tmp);
  } else if (pt == DataTypeId::kInt32) {
    int32_t tmp;
    out_archive >> tmp;
    value.set_int32(tmp);
  } else if (pt == DataTypeId::kUInt32) {
    uint32_t tmp;
    out_archive >> tmp;
    value.set_uint32(tmp);
  } else if (pt == DataTypeId::kFloat) {
    float tmp;
    out_archive >> tmp;
    value.set_float(tmp);
  } else if (pt == DataTypeId::kInt64) {
    int64_t tmp;
    out_archive >> tmp;
    value.set_int64(tmp);
  } else if (pt == DataTypeId::kUInt64) {
    uint64_t tmp;
    out_archive >> tmp;
    value.set_uint64(tmp);
  } else if (pt == DataTypeId::kDouble) {
    double tmp;
    out_archive >> tmp;
    value.set_double(tmp);
  } else if (pt == DataTypeId::kVarchar) {
    std::string_view tmp;
    out_archive >> tmp;
    value.set_string_view(tmp);
  } else if (pt == DataTypeId::kDate) {
    uint32_t date_val;
    out_archive >> date_val;
    value.set_date(date_val);
  } else if (pt == DataTypeId::kTimestampMs) {
    int64_t date_time_val;
    out_archive >> date_time_val;
    value.set_datetime(date_time_val);
  } else if (pt == DataTypeId::kInterval) {
    Interval interval_val;
    out_archive >> interval_val.months >> interval_val.days >>
        interval_val.micros;
    value.set_interval(interval_val);
  } else {
    THROW_NOT_SUPPORTED_EXCEPTION("Not supported: " +
                                  std::to_string(value.type()));
  }

  return out_archive;
}

Property& Property::operator=(const Property& other) {
  if (this != &other) {
    type_ = other.type_;
    memcpy(&value_, &other.value_, sizeof(PropValue));
  }
  return *this;
}

bool Property::operator==(const Property& other) const {
  if (type_ != other.type_) {
    return false;
  } else {
    if (type() == DataTypeId::kInt32) {
      return value_.i == other.value_.i;
    } else if (type() == DataTypeId::kUInt32) {
      return value_.ui == other.value_.ui;
    } else if (type() == DataTypeId::kVarchar) {
      return value_.s == other.as_string_view();
    } else if (type() == DataTypeId::kEmpty) {
      return true;
    } else if (type() == DataTypeId::kDouble) {
      return value_.db == other.value_.db;
    } else if (type() == DataTypeId::kInt64) {
      return value_.l == other.value_.l;
    } else if (type() == DataTypeId::kUInt64) {
      return value_.ul == other.value_.ul;
    } else if (type() == DataTypeId::kBoolean) {
      return value_.b == other.value_.b;
    } else if (type() == DataTypeId::kFloat) {
      return value_.f == other.value_.f;
    } else {
      return false;
    }
  }
}

bool Property::operator<(const Property& other) const {
  if (type_ == other.type_) {
    if (type() == DataTypeId::kInt32) {
      return value_.i < other.value_.i;
    } else if (type() == DataTypeId::kInt64) {
      return value_.l < other.value_.l;
    } else if (type() == DataTypeId::kVarchar) {
      return value_.s < other.value_.s;
    } else if (type() == DataTypeId::kEmpty) {
      return false;
    } else if (type() == DataTypeId::kDouble) {
      return value_.db < other.value_.db;
    } else if (type() == DataTypeId::kUInt32) {
      return value_.ui < other.value_.ui;
    } else if (type() == DataTypeId::kUInt64) {
      return value_.ul < other.value_.ul;
    } else if (type() == DataTypeId::kBoolean) {
      return value_.b < other.value_.b;
    } else if (type() == DataTypeId::kFloat) {
      return value_.f < other.value_.f;
    } else if (type() == DataTypeId::kDate) {
      return value_.d.to_u32() < other.value_.d.to_u32();
    } else if (type() == DataTypeId::kTimestampMs) {
      return value_.dt.milli_second < other.value_.dt.milli_second;
    } else if (type() == DataTypeId::kInterval) {
      return value_.itv < other.value_.itv;
    } else {
      THROW_NOT_SUPPORTED_EXCEPTION(
          "Unsupported property type for comparison: " +
          std::to_string(type()));
    }
  } else {
    return type_ < other.type_;
  }
}
}  // namespace neug
