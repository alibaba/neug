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

namespace gs {

Property get_default_value(const DataTypeId& type) {
  Property default_value;
  switch (type) {
  case DataTypeId::EMPTY:
    break;
  case DataTypeId::BOOLEAN:
    default_value.set_bool(false);
    break;
  case DataTypeId::INTEGER:
    default_value.set_int32(0);
    break;
  case DataTypeId::UINTEGER:
    default_value.set_uint32(0);
    break;
  case DataTypeId::BIGINT:
    default_value.set_int64(0);
    break;
  case DataTypeId::UBIGINT:
    default_value.set_uint64(0);
    break;
  case DataTypeId::FLOAT:
    default_value.set_float(0.0f);
    break;
  case DataTypeId::DOUBLE:
    default_value.set_double(0.0);
    break;
  case DataTypeId::VARCHAR:
    default_value.set_string_view("");
    break;
  case DataTypeId::DATE:
    default_value.set_date(Date((int32_t) 0));
    break;
  case DataTypeId::TIMESTAMP_MS:
    default_value.set_datetime(DateTime((int64_t) 0));
    break;
  case DataTypeId::INTERVAL:
    default_value.set_interval(Interval());
    break;
  default:
    THROW_NOT_SUPPORTED_EXCEPTION(
        "Unsupported property type for default value: " + std::to_string(type));
  }
  return default_value;
}

InArchive& operator<<(InArchive& in_archive, const Property& value) {
  if (value.type() == DataTypeId::EMPTY) {
    in_archive << value.type();
  } else if (value.type() == DataTypeId::BOOLEAN) {
    in_archive << value.type() << value.as_bool();
  } else if (value.type() == DataTypeId::INTEGER) {
    in_archive << value.type() << value.as_int32();
  } else if (value.type() == DataTypeId::UINTEGER) {
    in_archive << value.type() << value.as_uint32();
  } else if (value.type() == DataTypeId::BIGINT) {
    in_archive << value.type() << value.as_int64();
  } else if (value.type() == DataTypeId::UBIGINT) {
    in_archive << value.type() << value.as_uint64();
  } else if (value.type() == DataTypeId::FLOAT) {
    in_archive << value.type() << value.as_float();
  } else if (value.type() == DataTypeId::DOUBLE) {
    in_archive << value.type() << value.as_double();
  } else if (value.type() == DataTypeId::VARCHAR) {
    in_archive << value.type() << value.as_string_view();
  } else if (value.type() == DataTypeId::DATE) {
    in_archive << value.type() << value.as_date().to_u32();
  } else if (value.type() == DataTypeId::TIMESTAMP_MS) {
    in_archive << value.type() << value.as_datetime().milli_second;
  } else if (value.type() == DataTypeId::INTERVAL) {
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
  if (pt == DataTypeId::EMPTY) {
  } else if (pt == DataTypeId::BOOLEAN) {
    bool tmp;
    out_archive >> tmp;
    value.set_bool(tmp);
  } else if (pt == DataTypeId::INTEGER) {
    int32_t tmp;
    out_archive >> tmp;
    value.set_int32(tmp);
  } else if (pt == DataTypeId::UINTEGER) {
    uint32_t tmp;
    out_archive >> tmp;
    value.set_uint32(tmp);
  } else if (pt == DataTypeId::FLOAT) {
    float tmp;
    out_archive >> tmp;
    value.set_float(tmp);
  } else if (pt == DataTypeId::BIGINT) {
    int64_t tmp;
    out_archive >> tmp;
    value.set_int64(tmp);
  } else if (pt == DataTypeId::UBIGINT) {
    uint64_t tmp;
    out_archive >> tmp;
    value.set_uint64(tmp);
  } else if (pt == DataTypeId::DOUBLE) {
    double tmp;
    out_archive >> tmp;
    value.set_double(tmp);
  } else if (pt == DataTypeId::VARCHAR) {
    std::string_view tmp;
    out_archive >> tmp;
    value.set_string_view(tmp);
  } else if (pt == DataTypeId::DATE) {
    uint32_t date_val;
    out_archive >> date_val;
    value.set_date(date_val);
  } else if (pt == DataTypeId::TIMESTAMP_MS) {
    int64_t date_time_val;
    out_archive >> date_time_val;
    value.set_datetime(date_time_val);
  } else if (pt == DataTypeId::INTERVAL) {
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
    if (type() == DataTypeId::INTEGER) {
      return value_.i == other.value_.i;
    } else if (type() == DataTypeId::UINTEGER) {
      return value_.ui == other.value_.ui;
    } else if (type() == DataTypeId::VARCHAR) {
      return value_.s == other.as_string_view();
    } else if (type() == DataTypeId::EMPTY) {
      return true;
    } else if (type() == DataTypeId::DOUBLE) {
      return value_.db == other.value_.db;
    } else if (type() == DataTypeId::BIGINT) {
      return value_.l == other.value_.l;
    } else if (type() == DataTypeId::UBIGINT) {
      return value_.ul == other.value_.ul;
    } else if (type() == DataTypeId::BOOLEAN) {
      return value_.b == other.value_.b;
    } else if (type() == DataTypeId::FLOAT) {
      return value_.f == other.value_.f;
    } else {
      return false;
    }
  }
}

bool Property::operator<(const Property& other) const {
  if (type_ == other.type_) {
    if (type() == DataTypeId::INTEGER) {
      return value_.i < other.value_.i;
    } else if (type() == DataTypeId::BIGINT) {
      return value_.l < other.value_.l;
    } else if (type() == DataTypeId::VARCHAR) {
      return value_.s < other.value_.s;
    } else if (type() == DataTypeId::EMPTY) {
      return false;
    } else if (type() == DataTypeId::DOUBLE) {
      return value_.db < other.value_.db;
    } else if (type() == DataTypeId::UINTEGER) {
      return value_.ui < other.value_.ui;
    } else if (type() == DataTypeId::UBIGINT) {
      return value_.ul < other.value_.ul;
    } else if (type() == DataTypeId::BOOLEAN) {
      return value_.b < other.value_.b;
    } else if (type() == DataTypeId::FLOAT) {
      return value_.f < other.value_.f;
    } else if (type() == DataTypeId::DATE) {
      return value_.d.to_u32() < other.value_.d.to_u32();
    } else if (type() == DataTypeId::TIMESTAMP_MS) {
      return value_.dt.milli_second < other.value_.dt.milli_second;
    } else if (type() == DataTypeId::INTERVAL) {
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
}  // namespace gs
