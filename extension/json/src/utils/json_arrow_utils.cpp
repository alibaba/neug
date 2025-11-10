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

#include "json_arrow_utils.h"
#include <glog/logging.h>
#include <algorithm>

namespace gs {
namespace extension {

std::unique_ptr<arrow::ArrayBuilder> createArrowBuilder(PropertyType type) {
  if (type == PropertyType::Int32()) {
    return std::make_unique<arrow::Int32Builder>();
  } else if (type == PropertyType::Int64()) {
    return std::make_unique<arrow::Int64Builder>();
  } else if (type == PropertyType::Double()) {
    return std::make_unique<arrow::DoubleBuilder>();
  } else if (type == PropertyType::Bool()) {
    return std::make_unique<arrow::BooleanBuilder>();
  } else if (type == PropertyType::Date()) {
    return std::make_unique<arrow::Date32Builder>();
  } else if (type == PropertyType::DateTime()) {
    return std::make_unique<arrow::TimestampBuilder>(
        arrow::timestamp(arrow::TimeUnit::MILLI), arrow::default_memory_pool());
  } else if (type == PropertyType::Interval()) {
    return std::make_unique<arrow::DurationBuilder>(
        arrow::duration(arrow::TimeUnit::MILLI), arrow::default_memory_pool());
  } else if (type == PropertyType::Timestamp()) {
    return std::make_unique<arrow::TimestampBuilder>(
        arrow::timestamp(arrow::TimeUnit::MILLI), arrow::default_memory_pool());
  } else {  // String
    return std::make_unique<arrow::StringBuilder>();
  }
}

bool appendJsonValueToBuilder(
    arrow::ArrayBuilder* builder,
    const rapidjson::Value& val,
    PropertyType type) {
  
  try {
    arrow::Status status;
    
    if (val.IsNull()) {
      status = builder->AppendNull();
    } else if (type == PropertyType::Int32()) {
      auto* int32Builder = static_cast<arrow::Int32Builder*>(builder);
      status = int32Builder->Append(
          val.IsInt() ? val.GetInt() : std::stoi(val.GetString()));
    } else if (type == PropertyType::Int64()) {
      auto* int64Builder = static_cast<arrow::Int64Builder*>(builder);
      status = int64Builder->Append(
          val.IsInt64() ? val.GetInt64() : std::stoll(val.GetString()));
    } else if (type == PropertyType::Double()) {
      auto* doubleBuilder = static_cast<arrow::DoubleBuilder*>(builder);
      status = doubleBuilder->Append(
          val.IsDouble() ? val.GetDouble() : std::stod(val.GetString()));
    } else if (type == PropertyType::Bool()) {
      auto* boolBuilder = static_cast<arrow::BooleanBuilder*>(builder);
      status = boolBuilder->Append(
          val.IsBool() ? val.GetBool() : (val.GetString() == std::string("true")));
    } else if (type == PropertyType::Date()) {
      auto* dateBuilder = static_cast<arrow::Date32Builder*>(builder);
      Date date(val.GetString());
      status = dateBuilder->Append(date.to_num_days());
    } else if (type == PropertyType::DateTime()) {
      auto* timestampBuilder = static_cast<arrow::TimestampBuilder*>(builder);
      DateTime datetime(val.GetString());
      status = timestampBuilder->Append(datetime.milli_second);
    } else if (type == PropertyType::Interval()) {
      auto* durationBuilder = static_cast<arrow::DurationBuilder*>(builder);
      Interval interval(std::string(val.GetString()));
      status = durationBuilder->Append(interval.to_mill_seconds());
    } else if (type == PropertyType::Timestamp()) {
      auto* timestampBuilder = static_cast<arrow::TimestampBuilder*>(builder);
      status = timestampBuilder->Append(
          val.IsInt64() ? val.GetInt64() : std::stoll(val.GetString()));
    } else {  // String
      auto* stringBuilder = static_cast<arrow::StringBuilder*>(builder);
      status = stringBuilder->Append(val.IsString() ? val.GetString() : "");
    }
    
    if (!status.ok()) {
      LOG(WARNING) << "Failed to append value: " << status.ToString();
      auto null_status = builder->AppendNull();
      if (!null_status.ok()) {
        LOG(WARNING) << "Failed to append null after failed append: " 
                     << null_status.ToString();
      }
      return false;
    }
    return true;
  } catch (const std::exception& e) {
    LOG(WARNING) << "Exception when appending value: " << e.what();
    auto null_status = builder->AppendNull();
    if (!null_status.ok()) {
      LOG(WARNING) << "Failed to append null after exception: " 
                   << null_status.ToString();
    }
    return false;
  }
}

PropertyType inferPropertyTypeFromValue(const rapidjson::Value& val) {
  if (val.IsNull()) {
    return PropertyType::String();
  } else if (val.IsBool()) {
    return PropertyType::Bool();
  } else if (val.IsInt()) {
    return PropertyType::Int32();
  } else if (val.IsInt64()) {
    return PropertyType::Int64();
  } else if (val.IsDouble()) {
    return PropertyType::Double();
  } else if (val.IsString()) {
    std::string str = val.GetString();
    // Detect date format (YYYY-MM-DD)
    if (str.size() == 10 && str[4] == '-' && str[7] == '-') {
      return PropertyType::Date();
    }
    // Detect datetime format
    if (str.size() >= 19 && (str[10] == ' ' || str[10] == 'T')) {
      return PropertyType::DateTime();
    }
    return PropertyType::String();
  }
  return PropertyType::String();
}

PropertyType mergePropertyTypes(PropertyType a, PropertyType b) {
  if (a == b) return a;
  
  // String can accommodate any type
  if (a == PropertyType::String() || b == PropertyType::String()) {
    return PropertyType::String();
  }
  
  // Int32 and Int64 -> Int64
  if ((a == PropertyType::Int32() && b == PropertyType::Int64()) ||
      (a == PropertyType::Int64() && b == PropertyType::Int32())) {
    return PropertyType::Int64();
  }
  
  // Int and Double -> Double
  if ((a == PropertyType::Int32() || a == PropertyType::Int64()) && 
      b == PropertyType::Double()) {
    return PropertyType::Double();
  }
  if ((b == PropertyType::Int32() || b == PropertyType::Int64()) && 
      a == PropertyType::Double()) {
    return PropertyType::Double();
  }
  
  // Date and DateTime -> DateTime
  if ((a == PropertyType::Date() && b == PropertyType::DateTime()) ||
      (a == PropertyType::DateTime() && b == PropertyType::Date())) {
    return PropertyType::DateTime();
  }
  
  // Other cases default to String
  return PropertyType::String();
}

}  // namespace extension
}  // namespace gs