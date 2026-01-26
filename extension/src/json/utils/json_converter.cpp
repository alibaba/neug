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

#include "json/json_converter.h"
#include <glog/logging.h>
#include <algorithm>

namespace neug {
namespace extension {

std::unique_ptr<arrow::ArrayBuilder> createArrowBuilder(DataTypeId type) {
  if (type == DataTypeId::kInt32) {
    return std::make_unique<arrow::Int32Builder>();
  } else if (type == DataTypeId::kInt64) {
    return std::make_unique<arrow::Int64Builder>();
  } else if (type == DataTypeId::kDouble) {
    return std::make_unique<arrow::DoubleBuilder>();
  } else if (type == DataTypeId::kBoolean) {
    return std::make_unique<arrow::BooleanBuilder>();
  } else if (type == DataTypeId::kDate) {
    return std::make_unique<arrow::Date32Builder>();
  } else if (type == DataTypeId::kTimestampMs) {
    return std::make_unique<arrow::TimestampBuilder>(
        arrow::timestamp(arrow::TimeUnit::MILLI), arrow::default_memory_pool());
  } else if (type == DataTypeId::kInterval) {
    return std::make_unique<arrow::DurationBuilder>(
        arrow::duration(arrow::TimeUnit::MILLI), arrow::default_memory_pool());
  } else {  // String
    return std::make_unique<arrow::StringBuilder>();
  }
}

bool appendJsonValueToBuilder(arrow::ArrayBuilder* builder,
                              const rapidjson::Value& val, DataTypeId type) {
  try {
    arrow::Status status;

    if (val.IsNull()) {
      status = builder->AppendNull();
    } else if (type == DataTypeId::kInt32) {
      auto* int32Builder = static_cast<arrow::Int32Builder*>(builder);
      status = int32Builder->Append(val.IsInt() ? val.GetInt()
                                                : std::stoi(val.GetString()));
    } else if (type == DataTypeId::kInt64) {
      auto* int64Builder = static_cast<arrow::Int64Builder*>(builder);
      status = int64Builder->Append(
          val.IsInt64() ? val.GetInt64() : std::stoll(val.GetString()));
    } else if (type == DataTypeId::kDouble) {
      auto* doubleBuilder = static_cast<arrow::DoubleBuilder*>(builder);
      status = doubleBuilder->Append(
          val.IsDouble() ? val.GetDouble() : std::stod(val.GetString()));
    } else if (type == DataTypeId::kBoolean) {
      auto* boolBuilder = static_cast<arrow::BooleanBuilder*>(builder);
      status = boolBuilder->Append(
          val.IsBool() ? val.GetBool()
                       : (val.GetString() == std::string("true")));
    } else if (type == DataTypeId::kDate) {
      auto* dateBuilder = static_cast<arrow::Date32Builder*>(builder);
      Date date(val.GetString());
      status = dateBuilder->Append(date.to_num_days());
    } else if (type == DataTypeId::kTimestampMs) {
      auto* timestampBuilder = static_cast<arrow::TimestampBuilder*>(builder);
      DateTime datetime(val.GetString());
      status = timestampBuilder->Append(datetime.milli_second);
    } else if (type == DataTypeId::kInterval) {
      auto* durationBuilder = static_cast<arrow::DurationBuilder*>(builder);
      Interval interval(std::string(val.GetString()));
      status = durationBuilder->Append(interval.to_mill_seconds());
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

DataTypeId inferPropertyTypeFromValue(const rapidjson::Value& val) {
  if (val.IsNull()) {
    return DataTypeId::kVarchar;
  } else if (val.IsBool()) {
    return DataTypeId::kBoolean;
  } else if (val.IsInt()) {
    return DataTypeId::kInt32;
  } else if (val.IsInt64()) {
    return DataTypeId::kInt64;
  } else if (val.IsDouble()) {
    return DataTypeId::kDouble;
  } else if (val.IsString()) {
    std::string str = val.GetString();
    // Detect date format (YYYY-MM-DD)
    if (str.size() == 10 && str[4] == '-' && str[7] == '-') {
      return DataTypeId::kDate;
    }
    // Detect datetime format
    if (str.size() >= 19 && (str[10] == ' ' || str[10] == 'T')) {
      return DataTypeId::kTimestampMs;
    }
    return DataTypeId::kVarchar;
  }
  return DataTypeId::kVarchar;
}

DataTypeId mergePropertyTypes(DataTypeId a, DataTypeId b) {
  if (a == b)
    return a;

  // String can accommodate any type
  if (a == DataTypeId::kVarchar || b == DataTypeId::kVarchar) {
    return DataTypeId::kVarchar;
  }

  // Int32 and Int64 -> Int64
  if ((a == DataTypeId::kInt32 && b == DataTypeId::kInt64) ||
      (a == DataTypeId::kInt64 && b == DataTypeId::kInt32)) {
    return DataTypeId::kInt64;
  }

  // Int and Double -> Double
  if ((a == DataTypeId::kInt32 || a == DataTypeId::kInt64) &&
      b == DataTypeId::kDouble) {
    return DataTypeId::kDouble;
  }
  if ((b == DataTypeId::kInt32 || b == DataTypeId::kInt64) &&
      a == DataTypeId::kDouble) {
    return DataTypeId::kDouble;
  }

  // Date and DateTime -> DateTime
  if ((a == DataTypeId::kDate && b == DataTypeId::kTimestampMs) ||
      (a == DataTypeId::kTimestampMs && b == DataTypeId::kDate)) {
    return DataTypeId::kTimestampMs;
  }

  // Other cases default to String
  return DataTypeId::kVarchar;
}

}  // namespace extension
}  // namespace neug