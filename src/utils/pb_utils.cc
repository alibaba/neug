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

#include "neug/utils/pb_utils.h"
#include <glog/logging.h>
#include <google/protobuf/stubs/port.h>
#include <rapidjson/document.h>
#include <rapidjson/encodings.h>
#include <rapidjson/prettywriter.h>
#include <rapidjson/rapidjson.h>
#include <rapidjson/stringbuffer.h>
#include <stddef.h>

#include <cstdint>
#include <limits>
#include <memory>
#include <sstream>
#include <stdexcept>
#include <unordered_set>
#include <utility>
#include "neug/execution/common/types/value.h"
#include "neug/generated/proto/plan/common.pb.h"
#include "neug/generated/proto/plan/expr.pb.h"
#include "neug/utils/bolt_utils.h"
#include "neug/utils/property/property.h"
#include "neug/utils/property/types.h"
#include "neug/utils/result.h"

namespace neug {

std::vector<std::string> parse_result_schema_column_names(
    const std::string& result_schema) {
  std::vector<std::string> column_names;

  if (result_schema.empty()) {
    return column_names;
  }

  try {
    YAML::Node schema = YAML::Load(result_schema);

    if (schema["returns"] && schema["returns"].IsSequence()) {
      for (const auto& return_item : schema["returns"]) {
        if (return_item["name"] && return_item["name"].IsScalar()) {
          column_names.push_back(return_item["name"].as<std::string>());
        }
      }
    }
  } catch (const YAML::Exception& e) {
    LOG(WARNING) << "Failed to parse result schema YAML: " << e.what()
                 << ", falling back to default column names";
    // Return empty vector to indicate parsing failure
    column_names.clear();
  }

  return column_names;
}

bool multiplicity_to_storage_strategy(
    const physical::CreateEdgeSchema::Multiplicity& multiplicity,
    EdgeStrategy& oe_strategy, EdgeStrategy& ie_strategy) {
  switch (multiplicity) {
  case physical::CreateEdgeSchema::ONE_TO_ONE:
    oe_strategy = EdgeStrategy::kSingle;
    ie_strategy = EdgeStrategy::kSingle;
    break;
  case physical::CreateEdgeSchema::ONE_TO_MANY:
    oe_strategy = EdgeStrategy::kMultiple;
    ie_strategy = EdgeStrategy::kSingle;
    break;
  case physical::CreateEdgeSchema::MANY_TO_ONE:
    oe_strategy = EdgeStrategy::kSingle;
    ie_strategy = EdgeStrategy::kMultiple;
    break;
  case physical::CreateEdgeSchema::MANY_TO_MANY:
    oe_strategy = EdgeStrategy::kMultiple;
    ie_strategy = EdgeStrategy::kMultiple;
    break;
  default:
    LOG(ERROR) << "Unknown multiplicity: " << multiplicity;
    return false;
  }
  return true;
}

bool primitive_type_to_property_type(
    const common::PrimitiveType& primitive_type, DataTypeId& out_type) {
  switch (primitive_type) {
  case common::PrimitiveType::DT_ANY:
    LOG(ERROR) << "Any type is not supported";
    return false;
  case common::PrimitiveType::DT_SIGNED_INT32:
    out_type = DataTypeId::kInt32;
    break;
  case common::PrimitiveType::DT_UNSIGNED_INT32:
    out_type = DataTypeId::kUInt32;
    break;
  case common::PrimitiveType::DT_SIGNED_INT64:
    out_type = DataTypeId::kInt64;
    break;
  case common::PrimitiveType::DT_UNSIGNED_INT64:
    out_type = DataTypeId::kUInt64;
    break;
  case common::PrimitiveType::DT_BOOL:
    out_type = DataTypeId::kBoolean;
    break;
  case common::PrimitiveType::DT_FLOAT:
    out_type = DataTypeId::kFloat;
    break;
  case common::PrimitiveType::DT_DOUBLE:
    out_type = DataTypeId::kDouble;
    break;
  case common::PrimitiveType::DT_NULL:
    out_type = DataTypeId::kEmpty;
    break;
  default:
    LOG(ERROR) << "Unknown primitive type: " << primitive_type;
    return false;
  }
  return true;
}

bool string_type_to_property_type(const common::String& string_type,
                                  DataTypeId& out_type) {
  switch (string_type.item_case()) {
  case common::String::kVarChar: {
    out_type = DataTypeId::kVarchar;
    break;
  }
  case common::String::kLongText: {
    out_type = DataTypeId::kVarchar;
    break;
  }
  case common::String::kChar: {
    // Currently, we implement fixed-char as varchar with fixed length.
    THROW_NOT_SUPPORTED_EXCEPTION("Char type is not supported yet");
  }
  case common::String::ITEM_NOT_SET: {
    LOG(ERROR) << "String type is not set: " << string_type.DebugString();
    return false;
  }
  default:
    LOG(ERROR) << "Unknown string type: " << string_type.DebugString();
    return false;
  }
  return true;
}

bool temporal_type_to_property_type(const common::Temporal& temporal_type,
                                    DataTypeId& out_type) {
  switch (temporal_type.item_case()) {
  case common::Temporal::kDate32:
    out_type = DataTypeId::kDate;
    break;
  case common::Temporal::kDateTime:
    out_type = DataTypeId::kTimestampMs;
    break;
  case common::Temporal::kTimestamp:
    out_type = DataTypeId::kTimestampMs;
    break;
  case common::Temporal::kDate:
    // TODO(zhanglei): Parse format
    out_type = DataTypeId::kDate;
    break;
  case common::Temporal::kInterval:
    out_type = DataTypeId::kInterval;
    break;
  default:
    LOG(ERROR) << "Unknown temporal type: " << temporal_type.DebugString();
    return false;
  }
  return true;
}

bool data_type_to_property_type(const common::DataType& data_type,
                                DataTypeId& out_type) {
  switch (data_type.item_case()) {
  case common::DataType::kPrimitiveType: {
    return primitive_type_to_property_type(data_type.primitive_type(),
                                           out_type);
  }
  case common::DataType::kDecimal: {
    LOG(ERROR) << "Decimal type is not supported";
    return false;
  }
  case common::DataType::kString: {
    return string_type_to_property_type(data_type.string(), out_type);
  }
  case common::DataType::kTemporal: {
    return temporal_type_to_property_type(data_type.temporal(), out_type);
  }
  case common::DataType::kArray: {
    LOG(ERROR) << "Array type is not supported";
    return false;
  }
  case common::DataType::kMap: {
    LOG(ERROR) << "Map type is not supported";
    return false;
  }
  case common::DataType::ITEM_NOT_SET: {
    LOG(ERROR) << "Data type is not set: " << data_type.DebugString();
    return false;
  }
  default:
    LOG(ERROR) << "Unknown data type: " << data_type.DebugString();
    return false;
  }
}

bool common_value_to_value(const DataTypeId type, const common::Value& value,
                           execution::Value& out_value) {
  switch (value.item_case()) {
  case common::Value::kBoolean:
    out_value = execution::Value::BOOLEAN(value.boolean());
    break;
  case common::Value::kI32:
    out_value = execution::Value::INT32(value.i32());
    break;
  case common::Value::kI64:
    out_value = execution::Value::INT64(value.i64());
    break;
  case common::Value::kU32:
    out_value = execution::Value::UINT32(value.u32());
    break;
  case common::Value::kU64:
    out_value = execution::Value::UINT64(value.u64());
    break;
  case common::Value::kF32:
    out_value = execution::Value::FLOAT(value.f32());
    break;
  case common::Value::kF64:
    out_value = execution::Value::DOUBLE(value.f64());
    break;
  case common::Value::kStr:
    if (type == DataTypeId::kDate) {
      // Special handling for date stored as string
      Date date(value.str());
      out_value = execution::Value::DATE(date);
      break;
    } else if (type == DataTypeId::kTimestampMs) {
      // Special handling for datetime stored as string
      DateTime datetime(value.str());
      out_value = execution::Value::TIMESTAMPMS(datetime);
      break;
    } else if (type == DataTypeId::kInterval) {
      // Special handling for interval stored as string
      Interval interval(value.str());
      out_value = execution::Value::INTERVAL(interval);
      break;
    } else {
      LOG(INFO) << "Setting string value: " << value.str()
                << " for type: " << std::to_string(type);
      out_value = execution::Value::STRING(value.str());
    }
    break;
  case common::Value::kDate:
    out_value = execution::Value::DATE(Date(value.date().item()));
    break;
  default:
    LOG(ERROR) << "Unknown value type: " << value.DebugString();
    return false;
  }
  return true;
}

neug::result<std::vector<std::pair<std::string, execution::Value>>>
property_defs_to_value(
    const google::protobuf::RepeatedPtrField<physical::PropertyDef>&
        properties) {
  std::vector<std::pair<std::string, execution::Value>> result;
  for (const auto& property : properties) {
    const auto& name = property.name();
    execution::Value default_value(DataType::SQLNULL);
    DataTypeId type;
    if (!data_type_to_property_type(property.type(), type)) {
      RETURN_ERROR(Status(StatusCode::ERR_INVALID_ARGUMENT,
                          "Invalid property type: " + property.DebugString()));
    }

    if (property.has_default_value()) {
      if (!common_value_to_value(type, property.default_value(),
                                 default_value)) {
        RETURN_ERROR(
            Status(StatusCode::ERR_INVALID_ARGUMENT,
                   "Invalid default value: " + property.DebugString()));
      } else {
        VLOG(10) << "Default value convert to any success:"
                 << property.default_value().DebugString();
      }
    } else {
      default_value = execution::property_to_value(get_default_value(type));
      VLOG(1) << "No default value, use type default:"
              << default_value.to_string()
              << " type: " << default_value.type().ToString();
    }
    result.emplace_back(name, default_value);
  }
  return result;
}

// Convert to a bool representing error_on_conflict.
bool conflict_action_to_bool(const physical::ConflictAction& action) {
  if (action == physical::ConflictAction::ON_CONFLICT_THROW) {
    return true;
  } else if (action == physical::ConflictAction::ON_CONFLICT_DO_NOTHING) {
    return false;
  } else {
    LOG(FATAL) << "invalid action: " << action;
    return false;  // to suppress warning
  }
}

}  // namespace neug
