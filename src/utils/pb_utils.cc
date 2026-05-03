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
    const common::PrimitiveType& primitive_type, DataType& out_type) {
  switch (primitive_type) {
  case common::PrimitiveType::DT_ANY:
    LOG(ERROR) << "Any type is not supported";
    return false;
  case common::PrimitiveType::DT_SIGNED_INT32:
    out_type = DataType::INT32;
    break;
  case common::PrimitiveType::DT_UNSIGNED_INT32:
    out_type = DataType::UINT32;
    break;
  case common::PrimitiveType::DT_SIGNED_INT64:
    out_type = DataType::INT64;
    break;
  case common::PrimitiveType::DT_UNSIGNED_INT64:
    out_type = DataType::UINT64;
    break;
  case common::PrimitiveType::DT_BOOL:
    out_type = DataType::BOOLEAN;
    break;
  case common::PrimitiveType::DT_FLOAT:
    out_type = DataType::FLOAT;
    break;
  case common::PrimitiveType::DT_DOUBLE:
    out_type = DataType::DOUBLE;
    break;
  case common::PrimitiveType::DT_NULL:
    out_type = DataType::SQLNULL;
    break;
  default:
    LOG(ERROR) << "Unknown primitive type: " << primitive_type;
    return false;
  }
  return true;
}

bool string_type_to_property_type(const common::String& string_type,
                                  DataType& out_type) {
  switch (string_type.item_case()) {
  case common::String::kVarChar: {
    uint16_t max_length = STRING_DEFAULT_MAX_LENGTH;
    if (string_type.has_var_char()) {
      auto str_info = string_type.var_char();
      if (str_info.max_length() > 0) {
        max_length = str_info.max_length();
      }
    }
    out_type = DataType::Varchar(max_length);
    break;
  }
  case common::String::kLongText: {
    out_type = DataType::Varchar(STRING_DEFAULT_MAX_LENGTH);
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
                                    DataType& out_type) {
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
                                DataType& out_type) {
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
    // A List/Array property: recursively resolve the element type.
    const auto& array_type = data_type.array();
    if (!array_type.has_component_type()) {
      LOG(ERROR) << "Array type missing component type: "
                 << data_type.DebugString();
      return false;
    }
    DataType child_type;
    if (!data_type_to_property_type(array_type.component_type(), child_type)) {
      return false;
    }
    out_type = DataType::List(child_type);
    break;
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
  return true;
}

bool common_value_to_value(const DataType& type, const common::Value& value,
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
    if (type.id() == DataTypeId::kDate) {
      // Special handling for date stored as string
      Date date(value.str());
      out_value = execution::Value::DATE(date);
      break;
    } else if (type.id() == DataTypeId::kTimestampMs) {
      // Special handling for datetime stored as string
      DateTime datetime(value.str());
      out_value = execution::Value::TIMESTAMPMS(datetime);
      break;
    } else if (type.id() == DataTypeId::kInterval) {
      // Special handling for interval stored as string
      Interval interval(value.str());
      out_value = execution::Value::INTERVAL(interval);
      break;
    } else {
      auto str_type_info = type.RawExtraTypeInfo();
      uint16_t max_length =
          str_type_info ? str_type_info->Cast<StringTypeInfo>().max_length
                        : STRING_DEFAULT_MAX_LENGTH;
      out_value = execution::Value::VARCHAR(value.str(), max_length);
    }
    break;
  case common::Value::kDate:
    out_value = execution::Value::DATE(Date(value.date().item()));
    break;
  case common::Value::kI32Array: {
    DataType child_type = ListType::GetChildType(type);
    std::vector<execution::Value> items;
    for (auto v : value.i32_array().item()) {
      items.emplace_back(execution::Value::INT32(v));
    }
    out_value = execution::Value::LIST(child_type, std::move(items));
    break;
  }
  case common::Value::kI64Array: {
    DataType child_type = ListType::GetChildType(type);
    std::vector<execution::Value> items;
    for (auto v : value.i64_array().item()) {
      items.emplace_back(execution::Value::INT64(v));
    }
    out_value = execution::Value::LIST(child_type, std::move(items));
    break;
  }
  case common::Value::kF64Array: {
    DataType child_type = ListType::GetChildType(type);
    std::vector<execution::Value> items;
    if (child_type.id() == DataTypeId::kFloat) {
      for (auto v : value.f64_array().item()) {
        items.emplace_back(execution::Value::FLOAT(static_cast<float>(v)));
      }
    } else {
      for (auto v : value.f64_array().item()) {
        items.emplace_back(execution::Value::DOUBLE(v));
      }
    }
    out_value = execution::Value::LIST(child_type, std::move(items));
    break;
  }
  case common::Value::kStrArray: {
    DataType child_type = ListType::GetChildType(type);
    std::vector<execution::Value> items;
    uint16_t max_len = STRING_DEFAULT_MAX_LENGTH;
    if (child_type.RawExtraTypeInfo()) {
      max_len =
          child_type.RawExtraTypeInfo()->Cast<StringTypeInfo>().max_length;
    }
    for (const auto& s : value.str_array().item()) {
      items.emplace_back(execution::Value::VARCHAR(s, max_len));
    }
    out_value = execution::Value::LIST(child_type, std::move(items));
    break;
  }
  default:
    LOG(ERROR) << "Unknown value type: " << value.DebugString();
    return false;
  }
  return true;
}

// Convert a protobuf Expression (e.g. ToList, scalar const) into an
// execution::Value.  This is needed because the compiler serialises default
// values for complex types (nested lists) into the `default_expr` field of
// PropertyDef rather than the flat `default_value` field.
static bool common_expr_to_value(const DataType& type,
                                 const common::Expression& expr,
                                 execution::Value& out_value) {
  if (expr.operators_size() == 0) {
    LOG(ERROR) << "Empty expression in default_expr";
    return false;
  }
  const auto& opr = expr.operators(0);

  if (opr.has_to_list()) {
    if (type.id() != DataTypeId::kList) {
      LOG(ERROR) << "ToList expression but type is not list: "
                 << type.ToString();
      return false;
    }
    DataType child_type = ListType::GetChildType(type);
    const auto& to_list = opr.to_list();
    std::vector<execution::Value> items;
    items.reserve(to_list.fields_size());
    for (const auto& field_expr : to_list.fields()) {
      execution::Value child_value(DataType::SQLNULL);
      if (!common_expr_to_value(child_type, field_expr, child_value)) {
        LOG(ERROR) << "Failed to convert ToList child expression";
        return false;
      }
      items.push_back(std::move(child_value));
    }
    out_value = execution::Value::LIST(child_type, std::move(items));
    return true;
  }

  if (opr.has_const_()) {
    return common_value_to_value(type, opr.const_(), out_value);
  }

  LOG(ERROR) << "Unsupported expression operator in default_expr: "
             << expr.DebugString();
  return false;
}

neug::result<std::vector<std::pair<std::string, execution::Value>>>
property_defs_to_value(
    const google::protobuf::RepeatedPtrField<physical::PropertyDef>&
        properties) {
  std::vector<std::pair<std::string, execution::Value>> result;
  for (const auto& property : properties) {
    const auto& name = property.name();
    execution::Value default_value(DataType::SQLNULL);
    DataType type;
    if (!data_type_to_property_type(property.type(), type)) {
      RETURN_ERROR(Status(StatusCode::ERR_INVALID_ARGUMENT,
                          "Invalid property type: " + property.DebugString()));
    }

    if (property.has_default_expr() &&
        common_expr_to_value(type, property.default_expr(), default_value)) {
      VLOG(10) << "Default expr convert to value success:"
               << property.default_expr().DebugString();
    } else if (property.has_default_value()) {
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
      if (type.id() == DataTypeId::kVarchar) {
        int32_t width =
            type.RawExtraTypeInfo()
                ? type.RawExtraTypeInfo()->Cast<StringTypeInfo>().max_length
                : STRING_DEFAULT_MAX_LENGTH;
        default_value = execution::Value::VARCHAR(
            std::string(get_default_value(type.id()).as_string_view()), width);
      } else {
        default_value =
            execution::property_to_value(get_default_value(type.id()), type);
      }

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
