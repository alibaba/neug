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
#include <google/protobuf/util/json_util.h>
#include <rapidjson/document.h>
#include <rapidjson/encodings.h>
#include <rapidjson/prettywriter.h>
#include <rapidjson/rapidjson.h>
#include <rapidjson/stringbuffer.h>
#include <stddef.h>
#include "neug/utils/exception/exception.h"

#include <cstdint>
#include <exception>
#include <limits>
#include <memory>
#include <sstream>
#include <stdexcept>
#include <unordered_set>
#include <utility>
#include "neug/execution/common/types/value.h"
#include "neug/execution/expression/expr.h"
#include "neug/generated/proto/plan/common.pb.h"
#include "neug/generated/proto/plan/expr.pb.h"
#include "neug/utils/bolt_utils.h"
#include "neug/utils/property/default_value.h"
#include "neug/utils/property/types.h"
#include "neug/utils/result.h"

namespace neug {

std::string proto_to_string(const google::protobuf::Message& proto) {
  std::string json_str;
  google::protobuf::util::JsonPrintOptions options;
  options.add_whitespace = true;
#if PROTOBUF_VERSION < 4026000
  options.always_print_primitive_fields = true;
#else
  options.always_print_fields_with_no_presence = true;
#endif
  options.preserve_proto_field_names = true;
  auto status =
      google::protobuf::util::MessageToJsonString(proto, &json_str, options);
  if (!status.ok()) {
    THROW_RUNTIME_ERROR("Failed to convert proto to string: " +
                        status.ToString());
  }
  return json_str;
}

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
    size_t max_length = STRING_DEFAULT_MAX_LENGTH;
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
    const auto& array = data_type.array();
    DataType child_type;
    if (!data_type_to_property_type(array.component_type(), child_type)) {
      LOG(ERROR) << "Failed to parse array component type";
      return false;
    }
    auto fixed_length = array.fixed_length();
    if (fixed_length == 0) {
      LOG(ERROR) << "Array fixed_length must be greater than 0: "
                 << data_type.DebugString();
      return false;
    }
    out_type = DataType::Array(child_type, fixed_length);
    return true;
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

bool default_expression_to_value(const DataType& type,
                                 const common::Expression& expression,
                                 execution::Value& out_value) {
  try {
    auto expr = execution::parse_expression(
        expression, execution::ContextMeta{}, execution::VarType::kRecord);
    if (!expr) {
      LOG(ERROR) << "Failed to parse default expression: "
                 << expression.DebugString();
      return false;
    }
    auto bound_expr = expr->bind(nullptr, execution::ParamsMap{});
    if (!bound_expr) {
      LOG(ERROR) << "Failed to bind default expression: "
                 << expression.DebugString();
      return false;
    }

    execution::DataChunk empty_chunk;
    out_value = bound_expr->Cast<execution::RecordExprBase>().eval_record(
        empty_chunk, 0);
  } catch (const std::exception& e) {
    LOG(ERROR) << "Failed to evaluate default expression: "
               << expression.DebugString() << ", reason: " << e.what();
    return false;
  } catch (...) {
    LOG(ERROR) << "Failed to evaluate default expression: "
               << expression.DebugString();
    return false;
  }

  if (out_value.type() != type) {
    LOG(ERROR) << "Default expression type mismatch, expected "
               << type.ToString() << ", got " << out_value.type().ToString()
               << ": " << expression.DebugString();
    return false;
  }
  if (type.id() == DataTypeId::kVarchar) {
    size_t max_length = STRING_DEFAULT_MAX_LENGTH;
    if (type.getExtraTypeInfo()) {
      max_length = type.getExtraTypeInfo()->Cast<StringTypeInfo>().max_length;
    }
    if (max_length <= std::numeric_limits<uint16_t>::max()) {
      out_value =
          execution::Value::VARCHAR(execution::StringValue::Get(out_value),
                                    static_cast<uint16_t>(max_length));
    }
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
    DataType type;
    if (!data_type_to_property_type(property.type(), type)) {
      RETURN_ERROR(Status(StatusCode::ERR_INVALID_ARGUMENT,
                          "Invalid property type: " + property.DebugString()));
    }

    if (property.has_default_expr()) {
      if (!default_expression_to_value(type, property.default_expr(),
                                       default_value)) {
        RETURN_ERROR(
            Status(StatusCode::ERR_INVALID_ARGUMENT,
                   "Invalid default value: " + property.DebugString()));
      } else {
        VLOG(10) << "Default value convert to any success:"
                 << property.default_expr().DebugString();
      }
    } else {
      default_value = get_default_value(type);

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
    THROW_INVALID_ARGUMENT_EXCEPTION("invalid action: " +
                                     std::to_string(static_cast<int>(action)));
    return false;  // to suppress warning
  }
}

}  // namespace neug
