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
#include <glog/logging.h>               // for LOG, LogMessage, COMP...
#include <stdint.h>                     // for uint16_t
#include <limits>                       // for numeric_limits
#include <ostream>                      // for operator<<, basic_ost...
#include <utility>                      // for move, __tuple_element_t
#include "neug/utils/property/types.h"  // for PropertyType, Any
#include "neug/utils/result.h"          // for Result, Status, Statu...

namespace gs {

Any get_default_value(const PropertyType& type) {
  Any default_value;
  switch (type.type_enum) {
  case impl::PropertyTypeImpl::kEmpty:
    break;
  case impl::PropertyTypeImpl::kBool:
    default_value.set_bool(false);
    break;
  case impl::PropertyTypeImpl::kInt32:
    default_value.set_i32(0);
    break;
  case impl::PropertyTypeImpl::kUInt32:
    default_value.set_u32(0);
    break;
  case impl::PropertyTypeImpl::kInt64:
    default_value.set_i64(0);
    break;
  case impl::PropertyTypeImpl::kUInt64:
    default_value.set_u64(0);
    break;
  case impl::PropertyTypeImpl::kFloat:
    default_value.set_float(0.0f);
    break;
  case impl::PropertyTypeImpl::kDouble:
    default_value.set_double(0.0);
    break;
  default:
    THROW_NOT_SUPPORTED_EXCEPTION(
        "Unsupported property type for default value: " + type.ToString());
  }
  return default_value;
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
    const common::PrimitiveType& primitive_type, PropertyType& out_type) {
  switch (primitive_type) {
  case common::PrimitiveType::DT_ANY:
    LOG(ERROR) << "Any type is not supported";
    return false;
  case common::PrimitiveType::DT_SIGNED_INT32:
    out_type = PropertyType::Int32();
    break;
  case common::PrimitiveType::DT_UNSIGNED_INT32:
    out_type = PropertyType::UInt32();
    break;
  case common::PrimitiveType::DT_SIGNED_INT64:
    out_type = PropertyType::Int64();
    break;
  case common::PrimitiveType::DT_UNSIGNED_INT64:
    out_type = PropertyType::UInt64();
    break;
  case common::PrimitiveType::DT_BOOL:
    out_type = PropertyType::Bool();
    break;
  case common::PrimitiveType::DT_FLOAT:
    out_type = PropertyType::Float();
    break;
  case common::PrimitiveType::DT_DOUBLE:
    out_type = PropertyType::Double();
    break;
  case common::PrimitiveType::DT_NULL:
    out_type = PropertyType::Empty();
    break;
  default:
    LOG(ERROR) << "Unknown primitive type: " << primitive_type;
    return false;
  }
  return true;
}

bool string_type_to_property_type(const common::String& string_type,
                                  PropertyType& out_type) {
  switch (string_type.item_case()) {
  case common::String::kVarChar: {
    // Take care of the casting from uint32_t to uint16_t
    if (string_type.var_char().max_length() >
        std::numeric_limits<uint16_t>::max()) {
      LOG(WARNING) << "VarChar max length exceeds uint16_t limit, "
                   << "using max uint16_t value instead.";
      out_type = PropertyType::Varchar(std::numeric_limits<uint16_t>::max());
    } else {
      out_type = PropertyType::Varchar(string_type.var_char().max_length());
      break;
    }
  }
  case common::String::kLongText: {
    out_type = PropertyType::StringView();
    break;
  }
  case common::String::kChar: {
    // Currently, we implement fixed-char as varchar with fixed length.
    out_type = PropertyType::Varchar(string_type.char_().fixed_length());
    break;
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
                                    PropertyType& out_type) {
  switch (temporal_type.item_case()) {
  case common::Temporal::kDate32:
    out_type = PropertyType::Date();
    break;
  case common::Temporal::kDateTime:
    out_type = PropertyType::DateTime();
    break;
  case common::Temporal::kTimestamp:
    out_type = PropertyType::Timestamp();
    break;
  case common::Temporal::kDate:
    // TODO: Parse format
    out_type = PropertyType::Date();
    break;
  case common::Temporal::kInterval:
    out_type = PropertyType::Interval();
    break;
  default:
    LOG(ERROR) << "Unknown temporal type: " << temporal_type.DebugString();
    return false;
  }
  return true;
}

bool data_type_to_property_type(const common::DataType& data_type,
                                PropertyType& out_type) {
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

bool common_value_to_any(const common::Value& value, Any& out_any) {
  if (value.item_case() == common::Value::ITEM_NOT_SET) {
    LOG(ERROR) << "Value is not set: " << value.DebugString();
    return false;
  }
  switch (value.item_case()) {
  case common::Value::kBoolean:
    out_any.set_bool(value.boolean());
    break;
  case common::Value::kI32:
    out_any.set_i32(value.i32());
    break;
  case common::Value::kI64:
    out_any.set_i64(value.i64());
    break;
  case common::Value::kF64:
    out_any.set_double(value.f64());
    break;
  case common::Value::kStr:
    out_any.set_string(value.str());
    break;
  case common::Value::kDate:
    LOG(ERROR) << "Date type is not supported";
    return false;
  case common::Value::kU32:
    out_any.set_u32(value.u32());
    break;
  case common::Value::kU64:
    out_any.set_u64(value.u64());
    break;
  default:
    LOG(ERROR) << "Unknown value type: " << value.DebugString();
    return false;
  }
  return true;
}

Result<std::vector<std::tuple<PropertyType, std::string, Any>>>
property_defs_to_tuple(
    const google::protobuf::RepeatedPtrField<physical::PropertyDef>&
        properties) {
  std::vector<std::tuple<PropertyType, std::string, Any>> result;
  for (const auto& property : properties) {
    std::tuple<PropertyType, std::string, Any> tuple;
    std::get<1>(tuple) = property.name();
    if (!data_type_to_property_type(property.type(), std::get<0>(tuple))) {
      return Result<std::vector<std::tuple<PropertyType, std::string, Any>>>(
          Status(StatusCode::ERR_INVALID_ARGUMENT,
                 "Invalid property type: " + property.DebugString()));
    }
    if (property.has_default_value()) {
      if (!common_value_to_any(property.default_value(), std::get<2>(tuple))) {
        return Result<std::vector<std::tuple<PropertyType, std::string, Any>>>(
            Status(StatusCode::ERR_INVALID_ARGUMENT,
                   "Invalid default value: " + property.DebugString()));
      } else {
        // Use default default value if it is not set
        VLOG(1) << "Default value specified by plan, use system default "
                   "value";
        std::get<2>(tuple) = get_default_value(std::get<0>(tuple));
      }
    }
    result.emplace_back(std::move(tuple));
  }
  return Result<std::vector<std::tuple<PropertyType, std::string, Any>>>(
      Status(StatusCode::OK), std::move(result));
}

// Convert to a bool representing error_on_conflict.
bool conflict_action_to_bool(const physical::ConflictAction& action) {
  if (action == physical::ConflictAction::ON_CONFLICT_THROW) {
    return true;
  } else if (action == physical::ConflictAction::ON_CONFLICT_DO_NOTHING) {
    return false;
  } else {
    LOG(FATAL) << "invalid action: " << action;
  }
}

Any const_value_to_any(const common::Value& value) {
  switch (value.item_case()) {
  case common::Value::ItemCase::kI32: {
    return Any::From(value.i32());
  }
  case common::Value::ItemCase::kI64: {
    return Any::From(value.i64());
  }
  case common::Value::ItemCase::kU32: {
    return Any::From(value.u32());
  }
  case common::Value::ItemCase::kU64: {
    return Any::From(value.u64());
  }
  case common::Value::ItemCase::kF64: {
    return Any::From(value.f64());
  }
  case common::Value::ItemCase::kF32: {
    return Any::From(value.f32());
  }
  case common::Value::ItemCase::kBoolean: {
    return Any::From(value.boolean());
  }
  case common::Value::ItemCase::kStr: {
    return Any::From(value.str());
  }
  default: {
    THROW_RUNTIME_ERROR("Unsupported constant value type: " +
                        value.DebugString());
  }
  }
}

Any expr_opr_value_to_any(const common::ExprOpr& value) {
  switch (value.item_case()) {
  case common::ExprOpr::ItemCase::kConst: {
    return const_value_to_any(value.const_());
  }
  case common::ExprOpr::ItemCase::kToDate: {
    return Any::From(Date(value.to_date().date_str()));
  }
  case common::ExprOpr::ItemCase::kToDatetime: {
    return Any::From(DateTime(value.to_datetime().datetime_str()));
  }
  case common::ExprOpr::ItemCase::kToInterval: {
    return Any::From(Interval(value.to_interval().interval_str()));
  }
  default: {
    THROW_RUNTIME_ERROR("Unsupported ExprOpr value type: " +
                        value.DebugString());
  }
  }
}

}  // namespace gs
