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

#include "neug/utils/reader/type_converter.h"

#include <arrow/type.h>
#include <arrow/type_fwd.h>
#include <glog/logging.h>

#include <memory>

#include "neug/generated/proto/plan/basic_type.pb.h"
#include "neug/utils/exception/exception.h"

namespace neug {
namespace reader {

std::shared_ptr<arrow::DataType> ArrowTypeConverter::convert(
    const ::common::DataType& type) {
  switch (type.item_case()) {
  case ::common::DataType::kPrimitiveType: {
    // Handle primitive types
    switch (type.primitive_type()) {
    case ::common::PrimitiveType::DT_BOOL:
      return arrow::boolean();
    case ::common::PrimitiveType::DT_SIGNED_INT32:
      return arrow::int32();
    case ::common::PrimitiveType::DT_UNSIGNED_INT32:
      return arrow::uint32();
    case ::common::PrimitiveType::DT_SIGNED_INT64:
      return arrow::int64();
    case ::common::PrimitiveType::DT_UNSIGNED_INT64:
      return arrow::uint64();
    case ::common::PrimitiveType::DT_FLOAT:
      return arrow::float32();
    case ::common::PrimitiveType::DT_DOUBLE:
      return arrow::float64();
    default:
      THROW_CONVERSION_EXCEPTION(
          "Unsupported PrimitiveType: " +
          std::to_string(static_cast<int>(type.primitive_type())) +
          ". Only BOOL, INT32, UINT32, INT64, UINT64, FLOAT, DOUBLE are "
          "supported.");
    }
  }
  case ::common::DataType::kString: {
    // Handle string types (all string variants map to utf8)
    return arrow::large_utf8();
  }
  case ::common::DataType::kTemporal: {
    // Handle temporal types
    const auto& temporal = type.temporal();
    switch (temporal.item_case()) {
    case ::common::Temporal::kDate32:
      return arrow::date32();
    case ::common::Temporal::kDate:
      return arrow::date64();
    case ::common::Temporal::kDateTime:
    case ::common::Temporal::kTimestamp:
      return arrow::timestamp(arrow::TimeUnit::MILLI);
    case ::common::Temporal::kInterval:
      return arrow::large_utf8();  // Keep consistent with the implementation
                                   // used by storage to parse this type.
    default:
      THROW_CONVERSION_EXCEPTION(
          "Unsupported Temporal type. Only DATE, TIMESTAMP, INTERVAL are "
          "supported.");
    }
  }
  case ::common::DataType::kArray: {
    // Handle array type
    const auto& array = type.array();
    auto componentType = convert(array.component_type());
    if (!componentType) {
      THROW_CONVERSION_EXCEPTION(
          "Failed to convert ARRAY component type to Arrow DataType");
    }
    // Use fixed_size_list if max_length is set, otherwise use list
    if (array.max_length() > 0) {
      return arrow::fixed_size_list(componentType,
                                    static_cast<int32_t>(array.max_length()));
    } else {
      return arrow::list(componentType);
    }
  }
  case ::common::DataType::kMap: {
    // Handle map type
    const auto& map = type.map();
    auto keyType = convert(map.key_type());
    auto valueType = convert(map.value_type());
    if (!keyType) {
      THROW_CONVERSION_EXCEPTION(
          "Failed to convert MAP key type to Arrow DataType");
    }
    if (!valueType) {
      THROW_CONVERSION_EXCEPTION(
          "Failed to convert MAP value type to Arrow DataType");
    }
    return arrow::map(keyType, valueType);
  }
  case ::common::DataType::kDecimal:
  case ::common::DataType::ITEM_NOT_SET:
  default:
    THROW_CONVERSION_EXCEPTION(
        "Unsupported DataType. Only ARRAY, MAP, and basic types (BOOL, INT32, "
        "UINT32, INT64, UINT64, FLOAT, DOUBLE, STRING, DATE, TIMESTAMP, "
        "INTERVAL) are supported.");
  }
}

std::shared_ptr<::common::DataType> ArrowTypeConverter::convert(
    const arrow::DataType& arrowType) {
  auto commonType = std::make_shared<::common::DataType>();

  // Handle primitive types
  switch (arrowType.id()) {
  case arrow::Type::BOOL:
    commonType->set_primitive_type(::common::PrimitiveType::DT_BOOL);
    break;

  case arrow::Type::INT8:
  case arrow::Type::INT16:
  case arrow::Type::INT32:
    commonType->set_primitive_type(::common::PrimitiveType::DT_SIGNED_INT32);
    break;

  case arrow::Type::UINT8:
  case arrow::Type::UINT16:
  case arrow::Type::UINT32:
    commonType->set_primitive_type(::common::PrimitiveType::DT_UNSIGNED_INT32);
    break;

  case arrow::Type::INT64:
    commonType->set_primitive_type(::common::PrimitiveType::DT_SIGNED_INT64);
    break;

  case arrow::Type::UINT64:
    commonType->set_primitive_type(::common::PrimitiveType::DT_UNSIGNED_INT64);
    break;

  case arrow::Type::FLOAT:
    commonType->set_primitive_type(::common::PrimitiveType::DT_FLOAT);
    break;

  case arrow::Type::DOUBLE:
    commonType->set_primitive_type(::common::PrimitiveType::DT_DOUBLE);
    break;

  case arrow::Type::STRING:
  case arrow::Type::LARGE_STRING:
    commonType->mutable_string()->mutable_var_char();
    break;

  case arrow::Type::DATE32:
  case arrow::Type::DATE64: {
    auto* temporal = commonType->mutable_temporal();
    temporal->mutable_date();
    break;
  }

  case arrow::Type::TIMESTAMP: {
    auto* temporal = commonType->mutable_temporal();
    temporal->mutable_timestamp();
    break;
  }

  case arrow::Type::DURATION: {
    auto* temporal = commonType->mutable_temporal();
    temporal->mutable_interval();
    break;
  }

  case arrow::Type::LIST:
  case arrow::Type::LARGE_LIST: {
    auto* array = commonType->mutable_array();
    auto listType = static_cast<const arrow::ListType*>(&arrowType);
    auto componentType = convert(*listType->value_type());
    if (!componentType) {
      THROW_CONVERSION_EXCEPTION(
          "Failed to convert ARRAY component type from Arrow DataType");
    }
    *array->mutable_component_type() = *componentType;
    break;
  }

  case arrow::Type::FIXED_SIZE_LIST: {
    auto* array = commonType->mutable_array();
    auto fixedListType =
        static_cast<const arrow::FixedSizeListType*>(&arrowType);
    auto componentType = convert(*fixedListType->value_type());
    if (!componentType) {
      THROW_CONVERSION_EXCEPTION(
          "Failed to convert FIXED_SIZE_LIST component type from Arrow "
          "DataType");
    }
    *array->mutable_component_type() = *componentType;
    array->set_max_length(fixedListType->list_size());
    break;
  }

  case arrow::Type::MAP: {
    auto* map = commonType->mutable_map();
    auto mapType = static_cast<const arrow::MapType*>(&arrowType);
    auto keyType = convert(*mapType->key_type());
    auto valueType = convert(*mapType->item_type());
    if (!keyType) {
      THROW_CONVERSION_EXCEPTION(
          "Failed to convert MAP key type from Arrow DataType");
    }
    if (!valueType) {
      THROW_CONVERSION_EXCEPTION(
          "Failed to convert MAP value type from Arrow DataType");
    }
    *map->mutable_key_type() = *keyType;
    *map->mutable_value_type() = *valueType;
    break;
  }

  default:
    LOG(WARNING) << "Unsupported Arrow type: " << arrowType.ToString()
                 << ", defaulting to string";
    // Default to string type for unsupported types
    commonType->mutable_string()->mutable_var_char();
    break;
  }

  return commonType;
}

}  // namespace reader
}  // namespace neug
