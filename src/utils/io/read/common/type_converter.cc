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

#include "neug/utils/io/read/common/type_converter.h"

#include <memory>

#include "neug/generated/proto/plan/basic_type.pb.h"
#include "neug/utils/exception/exception.h"

namespace neug {
namespace reader {

DataType NeuGTypeConverter::convert(const ::common::DataType& type) const {
  switch (type.item_case()) {
  case ::common::DataType::kPrimitiveType: {
    switch (type.primitive_type()) {
    case ::common::PrimitiveType::DT_BOOL:
      return DataType(DataTypeId::kBoolean);
    case ::common::PrimitiveType::DT_SIGNED_INT32:
      return DataType(DataTypeId::kInt32);
    case ::common::PrimitiveType::DT_UNSIGNED_INT32:
      return DataType(DataTypeId::kUInt32);
    case ::common::PrimitiveType::DT_SIGNED_INT64:
      return DataType(DataTypeId::kInt64);
    case ::common::PrimitiveType::DT_UNSIGNED_INT64:
      return DataType(DataTypeId::kUInt64);
    case ::common::PrimitiveType::DT_FLOAT:
      return DataType(DataTypeId::kFloat);
    case ::common::PrimitiveType::DT_DOUBLE:
      return DataType(DataTypeId::kDouble);
    default:
      THROW_CONVERSION_EXCEPTION(
          "Unsupported PrimitiveType for NeuG conversion: " +
          std::to_string(static_cast<int>(type.primitive_type())));
    }
  }
  case ::common::DataType::kString:
    return DataType(DataTypeId::kVarchar);
  case ::common::DataType::kTemporal: {
    const auto& temporal = type.temporal();
    switch (temporal.item_case()) {
    case ::common::Temporal::kDate32:
    case ::common::Temporal::kDate:
      return DataType(DataTypeId::kDate);
    case ::common::Temporal::kDateTime:
    case ::common::Temporal::kTimestamp:
      return DataType(DataTypeId::kTimestampMs);
    case ::common::Temporal::kInterval:
      return DataType(DataTypeId::kInterval);
    default:
      THROW_CONVERSION_EXCEPTION("Unsupported Temporal type for NeuG conversion");
    }
  }
  default:
    THROW_CONVERSION_EXCEPTION("Unsupported DataType for NeuG conversion");
  }
}

std::shared_ptr<::common::DataType> NeuGTypeConverter::convert(
    const DataType& type) const {
  return inferCommonType(type);
}

std::shared_ptr<::common::DataType> NeuGTypeConverter::inferCommonType(
    const DataType& type) const {
  auto commonType = std::make_shared<::common::DataType>();
  switch (type.id()) {
  case DataTypeId::kBoolean:
    commonType->set_primitive_type(::common::PrimitiveType::DT_BOOL);
    break;
  case DataTypeId::kInt32:
    commonType->set_primitive_type(::common::PrimitiveType::DT_SIGNED_INT32);
    break;
  case DataTypeId::kUInt32:
    commonType->set_primitive_type(::common::PrimitiveType::DT_UNSIGNED_INT32);
    break;
  case DataTypeId::kInt64:
    commonType->set_primitive_type(::common::PrimitiveType::DT_SIGNED_INT64);
    break;
  case DataTypeId::kUInt64:
    commonType->set_primitive_type(::common::PrimitiveType::DT_UNSIGNED_INT64);
    break;
  case DataTypeId::kFloat:
    commonType->set_primitive_type(::common::PrimitiveType::DT_FLOAT);
    break;
  case DataTypeId::kDouble:
    commonType->set_primitive_type(::common::PrimitiveType::DT_DOUBLE);
    break;
  case DataTypeId::kVarchar: {
    auto strType = std::make_unique<::common::String>();
    auto varChar = std::make_unique<::common::String::VarChar>();
    strType->set_allocated_var_char(varChar.release());
    commonType->set_allocated_string(strType.release());
    break;
  }
  case DataTypeId::kDate: {
    auto temporal = std::make_unique<::common::Temporal>();
    temporal->mutable_date();
    commonType->set_allocated_temporal(temporal.release());
    break;
  }
  case DataTypeId::kTimestampMs: {
    auto temporal = std::make_unique<::common::Temporal>();
    temporal->mutable_timestamp();
    commonType->set_allocated_temporal(temporal.release());
    break;
  }
  case DataTypeId::kInterval: {
    auto temporal = std::make_unique<::common::Temporal>();
    temporal->mutable_interval();
    commonType->set_allocated_temporal(temporal.release());
    break;
  }
  default:
    THROW_CONVERSION_EXCEPTION("Unsupported NeuG DataType for common conversion");
  }
  return commonType;
}

}  // namespace reader
}  // namespace neug
