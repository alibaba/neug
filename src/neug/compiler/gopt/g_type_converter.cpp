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

#include "neug/compiler/gopt/g_type_converter.h"

#include <google/protobuf/wrappers.pb.h>
#include <memory>
#include "neug/compiler/binder/expression/node_expression.h"
#include "neug/compiler/binder/expression/rel_expression.h"
#include "neug/compiler/catalog/catalog_entry/node_table_catalog_entry.h"
#include "neug/compiler/common/cast.h"
#include "neug/compiler/common/types/types.h"
#include "neug/compiler/gopt/g_constants.h"
#include "neug/compiler/gopt/g_graph_type.h"
#include "neug/compiler/gopt/g_rel_table_entry.h"
#include "neug/proto_generated_gie/basic_type.pb.h"
#include "neug/proto_generated_gie/common.pb.h"
#include "neug/proto_generated_gie/type.pb.h"
#include "neug/utils/exception/exception.h"

namespace gs {
namespace gopt {

std::unique_ptr<::common::IrDataType> GTypeConverter::convertNodeType(
    const GNodeType& nodeType) {
  auto result = std::make_unique<::common::IrDataType>();
  auto graphType = std::make_unique<::common::GraphDataType>();
  for (auto nodeTable : nodeType.nodeTables) {
    auto elementType = convertNodeTable(nodeTable);
    graphType->mutable_graph_data_type()->AddAllocated(elementType.release());
  }
  graphType->set_element_opt(::common::GraphDataType::GraphElementOpt::
                                 GraphDataType_GraphElementOpt_VERTEX);
  result->set_allocated_graph_type(graphType.release());
  return result;
}

std::unique_ptr<::common::IrDataType> GTypeConverter::convertRelType(
    const GRelType& relType) {
  auto result = std::make_unique<::common::IrDataType>();
  auto graphType = std::make_unique<::common::GraphDataType>();
  for (auto relTable : relType.relTables) {
    auto elementType = convertRelTable(relTable);
    graphType->mutable_graph_data_type()->AddAllocated(elementType.release());
  }
  graphType->set_element_opt(::common::GraphDataType_GraphElementOpt::
                                 GraphDataType_GraphElementOpt_EDGE);
  result->set_allocated_graph_type(graphType.release());
  return result;
}

std::unique_ptr<::common::IrDataType> GTypeConverter::convertArrayType(
    const common::LogicalType& type, const binder::Expression& expr) {
  auto arrayType = std::make_unique<::common::Array>();
  LOG(INFO) << "Converting ARRAY child type: " << type.toString();
  auto childType = convertLogicalType(type, expr);
  if (!childType) {
    throw exception::Exception("Failed to convert child type for ARRAY type: " +
                               type.toString());
  }
  if (!childType->has_data_type()) {
    throw exception::Exception(
        "Child type for ARRAY type must be a data type: " + type.toString());
  } else {
    // Otherwise, we can directly set the data type
    arrayType->mutable_component_type()->CopyFrom(childType->data_type());
  }
  auto result = std::make_unique<::common::IrDataType>();
  result->mutable_data_type()->set_allocated_array(arrayType.release());
  LOG(INFO) << "Converted ARRAY type: " << result->DebugString();
  return result;
}

std::unique_ptr<::common::IrDataType> GTypeConverter::convertLogicalType(
    const common::LogicalType& type, const binder::Expression& expr) {
  switch (type.getLogicalTypeID()) {
  case common::LogicalTypeID::NODE: {
    if (auto nodeExpr = expr.constPtrCast<binder::NodeExpression>()) {
      return convertNodeType(gopt::GNodeType(*nodeExpr));
    } else {
      throw exception::Exception(
          "Expected NodeExpression for NODE type, "
          "but got: " +
          expr.toString());
    }
    break;
  }
  case common::LogicalTypeID::REL:
  case common::LogicalTypeID::RECURSIVE_REL: {
    if (auto relExpr = expr.constPtrCast<binder::RelExpression>()) {
      return convertRelType(gopt::GRelType(*relExpr));
    } else {
      throw exception::Exception(
          "Expected RelExpression for REL type, "
          "but got: " +
          expr.toString());
    }
    break;
  }
  case common::LogicalTypeID::ARRAY: {
    auto extraTypeInfo = type.getExtraTypeInfo();
    CHECK(extraTypeInfo) << "Array type should have extra type info: " +
                                type.toString();
    auto const_off = const_cast<common::ExtraTypeInfo*>(extraTypeInfo);
    CHECK(const_off) << "Array type has null extra type info: " +
                            type.toString();
    auto array_type_info =
        gs::common::ku_dynamic_cast<gs::common::ArrayTypeInfo*>(const_off);
    CHECK(array_type_info) << "Expected ArrayTypeInfo for ARRAY type, ";
    auto& child_type = array_type_info->getChildType();
    return convertArrayType(child_type, expr);
    break;
  }
  case common::LogicalTypeID::LIST: {
    LOG(INFO) << "Converting LIST type: " << type.toString();
    auto extraTypeInfo = type.getExtraTypeInfo();
    CHECK(extraTypeInfo) << "List type should have extra type info: " +
                                type.toString();
    auto const_off = const_cast<common::ExtraTypeInfo*>(extraTypeInfo);
    CHECK(const_off) << "List type has null extra type info: " +
                            type.toString();
    auto list_type_info =
        gs::common::ku_dynamic_cast<gs::common::ListTypeInfo*>(const_off);
    CHECK(list_type_info) << "Expected ListTypeInfo for LIST type, ";
    auto& child_type = list_type_info->getChildType();
    return convertArrayType(child_type, expr);
    break;
  }
  default:
    // For other types, we can convert them directly
    LOG(INFO) << "Converting simple logical type: " << type.toString();
    return convertSimpleLogicalType(type);
  }
}

std::unique_ptr<::common::IrDataType> GTypeConverter::convertSimpleLogicalType(
    const common::LogicalType& type) {
  auto result = std::make_unique<::common::DataType>();
  switch (type.getLogicalTypeID()) {
  case common::LogicalTypeID::BOOL: {
    result->set_primitive_type(::common::PrimitiveType::DT_BOOL);
    break;
  }
  case common::LogicalTypeID::INT32: {
    result->set_primitive_type(::common::PrimitiveType::DT_SIGNED_INT32);
    break;
  }
  case common::LogicalTypeID::INT64: {
    result->set_primitive_type(::common::PrimitiveType::DT_SIGNED_INT64);
    break;
  }
  case common::LogicalTypeID::UINT32: {
    result->set_primitive_type(::common::PrimitiveType::DT_UNSIGNED_INT32);
    break;
  }
  case common::LogicalTypeID::UINT64: {
    result->set_primitive_type(::common::PrimitiveType::DT_UNSIGNED_INT64);
    break;
  }
  case common::LogicalTypeID::FLOAT: {
    result->set_primitive_type(::common::PrimitiveType::DT_FLOAT);
    break;
  }
  case common::LogicalTypeID::DOUBLE: {
    result->set_primitive_type(::common::PrimitiveType::DT_DOUBLE);
    break;
  }
  case common::LogicalTypeID::STRING: {
    auto strType = std::make_unique<::common::String>();
    auto varChar = std::make_unique<::common::String::VarChar>();
    varChar->set_max_length(gs::Constants::VARCHAR_MAX_LENGTH);
    strType->set_allocated_var_char(varChar.release());
    result->set_allocated_string(strType.release());
    break;
  }
  case common::LogicalTypeID::INTERNAL_ID: {
    result->set_primitive_type(::common::PrimitiveType::DT_SIGNED_INT64);
    break;
  }
  case common::LogicalTypeID::DATE32: {
    auto temporalType = std::make_unique<::common::Temporal>();
    temporalType->set_allocated_date32(new ::common::Temporal::Date32());
    result->set_allocated_temporal(temporalType.release());
    break;
  }
  case common::LogicalTypeID::TIMESTAMP64: {
    auto temporalType = std::make_unique<::common::Temporal>();
    temporalType->set_allocated_timestamp(new ::common::Temporal::Timestamp());
    result->set_allocated_temporal(temporalType.release());
    break;
  }
  case common::LogicalTypeID::DATE: {
    auto temporalType = std::make_unique<::common::Temporal>();
    auto date = std::make_unique<::common::Temporal_Date>();
    date->set_date_format(
        ::common::Temporal::DateFormat::Temporal_DateFormat_DF_YYYY_MM_DD);
    temporalType->set_allocated_date(date.release());
    result->set_allocated_temporal(temporalType.release());
    break;
  }
  case common::LogicalTypeID::TIMESTAMP: {
    auto temporalType = std::make_unique<::common::Temporal>();
    auto datetime = std::make_unique<::common::Temporal_DateTime>();
    datetime->set_date_time_format(
        ::common::Temporal::DateTimeFormat::
            Temporal_DateTimeFormat_DTF_YYYY_MM_DD_HH_MM_SS_SSS);
    datetime->set_time_zone_format(
        ::common::Temporal::TimeZoneFormat::Temporal_TimeZoneFormat_TZF_UTC);
    temporalType->set_allocated_date_time(datetime.release());
    result->set_allocated_temporal(temporalType.release());
    break;
  }
  case common::LogicalTypeID::INTERVAL: {
    auto temporalType = std::make_unique<::common::Temporal>();
    temporalType->set_allocated_interval(new ::common::Temporal::Interval());
    result->set_allocated_temporal(temporalType.release());
    break;
  }
  default:
    throw exception::Exception("Unsupported basic type for conversion: " +
                               type.toString());
  }
  auto irType = std::make_unique<::common::IrDataType>();
  irType->set_allocated_data_type(result.release());
  return irType;
}

std::unique_ptr<::common::GraphDataType::GraphElementType>
GTypeConverter::convertNodeTable(catalog::NodeTableCatalogEntry* nodeTable) {
  auto result = std::make_unique<::common::GraphDataType::GraphElementType>();
  auto labelType =
      std::make_unique<::common::GraphDataType::GraphElementLabel>();
  labelType->set_label(nodeTable->getTableID());
  result->set_allocated_label(labelType.release());
  // todo: set properties
  return result;
}

std::unique_ptr<::common::GraphDataType::GraphElementType>
GTypeConverter::convertRelTable(catalog::GRelTableCatalogEntry* relTable) {
  auto result = std::make_unique<::common::GraphDataType::GraphElementType>();
  auto labelType =
      std::make_unique<::common::GraphDataType::GraphElementLabel>();
  labelType->set_label(relTable->getLabelId());
  auto srcLabel = std::make_unique<google::protobuf::Int32Value>();
  srcLabel->set_value(relTable->getSrcTableID());
  labelType->set_allocated_src_label(srcLabel.release());
  auto dstLabel = std::make_unique<google::protobuf::Int32Value>();
  dstLabel->set_value(relTable->getDstTableID());
  labelType->set_allocated_dst_label(dstLabel.release());
  result->set_allocated_label(labelType.release());
  // todo: set properties
  return result;
}

}  // namespace gopt
}  // namespace gs