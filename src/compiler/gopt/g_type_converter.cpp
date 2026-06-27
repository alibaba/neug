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
#include <cstdint>
#include <limits>
#include <memory>
#include <vector>
#include "neug/compiler/binder/expression/case_expression.h"
#include "neug/compiler/binder/expression/expression.h"
#include "neug/compiler/binder/expression/node_expression.h"
#include "neug/compiler/binder/expression/rel_expression.h"
#include "neug/compiler/binder/expression/scalar_function_expression.h"
#include "neug/compiler/catalog/catalog_entry/node_table_catalog_entry.h"
#include "neug/compiler/common/cast.h"
#include "neug/compiler/common/enums/expression_type.h"
#include "neug/compiler/common/types/types.h"
#include "neug/compiler/gopt/g_constants.h"
#include "neug/compiler/gopt/g_graph_type.h"
#include "neug/compiler/gopt/g_rel_table_entry.h"
#include "neug/compiler/gopt/g_scalar_type.h"
#include "neug/compiler/gopt/g_type_utils.h"
#include "neug/generated/proto/plan/basic_type.pb.h"
#include "neug/generated/proto/plan/common.pb.h"
#include "neug/generated/proto/plan/type.pb.h"
#include "neug/utils/exception/exception.h"

namespace neug {
namespace gopt {

std::unique_ptr<::common::IrDataType> GPhysicalTypeConverter::convertNodeType(
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

std::unique_ptr<::common::IrDataType> GPhysicalTypeConverter::convertPathType(
    const GRelType& relType) {
  auto result = std::make_unique<::common::IrDataType>();
  auto graphType = std::make_unique<::common::GraphDataType>();
  for (auto relTable : relType.relTables) {
    auto elementType = convertRelTable(relTable);
    graphType->mutable_graph_data_type()->AddAllocated(elementType.release());
  }
  graphType->set_element_opt(::common::GraphDataType_GraphElementOpt::
                                 GraphDataType_GraphElementOpt_PATH);
  result->set_allocated_graph_type(graphType.release());
  return result;
}

std::unique_ptr<::common::IrDataType> GPhysicalTypeConverter::convertRelType(
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

const binder::Expression* childFunction(const binder::Expression* curExpr,
                                        ScalarType targetType) {
  if (!curExpr)
    return nullptr;
  if (curExpr->expressionType == common::ExpressionType::FUNCTION) {
    auto& scalarExpr = curExpr->constCast<binder::ScalarFunctionExpression>();
    GScalarType scalarType{scalarExpr};
    if (scalarType.getType() == targetType) {
      return curExpr;
    }
  }
  binder::expression_vector children;
  if (curExpr->expressionType == common::ExpressionType::CASE_ELSE) {
    auto& caseExpr = curExpr->constCast<binder::CaseExpression>();
    for (size_t pos = 0; pos < caseExpr.getNumCaseAlternatives(); ++pos) {
      auto alternative = caseExpr.getCaseAlternative(pos);
      children.push_back(alternative->thenExpression);
      children.push_back(alternative->whenExpression);
    }
    children.push_back(caseExpr.getElseExpression());
  } else {
    children = curExpr->getChildren();
  }
  for (auto child : children) {
    auto result = childFunction(child.get(), targetType);
    if (result)
      return result;
  }
  return nullptr;
}

std::unique_ptr<::common::IrDataType> GPhysicalTypeConverter::convertStructType(
    const neug::DataType& type) {
  auto& fieldNames = common::StructType::GetFieldNames(type);
  auto& childTypes = common::StructType::GetChildTypes(type);
  auto tupleType = std::make_unique<::common::Tuple>();
  for (size_t i = 0; i < fieldNames.size(); i++) {
    auto childType = convertLogicalType(childTypes[i]);
    if (!childType) {
      THROW_EXCEPTION_WITH_FILE_LINE(
          "Failed to convert child type for TUPLE type: " + type.ToString());
    }
    if (!childType->has_data_type()) {
      THROW_EXCEPTION_WITH_FILE_LINE(
          "Component type of TUPLE should be basic, others are unsupported");
    }
    // Otherwise, we can directly set the data type
    tupleType->add_component_types()->CopyFrom(childType->data_type());
  }
  auto result = std::make_unique<::common::IrDataType>();
  result->mutable_data_type()->set_allocated_tuple(tupleType.release());
  return result;
}

std::unique_ptr<::common::IrDataType> GPhysicalTypeConverter::convertArrayType(
    const neug::DataType& type, uint64_t fixedLength) {
  auto result = std::make_unique<::common::IrDataType>();
  VLOG(1) << "Converting ARRAY child type: " << type.ToString();
  auto childType = convertLogicalType(type);
  if (!childType) {
    THROW_EXCEPTION_WITH_FILE_LINE(
        "Failed to convert child type for ARRAY type: " + type.ToString());
  }
  if (childType->has_graph_type()) {
    auto listType = std::make_unique<::common::GraphTypeList>();
    listType->set_allocated_component_type(childType->release_graph_type());
    result->set_allocated_list_type(listType.release());
  } else if (childType->has_data_type()) {
    auto arrayType = std::make_unique<::common::Array>();
    arrayType->set_allocated_component_type(childType->release_data_type());
    arrayType->set_fixed_length(fixedLength);
    result->mutable_data_type()->set_allocated_array(arrayType.release());
  } else {
    LOG(WARNING) << "Component type of Array should be basic or graph element, "
                    "others are "
                    "unsupported, return ANY instead.";
    result->mutable_data_type()->set_primitive_type(
        ::common::PrimitiveType::DT_ANY);
  }
  VLOG(1) << "Converted ARRAY type: " << result->DebugString();
  return result;
}

std::unique_ptr<::common::IrDataType> GPhysicalTypeConverter::convertListType(
    const neug::DataType& type) {
  auto result = std::make_unique<::common::IrDataType>();
  VLOG(1) << "Converting LIST child type: " << type.ToString();
  auto childType = convertLogicalType(type);
  if (!childType) {
    THROW_EXCEPTION_WITH_FILE_LINE(
        "Failed to convert child type for LIST type: " + type.ToString());
  }
  if (childType->has_graph_type()) {
    auto listType = std::make_unique<::common::GraphTypeList>();
    listType->set_allocated_component_type(childType->release_graph_type());
    result->set_allocated_list_type(listType.release());
  } else if (childType->has_data_type()) {
    auto listType = std::make_unique<::common::List>();
    listType->set_allocated_component_type(childType->release_data_type());
    result->mutable_data_type()->set_allocated_list(listType.release());
  } else {
    LOG(WARNING) << "Component type of List should be basic or graph element, "
                    "others are unsupported, return ANY instead.";
    result->mutable_data_type()->set_primitive_type(
        ::common::PrimitiveType::DT_ANY);
  }
  VLOG(1) << "Converted LIST type: " << result->DebugString();
  return result;
}

GNodeType* convertGNodeType(const neug::DataType& type) {
  auto extraTypeInfo = type.getExtraTypeInfo();
  if (!extraTypeInfo) {
    return nullptr;
  }
  auto nodeTypeInfo =
      dynamic_cast<const neug::common::GNodeTypeInfo*>(extraTypeInfo);
  if (!nodeTypeInfo || !nodeTypeInfo->getNodeType()) {
    return nullptr;
  }
  return nodeTypeInfo->getNodeType();
}

GRelType* convertGRelType(const neug::DataType& type) {
  auto extraTypeInfo = type.getExtraTypeInfo();
  if (!extraTypeInfo) {
    return nullptr;
  }
  auto relTypeInfo =
      dynamic_cast<const neug::common::GRelTypeInfo*>(extraTypeInfo);
  if (!relTypeInfo || !relTypeInfo->getRelType()) {
    return nullptr;
  }
  return relTypeInfo->getRelType();
}

std::unique_ptr<::common::IrDataType>
GPhysicalTypeConverter::convertLogicalType(const neug::DataType& type) {
  switch (type.id()) {
  case common::DataTypeId::kVertex: {
    auto gNodeType = convertGNodeType(type);
    if (gNodeType) {
      return convertNodeType(*gNodeType);
    } else {
      LOG(WARNING) << "Expected NodeType for NODE type, "
                   << "but got: " << type.ToString()
                   << " , return NODE type with empty label";
      return convertNodeType(gopt::GNodeType({}));
    }
    break;
  }
  case common::DataTypeId::kEdge: {
    auto gRelType = convertGRelType(type);
    if (gRelType) {
      return convertRelType(*gRelType);
    } else {
      LOG(WARNING) << "Expected RelType for REL type, "
                   << "but got: " << type.ToString()
                   << " , return REL type with empty label";
      return convertRelType(gopt::GRelType({}));
    }
    break;
  }
  case common::DataTypeId::kPath: {
    if (common::getPhysicalType(type.id()) != common::PhysicalTypeID::STRUCT) {
      LOG(WARNING) << "Expected StructType for RECURSIVE_REL type, "
                   << "but got: " << type.ToString()
                   << " , return RECURSIVE_REL type with empty label";
      return convertPathType(gopt::GRelType({}));
    }
    auto fieldIdx =
        common::StructType::GetFieldIdx(type, common::InternalKeyword::RELS);
    if (fieldIdx == common::INVALID_STRUCT_FIELD_IDX) {
      LOG(WARNING) << "Expected RELS field for RECURSIVE_REL type, "
                   << "but got: " << type.ToString()
                   << " , return RECURSIVE_REL type with empty label";
      return convertPathType(gopt::GRelType({}));
    }

    const auto& relsType = common::StructType::GetChildType(type, fieldIdx);
    if (common::getPhysicalType(relsType.id()) ==
        common::PhysicalTypeID::LIST) {
      auto& childType = common::ListType::GetChildType(relsType);
      auto gRelType = convertGRelType(childType);
      if (gRelType) {
        return convertPathType(*gRelType);
      } else {
        LOG(WARNING) << "Expected RelType for RECURSIVE_REL type, "
                     << "but got: " << childType.ToString()
                     << " , return RECURSIVE_REL type with empty label";
        return convertPathType(gopt::GRelType({}));
      }
    } else if (common::getPhysicalType(relsType.id()) ==
               common::PhysicalTypeID::ARRAY) {
      auto& childType = common::ArrayType::GetChildType(relsType);
      auto gRelType = convertGRelType(childType);
      if (gRelType) {
        return convertPathType(*gRelType);
      } else {
        LOG(WARNING) << "Expected RelType for RECURSIVE_REL type, "
                     << "but got: " << childType.ToString()
                     << " , return RECURSIVE_REL type with empty label";
        return convertPathType(gopt::GRelType({}));
      }
    } else {
      LOG(WARNING) << "Expected ListType or ArrayType for RECURSIVE_REL type, "
                   << "but got: " << relsType.ToString()
                   << " , return RECURSIVE_REL type with empty label";
      return convertPathType(gopt::GRelType({}));
    }
    break;
  }
  case common::DataTypeId::kArray: {
    auto& child_type = common::ArrayType::GetChildType(type);
    uint64_t fixedLength = common::ArrayType::GetNumElements(type);
    if (fixedLength >
        static_cast<uint64_t>(std::numeric_limits<uint32_t>::max())) {
      THROW_EXCEPTION_WITH_FILE_LINE(
          "Fixed length of ARRAY type is too large: " +
          std::to_string(fixedLength));
    }
    return convertArrayType(child_type, fixedLength);
  }
  case common::DataTypeId::kList: {
    VLOG(1) << "Converting LIST type: " << type.ToString();
    auto& child_type = common::ListType::GetChildType(type);
    return convertListType(child_type);
  }
  case common::DataTypeId::kStruct: {
    return convertStructType(type);
    break;
  }
  default:
    // For other types, we can convert them directly
    VLOG(1) << "Converting simple logical type: " << type.ToString();
    return convertSimpleLogicalType(type);
  }
}

std::unique_ptr<::common::IrDataType>
GPhysicalTypeConverter::convertSimpleLogicalType(const neug::DataType& type) {
  auto result = std::make_unique<::common::DataType>();
  switch (type.id()) {
  case common::DataTypeId::kUnknown: {
    result->set_primitive_type(::common::PrimitiveType::DT_ANY);
    break;
  }
  case common::DataTypeId::kBoolean: {
    result->set_primitive_type(::common::PrimitiveType::DT_BOOL);
    break;
  }
  case common::DataTypeId::kInt32: {
    result->set_primitive_type(::common::PrimitiveType::DT_SIGNED_INT32);
    break;
  }
  case common::DataTypeId::kInt64: {
    result->set_primitive_type(::common::PrimitiveType::DT_SIGNED_INT64);
    break;
  }
  case common::DataTypeId::kUInt32: {
    result->set_primitive_type(::common::PrimitiveType::DT_UNSIGNED_INT32);
    break;
  }
  case common::DataTypeId::kUInt64: {
    result->set_primitive_type(::common::PrimitiveType::DT_UNSIGNED_INT64);
    break;
  }
  case common::DataTypeId::kFloat: {
    result->set_primitive_type(::common::PrimitiveType::DT_FLOAT);
    break;
  }
  case common::DataTypeId::kDouble: {
    result->set_primitive_type(::common::PrimitiveType::DT_DOUBLE);
    break;
  }
  case common::DataTypeId::kVarchar: {
    auto extraInfo = type.getExtraTypeInfo();
    size_t maxLen;
    if (!extraInfo) {
      maxLen = VARCHAR_DEFAULT_LENGTH;
    } else {
      auto& stringTypeInfo = extraInfo->Cast<neug::StringTypeInfo>();
      maxLen = stringTypeInfo.max_length;
    }
    auto strType = std::make_unique<::common::String>();
    auto varChar = std::make_unique<::common::String::VarChar>();
    varChar->set_max_length(maxLen);
    strType->set_allocated_var_char(varChar.release());
    result->set_allocated_string(strType.release());
    break;
  }
  case common::DataTypeId::kInternalId: {
    result->set_primitive_type(::common::PrimitiveType::DT_SIGNED_INT64);
    break;
  }
  case common::DataTypeId::kDate: {
    auto temporalType = std::make_unique<::common::Temporal>();
    auto date = new ::common::Temporal::Date();
    date->set_date_format(::common::Temporal::DF_YYYY_MM_DD);
    temporalType->set_allocated_date(date);
    result->set_allocated_temporal(temporalType.release());
    break;
  }
  case common::DataTypeId::kTimestampMs: {
    auto temporalType = std::make_unique<::common::Temporal>();
    auto dateTime = new ::common::Temporal::DateTime();
    dateTime->set_date_time_format(
        ::common::Temporal::DTF_YYYY_MM_DD_HH_MM_SS_SSS);
    dateTime->set_time_zone_format(::common::Temporal::TZF_UTC);
    temporalType->set_allocated_date_time(dateTime);
    result->set_allocated_temporal(temporalType.release());
    break;
  }
  case common::DataTypeId::kInterval: {
    auto temporalType = std::make_unique<::common::Temporal>();
    temporalType->set_allocated_interval(new ::common::Temporal::Interval());
    result->set_allocated_temporal(temporalType.release());
    break;
  }
  default:
    THROW_EXCEPTION_WITH_FILE_LINE("Unsupported basic type for conversion: " +
                                   type.ToString());
  }
  auto irType = std::make_unique<::common::IrDataType>();
  irType->set_allocated_data_type(result.release());
  return irType;
}

std::unique_ptr<::common::GraphDataType::GraphElementType>
GPhysicalTypeConverter::convertNodeTable(
    catalog::NodeTableCatalogEntry* nodeTable) {
  auto result = std::make_unique<::common::GraphDataType::GraphElementType>();
  auto labelType =
      std::make_unique<::common::GraphDataType::GraphElementLabel>();
  labelType->set_label(nodeTable->getTableID());
  result->set_allocated_label(labelType.release());
  // todo: set properties
  return result;
}

std::unique_ptr<::common::GraphDataType::GraphElementType>
GPhysicalTypeConverter::convertRelTable(
    catalog::GRelTableCatalogEntry* relTable) {
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

neug::DataType GLogicalTypeConverter::convertDataType(
    const ::common::DataType& type) {
  switch (type.item_case()) {
  case ::common::DataType::kPrimitiveType: {
    // Handle primitive types
    switch (type.primitive_type()) {
    case ::common::PrimitiveType::DT_ANY:
      return neug::DataType(DataTypeId::kUnknown);
    case ::common::PrimitiveType::DT_BOOL:
      return neug::DataType(DataTypeId::kBoolean);
    case ::common::PrimitiveType::DT_SIGNED_INT32:
      return neug::DataType(DataTypeId::kInt32);
    case ::common::PrimitiveType::DT_UNSIGNED_INT32:
      return neug::DataType(DataTypeId::kUInt32);
    case ::common::PrimitiveType::DT_SIGNED_INT64:
      return neug::DataType(DataTypeId::kInt64);
    case ::common::PrimitiveType::DT_UNSIGNED_INT64:
      return neug::DataType(DataTypeId::kUInt64);
    case ::common::PrimitiveType::DT_FLOAT:
      return neug::DataType(DataTypeId::kFloat);
    case ::common::PrimitiveType::DT_DOUBLE:
      return neug::DataType(DataTypeId::kDouble);
    case ::common::PrimitiveType::DT_NULL:
      return neug::DataType(DataTypeId::kUnknown);
    default:
      THROW_EXCEPTION_WITH_FILE_LINE(
          "Unsupported PrimitiveType: " +
          std::to_string(static_cast<int>(type.primitive_type())));
    }
  }
  case ::common::DataType::kString: {
    // Handle string types - all variants map to STRING
    return neug::DataType::Varchar();
  }
  case ::common::DataType::kTemporal: {
    // Handle temporal types
    const auto& temporal = type.temporal();
    switch (temporal.item_case()) {
    case ::common::Temporal::kDate:
    case ::common::Temporal::kDate32:
      return neug::DataType(DataTypeId::kDate);
    case ::common::Temporal::kDateTime:
    case ::common::Temporal::kTimestamp:
      return neug::DataType(DataTypeId::kTimestampMs);
    case ::common::Temporal::kInterval:
      return neug::DataType(DataTypeId::kInterval);
    case ::common::Temporal::kTime:
    case ::common::Temporal::kTime32:
      // Time types are not directly supported, map to TIMESTAMP
      return neug::DataType(DataTypeId::kTimestampMs);
    default:
      THROW_EXCEPTION_WITH_FILE_LINE(
          "Unsupported Temporal type in convertDataType");
    }
  }
  case ::common::DataType::kArray: {
    const auto& array = type.array();
    auto childType = convertDataType(array.component_type());
    return neug::DataType::Array(std::move(childType), array.fixed_length());
  }
  case ::common::DataType::kList: {
    const auto& list = type.list();
    auto childType = convertDataType(list.component_type());
    return neug::DataType::List(std::move(childType));
  }
  case ::common::DataType::kMap: {
    // Handle map type
    const auto& map = type.map();
    auto keyType = convertDataType(map.key_type());
    auto valueType = convertDataType(map.value_type());
    return neug::DataType::Map(std::move(keyType), std::move(valueType));
  }
  case ::common::DataType::kTuple: {
    // Handle tuple type - convert to STRUCT
    const auto& tuple = type.tuple();
    std::vector<std::string> fieldNames;
    std::vector<neug::DataType> fieldTypes;
    fieldNames.reserve(tuple.component_types_size());
    fieldTypes.reserve(tuple.component_types_size());
    for (int i = 0; i < tuple.component_types_size(); ++i) {
      auto componentType = convertDataType(tuple.component_types(i));
      fieldNames.push_back("field_" + std::to_string(i));
      fieldTypes.push_back(std::move(componentType));
    }
    return neug::DataType::Struct(std::move(fieldNames), std::move(fieldTypes));
  }
  case ::common::DataType::ITEM_NOT_SET:
  default:
    THROW_EXCEPTION_WITH_FILE_LINE(
        "Unsupported DataType in convertDataType: item_case = " +
        std::to_string(static_cast<int>(type.item_case())));
  }
}

}  // namespace gopt
}  // namespace neug
