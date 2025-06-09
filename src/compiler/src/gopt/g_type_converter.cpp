#include "gopt/g_type_converter.h"

#include <google/protobuf/wrappers.pb.h>
#include <cstdint>
#include <memory>
#include "binder/expression/node_expression.h"
#include "binder/expression/rel_expression.h"
#include "catalog/catalog_entry/node_table_catalog_entry.h"
#include "common/exception/exception.h"
#include "common/types/types.h"
#include "gopt/g_constants.h"
#include "gopt/g_graph_type.h"
#include "gopt/g_rel_table_entry.h"
#include "src/proto_generated_gie/basic_type.pb.h"
#include "src/proto_generated_gie/type.pb.h"

namespace kuzu {
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

std::unique_ptr<::common::IrDataType> GTypeConverter::convertType(
    const common::LogicalType& type, const binder::Expression& expr) {
  switch (type.getLogicalTypeID()) {
  case common::LogicalTypeID::NODE: {
    if (auto nodeExpr = expr.constPtrCast<binder::NodeExpression>()) {
      std::vector<catalog::NodeTableCatalogEntry*> nodeTables;
      for (auto& entry : nodeExpr->getEntries()) {
        auto nodeTable = entry->ptrCast<catalog::NodeTableCatalogEntry>();
        if (!nodeTable) {
          throw common::Exception(
              "Expected NodeTableCatalogEntry for NODE "
              "type, but got: " +
              entry->getName());
        }
        nodeTables.emplace_back(nodeTable);
      }
      return convertNodeType(gopt::GNodeType(nodeTables));
    } else {
      throw common::Exception(
          "Expected NodeExpression for NODE type, "
          "but got: " +
          expr.toString());
    }
    break;
  }
  case common::LogicalTypeID::REL: {
    if (auto relExpr = expr.constPtrCast<binder::RelExpression>()) {
      std::vector<catalog::GRelTableCatalogEntry*> relTables;
      for (auto& entry : relExpr->getEntries()) {
        auto relTable = entry->ptrCast<catalog::GRelTableCatalogEntry>();
        if (!relTable) {
          throw common::Exception(
              "Expected GRelTableCatalogEntry for REL "
              "type, but got: " +
              entry->getName());
        }
        relTables.emplace_back(relTable);
      }
      return convertRelType(gopt::GRelType(relTables));
    } else {
      throw common::Exception(
          "Expected RelExpression for REL type, "
          "but got: " +
          expr.toString());
    }
    break;
  }
  default:
    // For other types, we can convert them directly
    return convertLogicalType(type);
  }
}

std::unique_ptr<::common::IrDataType> GTypeConverter::convertLogicalType(
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
    varChar->set_max_length(kuzu::Constants::VARCHAR_MAX_LENGTH);
    strType->set_allocated_var_char(varChar.release());
    result->set_allocated_string(strType.release());
    break;
  }
  case common::LogicalTypeID::INTERNAL_ID: {
    result->set_primitive_type(::common::PrimitiveType::DT_SIGNED_INT32);
    break;
  }
  // case common::LogicalTypeID::NODE: {
  //   auto extraInfo = type.getExtraTypeInfo();
  //   if (auto structInfo = extraInfo->constPtrCast<common::StructTypeInfo>())
  //   {
  //     for (auto name : structInfo->getChildrenNames()) {
  //       std::cout << "name: " << name << std::endl;
  //     }
  //     for (auto type : structInfo->getChildrenTypes()) {
  //       std::cout << "type: " << type->toString() << std::endl;
  //     }
  //   }
  //   // break;
  // }
  default:
    throw common::Exception("Unsupported basic type for conversion: " +
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
}  // namespace kuzu