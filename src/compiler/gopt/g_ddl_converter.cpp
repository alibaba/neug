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

#include "src/include/gopt/g_ddl_converter.h"

#include "src/include/binder/ddl/bound_alter_info.h"
#include "src/include/catalog/catalog_entry/catalog_entry_type.h"
#include "src/include/catalog/catalog_entry/table_catalog_entry.h"
#include "src/include/common/enums/alter_type.h"
#include "src/include/common/exception/exception.h"
#include "src/include/gopt/g_catalog.h"
#include "src/include/gopt/g_constants.h"
#include "src/include/gopt/g_query_converter.h"
#include "src/include/gopt/g_type_utils.h"
#include "src/include/planner/operator/logical_plan.h"
#include "src/proto_generated_gie/common.pb.h"
#include "src/proto_generated_gie/cypher_ddl.pb.h"

#include <memory>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace gs {
namespace gopt {

std::unique_ptr<::physical::DDLPlan> GDDLConverter::convert(
    const planner::LogicalPlan& plan) {
  auto& op = plan.getLastOperatorRef();
  switch (op.getOperatorType()) {
  case planner::LogicalOperatorType::CREATE_TABLE:
    return convertCreateTable(
        static_cast<const planner::LogicalCreateTable&>(op));
  case planner::LogicalOperatorType::DROP:
    return convertDropTable(static_cast<const planner::LogicalDrop&>(op));
  case planner::LogicalOperatorType::ALTER:
    return convertAlterTable(static_cast<const planner::LogicalAlter&>(op));
  default:
    throw std::runtime_error(
        "Invalid logical operator type for DDL conversion");
  }
}

std::unique_ptr<::physical::DDLPlan> GDDLConverter::convertCreateTable(
    const planner::LogicalCreateTable& op) {
  const auto* info = op.getInfo();
  if (!info) {
    throw std::runtime_error("Invalid operation info");
  }

  switch (info->type) {
  case catalog::CatalogEntryType::NODE_TABLE_ENTRY:
    return convertToCreateVertexSchema(op);
  case catalog::CatalogEntryType::REL_TABLE_ENTRY:
    return convertToCreateEdgeSchema(op);
  default:
    throw std::runtime_error("Invalid table type for create table");
  }
}

std::unique_ptr<::physical::DDLPlan> GDDLConverter::convertDropTable(
    const planner::LogicalDrop& op) {
  auto& info = op.getDropInfo();
  if (info.dropType != gs::common::DropType::TABLE) {
    throw std::runtime_error("Expected DROP TABLE type");
  }

  if (checkEntryType(info.name,
                     gs::catalog::CatalogEntryType::NODE_TABLE_ENTRY)) {
    return convertToDropVertexSchema(op);
  } else if (checkEntryType(info.name,
                            gs::catalog::CatalogEntryType::REL_TABLE_ENTRY)) {
    return convertToDropEdgeSchema(op);
  }

  throw std::runtime_error("Invalid table type for drop table");
}

std::unique_ptr<::physical::DDLPlan> GDDLConverter::convertAlterTable(
    const planner::LogicalAlter& op) {
  const auto* info = op.getInfo();

  // Check table type
  if (checkEntryType(info->tableName,
                     gs::catalog::CatalogEntryType::NODE_TABLE_ENTRY)) {
    switch (info->alterType) {
    case gs::common::AlterType::ADD_PROPERTY:
      return convertToAddVertexPropertySchema(op);
    case gs::common::AlterType::DROP_PROPERTY:
      return convertToDropVertexPropertySchema(op);
    case gs::common::AlterType::RENAME_PROPERTY:
      return convertToRenameVertexPropertySchema(op);
    case gs::common::AlterType::RENAME:
      return convertToRenameVertexTypeSchema(op);
    default:
      throw std::runtime_error("Invalid alter type for vertex schema");
    }
  } else if (checkEntryType(info->tableName,
                            gs::catalog::CatalogEntryType::REL_TABLE_ENTRY)) {
    switch (info->alterType) {
    case gs::common::AlterType::ADD_PROPERTY:
      return convertToAddEdgePropertySchema(op);
    case gs::common::AlterType::DROP_PROPERTY:
      return convertToDropEdgePropertySchema(op);
    case gs::common::AlterType::RENAME_PROPERTY:
      return convertToRenameEdgePropertySchema(op);
    case gs::common::AlterType::RENAME:
      return convertToRenameEdgeTypeSchema(op);
    default:
      throw std::runtime_error("Invalid alter type for edge schema");
    }
  }

  throw std::runtime_error("Invalid table type for alter table");
}

std::unique_ptr<::physical::DDLPlan> GDDLConverter::convertToCreateVertexSchema(
    const planner::LogicalCreateTable& op) {
  const auto* info = op.getInfo();
  if (!info) {
    throw std::runtime_error("Invalid operation info");
  }

  if (info->type != catalog::CatalogEntryType::NODE_TABLE_ENTRY) {
    throw common::Exception("Expected Create Table Type for vertex schema");
  }

  const auto* nodeInfo =
      info->extraInfo->constPtrCast<binder::BoundExtraCreateNodeTableInfo>();
  if (!nodeInfo) {
    throw std::runtime_error("Invalid node table info");
  }

  auto ddl_plan = std::make_unique<::physical::DDLPlan>();
  auto* create_vertex = ddl_plan->mutable_create_vertex_schema();

  // Set vertex type
  auto* vertex_type = create_vertex->mutable_vertex_type();
  vertex_type->set_name(info->tableName);

  // Set properties
  for (const auto& prop : nodeInfo->propertyDefinitions) {
    if (gopt::GQueryConvertor::skipColumn(prop.getName())) {
      continue;  // Skip internal properties
    }
    auto* propertyDef = create_vertex->add_properties();
    propertyDef->set_name(prop.getName());
    auto irType = typeConverter.convertSimpleLogicalType(prop.getType());
    *propertyDef->mutable_type() = std::move(*irType->mutable_data_type());
  }

  // Set primary key
  if (!nodeInfo->primaryKeyName.empty()) {
    create_vertex->add_primary_key(nodeInfo->primaryKeyName);
  }

  // Set conflict action
  create_vertex->set_conflict_action(
      static_cast<::physical::ConflictAction>(info->onConflict));

  return ddl_plan;
}

std::unique_ptr<::physical::DDLPlan> GDDLConverter::convertToCreateEdgeSchema(
    const planner::LogicalCreateTable& op) {
  const auto* info = op.getInfo();
  if (!info) {
    throw std::runtime_error("Invalid operation info");
  }

  if (info->type != catalog::CatalogEntryType::REL_TABLE_ENTRY) {
    throw common::Exception("Expected Create Table Type for edge schema");
  }

  const auto* relInfo =
      info->extraInfo->constPtrCast<binder::BoundExtraCreateRelTableInfo>();
  if (!relInfo) {
    throw std::runtime_error("Invalid relation table info");
  }

  auto ddl_plan = std::make_unique<::physical::DDLPlan>();
  auto* create_edge = ddl_plan->mutable_create_edge_schema();

  // Construct EdgeLabel and convert to EdgeType
  EdgeLabel edgeLabel(info->tableName, getVertexLabelName(relInfo->srcTableID),
                      getVertexLabelName(relInfo->dstTableID));
  auto edgeType = convertToEdgeType(edgeLabel);
  create_edge->set_allocated_edge_type(edgeType.release());

  // Set properties
  for (const auto& prop : relInfo->propertyDefinitions) {
    if (gopt::GQueryConvertor::skipColumn(prop.getName())) {
      continue;  // Skip internal properties
    }
    auto* propertyDef = create_edge->add_properties();
    propertyDef->set_name(prop.getName());
    auto irType = typeConverter.convertSimpleLogicalType(prop.getType());
    *propertyDef->mutable_type() = std::move(*irType->mutable_data_type());
  }

  // Set conflict action
  create_edge->set_conflict_action(
      static_cast<::physical::ConflictAction>(info->onConflict));

  // Set multiplicity
  if (relInfo->srcMultiplicity == common::RelMultiplicity::ONE &&
      relInfo->dstMultiplicity == common::RelMultiplicity::ONE) {
    create_edge->set_multiplicity(::physical::CreateEdgeSchema::ONE_TO_ONE);
  } else if (relInfo->srcMultiplicity == common::RelMultiplicity::ONE &&
             relInfo->dstMultiplicity == common::RelMultiplicity::MANY) {
    create_edge->set_multiplicity(::physical::CreateEdgeSchema::ONE_TO_MANY);
  } else if (relInfo->srcMultiplicity == common::RelMultiplicity::MANY &&
             relInfo->dstMultiplicity == common::RelMultiplicity::ONE) {
    create_edge->set_multiplicity(::physical::CreateEdgeSchema::MANY_TO_ONE);
  } else {
    create_edge->set_multiplicity(::physical::CreateEdgeSchema::MANY_TO_MANY);
  }

  return ddl_plan;
}

std::unique_ptr<::physical::DDLPlan> GDDLConverter::convertToDropVertexSchema(
    const planner::LogicalDrop& op) {
  auto& info = op.getDropInfo();
  if (info.dropType != gs::common::DropType::TABLE ||
      !checkEntryType(info.name,
                      gs::catalog::CatalogEntryType::NODE_TABLE_ENTRY)) {
    throw std::runtime_error("Expected DROP TABLE type for vertex schema");
  }

  auto ddl_plan = std::make_unique<::physical::DDLPlan>();
  auto* drop_vertex = ddl_plan->mutable_drop_vertex_schema();

  // Set vertex type name
  auto typeName = std::make_unique<::common::NameOrId>();
  typeName->set_name(info.name);
  drop_vertex->set_allocated_vertex_type(typeName.release());

  // Set conflict action
  drop_vertex->set_conflict_action(
      static_cast<::physical::ConflictAction>(info.conflictAction));

  return ddl_plan;
}

std::unique_ptr<::physical::DDLPlan> GDDLConverter::convertToDropEdgeSchema(
    const planner::LogicalDrop& op) {
  auto& info = op.getDropInfo();
  if (info.dropType != gs::common::DropType::TABLE ||
      !checkEntryType(info.name,
                      gs::catalog::CatalogEntryType::REL_TABLE_ENTRY)) {
    throw std::runtime_error("Expected DROP TABLE type for edge schema");
  }
  auto ddl_plan = std::make_unique<::physical::DDLPlan>();
  auto* drop_edge = ddl_plan->mutable_drop_edge_schema();

  // Check edge labels count
  std::vector<EdgeLabel> edgeLabels;
  getEdgeLabels(info.name, edgeLabels);
  if (edgeLabels.size() != 1) {
    throw std::runtime_error("Edge type must have exactly one edge label");
  }

  // Set edge type name
  auto* edgeType = drop_edge->mutable_edge_type();
  *edgeType = std::move(*convertToEdgeType(edgeLabels[0]));

  // Set conflict action
  drop_edge->set_conflict_action(
      static_cast<::physical::ConflictAction>(info.conflictAction));

  return ddl_plan;
}

std::unique_ptr<::physical::DDLPlan>
GDDLConverter::convertToAddVertexPropertySchema(
    const planner::LogicalAlter& op) {
  const auto* info = op.getInfo();
  if (info->alterType != gs::common::AlterType::ADD_PROPERTY ||
      !checkEntryType(info->tableName,
                      gs::catalog::CatalogEntryType::NODE_TABLE_ENTRY)) {
    throw std::runtime_error(
        "Expected ADD_PROPERTY alter type for vertex schema");
  }

  // Get alter info from operator
  auto ddl_plan = std::make_unique<::physical::DDLPlan>();
  auto* add_property = ddl_plan->mutable_add_vertex_property_schema();

  // Set vertex type name
  auto typeName = std::make_unique<::common::NameOrId>();
  typeName->set_name(info->tableName);
  add_property->set_allocated_vertex_type(typeName.release());

  const binder::BoundExtraAddPropertyInfo* extraInfo =
      info->extraInfo->constPtrCast<binder::BoundExtraAddPropertyInfo>();

  auto& propertyDef = extraInfo->propertyDefinition;

  // Add property definition
  auto* property = add_property->add_properties();
  property->set_name(propertyDef.getName());
  auto irType = typeConverter.convertSimpleLogicalType(propertyDef.getType());
  *property->mutable_type() = std::move(*irType->mutable_data_type());

  // Set conflict action
  add_property->set_conflict_action(
      static_cast<::physical::ConflictAction>(info->onConflict));

  return ddl_plan;
}

std::unique_ptr<::physical::DDLPlan>
GDDLConverter::convertToAddEdgePropertySchema(const planner::LogicalAlter& op) {
  const auto* info = op.getInfo();
  if (info->alterType != gs::common::AlterType::ADD_PROPERTY ||
      !checkEntryType(info->tableName,
                      gs::catalog::CatalogEntryType::REL_TABLE_ENTRY)) {
    throw std::runtime_error(
        "Expected ADD_PROPERTY alter type for edge schema");
  }

  // Get alter info from operator
  auto ddl_plan = std::make_unique<::physical::DDLPlan>();
  auto* add_property = ddl_plan->mutable_add_edge_property_schema();

  // Check edge labels count and set edge type
  std::vector<EdgeLabel> edgeLabels;
  getEdgeLabels(info->tableName, edgeLabels);
  if (edgeLabels.size() != 1) {
    throw std::runtime_error("Edge type must have exactly one edge label");
  }
  auto* edgeType = add_property->mutable_edge_type();
  *edgeType = std::move(*convertToEdgeType(edgeLabels[0]));

  const binder::BoundExtraAddPropertyInfo* extraInfo =
      info->extraInfo->constPtrCast<binder::BoundExtraAddPropertyInfo>();

  auto& propertyDef = extraInfo->propertyDefinition;

  // Add property definition
  auto* property = add_property->add_properties();
  property->set_name(propertyDef.getName());
  auto irType = typeConverter.convertSimpleLogicalType(propertyDef.getType());
  *property->mutable_type() = std::move(*irType->mutable_data_type());

  // Set conflict action
  add_property->set_conflict_action(
      static_cast<::physical::ConflictAction>(info->onConflict));

  return ddl_plan;
}

std::unique_ptr<::physical::DDLPlan>
GDDLConverter::convertToDropVertexPropertySchema(
    const planner::LogicalAlter& op) {
  const auto* info = op.getInfo();
  if (info->alterType != gs::common::AlterType::DROP_PROPERTY ||
      !checkEntryType(info->tableName,
                      gs::catalog::CatalogEntryType::NODE_TABLE_ENTRY)) {
    throw std::runtime_error(
        "Expected DROP_PROPERTY alter type for vertex schema");
  }

  // Get alter info from operator
  auto ddl_plan = std::make_unique<::physical::DDLPlan>();
  auto* drop_property = ddl_plan->mutable_drop_vertex_property_schema();

  // Set vertex type
  auto* vertex_type = drop_property->mutable_vertex_type();
  vertex_type->set_name(info->tableName);

  // Get property name to drop
  const binder::BoundExtraDropPropertyInfo* extraInfo =
      info->extraInfo->constPtrCast<binder::BoundExtraDropPropertyInfo>();

  *drop_property->add_properties() = std::move(extraInfo->propertyName);

  drop_property->set_conflict_action(
      static_cast<::physical::ConflictAction>(info->onConflict));

  return ddl_plan;
}

std::unique_ptr<::physical::DDLPlan>
GDDLConverter::convertToDropEdgePropertySchema(
    const planner::LogicalAlter& op) {
  const auto* info = op.getInfo();
  if (info->alterType != gs::common::AlterType::DROP_PROPERTY ||
      !checkEntryType(info->tableName,
                      gs::catalog::CatalogEntryType::REL_TABLE_ENTRY)) {
    throw std::runtime_error(
        "Expected DROP_PROPERTY alter type for edge schema");
  }

  // Get alter info from operator
  auto ddl_plan = std::make_unique<::physical::DDLPlan>();
  auto* drop_property = ddl_plan->mutable_drop_edge_property_schema();

  // Get edge type
  std::vector<EdgeLabel> edgeLabels;
  getEdgeLabels(info->tableName, edgeLabels);
  if (edgeLabels.size() != 1) {
    throw std::runtime_error("Edge type must have exactly one edge label");
  }
  auto* edgeType = drop_property->mutable_edge_type();
  *edgeType = std::move(*convertToEdgeType(edgeLabels[0]));

  // Get property name to drop
  const binder::BoundExtraDropPropertyInfo* extraInfo =
      info->extraInfo->constPtrCast<binder::BoundExtraDropPropertyInfo>();
  *drop_property->add_properties() = std::move(extraInfo->propertyName);

  drop_property->set_conflict_action(
      static_cast<::physical::ConflictAction>(info->onConflict));

  return ddl_plan;
}

std::unique_ptr<::physical::DDLPlan>
GDDLConverter::convertToRenameVertexPropertySchema(
    const planner::LogicalAlter& op) {
  const auto* info = op.getInfo();
  if (info->alterType != gs::common::AlterType::RENAME_PROPERTY ||
      !checkEntryType(info->tableName,
                      gs::catalog::CatalogEntryType::NODE_TABLE_ENTRY)) {
    throw std::runtime_error(
        "Expected RENAME_PROPERTY alter type for vertex schema");
  }

  auto ddl_plan = std::make_unique<::physical::DDLPlan>();
  auto* rename_property = ddl_plan->mutable_rename_vertex_property_schema();

  // Set vertex type
  auto* vertex_type = rename_property->mutable_vertex_type();
  vertex_type->set_name(info->tableName);

  // Get old and new property names
  const binder::BoundExtraRenamePropertyInfo* extraInfo =
      info->extraInfo->constPtrCast<binder::BoundExtraRenamePropertyInfo>();
  auto* mappings = rename_property->mutable_mappings();
  (*mappings)[extraInfo->oldName] = extraInfo->newName;

  // Set conflict action
  rename_property->set_conflict_action(
      static_cast<::physical::ConflictAction>(info->onConflict));

  return ddl_plan;
}

std::unique_ptr<::physical::DDLPlan>
GDDLConverter::convertToRenameEdgePropertySchema(
    const planner::LogicalAlter& op) {
  const auto* info = op.getInfo();
  if (info->alterType != gs::common::AlterType::RENAME_PROPERTY ||
      !checkEntryType(info->tableName,
                      gs::catalog::CatalogEntryType::REL_TABLE_ENTRY)) {
    throw std::runtime_error(
        "Expected RENAME_PROPERTY alter type for edge schema");
  }

  auto ddl_plan = std::make_unique<::physical::DDLPlan>();
  auto* rename_property = ddl_plan->mutable_rename_edge_property_schema();

  // Get edge type
  std::vector<EdgeLabel> edgeLabels;
  getEdgeLabels(info->tableName, edgeLabels);
  if (edgeLabels.size() != 1) {
    throw std::runtime_error("Edge type must have exactly one edge label");
  }
  auto* edgeType = rename_property->mutable_edge_type();
  *edgeType = std::move(*convertToEdgeType(edgeLabels[0]));

  // Get old and new property names
  const binder::BoundExtraRenamePropertyInfo* extraInfo =
      info->extraInfo->constPtrCast<binder::BoundExtraRenamePropertyInfo>();
  auto* mappings = rename_property->mutable_mappings();
  (*mappings)[extraInfo->oldName] = extraInfo->newName;

  // Set conflict action
  rename_property->set_conflict_action(
      static_cast<::physical::ConflictAction>(info->onConflict));

  return ddl_plan;
}

std::unique_ptr<::physical::DDLPlan>
GDDLConverter::convertToRenameVertexTypeSchema(
    const planner::LogicalAlter& op) {
  const auto* info = op.getInfo();
  if (info->alterType != gs::common::AlterType::RENAME ||
      !checkEntryType(info->tableName,
                      gs::catalog::CatalogEntryType::NODE_TABLE_ENTRY)) {
    throw std::runtime_error(
        "Expected RENAME_TABLE alter type for vertex schema");
  }

  auto ddl_plan = std::make_unique<::physical::DDLPlan>();
  auto* rename_vertex = ddl_plan->mutable_rename_vertex_type_schema();

  // Set old vertex type name
  auto oldTypeName = std::make_unique<::common::NameOrId>();
  oldTypeName->set_name(info->tableName);
  rename_vertex->set_allocated_old_type(oldTypeName.release());

  // Set new vertex type name
  const binder::BoundExtraRenameTableInfo* extraInfo =
      info->extraInfo->constPtrCast<binder::BoundExtraRenameTableInfo>();
  auto newTypeName = std::make_unique<::common::NameOrId>();
  newTypeName->set_name(extraInfo->newName);
  rename_vertex->set_allocated_new_type(newTypeName.release());

  // Set conflict action
  rename_vertex->set_conflict_action(
      static_cast<::physical::ConflictAction>(info->onConflict));

  return ddl_plan;
}

std::unique_ptr<::physical::DDLPlan>
GDDLConverter::convertToRenameEdgeTypeSchema(const planner::LogicalAlter& op) {
  const auto* info = op.getInfo();
  if (info->alterType != gs::common::AlterType::RENAME ||
      !checkEntryType(info->tableName,
                      gs::catalog::CatalogEntryType::REL_TABLE_ENTRY)) {
    throw std::runtime_error(
        "Expected RENAME_TABLE alter type for edge schema");
  }

  auto ddl_plan = std::make_unique<::physical::DDLPlan>();
  auto* rename_edge = ddl_plan->mutable_rename_edge_type_schema();

  // Get old edge type
  std::vector<EdgeLabel> edgeLabels;
  getEdgeLabels(info->tableName, edgeLabels);
  if (edgeLabels.size() != 1) {
    throw std::runtime_error("Edge type must have exactly one edge label");
  }
  auto* oldEdgeType = rename_edge->mutable_old_type();
  *oldEdgeType = std::move(*convertToEdgeType(edgeLabels[0]));

  std::vector<EdgeLabel> newEdgeLabels;
  // Set new edge type name
  const binder::BoundExtraRenameTableInfo* extraInfo =
      info->extraInfo->constPtrCast<binder::BoundExtraRenameTableInfo>();
  for (auto& edgeLabel : edgeLabels) {
    newEdgeLabels.emplace_back(extraInfo->newName, edgeLabel.srcTypeName,
                               edgeLabel.dstTypeName);
  }

  if (newEdgeLabels.size() != 1) {
    throw std::runtime_error("New edge type must have exactly one edge label");
  }
  auto* newEdgeType = rename_edge->mutable_new_type();
  *newEdgeType = std::move(*convertToEdgeType(newEdgeLabels[0]));

  // Set conflict action
  rename_edge->set_conflict_action(
      static_cast<::physical::ConflictAction>(info->onConflict));

  return ddl_plan;
}

std::unique_ptr<::physical::EdgeType> GDDLConverter::convertToEdgeType(
    const EdgeLabel& label) {
  auto edgeType = std::make_unique<physical::EdgeType>();

  auto typeName = std::make_unique<::common::NameOrId>();
  typeName->set_name(label.typeName);
  edgeType->set_allocated_type_name(typeName.release());

  auto srcTypeName = std::make_unique<::common::NameOrId>();
  srcTypeName->set_name(label.srcTypeName);
  edgeType->set_allocated_src_type_name(srcTypeName.release());

  auto dstTypeName = std::make_unique<::common::NameOrId>();
  dstTypeName->set_name(label.dstTypeName);
  edgeType->set_allocated_dst_type_name(dstTypeName.release());

  return edgeType;
}

void GDDLConverter::getEdgeLabels(const std::string& labelName,
                                  std::vector<EdgeLabel>& edgeLabels) {
  checkCatalogInitialized();

  const auto& transaction = gs::Constants::DEFAULT_TRANSACTION;
  std::vector<gs::catalog::GRelTableCatalogEntry*> relTableEntries;

  if (catalog->containsRelGroup(&transaction, labelName)) {
    auto* groupEntry = catalog->getRelGroupEntry(&transaction, labelName);
    const auto& relTableIds = groupEntry->getRelTableIDs();
    relTableEntries.reserve(relTableIds.size());

    for (const auto& tableId : relTableIds) {
      auto* entry = catalog->getTableCatalogEntry(&transaction, tableId);
      if (!entry ||
          entry->getType() != catalog::CatalogEntryType::REL_TABLE_ENTRY) {
        throw std::runtime_error("Edge Table Entry Not found: " + tableId);
      }
      auto* edgeTableEntry =
          static_cast<gs::catalog::GRelTableCatalogEntry*>(entry);
      relTableEntries.push_back(edgeTableEntry);
    }
  } else {
    auto* entry = catalog->getTableCatalogEntry(&transaction, labelName);
    if (!entry ||
        entry->getType() != catalog::CatalogEntryType::REL_TABLE_ENTRY) {
      throw std::runtime_error("Edge table entry not found: " + labelName);
    }
    auto* edgeTableEntry =
        static_cast<gs::catalog::GRelTableCatalogEntry*>(entry);
    relTableEntries.push_back(edgeTableEntry);
  }

  edgeLabels.reserve(relTableEntries.size());
  for (const auto* edgeTableEntry : relTableEntries) {
    const std::string& srcLabelName =
        getVertexLabelName(edgeTableEntry->getSrcTableID());
    const std::string& dstLabelName =
        getVertexLabelName(edgeTableEntry->getDstTableID());
    edgeLabels.emplace_back(labelName, srcLabelName, dstLabelName);
  }
}

std::string GDDLConverter::getVertexLabelName(gs::common::oid_t tableId) {
  checkCatalogInitialized();

  auto* entry = catalog->getTableCatalogEntry(
      &gs::Constants::DEFAULT_TRANSACTION, tableId);
  if (!entry ||
      entry->getType() != catalog::CatalogEntryType::NODE_TABLE_ENTRY) {
    throw std::runtime_error("Node table entry not found for id: " +
                             std::to_string(tableId));
  }
  return entry->getName();
}

bool GDDLConverter::checkEntryType(const std::string& labelName,
                                   catalog::CatalogEntryType expectedType) {
  checkCatalogInitialized();

  auto* entry = catalog->getTableCatalogEntry(
      &gs::Constants::DEFAULT_TRANSACTION, labelName);
  if (!entry) {
    throw std::runtime_error("Catalog entry not found for label: " + labelName);
  }
  return entry->getType() == expectedType;
}

}  // namespace gopt
}  // namespace gs