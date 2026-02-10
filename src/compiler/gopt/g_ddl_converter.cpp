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

#include "neug/compiler/gopt/g_ddl_converter.h"

#include "neug/compiler/binder/ddl/bound_alter_info.h"
#include "neug/compiler/catalog/catalog_entry/catalog_entry_type.h"
#include "neug/compiler/catalog/catalog_entry/table_catalog_entry.h"
#include "neug/compiler/common/enums/alter_type.h"
#include "neug/compiler/common/enums/expression_type.h"
#include "neug/compiler/gopt/g_catalog.h"
#include "neug/compiler/gopt/g_constants.h"
#include "neug/compiler/gopt/g_query_converter.h"
#include "neug/compiler/gopt/g_type_utils.h"
#include "neug/compiler/parser/expression/parsed_literal_expression.h"
#include "neug/compiler/planner/operator/logical_plan.h"
#include "neug/generated/proto/plan/common.pb.h"
#include "neug/generated/proto/plan/cypher_ddl.pb.h"
#include "neug/generated/proto/plan/physical.pb.h"
#include "neug/utils/exception/exception.h"

#include <memory>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace neug {
namespace gopt {

void GDDLConverter::convertCreateTable(const planner::LogicalCreateTable& op,
                                       ::physical::PhysicalPlan* plan) {
  const auto* info = op.getInfo();
  if (!info) {
    THROW_INVALID_ARGUMENT_EXCEPTION("Invalid operation info");
  }

  switch (info->type) {
  case catalog::CatalogEntryType::NODE_TABLE_ENTRY:
    plan->mutable_plan()->AddAllocated(
        convertToCreateVertexSchema(op).release());
    break;
  case catalog::CatalogEntryType::REL_TABLE_ENTRY:
    plan->mutable_plan()->AddAllocated(convertToCreateEdgeSchema(op).release());
    break;
  case catalog::CatalogEntryType::REL_GROUP_ENTRY:
    plan->mutable_plan()->AddAllocated(
        convertToCreateEdgeGroupSchema(op).release());
    break;
  default:
    THROW_INVALID_ARGUMENT_EXCEPTION(
        "Unsupported catalog entry type for create");
  }
}

void GDDLConverter::convertDropTable(const planner::LogicalDrop& op,
                                     ::physical::PhysicalPlan* plan) {
  auto& info = op.getDropInfo();
  if (info.dropType != neug::common::DropType::TABLE) {
    THROW_INVALID_ARGUMENT_EXCEPTION("Expected DROP TABLE type");
  }

  if (checkEntryType(info.name,
                     neug::catalog::CatalogEntryType::NODE_TABLE_ENTRY)) {
    plan->mutable_plan()->AddAllocated(convertToDropVertexSchema(op).release());
  } else if (checkEntryType(info.name,
                            neug::catalog::CatalogEntryType::REL_TABLE_ENTRY)) {
    plan->mutable_plan()->AddAllocated(convertToDropEdgeSchema(op).release());
  } else {
    THROW_RUNTIME_ERROR("Invalid table type for drop table");
  }
}

void GDDLConverter::convertAlterTable(const planner::LogicalAlter& op,
                                      ::physical::PhysicalPlan* plan) {
  const auto* info = op.getInfo();

  // Check table type
  if (checkEntryType(info->tableName,
                     neug::catalog::CatalogEntryType::NODE_TABLE_ENTRY)) {
    switch (info->alterType) {
    case neug::common::AlterType::ADD_PROPERTY:
      plan->mutable_plan()->AddAllocated(
          convertToAddVertexPropertySchema(op).release());
      break;
    case neug::common::AlterType::DROP_PROPERTY:
      plan->mutable_plan()->AddAllocated(
          convertToDropVertexPropertySchema(op).release());
      break;
    case neug::common::AlterType::RENAME_PROPERTY:
      plan->mutable_plan()->AddAllocated(
          convertToRenameVertexPropertySchema(op).release());
      break;
    case neug::common::AlterType::RENAME:
      plan->mutable_plan()->AddAllocated(
          convertToRenameVertexTypeSchema(op).release());
      break;
    default:
      THROW_RUNTIME_ERROR("Invalid alter type for vertex schema");
    }
  } else if (checkEntryType(info->tableName,
                            neug::catalog::CatalogEntryType::REL_TABLE_ENTRY)) {
    switch (info->alterType) {
    case neug::common::AlterType::ADD_PROPERTY:
      plan->mutable_plan()->AddAllocated(
          convertToAddEdgePropertySchema(op).release());
      break;
    case neug::common::AlterType::DROP_PROPERTY:
      plan->mutable_plan()->AddAllocated(
          convertToDropEdgePropertySchema(op).release());
      break;
    case neug::common::AlterType::RENAME_PROPERTY:
      plan->mutable_plan()->AddAllocated(
          convertToRenameEdgePropertySchema(op).release());
      break;
    case neug::common::AlterType::RENAME:
      plan->mutable_plan()->AddAllocated(
          convertToRenameEdgeTypeSchema(op).release());
      break;
    default:
      THROW_RUNTIME_ERROR("Invalid alter type for edge schema");
    }
  } else {
    THROW_INVALID_ARGUMENT_EXCEPTION("Invalid table type for alter table");
  }
}

std::unique_ptr<::physical::PhysicalOpr>
GDDLConverter::convertToCreateVertexSchema(
    const planner::LogicalCreateTable& op) {
  const auto* info = op.getInfo();
  if (!info) {
    THROW_RUNTIME_ERROR("Invalid operation info");
  }

  if (info->type != catalog::CatalogEntryType::NODE_TABLE_ENTRY) {
    THROW_INVALID_ARGUMENT_EXCEPTION(
        "Expected Create Table Type for vertex schema");
  }

  const auto* nodeInfo =
      info->extraInfo->constPtrCast<binder::BoundExtraCreateNodeTableInfo>();
  if (!nodeInfo) {
    THROW_INVALID_ARGUMENT_EXCEPTION("Invalid node table info");
  }

  auto physical_opr = std::make_unique<::physical::PhysicalOpr>();
  auto* create_vertex =
      physical_opr->mutable_opr()->mutable_create_vertex_schema();

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
    propertyDef->set_allocated_default_value(
        exprConverter.convertDefaultValue(prop).release());
  }

  // Set primary key
  if (!nodeInfo->primaryKeyName.empty()) {
    create_vertex->add_primary_key(nodeInfo->primaryKeyName);
  }

  // Set conflict action
  create_vertex->set_conflict_action(
      static_cast<::physical::ConflictAction>(info->onConflict));

  return physical_opr;
}

std::unique_ptr<::physical::CreateEdgeSchema::TypeInfo>
GDDLConverter::convertToEdgeTypeInfo(const binder::BoundCreateTableInfo& info,
                                     const std::string& edgeName) {
  if (info.type != catalog::CatalogEntryType::REL_TABLE_ENTRY) {
    THROW_EXCEPTION_WITH_FILE_LINE(
        "Expected Create Table Type for edge schema");
  }
  const auto* relInfo =
      info.extraInfo->constPtrCast<binder::BoundExtraCreateRelTableInfo>();
  if (!relInfo) {
    THROW_RUNTIME_ERROR("Invalid relation table info");
  }
  EdgeLabel edgeLabel(edgeName, getVertexLabelName(relInfo->srcTableID),
                      getVertexLabelName(relInfo->dstTableID));
  auto edgeType = convertToEdgeType(edgeLabel);
  auto typeInfoPB = std::make_unique<::physical::CreateEdgeSchema::TypeInfo>();
  typeInfoPB->set_allocated_edge_type(edgeType.release());
  // Set multiplicity
  if (relInfo->srcMultiplicity == common::RelMultiplicity::ONE &&
      relInfo->dstMultiplicity == common::RelMultiplicity::ONE) {
    typeInfoPB->set_multiplicity(::physical::CreateEdgeSchema::ONE_TO_ONE);
  } else if (relInfo->srcMultiplicity == common::RelMultiplicity::ONE &&
             relInfo->dstMultiplicity == common::RelMultiplicity::MANY) {
    typeInfoPB->set_multiplicity(::physical::CreateEdgeSchema::ONE_TO_MANY);
  } else if (relInfo->srcMultiplicity == common::RelMultiplicity::MANY &&
             relInfo->dstMultiplicity == common::RelMultiplicity::ONE) {
    typeInfoPB->set_multiplicity(::physical::CreateEdgeSchema::MANY_TO_ONE);
  } else {
    typeInfoPB->set_multiplicity(::physical::CreateEdgeSchema::MANY_TO_MANY);
  }
  return typeInfoPB;
}

std::unique_ptr<::physical::PhysicalOpr>
GDDLConverter::convertToCreateEdgeGroupSchema(
    const planner::LogicalCreateTable& op) {
  const auto* info = op.getInfo();
  if (!info) {
    THROW_RUNTIME_ERROR("Invalid operation info");
  }

  if (info->type != catalog::CatalogEntryType::REL_GROUP_ENTRY) {
    THROW_EXCEPTION_WITH_FILE_LINE(
        "Expected Create Table Type for edge group schema");
  }
  const auto* relGroupInfo =
      info->extraInfo
          ->constPtrCast<binder::BoundExtraCreateRelTableGroupInfo>();
  if (!relGroupInfo) {
    THROW_RUNTIME_ERROR("Invalid relation group table info");
  }

  if (relGroupInfo->infos.empty()) {
    THROW_RUNTIME_ERROR("Relation group table info should not be empty");
  }

  auto physical_opr = std::make_unique<::physical::PhysicalOpr>();
  auto create_edge = physical_opr->mutable_opr()->mutable_create_edge_schema();

  // set edge type with multiplicity
  for (auto& relInfo : relGroupInfo->infos) {
    *create_edge->add_type_info() =
        std::move(*convertToEdgeTypeInfo(relInfo, info->tableName));
  }

  auto firstRelInfo =
      relGroupInfo->infos[0]
          .extraInfo->constPtrCast<binder::BoundExtraCreateRelTableInfo>();

  // Set properties
  for (const auto& prop : firstRelInfo->propertyDefinitions) {
    if (gopt::GQueryConvertor::skipColumn(prop.getName())) {
      continue;  // Skip internal properties
    }
    auto* propertyDef = create_edge->add_properties();
    propertyDef->set_name(prop.getName());
    auto irType = typeConverter.convertSimpleLogicalType(prop.getType());
    *propertyDef->mutable_type() = std::move(*irType->mutable_data_type());
    propertyDef->set_allocated_default_value(
        exprConverter.convertDefaultValue(prop).release());
  }

  // Set conflict action
  create_edge->set_conflict_action(
      static_cast<::physical::ConflictAction>(info->onConflict));

  return physical_opr;
}

std::unique_ptr<::physical::PhysicalOpr>
GDDLConverter::convertToCreateEdgeSchema(
    const planner::LogicalCreateTable& op) {
  const auto* info = op.getInfo();
  if (!info) {
    THROW_RUNTIME_ERROR("Invalid operation info");
  }

  if (info->type != catalog::CatalogEntryType::REL_TABLE_ENTRY) {
    THROW_EXCEPTION_WITH_FILE_LINE(
        "Expected Create Table Type for edge schema");
  }

  const auto* relInfo =
      info->extraInfo->constPtrCast<binder::BoundExtraCreateRelTableInfo>();
  if (!relInfo) {
    THROW_RUNTIME_ERROR("Invalid relation table info");
  }

  auto physical_opr = std::make_unique<::physical::PhysicalOpr>();
  auto create_edge = physical_opr->mutable_opr()->mutable_create_edge_schema();
  // set edge type with multiplicity
  *create_edge->add_type_info() =
      std::move(*convertToEdgeTypeInfo(*info, info->tableName));

  // Set properties
  for (const auto& prop : relInfo->propertyDefinitions) {
    if (gopt::GQueryConvertor::skipColumn(prop.getName())) {
      continue;  // Skip internal properties
    }
    auto* propertyDef = create_edge->add_properties();
    propertyDef->set_name(prop.getName());
    auto irType = typeConverter.convertSimpleLogicalType(prop.getType());
    *propertyDef->mutable_type() = std::move(*irType->mutable_data_type());
    propertyDef->set_allocated_default_value(
        exprConverter.convertDefaultValue(prop).release());
  }

  // Set conflict action
  create_edge->set_conflict_action(
      static_cast<::physical::ConflictAction>(info->onConflict));

  return physical_opr;
}

std::unique_ptr<::physical::PhysicalOpr>
GDDLConverter::convertToDropVertexSchema(const planner::LogicalDrop& op) {
  auto& info = op.getDropInfo();
  if (info.dropType != neug::common::DropType::TABLE ||
      !checkEntryType(info.name,
                      neug::catalog::CatalogEntryType::NODE_TABLE_ENTRY)) {
    THROW_RUNTIME_ERROR("Expected DROP TABLE type for vertex schema");
  }

  auto physical_opr = std::make_unique<::physical::PhysicalOpr>();
  auto* drop_vertex = physical_opr->mutable_opr()->mutable_drop_vertex_schema();

  // Set vertex type name
  auto typeName = std::make_unique<::common::NameOrId>();
  typeName->set_name(info.name);
  drop_vertex->set_allocated_vertex_type(typeName.release());

  // Set conflict action
  drop_vertex->set_conflict_action(
      static_cast<::physical::ConflictAction>(info.conflictAction));

  return physical_opr;
}

std::unique_ptr<::physical::PhysicalOpr> GDDLConverter::convertToDropEdgeSchema(
    const planner::LogicalDrop& op) {
  auto& info = op.getDropInfo();
  if (info.dropType != neug::common::DropType::TABLE ||
      !checkEntryType(info.name,
                      neug::catalog::CatalogEntryType::REL_TABLE_ENTRY)) {
    THROW_RUNTIME_ERROR("Expected DROP TABLE type for edge schema");
  }
  auto physical_opr = std::make_unique<::physical::PhysicalOpr>();
  auto* drop_edge = physical_opr->mutable_opr()->mutable_drop_edge_schema();

  // Check edge labels count
  std::vector<EdgeLabel> edgeLabels;
  getEdgeLabels(info.name, edgeLabels);
  if (edgeLabels.size() != 1) {
    THROW_RUNTIME_ERROR("Edge type must have exactly one edge label");
  }

  // Set edge type name
  auto* edgeType = drop_edge->mutable_edge_type();
  *edgeType = std::move(*convertToEdgeType(edgeLabels[0]));

  // Set conflict action
  drop_edge->set_conflict_action(
      static_cast<::physical::ConflictAction>(info.conflictAction));

  return physical_opr;
}

std::unique_ptr<::physical::PhysicalOpr>
GDDLConverter::convertToAddVertexPropertySchema(
    const planner::LogicalAlter& op) {
  const auto* info = op.getInfo();
  if (info->alterType != neug::common::AlterType::ADD_PROPERTY ||
      !checkEntryType(info->tableName,
                      neug::catalog::CatalogEntryType::NODE_TABLE_ENTRY)) {
    THROW_RUNTIME_ERROR("Expected ADD_PROPERTY alter type for vertex schema");
  }

  // Get alter info from operator
  auto physical_opr = std::make_unique<::physical::PhysicalOpr>();
  auto* add_property =
      physical_opr->mutable_opr()->mutable_add_vertex_property_schema();

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
  property->set_allocated_default_value(
      exprConverter.convertDefaultValue(propertyDef).release());

  // Set conflict action
  add_property->set_conflict_action(
      static_cast<::physical::ConflictAction>(info->onConflict));

  return physical_opr;
}

std::unique_ptr<::physical::PhysicalOpr>
GDDLConverter::convertToAddEdgePropertySchema(const planner::LogicalAlter& op) {
  const auto* info = op.getInfo();
  if (info->alterType != neug::common::AlterType::ADD_PROPERTY ||
      !checkEntryType(info->tableName,
                      neug::catalog::CatalogEntryType::REL_TABLE_ENTRY)) {
    THROW_RUNTIME_ERROR("Expected ADD_PROPERTY alter type for edge schema");
  }

  // Get alter info from operator
  auto physical_opr = std::make_unique<::physical::PhysicalOpr>();
  auto* add_property =
      physical_opr->mutable_opr()->mutable_add_edge_property_schema();

  // Check edge labels count and set edge type
  std::vector<EdgeLabel> edgeLabels;
  getEdgeLabels(info->tableName, edgeLabels);
  if (edgeLabels.size() != 1) {
    THROW_RUNTIME_ERROR("Edge type must have exactly one edge label");
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

  return physical_opr;
}

std::unique_ptr<::physical::PhysicalOpr>
GDDLConverter::convertToDropVertexPropertySchema(
    const planner::LogicalAlter& op) {
  const auto* info = op.getInfo();
  if (info->alterType != neug::common::AlterType::DROP_PROPERTY ||
      !checkEntryType(info->tableName,
                      neug::catalog::CatalogEntryType::NODE_TABLE_ENTRY)) {
    THROW_RUNTIME_ERROR("Expected DROP_PROPERTY alter type for vertex schema");
  }

  // Get alter info from operator
  auto physical_opr = std::make_unique<::physical::PhysicalOpr>();
  auto* drop_property =
      physical_opr->mutable_opr()->mutable_drop_vertex_property_schema();

  // Set vertex type
  auto* vertex_type = drop_property->mutable_vertex_type();
  vertex_type->set_name(info->tableName);

  // Get property name to drop
  const binder::BoundExtraDropPropertyInfo* extraInfo =
      info->extraInfo->constPtrCast<binder::BoundExtraDropPropertyInfo>();

  *drop_property->add_properties() = std::move(extraInfo->propertyName);

  drop_property->set_conflict_action(
      static_cast<::physical::ConflictAction>(info->onConflict));

  return physical_opr;
}

std::unique_ptr<::physical::PhysicalOpr>
GDDLConverter::convertToDropEdgePropertySchema(
    const planner::LogicalAlter& op) {
  const auto* info = op.getInfo();
  if (info->alterType != neug::common::AlterType::DROP_PROPERTY ||
      !checkEntryType(info->tableName,
                      neug::catalog::CatalogEntryType::REL_TABLE_ENTRY)) {
    THROW_RUNTIME_ERROR("Expected DROP_PROPERTY alter type for edge schema");
  }

  // Get alter info from operator
  auto physical_opr = std::make_unique<::physical::PhysicalOpr>();
  auto* drop_property =
      physical_opr->mutable_opr()->mutable_drop_edge_property_schema();

  // Get edge type
  std::vector<EdgeLabel> edgeLabels;
  getEdgeLabels(info->tableName, edgeLabels);
  if (edgeLabels.size() != 1) {
    THROW_RUNTIME_ERROR("Edge type must have exactly one edge label");
  }
  auto* edgeType = drop_property->mutable_edge_type();
  *edgeType = std::move(*convertToEdgeType(edgeLabels[0]));

  // Get property name to drop
  const binder::BoundExtraDropPropertyInfo* extraInfo =
      info->extraInfo->constPtrCast<binder::BoundExtraDropPropertyInfo>();
  *drop_property->add_properties() = std::move(extraInfo->propertyName);

  drop_property->set_conflict_action(
      static_cast<::physical::ConflictAction>(info->onConflict));

  return physical_opr;
}

std::unique_ptr<::physical::PhysicalOpr>
GDDLConverter::convertToRenameVertexPropertySchema(
    const planner::LogicalAlter& op) {
  const auto* info = op.getInfo();
  if (info->alterType != neug::common::AlterType::RENAME_PROPERTY ||
      !checkEntryType(info->tableName,
                      neug::catalog::CatalogEntryType::NODE_TABLE_ENTRY)) {
    THROW_RUNTIME_ERROR(
        "Expected RENAME_PROPERTY alter type for vertex schema");
  }

  auto physical_opr = std::make_unique<::physical::PhysicalOpr>();
  auto* rename_property =
      physical_opr->mutable_opr()->mutable_rename_vertex_property_schema();

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

  return physical_opr;
}

std::unique_ptr<::physical::PhysicalOpr>
GDDLConverter::convertToRenameEdgePropertySchema(
    const planner::LogicalAlter& op) {
  const auto* info = op.getInfo();
  if (info->alterType != neug::common::AlterType::RENAME_PROPERTY ||
      !checkEntryType(info->tableName,
                      neug::catalog::CatalogEntryType::REL_TABLE_ENTRY)) {
    THROW_RUNTIME_ERROR("Expected RENAME_PROPERTY alter type for edge schema");
  }

  auto physical_opr = std::make_unique<::physical::PhysicalOpr>();
  auto* rename_property =
      physical_opr->mutable_opr()->mutable_rename_edge_property_schema();

  // Get edge type
  std::vector<EdgeLabel> edgeLabels;
  getEdgeLabels(info->tableName, edgeLabels);
  if (edgeLabels.size() != 1) {
    THROW_RUNTIME_ERROR("Edge type must have exactly one edge label");
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

  return physical_opr;
}

std::unique_ptr<::physical::PhysicalOpr>
GDDLConverter::convertToRenameVertexTypeSchema(
    const planner::LogicalAlter& op) {
  const auto* info = op.getInfo();
  if (info->alterType != neug::common::AlterType::RENAME ||
      !checkEntryType(info->tableName,
                      neug::catalog::CatalogEntryType::NODE_TABLE_ENTRY)) {
    THROW_RUNTIME_ERROR("Expected RENAME_TABLE alter type for vertex schema");
  }

  auto physical_opr = std::make_unique<::physical::PhysicalOpr>();
  auto* rename_vertex =
      physical_opr->mutable_opr()->mutable_rename_vertex_type_schema();

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

  return physical_opr;
}

std::unique_ptr<::physical::PhysicalOpr>
GDDLConverter::convertToRenameEdgeTypeSchema(const planner::LogicalAlter& op) {
  const auto* info = op.getInfo();
  if (info->alterType != neug::common::AlterType::RENAME ||
      !checkEntryType(info->tableName,
                      neug::catalog::CatalogEntryType::REL_TABLE_ENTRY)) {
    THROW_RUNTIME_ERROR("Expected RENAME_TABLE alter type for edge schema");
  }

  auto physical_opr = std::make_unique<::physical::PhysicalOpr>();
  auto* rename_edge =
      physical_opr->mutable_opr()->mutable_rename_edge_type_schema();

  // Get old edge type
  std::vector<EdgeLabel> edgeLabels;
  getEdgeLabels(info->tableName, edgeLabels);
  if (edgeLabels.size() != 1) {
    THROW_RUNTIME_ERROR("Edge type must have exactly one edge label");
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
    THROW_RUNTIME_ERROR("New edge type must have exactly one edge label");
  }
  auto* newEdgeType = rename_edge->mutable_new_type();
  *newEdgeType = std::move(*convertToEdgeType(newEdgeLabels[0]));

  // Set conflict action
  rename_edge->set_conflict_action(
      static_cast<::physical::ConflictAction>(info->onConflict));

  return physical_opr;
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

  const auto& transaction = neug::Constants::DEFAULT_TRANSACTION;
  std::vector<neug::catalog::GRelTableCatalogEntry*> relTableEntries;

  if (catalog->containsRelGroup(&transaction, labelName)) {
    auto* groupEntry = catalog->getRelGroupEntry(&transaction, labelName);
    const auto& relTableIds = groupEntry->getRelTableIDs();
    relTableEntries.reserve(relTableIds.size());

    for (const auto& tableId : relTableIds) {
      auto* entry = catalog->getTableCatalogEntry(&transaction, tableId);
      if (!entry ||
          entry->getType() != catalog::CatalogEntryType::REL_TABLE_ENTRY) {
        THROW_RUNTIME_ERROR("Edge Table Entry Not found: " + tableId);
      }
      auto* edgeTableEntry =
          static_cast<neug::catalog::GRelTableCatalogEntry*>(entry);
      relTableEntries.push_back(edgeTableEntry);
    }
  } else {
    auto* entry = catalog->getTableCatalogEntry(&transaction, labelName);
    if (!entry ||
        entry->getType() != catalog::CatalogEntryType::REL_TABLE_ENTRY) {
      THROW_RUNTIME_ERROR("Edge table entry not found: " + labelName);
    }
    auto* edgeTableEntry =
        static_cast<neug::catalog::GRelTableCatalogEntry*>(entry);
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

std::string GDDLConverter::getVertexLabelName(neug::common::oid_t tableId) {
  checkCatalogInitialized();

  auto* entry = catalog->getTableCatalogEntry(
      &neug::Constants::DEFAULT_TRANSACTION, tableId);
  if (!entry ||
      entry->getType() != catalog::CatalogEntryType::NODE_TABLE_ENTRY) {
    THROW_RUNTIME_ERROR("Node table entry not found for id: " +
                        std::to_string(tableId));
  }
  return entry->getName();
}

bool GDDLConverter::checkEntryType(const std::string& labelName,
                                   catalog::CatalogEntryType expectedType) {
  checkCatalogInitialized();

  auto* entry = catalog->getTableCatalogEntry(
      &neug::Constants::DEFAULT_TRANSACTION, labelName);
  if (!entry) {
    THROW_RUNTIME_ERROR("Catalog entry not found for label: " + labelName);
  }
  return entry->getType() == expectedType;
}

}  // namespace gopt
}  // namespace neug