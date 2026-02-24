/**
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * This file is originally from the Kùzu project
 * (https://github.com/kuzudb/kuzu) Licensed under the MIT License. Modified by
 * Zhou Xiaoli in 2025 to support Neug-specific features.
 */

#include "neug/compiler/parser/ddl/alter.h"
#include "neug/compiler/parser/ddl/create_sequence.h"
#include "neug/compiler/parser/ddl/create_table.h"
#include "neug/compiler/parser/ddl/create_type.h"
#include "neug/compiler/parser/ddl/drop.h"
#include "neug/compiler/parser/ddl/drop_info.h"
#include "neug/compiler/parser/transformer.h"
#include "neug/utils/exception/exception.h"

using namespace neug::common;
using namespace neug::catalog;

namespace neug {
namespace parser {

std::unique_ptr<Statement> Transformer::transformAlterTable(
    CypherParser::NEUG_AlterTableContext& ctx) {
  if (ctx.nEUG_AlterOptions()->nEUG_AddProperty()) {
    return transformAddProperty(ctx);
  } else if (ctx.nEUG_AlterOptions()->nEUG_DropProperty()) {
    return transformDropProperty(ctx);
  } else if (ctx.nEUG_AlterOptions()->nEUG_RenameTable()) {
    return transformRenameTable(ctx);
  } else {
    return transformRenameProperty(ctx);
  }
}

std::string Transformer::getPKName(
    CypherParser::NEUG_CreateNodeTableContext& ctx) {
  auto pkCount = 0;
  std::string pkName;
  auto& propertyDefinitions = *ctx.nEUG_PropertyDefinitions();
  for (auto& definition : propertyDefinitions.nEUG_PropertyDefinition()) {
    if (definition->PRIMARY() && definition->KEY()) {
      pkCount++;
      pkName = transformPrimaryKey(*definition->nEUG_ColumnDefinition());
    }
  }
  if (ctx.nEUG_CreateNodeConstraint()) {
    // In the case where no pkName has been found, or the Node Constraint's name
    // is different than the pkName found, add the counter.
    if (pkCount == 0 ||
        transformPrimaryKey(*ctx.nEUG_CreateNodeConstraint()) != pkName) {
      pkCount++;
    }
    pkName = transformPrimaryKey(*ctx.nEUG_CreateNodeConstraint());
  }
  if (pkCount == 0) {
    // Raise exception when no PRIMARY KEY is specified.
    THROW_PARSER_EXCEPTION("Can not find primary key.");
  } else if (pkCount > 1) {
    // Raise exception when multiple PRIMARY KEY are specified.
    THROW_PARSER_EXCEPTION("Found multiple primary keys.");
  }
  return pkName;
}

static ConflictAction getConflictAction(
    CypherParser::NEUG_IfNotExistsContext* ctx) {
  if (ctx != nullptr) {
    return ConflictAction::ON_CONFLICT_DO_NOTHING;
  }
  return ConflictAction::ON_CONFLICT_THROW;
}

std::unique_ptr<Statement> Transformer::transformCreateNodeTable(
    CypherParser::NEUG_CreateNodeTableContext& ctx) {
  auto tableName = transformSchemaName(*ctx.oC_SchemaName());
  std::string pkName;
  pkName = getPKName(ctx);
  auto createTableInfo =
      CreateTableInfo(CatalogEntryType::NODE_TABLE_ENTRY, tableName,
                      getConflictAction(ctx.nEUG_IfNotExists()));
  createTableInfo.propertyDefinitions =
      transformPropertyDefinitions(*ctx.nEUG_PropertyDefinitions());
  createTableInfo.extraInfo =
      std::make_unique<ExtraCreateNodeTableInfo>(pkName);
  return std::make_unique<CreateTable>(std::move(createTableInfo));
}

static bool requireRelGroup(
    const std::vector<std::pair<std::string, std::string>>& fromToPairs) {
  return fromToPairs.size() > 1;
}

std::unique_ptr<Statement> Transformer::transformCreateRelTable(
    CypherParser::NEUG_CreateRelTableContext& ctx) {
  auto tableName = transformSchemaName(*ctx.oC_SchemaName());
  std::string relMultiplicity = "MANY_MANY";
  if (ctx.oC_SymbolicName()) {
    relMultiplicity = transformSymbolicName(*ctx.oC_SymbolicName());
  }
  options_t options;
  if (ctx.nEUG_Options()) {
    options = transformOptions(*ctx.nEUG_Options());
  }
  std::vector<std::pair<std::string, std::string>> fromToPairs;
  for (auto& fromTo : ctx.nEUG_FromToConnections()->nEUG_FromToConnection()) {
    auto src = transformSchemaName(*fromTo->oC_SchemaName(0));
    auto dst = transformSchemaName(*fromTo->oC_SchemaName(1));
    fromToPairs.emplace_back(src, dst);
  }

  std::unique_ptr<ExtraCreateTableInfo> extraInfo;
  auto entryType = CatalogEntryType::DUMMY_ENTRY;
  if (requireRelGroup(fromToPairs)) {
    entryType = CatalogEntryType::REL_GROUP_ENTRY;
    extraInfo = std::make_unique<ExtraCreateRelTableGroupInfo>(
        relMultiplicity, std::move(fromToPairs), std::move(options));
  } else {
    entryType = CatalogEntryType::REL_TABLE_ENTRY;
    extraInfo = std::make_unique<ExtraCreateRelTableInfo>(
        relMultiplicity, fromToPairs[0].first, fromToPairs[0].second,
        std::move(options));
  }
  auto conflictAction = getConflictAction(ctx.nEUG_IfNotExists());
  auto createTableInfo = CreateTableInfo(entryType, tableName, conflictAction);
  if (ctx.nEUG_PropertyDefinitions()) {
    createTableInfo.propertyDefinitions =
        transformPropertyDefinitions(*ctx.nEUG_PropertyDefinitions());
  }
  createTableInfo.extraInfo = std::move(extraInfo);
  return std::make_unique<CreateTable>(std::move(createTableInfo));
}

std::unique_ptr<Statement> Transformer::transformCreateSequence(
    CypherParser::NEUG_CreateSequenceContext& ctx) {
  auto sequenceName = transformSchemaName(*ctx.oC_SchemaName());
  auto createSequenceInfo = CreateSequenceInfo(
      sequenceName, ctx.nEUG_IfNotExists()
                        ? common::ConflictAction::ON_CONFLICT_DO_NOTHING
                        : common::ConflictAction::ON_CONFLICT_THROW);
  std::unordered_set<SequenceInfoType> applied;
  for (auto seqOption : ctx.nEUG_SequenceOptions()) {
    SequenceInfoType type;  // NOLINT(*-init-variables)
    std::string typeString;
    CypherParser::OC_IntegerLiteralContext* valCtx = nullptr;
    std::string* valOption = nullptr;
    if (seqOption->nEUG_StartWith()) {
      type = SequenceInfoType::START;
      typeString = "START";
      valCtx = seqOption->nEUG_StartWith()->oC_IntegerLiteral();
      valOption = &createSequenceInfo.startWith;
      *valOption = seqOption->nEUG_StartWith()->MINUS() ? "-" : "";
    } else if (seqOption->nEUG_IncrementBy()) {
      type = SequenceInfoType::INCREMENT;
      typeString = "INCREMENT";
      valCtx = seqOption->nEUG_IncrementBy()->oC_IntegerLiteral();
      valOption = &createSequenceInfo.increment;
      *valOption = seqOption->nEUG_IncrementBy()->MINUS() ? "-" : "";
    } else if (seqOption->nEUG_MinValue()) {
      type = SequenceInfoType::MINVALUE;
      typeString = "MINVALUE";
      if (!seqOption->nEUG_MinValue()->NO()) {
        valCtx = seqOption->nEUG_MinValue()->oC_IntegerLiteral();
        valOption = &createSequenceInfo.minValue;
        *valOption = seqOption->nEUG_MinValue()->MINUS() ? "-" : "";
      }
    } else if (seqOption->nEUG_MaxValue()) {
      type = SequenceInfoType::MAXVALUE;
      typeString = "MAXVALUE";
      if (!seqOption->nEUG_MaxValue()->NO()) {
        valCtx = seqOption->nEUG_MaxValue()->oC_IntegerLiteral();
        valOption = &createSequenceInfo.maxValue;
        *valOption = seqOption->nEUG_MaxValue()->MINUS() ? "-" : "";
      }
    } else {  // seqOption->nEUG_Cycle()
      type = SequenceInfoType::CYCLE;
      typeString = "CYCLE";
      if (!seqOption->nEUG_Cycle()->NO()) {
        createSequenceInfo.cycle = true;
      }
    }
    if (applied.find(type) != applied.end()) {
      THROW_PARSER_EXCEPTION(typeString + " should be passed at most once.");
    }
    applied.insert(type);

    if (valCtx && valOption) {
      *valOption += valCtx->DecimalInteger()->getText();
    }
  }
  return std::make_unique<CreateSequence>(std::move(createSequenceInfo));
}

std::unique_ptr<Statement> Transformer::transformCreateType(
    CypherParser::NEUG_CreateTypeContext& ctx) {
  auto name = transformSchemaName(*ctx.oC_SchemaName());
  auto type = transformDataType(*ctx.nEUG_DataType());
  return std::make_unique<CreateType>(name, type);
}

DropType transformDropType(CypherParser::NEUG_DropContext& ctx) {
  if (ctx.TABLE()) {
    return DropType::TABLE;
  } else if (ctx.SEQUENCE()) {
    return DropType::SEQUENCE;
  } else {
    NEUG_UNREACHABLE;
  }
}

std::unique_ptr<Statement> Transformer::transformDrop(
    CypherParser::NEUG_DropContext& ctx) {
  auto name = transformSchemaName(*ctx.oC_SchemaName());
  auto dropType = transformDropType(ctx);
  auto conflictAction = ctx.nEUG_IfExists()
                            ? common::ConflictAction::ON_CONFLICT_DO_NOTHING
                            : common::ConflictAction::ON_CONFLICT_THROW;
  return std::make_unique<Drop>(
      DropInfo{std::move(name), dropType, conflictAction});
}

std::unique_ptr<Statement> Transformer::transformRenameTable(
    CypherParser::NEUG_AlterTableContext& ctx) {
  auto tableName = transformSchemaName(*ctx.oC_SchemaName());
  auto newName = transformSchemaName(
      *ctx.nEUG_AlterOptions()->nEUG_RenameTable()->oC_SchemaName());
  auto extraInfo = std::make_unique<ExtraRenameTableInfo>(std::move(newName));
  auto info = AlterInfo(AlterType::RENAME, tableName, std::move(extraInfo));
  return std::make_unique<Alter>(std::move(info));
}

std::unique_ptr<Statement> Transformer::transformAddProperty(
    CypherParser::NEUG_AlterTableContext& ctx) {
  auto tableName = transformSchemaName(*ctx.oC_SchemaName());
  auto addPropertyCtx = ctx.nEUG_AlterOptions()->nEUG_AddProperty();
  auto propertyName =
      transformPropertyKeyName(*addPropertyCtx->oC_PropertyKeyName());
  auto dataType = transformDataType(*addPropertyCtx->nEUG_DataType());
  std::unique_ptr<ParsedExpression> defaultValue = nullptr;
  if (addPropertyCtx->nEUG_Default()) {
    defaultValue =
        transformExpression(*addPropertyCtx->nEUG_Default()->oC_Expression());
  }
  auto extraInfo = std::make_unique<ExtraAddPropertyInfo>(
      std::move(propertyName), std::move(dataType), std::move(defaultValue));
  ConflictAction action = ConflictAction::ON_CONFLICT_THROW;
  if (addPropertyCtx->nEUG_IfNotExists()) {
    action = ConflictAction::ON_CONFLICT_DO_NOTHING;
  }
  auto info = AlterInfo(AlterType::ADD_PROPERTY, tableName,
                        std::move(extraInfo), action);
  return std::make_unique<Alter>(std::move(info));
}

std::unique_ptr<Statement> Transformer::transformDropProperty(
    CypherParser::NEUG_AlterTableContext& ctx) {
  auto tableName = transformSchemaName(*ctx.oC_SchemaName());
  auto dropProperty = ctx.nEUG_AlterOptions()->nEUG_DropProperty();
  auto propertyName =
      transformPropertyKeyName(*dropProperty->oC_PropertyKeyName());
  auto extraInfo =
      std::make_unique<ExtraDropPropertyInfo>(std::move(propertyName));
  common::ConflictAction action = common::ConflictAction::ON_CONFLICT_THROW;
  if (dropProperty->nEUG_IfExists()) {
    action = common::ConflictAction::ON_CONFLICT_DO_NOTHING;
  }
  auto info = AlterInfo(AlterType::DROP_PROPERTY, tableName,
                        std::move(extraInfo), action);
  return std::make_unique<Alter>(std::move(info));
}

std::unique_ptr<Statement> Transformer::transformRenameProperty(
    CypherParser::NEUG_AlterTableContext& ctx) {
  auto tableName = transformSchemaName(*ctx.oC_SchemaName());
  auto propertyName = transformPropertyKeyName(
      *ctx.nEUG_AlterOptions()->nEUG_RenameProperty()->oC_PropertyKeyName()[0]);
  auto newName = transformPropertyKeyName(
      *ctx.nEUG_AlterOptions()->nEUG_RenameProperty()->oC_PropertyKeyName()[1]);
  auto extraInfo =
      std::make_unique<ExtraRenamePropertyInfo>(propertyName, newName);
  auto info =
      AlterInfo(AlterType::RENAME_PROPERTY, tableName, std::move(extraInfo));
  return std::make_unique<Alter>(std::move(info));
}

std::unique_ptr<Statement> Transformer::transformCommentOn(
    CypherParser::NEUG_CommentOnContext& ctx) {
  auto tableName = transformSchemaName(*ctx.oC_SchemaName());
  auto comment = transformStringLiteral(*ctx.StringLiteral());
  auto extraInfo = std::make_unique<ExtraCommentInfo>(comment);
  auto info = AlterInfo(AlterType::COMMENT, tableName, std::move(extraInfo));
  return std::make_unique<Alter>(std::move(info));
}

std::vector<ParsedColumnDefinition> Transformer::transformColumnDefinitions(
    CypherParser::NEUG_ColumnDefinitionsContext& ctx) {
  std::vector<ParsedColumnDefinition> definitions;
  for (auto& definition : ctx.nEUG_ColumnDefinition()) {
    definitions.emplace_back(transformColumnDefinition(*definition));
  }
  return definitions;
}

ParsedColumnDefinition Transformer::transformColumnDefinition(
    CypherParser::NEUG_ColumnDefinitionContext& ctx) {
  auto propertyName = transformPropertyKeyName(*ctx.oC_PropertyKeyName());
  auto dataType = transformDataType(*ctx.nEUG_DataType());
  return ParsedColumnDefinition(propertyName, dataType);
}

std::vector<ParsedPropertyDefinition> Transformer::transformPropertyDefinitions(
    CypherParser::NEUG_PropertyDefinitionsContext& ctx) {
  std::vector<ParsedPropertyDefinition> definitions;
  for (auto& definition : ctx.nEUG_PropertyDefinition()) {
    auto columnDefinition =
        transformColumnDefinition(*definition->nEUG_ColumnDefinition());
    std::unique_ptr<ParsedExpression> defaultExpr = nullptr;
    if (definition->nEUG_Default()) {
      defaultExpr =
          transformExpression(*definition->nEUG_Default()->oC_Expression());
    }
    definitions.push_back(ParsedPropertyDefinition(std::move(columnDefinition),
                                                   std::move(defaultExpr)));
  }
  return definitions;
}

std::string Transformer::transformDataType(
    CypherParser::NEUG_DataTypeContext& ctx) {
  return ctx.getText();
}

std::string Transformer::transformPrimaryKey(
    CypherParser::NEUG_CreateNodeConstraintContext& ctx) {
  return transformPropertyKeyName(*ctx.oC_PropertyKeyName());
}

std::string Transformer::transformPrimaryKey(
    CypherParser::NEUG_ColumnDefinitionContext& ctx) {
  return transformPropertyKeyName(*ctx.oC_PropertyKeyName());
}

}  // namespace parser
}  // namespace neug
