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

#include "neug/compiler/common/assert.h"
#include "neug/compiler/parser/copy.h"
#include "neug/compiler/parser/expression/parsed_literal_expression.h"
#include "neug/compiler/parser/scan_source.h"
#include "neug/compiler/parser/transformer.h"

using namespace neug::common;

namespace neug {
namespace parser {

std::unique_ptr<Statement> Transformer::transformCopyTo(
    CypherParser::NEUG_CopyTOContext& ctx) {
  std::string filePath = transformStringLiteral(*ctx.StringLiteral());
  auto regularQuery = transformQuery(*ctx.oC_Query());
  auto copyTo =
      std::make_unique<CopyTo>(std::move(filePath), std::move(regularQuery));
  if (ctx.nEUG_Options()) {
    copyTo->setParsingOption(transformOptions(*ctx.nEUG_Options()));
  }
  return copyTo;
}

std::unique_ptr<Statement> Transformer::transformCopyFrom(
    CypherParser::NEUG_CopyFromContext& ctx) {
  auto source = transformScanSource(*ctx.nEUG_ScanSource());
  auto tableName = transformSchemaName(*ctx.oC_SchemaName());
  auto copyFrom =
      std::make_unique<CopyFrom>(std::move(source), std::move(tableName));
  CopyFromColumnInfo info;
  info.inputColumnOrder = ctx.nEUG_ColumnNames();
  if (ctx.nEUG_ColumnNames()) {
    info.columnNames = transformColumnNames(*ctx.nEUG_ColumnNames());
  }
  if (ctx.nEUG_Options()) {
    copyFrom->setParsingOption(transformOptions(*ctx.nEUG_Options()));
  }
  copyFrom->setColumnInfo(std::move(info));
  return copyFrom;
}

std::unique_ptr<Statement> Transformer::transformCopyFromByColumn(
    CypherParser::NEUG_CopyFromByColumnContext& ctx) {
  auto source =
      std::make_unique<FileScanSource>(transformFilePaths(ctx.StringLiteral()));
  auto tableName = transformSchemaName(*ctx.oC_SchemaName());
  auto copyFrom =
      std::make_unique<CopyFrom>(std::move(source), std::move(tableName));
  copyFrom->setByColumn();
  return copyFrom;
}

std::vector<std::string> Transformer::transformColumnNames(
    CypherParser::NEUG_ColumnNamesContext& ctx) {
  std::vector<std::string> columnNames;
  for (auto& schemaName : ctx.oC_SchemaName()) {
    columnNames.push_back(transformSchemaName(*schemaName));
  }
  return columnNames;
}

std::vector<std::string> Transformer::transformFilePaths(
    const std::vector<antlr4::tree::TerminalNode*>& stringLiteral) {
  std::vector<std::string> csvFiles;
  csvFiles.reserve(stringLiteral.size());
  for (auto& csvFile : stringLiteral) {
    csvFiles.push_back(transformStringLiteral(*csvFile));
  }
  return csvFiles;
}

std::unique_ptr<BaseScanSource> Transformer::transformScanSource(
    CypherParser::NEUG_ScanSourceContext& ctx) {
  if (ctx.nEUG_FilePaths()) {
    auto filePaths = transformFilePaths(ctx.nEUG_FilePaths()->StringLiteral());
    return std::make_unique<FileScanSource>(std::move(filePaths));
  } else if (ctx.oC_Query()) {
    auto query = transformQuery(*ctx.oC_Query());
    return std::make_unique<QueryScanSource>(std::move(query));
  } else if (ctx.oC_Variable()) {
    std::vector<std::string> objectNames;
    objectNames.push_back(transformVariable(*ctx.oC_Variable()));
    if (ctx.oC_SchemaName()) {
      objectNames.push_back(transformSchemaName(*ctx.oC_SchemaName()));
    }
    return std::make_unique<ObjectScanSource>(std::move(objectNames));
  } else if (ctx.oC_FunctionInvocation()) {
    auto functionExpression =
        transformFunctionInvocation(*ctx.oC_FunctionInvocation());
    return std::make_unique<TableFuncScanSource>(std::move(functionExpression));
  }
  NEUG_UNREACHABLE;
}

options_t Transformer::transformOptions(
    CypherParser::NEUG_OptionsContext& ctx) {
  options_t options;
  for (auto loadOption : ctx.nEUG_Option()) {
    auto optionName = transformSymbolicName(*loadOption->oC_SymbolicName());
    // Check if the literal exists, otherwise set the value to true by default
    if (loadOption->oC_Literal()) {
      // If there is a literal, transform it and use it as the value
      options.emplace(optionName, transformLiteral(*loadOption->oC_Literal()));
    } else {
      // If no literal is provided, set the default value to true
      options.emplace(optionName, std::make_unique<ParsedLiteralExpression>(
                                      Value(true), "true"));
    }
  }
  return options;
}

}  // namespace parser
}  // namespace neug
