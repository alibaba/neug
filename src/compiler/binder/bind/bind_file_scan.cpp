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

#include <memory>
#include "neug/compiler/binder/binder.h"
#include "neug/compiler/binder/bound_scan_source.h"
#include "neug/compiler/binder/expression/expression.h"
#include "neug/compiler/binder/expression/literal_expression.h"
#include "neug/compiler/common/string_format.h"
#include "neug/compiler/common/string_utils.h"
#include "neug/compiler/function/read_function.h"
#include "neug/compiler/function/table/bind_input.h"
#include "neug/compiler/gopt/g_type_converter.h"
#include "neug/compiler/main/client_context.h"
#include "neug/compiler/parser/expression/parsed_function_expression.h"
#include "neug/compiler/parser/scan_source.h"
#include "neug/utils/exception/exception.h"
#include "neug/utils/exception/message.h"
#include "neug/utils/reader/schema.h"

using namespace neug::parser;
using namespace neug::binder;
using namespace neug::common;
using namespace neug::function;
using namespace neug::catalog;

namespace neug {
namespace binder {

bool isCompressedFile(const std::filesystem::path& path) {
  return StringUtils::getLower(path.extension().string()) == ".gz";
}

std::string getFileExtension(const std::filesystem::path& path) {
  auto extension = path.extension();
  if (isCompressedFile(path)) {
    extension = path.stem().extension();
  }
  return extension.string();
}

FileTypeInfo bindSingleFileType(const main::ClientContext* context,
                                const std::string& filePath) {
  std::filesystem::path fileName(filePath);
  auto extension = getFileExtension(fileName);
  return FileTypeInfo{
      FileTypeUtils::getFileTypeFromExtension(extension),
      extension.substr(std::min<uint64_t>(1, extension.length()))};
}

FileTypeInfo Binder::bindFileTypeInfo(
    const std::vector<std::string>& filePaths) const {
  auto expectedFileType = FileTypeInfo{FileType::UNKNOWN, "" /* fileTypeStr */};
  for (auto& filePath : filePaths) {
    auto fileType = bindSingleFileType(clientContext, filePath);
    expectedFileType = (expectedFileType.fileType == FileType::UNKNOWN)
                           ? fileType
                           : expectedFileType;
    if (fileType.fileType != expectedFileType.fileType) {
      THROW_COPY_EXCEPTION(
          "Loading files with different types is not currently supported.");
    }
  }
  return expectedFileType;
}

std::vector<std::string> Binder::bindFilePaths(
    const std::vector<std::string>& filePaths) const {
  return filePaths;
}

case_insensitive_map_t<Value> Binder::bindParsingOptions(
    const options_t& parsingOptions) {
  case_insensitive_map_t<Value> options;
  for (auto& option : parsingOptions) {
    auto name = option.first;
    StringUtils::toUpper(name);
    auto expr = expressionBinder.bindExpression(*option.second);
    NEUG_ASSERT(expr->expressionType == ExpressionType::LITERAL);
    auto literalExpr = neug_dynamic_cast<LiteralExpression*>(expr.get());
    options.insert({name, literalExpr->getValue()});
  }
  return options;
}

std::unique_ptr<BoundBaseScanSource> Binder::bindScanSource(
    const BaseScanSource* source, const options_t& options,
    const std::vector<std::string>& columnNames,
    const std::vector<LogicalType>& columnTypes) {
  switch (source->type) {
  case ScanSourceType::FILE: {
    return bindFileScanSource(*source, options, columnNames, columnTypes);
  }
  case ScanSourceType::QUERY: {
    return bindQueryScanSource(*source, options, columnNames, columnTypes);
  }
  case ScanSourceType::OBJECT: {
    return bindObjectScanSource(*source, options, columnNames, columnTypes);
  }
  case ScanSourceType::TABLE_FUNC: {
    return bindTableFuncScanSource(*source, options, columnNames, columnTypes);
  }
  default:
    NEUG_UNREACHABLE;
  }
}

std::shared_ptr<reader::EntrySchema> sniff(const FileScanInfo& fileScanInfo,
                                           function::TableFunction* func) {
  // Create FileSchema
  reader::FileSchema fileSchema;
  fileSchema.paths = fileScanInfo.filePaths;
  fileSchema.format = fileScanInfo.fileTypeInfo.fileTypeStr;
  auto& options = fileSchema.options;
  for (auto& option : fileScanInfo.options) {
    options.insert({option.first, option.second.toString()});
  }
  auto readFunction = func->ptrCast<function::ReadFunction>();
  return readFunction->sniffFunc(fileSchema);
}

std::unique_ptr<BoundBaseScanSource> Binder::bindFileScanSource(
    const BaseScanSource& scanSource, const options_t& options,
    const std::vector<std::string>& columnNames,
    const std::vector<LogicalType>& columnTypes) {
  if (columnNames.size() != columnTypes.size()) {
    THROW_BINDER_EXCEPTION("Column names size " +
                           std::to_string(columnNames.size()) +
                           " does not match column types size " +
                           std::to_string(columnTypes.size()));
  }
  auto fileSource = scanSource.constPtrCast<FileScanSource>();
  auto filePaths = bindFilePaths(fileSource->filePaths);
  auto boundOptions = bindParsingOptions(options);
  FileTypeInfo fileTypeInfo;
  if (boundOptions.contains(FileScanInfo::FILE_FORMAT_OPTION_NAME)) {
    auto fileFormat =
        boundOptions.at(FileScanInfo::FILE_FORMAT_OPTION_NAME).toString();
    fileTypeInfo =
        FileTypeInfo{FileTypeUtils::fromString(fileFormat), fileFormat};
  } else {
    fileTypeInfo = bindFileTypeInfo(filePaths);
  }
  boundOptions.erase(FileScanInfo::FILE_FORMAT_OPTION_NAME);
  // Bind file configuration
  auto fileScanInfo =
      std::make_unique<FileScanInfo>(std::move(fileTypeInfo), filePaths);
  fileScanInfo->options = std::move(boundOptions);
  auto func = getScanFunction(fileScanInfo->fileTypeInfo, *fileScanInfo);

  // Bind table function
  auto bindInput = TableFuncBindInput();
  bindInput.addLiteralParam(Value::createValue(filePaths[0]));
  auto extraInput = std::make_unique<ExtraScanTableFuncBindInput>();
  extraInput->fileScanInfo = fileScanInfo->copy();
  auto& expectedColumnNames = extraInput->expectedColumnNames;
  auto& expectedColumnTypes = extraInput->expectedColumnTypes;

  auto sniffSchema = sniff(*fileScanInfo, func);
  if (sniffSchema->columnNames.size() != sniffSchema->columnTypes.size()) {
    THROW_BINDER_EXCEPTION("Sniffer Column names size " +
                           std::to_string(sniffSchema->columnNames.size()) +
                           " does not match column types size " +
                           std::to_string(sniffSchema->columnTypes.size()));
  }
  // User does not explicitly provide column names and types in pre-defined
  // schema, use sniffed schema instead.
  if (columnNames.empty()) {
    expectedColumnNames = sniffSchema->columnNames;
    auto typeConverter = gopt::GLogicalTypeConverter();
    for (auto& columnType : sniffSchema->columnTypes) {
      expectedColumnTypes.push_back(typeConverter.convertDataType(*columnType));
    }
  } else {
    // sniffed schema should have same column size as the pre-defined schema.
    if (sniffSchema->columnNames.size() != columnNames.size()) {
      THROW_SCHEMA_MISMATCH(stringFormat(
          "Sniffed schema has {} columns but {} columns were expected.",
          sniffSchema->columnNames.size(), columnNames.size()));
    }
    expectedColumnNames = columnNames;
    expectedColumnTypes = LogicalType::copy(columnTypes);
  }

  extraInput->tableFunction = func;
  bindInput.extraInput = std::move(extraInput);
  bindInput.binder = this;
  auto bindData = getScanFuncBindData(&bindInput);
  auto info = BoundTableScanInfo(*func, std::move(bindData));
  return std::make_unique<BoundTableScanSource>(ScanSourceType::FILE,
                                                std::move(info));
}

std::unique_ptr<BoundBaseScanSource> Binder::bindQueryScanSource(
    const BaseScanSource& scanSource, const options_t& options,
    const std::vector<std::string>& columnNames,
    const std::vector<LogicalType>&) {
  auto querySource = scanSource.constPtrCast<QueryScanSource>();
  auto boundStatement = bind(*querySource->statement);
  auto columns = boundStatement->getStatementResult()->getColumns();
  if (columns.size() != columnNames.size()) {
    THROW_SCHEMA_MISMATCH(
        stringFormat("Query returns {} columns but {} columns were expected.",
                     columns.size(), columnNames.size()));
  }
  auto scanInfo = BoundQueryScanSourceInfo(bindParsingOptions(options));
  return std::make_unique<BoundQueryScanSource>(std::move(boundStatement),
                                                std::move(scanInfo));
}

static TableFunction getObjectScanFunc(
    const std::string& dbName, const std::string& tableName,
    const main::ClientContext* clientContext) {
  THROW_EXCEPTION_WITH_FILE_LINE("get object scan func not implemented");
}

BoundTableScanInfo bindTableScanSourceInfo(
    Binder& binder, TableFunction func, const std::string& sourceName,
    std::unique_ptr<TableFuncBindData> bindData,
    const std::vector<std::string>& columnNames,
    const std::vector<LogicalType>& columnTypes) {
  expression_vector columns;
  if (columnTypes.empty()) {
  } else {
    if (bindData->getNumColumns() != columnTypes.size()) {
      THROW_BINDER_EXCEPTION(stringFormat(
          "{} has {} columns but {} columns were expected.", sourceName,
          bindData->getNumColumns(), columnTypes.size()));
    }
    for (auto i = 0u; i < bindData->getNumColumns(); ++i) {
      auto column = binder.createInvisibleVariable(
          columnNames[i], bindData->columns[i]->getDataType());
      binder.replaceExpressionInScope(bindData->columns[i]->toString(),
                                      columnNames[i], column);
      columns.push_back(column);
    }
    bindData->columns = columns;
  }
  return BoundTableScanInfo(func, std::move(bindData));
}

std::unique_ptr<BoundBaseScanSource> Binder::bindObjectScanSource(
    const BaseScanSource& scanSource, const options_t& options,
    const std::vector<std::string>& columnNames,
    const std::vector<LogicalType>& columnTypes) {
  auto objectSource = scanSource.constPtrCast<ObjectScanSource>();
  TableFunction func;
  std::unique_ptr<TableFuncBindData> bindData;
  std::string objectName;
  auto bindInput = TableFuncBindInput();
  bindInput.binder = this;
  if (objectSource->objectNames.size() == 1) {
    // Bind external object as table
    objectName = objectSource->objectNames[0];
    auto replacementData = clientContext->tryReplace(objectName);
    if (replacementData != nullptr) {  // Replace as python object
      func = replacementData->func;
      auto replaceExtraInput = std::make_unique<ExtraScanTableFuncBindInput>();
      replaceExtraInput->fileScanInfo.options = bindParsingOptions(options);
      replacementData->bindInput.extraInput = std::move(replaceExtraInput);
      replacementData->bindInput.binder = this;
      bindData = func.bindFunc(clientContext, &replacementData->bindInput);
    } else {
      THROW_BINDER_EXCEPTION(ExceptionMessage::variableNotInScope(objectName));
    }
  } else if (objectSource->objectNames.size() == 2) {
    // Bind external database table
    objectName =
        objectSource->objectNames[0] + "." + objectSource->objectNames[1];
    func = getObjectScanFunc(objectSource->objectNames[0],
                             objectSource->objectNames[1], clientContext);
    bindData = func.bindFunc(clientContext, &bindInput);
  } else {
    // LCOV_EXCL_START
    THROW_BINDER_EXCEPTION(
        stringFormat("Cannot find object {}.",
                     StringUtils::join(objectSource->objectNames, ",")));
    // LCOV_EXCL_STOP
  }
  auto info = bindTableScanSourceInfo(
      *this, func, objectName, std::move(bindData), columnNames, columnTypes);
  return std::make_unique<BoundTableScanSource>(ScanSourceType::OBJECT,
                                                std::move(info));
}

std::unique_ptr<BoundBaseScanSource> Binder::bindTableFuncScanSource(
    const BaseScanSource& scanSource, const options_t& options,
    const std::vector<std::string>& columnNames,
    const std::vector<LogicalType>& columnTypes) {
  if (!options.empty()) {
    THROW_BINDER_EXCEPTION(
        "No option is supported when copying from table functions.");
  }
  auto tableFuncScanSource = scanSource.constPtrCast<TableFuncScanSource>();
  auto& parsedFuncExpression =
      tableFuncScanSource->functionExpression
          ->constCast<parser::ParsedFunctionExpression>();
  auto boundTableFunc = bindTableFunc(parsedFuncExpression.getFunctionName(),
                                      *tableFuncScanSource->functionExpression,
                                      {} /* yieldVariables */);
  auto& tableFunc = boundTableFunc.func;
  auto info = bindTableScanSourceInfo(*this, tableFunc, tableFunc.name,
                                      std::move(boundTableFunc.bindData),
                                      columnNames, columnTypes);
  return std::make_unique<BoundTableScanSource>(ScanSourceType::OBJECT,
                                                std::move(info));
}

}  // namespace binder
}  // namespace neug
