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

#include "neug/compiler/binder/binder.h"
#include "neug/compiler/binder/bound_import_database.h"
#include "neug/compiler/common/copier_config/csv_reader_config.h"
#include "neug/compiler/common/file_system/virtual_file_system.h"
#include "neug/compiler/main/client_context.h"
#include "neug/compiler/parser/copy.h"
#include "neug/compiler/parser/parser.h"
#include "neug/compiler/parser/port_db.h"
#include "neug/utils/exception/exception.h"

using namespace neug::common;
using namespace neug::parser;

namespace neug {
namespace binder {

static std::string getQueryFromFile(VirtualFileSystem* vfs,
                                    const std::string& boundFilePath,
                                    const std::string& fileName,
                                    main::ClientContext* context) {
  auto filePath = vfs->joinPath(boundFilePath, fileName);
  if (!vfs->fileOrPathExists(filePath, context)) {
    if (fileName == PortDBConstants::INDEX_FILE_NAME) {
      return "";
    }
    THROW_BINDER_EXCEPTION(stringFormat("File {} does not exist.", filePath));
  }
  auto fileInfo = vfs->openFile(filePath, FileOpenFlags(FileFlags::READ_ONLY
#ifdef _WIN32
                                                        | FileFlags::BINARY
#endif
                                                        ));
  auto fsize = fileInfo->getFileSize();
  auto buffer = std::make_unique<char[]>(fsize);
  fileInfo->readFile(buffer.get(), fsize);
  return std::string(buffer.get(), fsize);
}

static std::string getColumnNamesToCopy(const CopyFrom& copyFrom) {
  std::string columns = "";
  std::string delimiter = "";
  for (auto& column : copyFrom.getCopyColumnInfo().columnNames) {
    columns += delimiter;
    columns += "`" + column + "`";
    if (delimiter == "") {
      delimiter = ",";
    }
  }
  if (columns.empty()) {
    return columns;
  }
  return stringFormat("({})", columns);
}

static std::string getCopyFilePath(const std::string& boundFilePath,
                                   const std::string& filePath) {
  if (filePath[0] == '/' || (std::isalpha(filePath[0]) && filePath[1] == ':')) {
    // Note:
    // Unix absolute path starts with '/'
    // Windows absolute path starts with "[DiskID]://"
    // This code path is for backward compatability, we used to export the
    // absolute path for csv files to copy.cypher files.
    return filePath;
  }

  auto path = boundFilePath + "/" + filePath;
#if defined(_WIN32)
  // TODO(Ziyi): This is a temporary workaround because our parser requires
  // input cypher queries to escape all special characters in string literal.
  // E.g. The user input query is: 'IMPORT DATABASE 'C:\\db\\uw''. The parser
  // removes any escaped characters and this function accepts the path parameter
  // as 'C:\db\uw'. Then the ImportDatabase operator gives the file path to
  // antlr4 parser directly without escaping any special characters in the path,
  // which causes a parser exception. However, the parser exception is not
  // thrown properly which leads to the undefined behaviour.
  size_t pos = 0;
  while ((pos = path.find('\\', pos)) != std::string::npos) {
    path.replace(pos, 1, "\\\\");
    pos += 2;
  }
#endif
  return path;
}

std::unique_ptr<BoundStatement> Binder::bindImportDatabaseClause(
    const Statement& statement) {
  auto& importDB = statement.constCast<ImportDB>();
  auto fs = clientContext->getVFSUnsafe();
  auto boundFilePath = fs->expandPath(clientContext, importDB.getFilePath());
  if (!fs->fileOrPathExists(boundFilePath, clientContext)) {
    THROW_BINDER_EXCEPTION(
        stringFormat("Directory {} does not exist.", boundFilePath));
  }
  std::string finalQueryStatements;
  finalQueryStatements += getQueryFromFile(
      fs, boundFilePath, PortDBConstants::SCHEMA_FILE_NAME, clientContext);
  // replace the path in copy from statement with the bound path
  auto copyQuery = getQueryFromFile(
      fs, boundFilePath, PortDBConstants::COPY_FILE_NAME, clientContext);
  if (!copyQuery.empty()) {
    auto parsedStatements = Parser::parseQuery(copyQuery);
    for (auto& parsedStatement : parsedStatements) {
      NEUG_ASSERT(parsedStatement->getStatementType() ==
                  StatementType::COPY_FROM);
      auto& copyFromStatement = parsedStatement->constCast<CopyFrom>();
      NEUG_ASSERT(copyFromStatement.getSource()->type == ScanSourceType::FILE);
      auto filePaths = copyFromStatement.getSource()
                           ->constPtrCast<FileScanSource>()
                           ->filePaths;
      NEUG_ASSERT(filePaths.size() == 1);
      auto fileTypeInfo = bindFileTypeInfo(filePaths);
      std::string query;
      auto copyFilePath = getCopyFilePath(boundFilePath, filePaths[0]);
      auto columnNames = getColumnNamesToCopy(copyFromStatement);
      if (fileTypeInfo.fileType == FileType::CSV) {
        auto csvConfig = CSVReaderConfig::construct(
            bindParsingOptions(copyFromStatement.getParsingOptions()));
        query = stringFormat("COPY `{}` {} FROM \"{}\" {};",
                             copyFromStatement.getTableName(), columnNames,
                             copyFilePath, csvConfig.option.toCypher());
      } else {
        query = stringFormat("COPY `{}` {} FROM \"{}\";",
                             copyFromStatement.getTableName(), columnNames,
                             copyFilePath);
      }
      finalQueryStatements += query;
    }
  }
  return std::make_unique<BoundImportDatabase>(
      boundFilePath, finalQueryStatements,
      getQueryFromFile(fs, boundFilePath, PortDBConstants::INDEX_FILE_NAME,
                       clientContext));
}

}  // namespace binder
}  // namespace neug
