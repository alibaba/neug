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

#pragma once
#include "neug/compiler/binder/binder.h"
#include "neug/compiler/binder/bound_statement.h"
#include "neug/compiler/binder/query/bound_regular_query.h"
#include "neug/compiler/common/copier_config/file_scan_info.h"

namespace neug {
namespace binder {

struct ExportedTableData {
  std::string tableName;
  std::unique_ptr<BoundRegularQuery> regularQuery;
  std::vector<std::string> columnNames;
  std::vector<common::LogicalType> columnTypes;

  const std::vector<common::LogicalType>& getColumnTypesRef() const {
    return columnTypes;
  }
  const BoundRegularQuery* getRegularQuery() const {
    return regularQuery.get();
  }
};

class BoundExportDatabase final : public BoundStatement {
  static constexpr common::StatementType type_ =
      common::StatementType::EXPORT_DATABASE;

 public:
  BoundExportDatabase(std::string filePath, common::FileTypeInfo fileTypeInfo,
                      std::vector<ExportedTableData> exportData,
                      common::case_insensitive_map_t<common::Value> csvOption)
      : BoundStatement{type_,
                       BoundStatementResult::createSingleStringColumnResult()},
        exportData(std::move(exportData)),
        boundFileInfo(std::move(fileTypeInfo),
                      std::vector{std::move(filePath)}) {
    boundFileInfo.options = std::move(csvOption);
  }

  std::string getFilePath() const { return boundFileInfo.filePaths[0]; }
  common::FileType getFileType() const {
    return boundFileInfo.fileTypeInfo.fileType;
  }
  common::case_insensitive_map_t<common::Value> getExportOptions() const {
    return boundFileInfo.options;
  }
  const common::FileScanInfo* getBoundFileInfo() const {
    return &boundFileInfo;
  }
  const std::vector<ExportedTableData>* getExportData() const {
    return &exportData;
  }

 private:
  std::vector<ExportedTableData> exportData;
  common::FileScanInfo boundFileInfo;
};

}  // namespace binder
}  // namespace neug
