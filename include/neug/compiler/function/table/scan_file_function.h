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

#include <mutex>
#include <vector>

#include "neug/compiler/binder/expression/expression.h"
#include "neug/compiler/common/constants.h"
#include "neug/compiler/common/copier_config/file_scan_info.h"
#include "neug/compiler/function/table/bind_data.h"
#include "neug/compiler/function/table/table_function.h"

namespace neug {
namespace common {
class FileSystem;
}

namespace function {

struct ScanFileSharedState : public TableFuncSharedState {
  const common::FileScanInfo fileScanInfo;
  uint64_t fileIdx;
  uint64_t blockIdx;

  ScanFileSharedState(common::FileScanInfo fileScanInfo, uint64_t numRows)
      : TableFuncSharedState{numRows},
        fileScanInfo{std::move(fileScanInfo)},
        fileIdx{0},
        blockIdx{0} {}

  std::pair<uint64_t, uint64_t> getNext() {
    std::lock_guard guard{mtx};
    return fileIdx >= fileScanInfo.getNumFiles()
               ? std::make_pair(UINT64_MAX, UINT64_MAX)
               : std::make_pair(fileIdx, blockIdx++);
  }
};

struct ScanFileWithProgressSharedState : ScanFileSharedState {
  main::ClientContext* context;
  uint64_t totalSize;  // TODO(Mattias): I think we should unify the design on
                       // how we calculate the progress bar for scanning. Can we
                       // simply rely on a numRowsScaned stored in the
                       // TableFuncSharedState to determine the progress.
  ScanFileWithProgressSharedState(common::FileScanInfo fileScanInfo,
                                  uint64_t numRows,
                                  main::ClientContext* context)
      : ScanFileSharedState{std::move(fileScanInfo), numRows},
        context{context},
        totalSize{0} {}
};

struct NEUG_API ScanFileBindData : public TableFuncBindData {
  common::FileScanInfo fileScanInfo;
  main::ClientContext* context;
  common::column_id_t numWarningDataColumns = 0;

  ScanFileBindData(binder::expression_vector columns, uint64_t numRows,
                   common::FileScanInfo fileScanInfo,
                   main::ClientContext* context)
      : TableFuncBindData{std::move(columns), numRows},
        fileScanInfo{std::move(fileScanInfo)},
        context{context} {}
  ScanFileBindData(binder::expression_vector columns, uint64_t numRows,
                   common::FileScanInfo fileScanInfo,
                   main::ClientContext* context,
                   common::column_id_t numWarningDataColumns)
      : TableFuncBindData{std::move(columns), numRows},
        fileScanInfo{std::move(fileScanInfo)},
        context{context},
        numWarningDataColumns{numWarningDataColumns} {}
  ScanFileBindData(const ScanFileBindData& other)
      : TableFuncBindData{other},
        fileScanInfo{other.fileScanInfo.copy()},
        context{other.context},
        numWarningDataColumns{other.numWarningDataColumns} {}

  bool getIgnoreErrorsOption() const override {
    return fileScanInfo.getOption(
        common::CopyConstants::IGNORE_ERRORS_OPTION_NAME,
        common::CopyConstants::DEFAULT_IGNORE_ERRORS);
  }

  std::unique_ptr<TableFuncBindData> copy() const override {
    return std::make_unique<ScanFileBindData>(*this);
  }
};

}  // namespace function
}  // namespace neug
