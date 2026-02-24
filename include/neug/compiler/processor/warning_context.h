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

#include <functional>
#include <mutex>
#include <vector>

#include "neug/compiler/common/types/types.h"
#include "neug/compiler/main/client_config.h"
#include "neug/compiler/processor/operator/persistent/reader/copy_from_error.h"
#include "neug/utils/api.h"

namespace neug {
namespace common {
class ValueVector;
}
namespace storage {
class ColumnChunkData;
}

namespace processor {

class SerialCSVReader;

struct WarningInfo {
  uint64_t queryID;
  PopulatedCopyFromError warning;

  WarningInfo(PopulatedCopyFromError warning, uint64_t queryID)
      : queryID(queryID), warning(std::move(warning)) {}
};

using populate_func_t =
    std::function<PopulatedCopyFromError(CopyFromFileError, common::idx_t)>;
using get_file_idx_func_t =
    std::function<common::idx_t(const CopyFromFileError&)>;

class NEUG_API WarningContext {
 public:
  explicit WarningContext(main::ClientConfig* clientConfig) {}

  void appendWarningMessages(const std::vector<CopyFromFileError>& messages) {}

  void populateWarnings(uint64_t queryID, populate_func_t populateFunc = {},
                        get_file_idx_func_t getFileIdxFunc = {}) {}

  void defaultPopulateAllWarnings(uint64_t queryID) {}

  const std::vector<WarningInfo> getPopulatedWarnings() const { return {}; }

  uint64_t getWarningCount(uint64_t queryID) { return 0; }
  void clearPopulatedWarnings() {}

  void setIgnoreErrorsForCurrentQuery(bool ignoreErrors) {}
  bool getIgnoreErrorsOption() const { return false; }

 private:
  std::mutex mtx;
  std::vector<CopyFromFileError> unpopulatedWarnings;
  std::vector<WarningInfo> populatedWarnings;
};

}  // namespace processor
}  // namespace neug
