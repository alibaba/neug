#pragma once

#include <functional>
#include <mutex>
#include <vector>

#include "src/include/common/api.h"
#include "src/include/common/types/types.h"
#include "src/include/main/client_config.h"
#include "src/include/processor/operator/persistent/reader/copy_from_error.h"

namespace gs {
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

class KUZU_API WarningContext {
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
}  // namespace gs
