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

#pragma once

#include <memory>
#include <vector>

#include "neug/execution/common/context.h"
#include "neug/utils/io/read/common/options.h"
#include "neug/utils/io/read/common/read_state.h"
#include "neug/utils/io/read/csv/csv_read_config.h"
#include "neug/utils/result.h"

namespace neug {

class IDataChunkSupplier;

namespace execution {
class Context;
}

namespace reader {

class CsvReader {
 public:
  explicit CsvReader(std::shared_ptr<ReadSharedState> sharedState,
                     std::unique_ptr<CsvOptionsBuilder> optionsBuilder);
  ~CsvReader();

  void read(std::shared_ptr<ReadLocalState> localState,
            execution::Context& ctx);

  result<std::shared_ptr<EntrySchema>> inferSchema();

 private:
  void full_read(
      const std::vector<std::shared_ptr<IDataChunkSupplier>>& suppliers,
      execution::Context& output, const CsvReadConfig& output_config);
  void batch_read(
      const std::vector<std::shared_ptr<IDataChunkSupplier>>& suppliers,
      execution::Context& output);

  std::shared_ptr<ReadSharedState> sharedState_;
  std::unique_ptr<CsvOptionsBuilder> optionsBuilder_;
};

}  // namespace reader
}  // namespace neug
