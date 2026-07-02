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

#include "neug/execution/io/chunk_supplier.h"
#include "neug/utils/io/read/common/read_state.h"
#include "neug/utils/io/read/common/schema.h"
#include "neug/utils/result.h"

namespace neug {
namespace reader {

/// Reads external files and yields row batches via IDataChunkSupplier.
class FileReader {
 public:
  virtual ~FileReader() = default;

  /// Returns an iterator-like supplier that yields DataChunk batches.
  /// A nullptr DataChunk from the supplier marks end of stream (EOF).
  /// Calling read() again after EOF has been signalled is undefined behavior.
  virtual std::shared_ptr<IDataChunkSupplier> read() = 0;

  virtual result<std::shared_ptr<EntrySchema>> inferSchema() = 0;
};

}  // namespace reader
}  // namespace neug
