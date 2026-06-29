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

#include <cstddef>
#include <memory>

#include "neug/execution/common/context.h"
#include "neug/utils/io/read/common/chunk_supplier.h"
#include "neug/utils/io/read/common/file_reader.h"
#include "neug/utils/io/read/common/read_state.h"

namespace neug {
namespace reader {

execution::Context toContext(std::shared_ptr<IDataChunkSupplier> supplier,
                             const ReadSharedState& state,
                             size_t fallback_column_count = 0);

inline execution::Context runFileReader(std::unique_ptr<FileReader> reader,
                                        const ReadSharedState& state,
                                        size_t fallback_column_count = 0) {
  return toContext(reader->read(), state, fallback_column_count);
}

}  // namespace reader
}  // namespace neug
