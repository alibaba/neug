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

#include "neug/utils/io/read/common/reader_utils.h"

#include "neug/utils/exception/exception.h"

namespace neug {
namespace reader {

execution::Context toContext(std::shared_ptr<IDataChunkSupplier> supplier,
                             const ReadSharedState& state,
                             size_t fallback_column_count) {
  int expected_cols = state.columnNum();
  if (expected_cols <= 0 && fallback_column_count > 0) {
    expected_cols = static_cast<int>(fallback_column_count);
  }

  execution::Context ctx;
  while (supplier) {
    auto chunk = supplier->GetNextChunk();
    if (!chunk) {
      break;
    }
    if (expected_cols > 0 &&
        static_cast<int>(chunk->col_num()) != expected_cols) {
      THROW_IO_EXCEPTION(
          "Column number mismatch between schema and file data, schema: " +
          std::to_string(expected_cols) + ", data: " +
          std::to_string(chunk->col_num()));
    }
    ctx.append_chunk(std::move(*chunk));
  }
  return ctx;
}

}  // namespace reader
}  // namespace neug
