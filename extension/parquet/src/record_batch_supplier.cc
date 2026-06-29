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

#include "parquet/record_batch_supplier.h"

#include <arrow/array.h>
#include <arrow/record_batch.h>
#include <arrow/type.h>

#include "neug/execution/common/data_chunk.h"
#include "neug/utils/exception/exception.h"
#include "parquet/arrow_context_column.h"

namespace neug {

std::shared_ptr<execution::DataChunk> RecordBatchChunkSupplier::GetNextChunk() {
  if (!reader_) {
    THROW_IO_EXCEPTION("Reader is null");
  }
  auto result = reader_->Next();
  if (result.ok()) {
    return execution::recordbatch_to_value_datachunk(result.ValueOrDie());
  }
  LOG(ERROR) << "Failed to get next batch: " << result.status().message();
  THROW_IO_EXCEPTION("Failed to get next batch: " + result.status().message());
}

}  // namespace neug
