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

#include <arrow/record_batch.h>
#include <cstdint>
#include <memory>

#include "neug/storages/loader/loader_utils.h"

namespace neug {

class RecordBatchChunkSupplier : public IDataChunkSupplier {
 public:
  RecordBatchChunkSupplier(
      const std::shared_ptr<arrow::RecordBatchReader>& reader, int64_t row_num)
      : row_num_(row_num), reader_(reader) {}

  std::shared_ptr<execution::DataChunk> GetNextChunk() override;

  int64_t RowNum() const override { return row_num_; }

 private:
  int64_t row_num_;
  std::shared_ptr<arrow::RecordBatchReader> reader_;
};

}  // namespace neug
