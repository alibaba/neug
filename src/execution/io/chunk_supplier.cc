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

#include "neug/execution/io/chunk_supplier.h"

#include "neug/columnar/data_chunk.h"

namespace neug {

MultiDataChunkSupplier::MultiDataChunkSupplier(
    std::vector<std::shared_ptr<columnar::DataChunk>> chunks)
    : chunks_(std::move(chunks)), index_(0) {}

std::shared_ptr<columnar::DataChunk> MultiDataChunkSupplier::GetNextChunk() {
  if (index_ >= chunks_.size()) {
    return nullptr;
  }
  return chunks_[index_++];
}

int64_t MultiDataChunkSupplier::RowNum() const {
  int64_t total = 0;
  for (const auto& chunk : chunks_) {
    total += static_cast<int64_t>(chunk->row_num());
  }
  return total;
}

ChunkSupplierWrapper::ChunkSupplierWrapper(
    std::vector<std::shared_ptr<IDataChunkSupplier>> suppliers)
    : suppliers_(std::move(suppliers)) {}

std::shared_ptr<columnar::DataChunk> ChunkSupplierWrapper::GetNextChunk() {
  while (current_supplier_index_ < suppliers_.size()) {
    auto chunk = suppliers_[current_supplier_index_]->GetNextChunk();
    if (chunk) {
      return chunk;
    }
    current_supplier_index_++;
  }
  return nullptr;
}

int64_t ChunkSupplierWrapper::RowNum() const {
  int64_t total_rows = 0;
  for (const auto& supplier : suppliers_) {
    total_rows += supplier->RowNum();
  }
  return total_rows;
}

}  // namespace neug
