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

#include <cstdint>
#include <memory>
#include <vector>

namespace neug {
namespace execution {
class DataChunk;
}

/// Iterator-like source of execution::DataChunk batches for file readers and
/// loaders.
class IDataChunkSupplier {
 public:
  virtual ~IDataChunkSupplier() = default;
  virtual std::shared_ptr<execution::DataChunk> GetNextChunk() = 0;
  virtual int64_t RowNum() const = 0;
};

/// Yields pre-materialized DataChunks one by one.
class MultiDataChunkSupplier : public IDataChunkSupplier {
 public:
  explicit MultiDataChunkSupplier(
      std::vector<std::shared_ptr<execution::DataChunk>> chunks);

  std::shared_ptr<execution::DataChunk> GetNextChunk() override;

  int64_t RowNum() const override;

 private:
  std::vector<std::shared_ptr<execution::DataChunk>> chunks_;
  size_t index_ = 0;
};

/// Wraps multiple IDataChunkSupplier instances into a single sequential stream.
class ChunkSupplierWrapper : public IDataChunkSupplier {
 public:
  explicit ChunkSupplierWrapper(
      std::vector<std::shared_ptr<IDataChunkSupplier>> suppliers);

  std::shared_ptr<execution::DataChunk> GetNextChunk() override;

  int64_t RowNum() const override;

 private:
  std::vector<std::shared_ptr<IDataChunkSupplier>> suppliers_;
  size_t current_supplier_index_ = 0;
};

}  // namespace neug
