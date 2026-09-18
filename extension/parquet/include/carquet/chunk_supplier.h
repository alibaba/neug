/** Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */
#pragma once

#include <carquet/carquet.h>

#include <cstdint>
#include <memory>
#include <vector>

#include "neug/generated/proto/plan/expr.pb.h"
#include "neug/storages/loader/loader_utils.h"
#include "neug/utils/io/read/common/schema.h"
#include "neug/utils/io/stream/input_stream.h"
#include "neug/utils/result.h"

namespace neug::parquet {

struct CarquetReaderOptions {
  int32_t batchSize = 65536;
  int32_t numThreads = 1;
  int64_t bufferSize = 1 << 20;
  bool bufferedStream = true;
  bool preBuffer = false;
  std::vector<int32_t> topLevelFields;
  std::vector<int32_t> rowGroups;
  std::shared_ptr<::common::Expression> rowGroupFilter;
};

// The only Carquet-to-NeuG supplier. Flat files use Carquet's bounded batch
// reader; files containing LIST or ARRAY values use its nested row-group API.
class CarquetChunkSupplier final : public IDataChunkSupplier {
 public:
  static result<std::shared_ptr<CarquetChunkSupplier>> create(
      std::unique_ptr<io::InputStream> input,
      CarquetReaderOptions options = {});

  ~CarquetChunkSupplier() override;

  CarquetChunkSupplier(const CarquetChunkSupplier&) = delete;
  CarquetChunkSupplier& operator=(const CarquetChunkSupplier&) = delete;

  std::shared_ptr<DataChunk> GetNextChunk() override;
  int64_t RowNum() const override { return rowNum_; }
  const std::shared_ptr<reader::EntrySchema>& schema() const {
    return entrySchema_;
  }
  size_t rowGroupCount() const { return allRowGroupCount_; }
  const std::vector<int32_t>& selectedRowGroups() const { return rowGroups_; }
  size_t rowGroupsRead() const { return nextRowGroup_; }
  size_t rowGroupsSkipped() const { return rowGroupsSkipped_; }

 private:
  CarquetChunkSupplier(
      carquet_reader_t* reader, carquet_batch_reader_t* batchReader,
      ArrowSchema flatSchema, std::shared_ptr<reader::EntrySchema> entrySchema,
      std::vector<int32_t> rowGroups, std::vector<int64_t> rowGroupRows,
      std::vector<int32_t> topLevelFields, std::vector<int32_t> prebufferLeaves,
      int32_t batchSize, bool preBuffer, size_t allRowGroupCount,
      size_t rowGroupsSkipped, int64_t rowNum);

  std::shared_ptr<DataChunk> readFlatChunk();
  std::shared_ptr<DataChunk> readNestedChunk();
  std::shared_ptr<DataChunk> slicePendingChunk();

  carquet_reader_t* reader_;
  carquet_batch_reader_t* batchReader_;
  ArrowSchema flatSchema_;
  std::shared_ptr<reader::EntrySchema> entrySchema_;
  std::vector<int32_t> rowGroups_;
  std::vector<int64_t> rowGroupRows_;
  std::vector<int32_t> topLevelFields_;
  std::vector<int32_t> prebufferLeaves_;
  std::shared_ptr<DataChunk> pendingChunk_;
  size_t pendingOffset_ = 0;
  int32_t batchSize_;
  bool preBuffer_;
  size_t allRowGroupCount_;
  size_t rowGroupsSkipped_;
  int64_t rowNum_;
  int64_t rowsRead_ = 0;
  size_t nextRowGroup_ = 0;
  bool finished_ = false;
};

}  // namespace neug::parquet
