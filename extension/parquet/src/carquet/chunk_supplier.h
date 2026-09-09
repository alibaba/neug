/** Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */
#pragma once

#include <carquet/carquet.h>

#include <cstdint>
#include <memory>

#include "neug/storages/loader/loader_utils.h"
#include "neug/utils/io/read/common/schema.h"
#include "neug/utils/io/stream/input_stream.h"
#include "neug/utils/result.h"

namespace neug::parquet {

struct CarquetReaderOptions {
  int32_t batchSize = 65536;
  int32_t numThreads = 1;
};

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

 private:
  CarquetChunkSupplier(carquet_reader_t* reader,
                       carquet_batch_reader_t* batchReader, ArrowSchema schema,
                       std::shared_ptr<reader::EntrySchema> entrySchema,
                       int64_t rowNum);

  carquet_reader_t* reader_;
  carquet_batch_reader_t* batchReader_;
  ArrowSchema schema_;
  std::shared_ptr<reader::EntrySchema> entrySchema_;
  int64_t rowNum_;
  int64_t rowsRead_ = 0;
  bool finished_ = false;
};

}  // namespace neug::parquet
