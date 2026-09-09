/** Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */

#include "chunk_supplier.h"

#include <limits>
#include <memory>
#include <string>
#include <utility>

#include "column_converter.h"
#include "input_adapter.h"
#include "neug/utils/exception/exception.h"
#include "schema_converter.h"

namespace neug::parquet {
namespace {

std::string carquetMessage(const std::string& action, carquet_status_t status,
                           const carquet_error_t& error) {
  std::string message = action + ": " + carquet_status_string(status);
  if (error.message[0] != '\0') {
    message += " (" + std::string(error.message) + ")";
  }
  return message;
}

struct BatchDeleter {
  void operator()(carquet_row_batch_t* batch) const {
    carquet_row_batch_free(batch);
  }
};

struct ArrowSchemaOwner : ArrowSchema {
  ArrowSchemaOwner() : ArrowSchema{} {}
  ~ArrowSchemaOwner() {
    if (release) {
      release(this);
    }
  }
};

struct ArrowArrayOwner : ArrowArray {
  ArrowArrayOwner() : ArrowArray{} {}
  ~ArrowArrayOwner() {
    if (release) {
      release(this);
    }
  }
};

void closeRejectedInput(std::unique_ptr<io::InputStream>& input) noexcept {
  if (!input) {
    return;
  }
  try {
    input->Close();
  } catch (...) {}
}

}  // namespace

CarquetChunkSupplier::CarquetChunkSupplier(
    carquet_reader_t* reader, carquet_batch_reader_t* batchReader,
    ArrowSchema schema, std::shared_ptr<reader::EntrySchema> entrySchema,
    int64_t rowNum)
    : reader_(reader),
      batchReader_(batchReader),
      schema_(schema),
      entrySchema_(std::move(entrySchema)),
      rowNum_(rowNum) {}

CarquetChunkSupplier::~CarquetChunkSupplier() {
  carquet_batch_reader_free(batchReader_);
  carquet_reader_close(reader_);
  if (schema_.release) {
    schema_.release(&schema_);
  }
}

result<std::shared_ptr<CarquetChunkSupplier>> CarquetChunkSupplier::create(
    std::unique_ptr<io::InputStream> input, CarquetReaderOptions options) {
  if (!input) {
    RETURN_STATUS_ERROR(StatusCode::ERR_INVALID_ARGUMENT,
                        "Parquet input stream is null");
  }
  if (options.batchSize <= 0 || options.numThreads < 0) {
    closeRejectedInput(input);
    RETURN_STATUS_ERROR(StatusCode::ERR_INVALID_ARGUMENT,
                        "Carquet batch size must be positive and thread count "
                        "must be non-negative");
  }

  carquet_error_t error = CARQUET_ERROR_INIT;
  std::unique_ptr<carquet_reader_t, decltype(&carquet_reader_close)> reader(
      openCarquetReader(std::move(input), nullptr, &error),
      carquet_reader_close);
  if (!reader) {
    RETURN_STATUS_ERROR(
        StatusCode::ERR_IO_ERROR,
        carquetMessage("Failed to open Parquet input", error.code, error));
  }

  ArrowSchemaOwner schema;
  const carquet_status_t exportStatus = carquet_arrow_export_schema(
      carquet_reader_schema(reader.get()), &schema, &error);
  if (exportStatus != CARQUET_OK) {
    RETURN_STATUS_ERROR(
        StatusCode::ERR_TYPE_CONVERSION,
        carquetMessage("Failed to export Parquet schema", exportStatus, error));
  }
  auto entrySchema = convertArrowCStructSchema(schema);
  if (!entrySchema) {
    return tl::unexpected(entrySchema.error());
  }

  carquet_batch_reader_config_t config;
  carquet_batch_reader_config_init(&config);
  config.batch_size = options.batchSize;
  config.num_threads = options.numThreads;
  config.use_mmap = false;
  std::unique_ptr<carquet_batch_reader_t, decltype(&carquet_batch_reader_free)>
      batchReader(carquet_batch_reader_create(reader.get(), &config, &error),
                  carquet_batch_reader_free);
  if (!batchReader) {
    RETURN_STATUS_ERROR(StatusCode::ERR_IO_ERROR,
                        carquetMessage("Failed to create Carquet batch reader",
                                       error.code, error));
  }

  const int64_t rowNum = carquet_reader_num_rows(reader.get());
  if (rowNum < 0) {
    RETURN_STATUS_ERROR(StatusCode::ERR_IO_ERROR,
                        "Carquet returned a negative row count");
  }
  auto supplier = std::unique_ptr<CarquetChunkSupplier>(
      new CarquetChunkSupplier(reader.get(), batchReader.get(), schema,
                               std::move(*entrySchema), rowNum));
  reader.release();
  batchReader.release();
  schema.release = nullptr;
  return std::shared_ptr<CarquetChunkSupplier>(std::move(supplier));
}

std::shared_ptr<DataChunk> CarquetChunkSupplier::GetNextChunk() {
  if (finished_) {
    return nullptr;
  }

  carquet_row_batch_t* rawBatch = nullptr;
  const carquet_status_t status =
      carquet_batch_reader_next(batchReader_, &rawBatch);
  std::unique_ptr<carquet_row_batch_t, BatchDeleter> batch(rawBatch);
  if (status == CARQUET_ERROR_END_OF_DATA) {
    finished_ = true;
    if (rowsRead_ != rowNum_) {
      THROW_IO_EXCEPTION("Carquet row count mismatch: expected " +
                         std::to_string(rowNum_) + ", read " +
                         std::to_string(rowsRead_));
    }
    return nullptr;
  }
  if (status != CARQUET_OK || !batch) {
    const carquet_error_t emptyError = CARQUET_ERROR_INIT;
    THROW_IO_EXCEPTION(
        carquetMessage("Failed to read Carquet batch", status, emptyError));
  }
  ArrowArrayOwner exported;
  carquet_error_t error = CARQUET_ERROR_INIT;
  const carquet_status_t exportStatus = carquet_arrow_export_batch(
      batch.get(), carquet_reader_schema(reader_), nullptr, &exported, &error);
  if (exportStatus != CARQUET_OK) {
    THROW_IO_EXCEPTION(
        carquetMessage("Failed to export Carquet batch", exportStatus, error));
  }
  batch.reset();

  auto converted = convertArrowCFlatStruct(schema_, exported);
  if (!converted) {
    THROW_IO_EXCEPTION(converted.error().ToString());
  }
  const size_t chunkRows = (*converted)->row_num();
  if (chunkRows > static_cast<size_t>(std::numeric_limits<int64_t>::max())) {
    THROW_IO_EXCEPTION("Carquet batch row count exceeds the supported range");
  }
  const int64_t rows = static_cast<int64_t>(chunkRows);
  if (rowsRead_ > rowNum_ - rows) {
    THROW_IO_EXCEPTION("Carquet returned more rows than the file metadata");
  }
  rowsRead_ += rows;
  return *converted;
}

}  // namespace neug::parquet
