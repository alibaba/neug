/** Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */

#include "carquet/chunk_supplier.h"

#include <algorithm>
#include <limits>
#include <memory>
#include <numeric>
#include <string>
#include <string_view>
#include <unordered_set>
#include <utility>

#include "carquet/column_converter.h"
#include "carquet/input_adapter.h"
#include "carquet/nested_converter.h"
#include "carquet/row_group_pruner.h"
#include "carquet/schema_converter.h"
#include "neug/utils/exception/exception.h"

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

struct ArrowArrayOwner : ArrowArray {
  ArrowArrayOwner() : ArrowArray{} {}
  ~ArrowArrayOwner() {
    if (release) {
      release(this);
    }
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

struct ArrowPairOwner {
  ~ArrowPairOwner() {
    if (array.release) {
      array.release(&array);
    }
    if (schema.release) {
      schema.release(&schema);
    }
  }

  ArrowSchema schema{};
  ArrowArray array{};
};

void closeRejectedInput(std::unique_ptr<io::InputStream>& input) noexcept {
  if (!input) {
    return;
  }
  try {
    input->Close();
  } catch (...) {}
}

bool containsNestedType(const ::common::DataType& type) {
  return type.item_case() == ::common::DataType::kList ||
         type.item_case() == ::common::DataType::kArray;
}

result<std::vector<int64_t>> readRowGroupRows(carquet_reader_t* reader,
                                              int64_t totalRows) {
  const int32_t count = carquet_reader_num_row_groups(reader);
  if (count < 0) {
    RETURN_STATUS_ERROR(StatusCode::ERR_IO_ERROR,
                        "Carquet returned a negative row-group count");
  }

  std::vector<int64_t> rows;
  rows.reserve(static_cast<size_t>(count));
  int64_t sum = 0;
  for (int32_t group = 0; group < count; ++group) {
    carquet_row_group_metadata_t metadata{};
    const carquet_status_t status =
        carquet_reader_row_group_metadata(reader, group, &metadata);
    if (status != CARQUET_OK || metadata.num_rows < 0 ||
        sum > std::numeric_limits<int64_t>::max() - metadata.num_rows) {
      RETURN_STATUS_ERROR(StatusCode::ERR_IO_ERROR,
                          "Carquet returned invalid row-group metadata");
    }
    sum += metadata.num_rows;
    rows.emplace_back(metadata.num_rows);
  }
  if (sum != totalRows) {
    RETURN_STATUS_ERROR(StatusCode::ERR_IO_ERROR,
                        "Carquet file and row-group counts do not match");
  }
  return rows;
}

result<void> validateTopLevelFields(
    const std::vector<int32_t>& fields,
    const std::shared_ptr<reader::EntrySchema>& schema) {
  if (fields.size() > schema->columnNames.size()) {
    RETURN_STATUS_ERROR(StatusCode::ERR_INVALID_ARGUMENT,
                        "Too many projected Carquet fields");
  }
  std::vector<bool> selected(schema->columnNames.size(), false);
  for (const int32_t field : fields) {
    if (field < 0 || static_cast<size_t>(field) >= selected.size() ||
        selected[static_cast<size_t>(field)]) {
      RETURN_STATUS_ERROR(StatusCode::ERR_INVALID_ARGUMENT,
                          "Invalid projected Carquet field index");
    }
    selected[static_cast<size_t>(field)] = true;
  }
  return {};
}

result<std::vector<int32_t>> selectRowGroups(
    const std::vector<int32_t>& requested,
    const std::vector<int64_t>& allRows) {
  if (requested.empty()) {
    std::vector<int32_t> result(allRows.size());
    std::iota(result.begin(), result.end(), 0);
    return result;
  }
  if (requested.size() > allRows.size()) {
    RETURN_STATUS_ERROR(StatusCode::ERR_INVALID_ARGUMENT,
                        "Too many selected Carquet row groups");
  }
  std::vector<bool> selected(allRows.size(), false);
  for (const int32_t group : requested) {
    if (group < 0 || static_cast<size_t>(group) >= selected.size() ||
        selected[static_cast<size_t>(group)]) {
      RETURN_STATUS_ERROR(StatusCode::ERR_INVALID_ARGUMENT,
                          "Invalid selected Carquet row-group index");
    }
    selected[static_cast<size_t>(group)] = true;
  }
  return requested;
}

result<std::vector<int32_t>> projectedLeafFields(
    const carquet_schema_t& schema,
    const std::shared_ptr<reader::EntrySchema>& entrySchema,
    const std::vector<int32_t>& topLevelFields) {
  if (topLevelFields.empty()) {
    return std::vector<int32_t>{};
  }
  std::unordered_set<std::string_view> selectedNames;
  selectedNames.reserve(topLevelFields.size());
  for (const int32_t field : topLevelFields) {
    selectedNames.emplace(entrySchema->columnNames[static_cast<size_t>(field)]);
  }

  const int32_t leafCount = carquet_schema_num_columns(&schema);
  if (leafCount < 0) {
    RETURN_STATUS_ERROR(StatusCode::ERR_IO_ERROR,
                        "Carquet returned a negative leaf-column count");
  }
  std::vector<int32_t> leaves;
  const char* path[64];
  for (int32_t leaf = 0; leaf < leafCount; ++leaf) {
    const int32_t depth = carquet_schema_column_path(&schema, leaf, path, 64);
    if (depth <= 0 || !path[0]) {
      RETURN_STATUS_ERROR(StatusCode::ERR_IO_ERROR,
                          "Carquet returned an invalid leaf-column path");
    }
    if (selectedNames.contains(path[0])) {
      leaves.emplace_back(leaf);
    }
  }
  if (leaves.empty() && !topLevelFields.empty()) {
    RETURN_STATUS_ERROR(StatusCode::ERR_IO_ERROR,
                        "Projected Parquet fields contain no leaf columns");
  }
  return leaves;
}

}  // namespace

CarquetChunkSupplier::CarquetChunkSupplier(
    carquet_reader_t* reader, carquet_batch_reader_t* batchReader,
    ArrowSchema flatSchema, std::shared_ptr<reader::EntrySchema> entrySchema,
    std::vector<int32_t> rowGroups, std::vector<int64_t> rowGroupRows,
    std::vector<int32_t> topLevelFields, std::vector<int32_t> prebufferLeaves,
    int32_t batchSize, bool preBuffer, size_t allRowGroupCount,
    size_t rowGroupsSkipped, int64_t rowNum)
    : reader_(reader),
      batchReader_(batchReader),
      flatSchema_(flatSchema),
      entrySchema_(std::move(entrySchema)),
      rowGroups_(std::move(rowGroups)),
      rowGroupRows_(std::move(rowGroupRows)),
      topLevelFields_(std::move(topLevelFields)),
      prebufferLeaves_(std::move(prebufferLeaves)),
      batchSize_(batchSize),
      preBuffer_(preBuffer),
      allRowGroupCount_(allRowGroupCount),
      rowGroupsSkipped_(rowGroupsSkipped),
      rowNum_(rowNum) {}

CarquetChunkSupplier::~CarquetChunkSupplier() {
  carquet_batch_reader_free(batchReader_);
  carquet_reader_close(reader_);
  if (flatSchema_.release) {
    flatSchema_.release(&flatSchema_);
  }
}

result<std::shared_ptr<CarquetChunkSupplier>> CarquetChunkSupplier::create(
    std::unique_ptr<io::InputStream> input, CarquetReaderOptions options) {
  if (!input) {
    RETURN_STATUS_ERROR(StatusCode::ERR_INVALID_ARGUMENT,
                        "Parquet input stream is null");
  }
  if (options.batchSize <= 0 || options.numThreads < 0 ||
      options.bufferSize <= 0 ||
      static_cast<uint64_t>(options.bufferSize) >
          std::numeric_limits<size_t>::max()) {
    closeRejectedInput(input);
    RETURN_STATUS_ERROR(StatusCode::ERR_INVALID_ARGUMENT,
                        "Carquet batch and buffer sizes must be positive and "
                        "thread count must be non-negative");
  }

  carquet_reader_options_t readerOptions;
  carquet_reader_options_init(&readerOptions);
  readerOptions.buffer_size =
      options.bufferedStream ? static_cast<size_t>(options.bufferSize) : 0;
  readerOptions.num_threads = options.numThreads;
  carquet_error_t error = CARQUET_ERROR_INIT;
  std::unique_ptr<carquet_reader_t, decltype(&carquet_reader_close)> reader(
      openCarquetReader(std::move(input), &readerOptions, &error),
      carquet_reader_close);
  if (!reader) {
    RETURN_STATUS_ERROR(
        StatusCode::ERR_IO_ERROR,
        carquetMessage("Failed to open Parquet input", error.code, error));
  }

  auto entrySchema = convertCarquetSchema(*carquet_reader_schema(reader.get()));
  if (!entrySchema) {
    return tl::unexpected(entrySchema.error());
  }
  auto validFields =
      validateTopLevelFields(options.topLevelFields, *entrySchema);
  if (!validFields) {
    return tl::unexpected(validFields.error());
  }

  const int64_t fileRows = carquet_reader_num_rows(reader.get());
  if (fileRows < 0) {
    RETURN_STATUS_ERROR(StatusCode::ERR_IO_ERROR,
                        "Carquet returned a negative row count");
  }
  auto allRowGroupRows = readRowGroupRows(reader.get(), fileRows);
  if (!allRowGroupRows) {
    return tl::unexpected(allRowGroupRows.error());
  }
  auto rowGroups = selectRowGroups(options.rowGroups, *allRowGroupRows);
  if (!rowGroups) {
    return tl::unexpected(rowGroups.error());
  }
  const size_t candidateRowGroups = rowGroups->size();
  if (options.rowGroupFilter) {
    auto pruner = CarquetRowGroupPruner::create(reader.get(), **entrySchema,
                                                options.rowGroupFilter.get());
    rowGroups->erase(std::remove_if(rowGroups->begin(), rowGroups->end(),
                                    [&](int32_t group) {
                                      return !pruner->mightMatch(group);
                                    }),
                     rowGroups->end());
  }
  const size_t rowGroupsSkipped = candidateRowGroups - rowGroups->size();
  std::vector<int64_t> rowGroupRows;
  rowGroupRows.reserve(rowGroups->size());
  int64_t rowNum = 0;
  for (const int32_t group : *rowGroups) {
    const int64_t rows = (*allRowGroupRows)[static_cast<size_t>(group)];
    if (rowNum > std::numeric_limits<int64_t>::max() - rows) {
      RETURN_STATUS_ERROR(StatusCode::ERR_IO_ERROR,
                          "Selected Carquet row count overflows");
    }
    rowNum += rows;
    rowGroupRows.emplace_back(rows);
  }

  bool nested = false;
  for (const auto& type : (*entrySchema)->columnTypes) {
    if (!type) {
      RETURN_STATUS_ERROR(StatusCode::ERR_TYPE_CONVERSION,
                          "Carquet returned a null column type");
    }
    nested |= containsNestedType(*type);
  }

  const bool needsRowGroupApi = nested || !options.topLevelFields.empty() ||
                                !options.rowGroups.empty() ||
                                options.preBuffer || options.rowGroupFilter;
  ArrowSchemaOwner flatSchema;
  std::unique_ptr<carquet_batch_reader_t, decltype(&carquet_batch_reader_free)>
      batchReader(nullptr, carquet_batch_reader_free);
  std::vector<int32_t> prebufferLeaves;
  if (needsRowGroupApi && options.preBuffer) {
    auto leaves = projectedLeafFields(*carquet_reader_schema(reader.get()),
                                      *entrySchema, options.topLevelFields);
    if (!leaves) {
      return tl::unexpected(leaves.error());
    }
    prebufferLeaves = std::move(*leaves);
  }
  if (!needsRowGroupApi) {
    const carquet_status_t exportStatus = carquet_arrow_export_schema(
        carquet_reader_schema(reader.get()), &flatSchema, &error);
    if (exportStatus != CARQUET_OK) {
      RETURN_STATUS_ERROR(StatusCode::ERR_TYPE_CONVERSION,
                          carquetMessage("Failed to export Parquet schema",
                                         exportStatus, error));
    }

    carquet_batch_reader_config_t config;
    carquet_batch_reader_config_init(&config);
    config.batch_size = options.batchSize;
    config.num_threads = options.numThreads;
    config.use_mmap = false;
    batchReader.reset(
        carquet_batch_reader_create(reader.get(), &config, &error));
    if (!batchReader) {
      RETURN_STATUS_ERROR(
          StatusCode::ERR_IO_ERROR,
          carquetMessage("Failed to create Carquet batch reader", error.code,
                         error));
    }
  }

  auto supplier =
      std::unique_ptr<CarquetChunkSupplier>(new CarquetChunkSupplier(
          reader.get(), batchReader.get(), flatSchema, std::move(*entrySchema),
          std::move(*rowGroups), std::move(rowGroupRows),
          std::move(options.topLevelFields), std::move(prebufferLeaves),
          options.batchSize, options.preBuffer, allRowGroupRows->size(),
          rowGroupsSkipped, rowNum));
  reader.release();
  batchReader.release();
  flatSchema.release = nullptr;
  return std::shared_ptr<CarquetChunkSupplier>(std::move(supplier));
}

std::shared_ptr<DataChunk> CarquetChunkSupplier::GetNextChunk() {
  if (finished_) {
    return nullptr;
  }
  if (pendingChunk_) {
    return slicePendingChunk();
  }
  auto chunk = batchReader_ ? readFlatChunk() : readNestedChunk();
  if (!chunk || chunk->row_num() <= static_cast<size_t>(batchSize_)) {
    return chunk;
  }
  pendingChunk_ = std::move(chunk);
  pendingOffset_ = 0;
  return slicePendingChunk();
}

std::shared_ptr<DataChunk> CarquetChunkSupplier::readFlatChunk() {
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
  if (status != CARQUET_OK || !rawBatch) {
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

  auto converted = convertArrowCFlatStruct(flatSchema_, exported);
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

std::shared_ptr<DataChunk> CarquetChunkSupplier::readNestedChunk() {
  if (nextRowGroup_ == rowGroupRows_.size()) {
    finished_ = true;
    if (rowsRead_ != rowNum_) {
      THROW_IO_EXCEPTION("Carquet row count mismatch: expected " +
                         std::to_string(rowNum_) + ", read " +
                         std::to_string(rowsRead_));
    }
    return nullptr;
  }

  const int32_t rowGroup = rowGroups_[nextRowGroup_];
  if (preBuffer_) {
    carquet_error_t prebufferError = CARQUET_ERROR_INIT;
    const carquet_status_t prebufferStatus = carquet_reader_prebuffer(
        reader_, rowGroup, prebufferLeaves_.data(),
        static_cast<int32_t>(prebufferLeaves_.size()), &prebufferError);
    if (prebufferStatus != CARQUET_OK) {
      THROW_IO_EXCEPTION(carquetMessage("Failed to prebuffer Carquet row group",
                                        prebufferStatus, prebufferError));
    }
  }

  ArrowPairOwner exported;
  carquet_error_t error = CARQUET_ERROR_INIT;
  const carquet_status_t status = carquet_reader_read_arrow_columns(
      reader_, rowGroup, topLevelFields_.data(),
      static_cast<int32_t>(topLevelFields_.size()), &exported.schema,
      &exported.array, &error);
  if (preBuffer_) {
    carquet_reader_release_prebuffer(reader_);
  }
  if (status != CARQUET_OK) {
    THROW_IO_EXCEPTION(
        carquetMessage("Failed to read Carquet row group", status, error));
  }

  auto converted = convertArrowCStruct(exported.schema, exported.array);
  if (!converted) {
    THROW_IO_EXCEPTION(converted.error().ToString());
  }
  const size_t chunkRows = (*converted)->row_num();
  if (chunkRows > static_cast<size_t>(std::numeric_limits<int64_t>::max())) {
    THROW_IO_EXCEPTION("Carquet row-group count exceeds the supported range");
  }
  const int64_t rows = static_cast<int64_t>(chunkRows);
  if (rows != rowGroupRows_[nextRowGroup_]) {
    THROW_IO_EXCEPTION(
        "Carquet row-group data does not match its metadata: expected " +
        std::to_string(rowGroupRows_[nextRowGroup_]) + ", read " +
        std::to_string(rows));
  }
  if (rowsRead_ > rowNum_ - rows) {
    THROW_IO_EXCEPTION("Carquet returned more rows than the file metadata");
  }
  rowsRead_ += rows;
  ++nextRowGroup_;
  return *converted;
}

std::shared_ptr<DataChunk> CarquetChunkSupplier::slicePendingChunk() {
  const size_t remaining = pendingChunk_->row_num() - pendingOffset_;
  const size_t count = std::min(remaining, static_cast<size_t>(batchSize_));
  sel_vec_t offsets(count);
  std::iota(offsets.begin(), offsets.end(), pendingOffset_);
  // DataChunk copies only shared column pointers. reshuffle materializes the
  // selected rows (LIST columns share their child storage), not the full group.
  auto output = std::make_shared<DataChunk>(*pendingChunk_);
  output->reshuffle(offsets);
  pendingOffset_ += count;
  if (pendingOffset_ == pendingChunk_->row_num()) {
    pendingChunk_.reset();
    pendingOffset_ = 0;
  }
  return output;
}

}  // namespace neug::parquet
