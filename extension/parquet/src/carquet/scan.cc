/** Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */

#include "carquet/scan.h"

#include <algorithm>
#include <cstdint>
#include <functional>
#include <future>
#include <limits>
#include <memory>
#include <string>
#include <thread>
#include <unordered_set>
#include <utility>
#include <vector>

#include "carquet/chunk_supplier.h"
#include "carquet/sniffer.h"
#include "neug/utils/exception/exception.h"
#include "neug/utils/io/read/common/options.h"
#include "neug/utils/io/read/common/row_expression_filter.h"

namespace neug::parquet {
namespace {

struct ScanOptions {
  bool batchRead;
  bool parallel;
  bool bufferedStream;
  bool preBuffer;
  bool enableIoCoalescing;
  int32_t rowBatchSize;
  int64_t bufferSize;
};

struct PhysicalProjection {
  std::vector<std::string> columnNames;
  std::vector<int32_t> topLevelFields;
};

struct RowGroupTask {
  std::string path;
  int32_t rowGroup;
};

using ChunkConsumer = std::function<void(std::shared_ptr<DataChunk>)>;

ScanOptions parseOptions(const reader::options_t& values) {
  reader::ReadOptions common;
  reader::Option<bool> buffered =
      reader::Option<bool>::BoolOption("BUFFERED_STREAM", true);
  reader::Option<bool> prebuffer =
      reader::Option<bool>::BoolOption("PRE_BUFFER", false);
  reader::Option<bool> coalescing =
      reader::Option<bool>::BoolOption("ENABLE_IO_COALESCING", true);
  reader::Option<int64_t> rowBatch =
      reader::Option<int64_t>::Int64Option("PARQUET_BATCH_ROWS", 65536);

  const int64_t rowBatchSize = rowBatch.get(values);
  const int64_t bufferSize = common.batch_size.get(values);
  if (rowBatchSize <= 0 || rowBatchSize > std::numeric_limits<int32_t>::max()) {
    THROW_INVALID_ARGUMENT_EXCEPTION(
        "PARQUET_BATCH_ROWS must be between 1 and INT32_MAX");
  }
  if (bufferSize <= 0) {
    THROW_INVALID_ARGUMENT_EXCEPTION("BATCH_SIZE must be positive");
  }
  return {
      .batchRead = common.batch_read.get(values),
      .parallel = common.use_threads.get(values),
      .bufferedStream = buffered.get(values),
      .preBuffer = prebuffer.get(values),
      .enableIoCoalescing = coalescing.get(values),
      .rowBatchSize = static_cast<int32_t>(rowBatchSize),
      .bufferSize = bufferSize,
  };
}

std::unique_ptr<io::InputStream> openInput(const reader::ReadSharedState& state,
                                           const std::string& path) {
  auto input = state.stream_opener ? state.stream_opener(path)
                                   : io::openLocalInputStream(path);
  if (!input) {
    THROW_IO_EXCEPTION("Failed to open Parquet input: " + path);
  }
  return input;
}

bool sameType(const ::common::DataType& expected,
              const ::common::DataType& actual) {
  // INTERVAL is stored as text and converted by the downstream loader.
  if (expected.has_temporal() && expected.temporal().has_interval() &&
      actual.has_string()) {
    return true;
  }
  if (expected.item_case() != actual.item_case()) {
    return false;
  }
  switch (expected.item_case()) {
  case ::common::DataType::kPrimitiveType:
    return expected.primitive_type() == actual.primitive_type();
  case ::common::DataType::kString:
    return true;
  case ::common::DataType::kTemporal: {
    const auto expectedCase = expected.temporal().item_case();
    const auto actualCase = actual.temporal().item_case();
    const bool expectedDate = expectedCase == ::common::Temporal::kDate ||
                              expectedCase == ::common::Temporal::kDate32;
    const bool actualDate = actualCase == ::common::Temporal::kDate ||
                            actualCase == ::common::Temporal::kDate32;
    const bool expectedTimestamp =
        expectedCase == ::common::Temporal::kDateTime ||
        expectedCase == ::common::Temporal::kTimestamp;
    const bool actualTimestamp = actualCase == ::common::Temporal::kDateTime ||
                                 actualCase == ::common::Temporal::kTimestamp;
    return (expectedDate && actualDate) ||
           (expectedTimestamp && actualTimestamp) || expectedCase == actualCase;
  }
  case ::common::DataType::kArray:
    return expected.array().fixed_length() == actual.array().fixed_length() &&
           sameType(expected.array().component_type(),
                    actual.array().component_type());
  case ::common::DataType::kList:
    return sameType(expected.list().component_type(),
                    actual.list().component_type());
  default:
    return expected.SerializeAsString() == actual.SerializeAsString();
  }
}

bool sameSchema(const reader::EntrySchema& expected,
                const reader::EntrySchema& actual) {
  if (expected.columnNames != actual.columnNames ||
      expected.columnTypes.size() != actual.columnTypes.size()) {
    return false;
  }
  for (size_t i = 0; i < expected.columnTypes.size(); ++i) {
    if (!expected.columnTypes[i] || !actual.columnTypes[i] ||
        !sameType(*expected.columnTypes[i], *actual.columnTypes[i])) {
      return false;
    }
  }
  return true;
}

PhysicalProjection buildPhysicalProjection(
    const reader::EntrySchema& schema,
    const std::vector<std::string>& projection,
    const std::shared_ptr<::common::Expression>& filter) {
  std::unordered_set<std::string> required;
  if (projection.empty()) {
    required.insert(schema.columnNames.begin(), schema.columnNames.end());
  } else {
    required.insert(projection.begin(), projection.end());
  }
  std::vector<const ::common::Expression*> pending{filter.get()};
  while (!pending.empty()) {
    const auto* expression = pending.back();
    pending.pop_back();
    if (!expression) {
      continue;
    }
    for (const auto& operation : expression->operators()) {
      switch (operation.item_case()) {
      case ::common::ExprOpr::kVar:
        if (!operation.var().tag().has_name() ||
            operation.var().has_property()) {
          THROW_INVALID_ARGUMENT_EXCEPTION(
              "File filter requires a column name without a graph property");
        }
        required.insert(operation.var().tag().name());
        break;
      case ::common::ExprOpr::kCase:
        for (const auto& branch : operation.case_().when_then_expressions()) {
          pending.push_back(&branch.when_expression());
          pending.push_back(&branch.then_result_expression());
        }
        pending.push_back(&operation.case_().else_result_expression());
        break;
      case ::common::ExprOpr::kScalarFunc:
        for (const auto& child : operation.scalar_func().parameters()) {
          pending.push_back(&child);
        }
        break;
      case ::common::ExprOpr::kUdfFunc:
        for (const auto& child : operation.udf_func().parameters()) {
          pending.push_back(&child);
        }
        break;
      case ::common::ExprOpr::kToTuple:
        for (const auto& child : operation.to_tuple().fields()) {
          pending.push_back(&child);
        }
        break;
      case ::common::ExprOpr::kToList:
        for (const auto& child : operation.to_list().fields()) {
          pending.push_back(&child);
        }
        break;
      case ::common::ExprOpr::kToArray:
        for (const auto& child : operation.to_array().fields()) {
          pending.push_back(&child);
        }
        break;
      default:
        break;
      }
    }
  }

  PhysicalProjection physical;
  for (size_t i = 0; i < schema.columnNames.size(); ++i) {
    if (required.erase(schema.columnNames[i]) > 0) {
      physical.columnNames.emplace_back(schema.columnNames[i]);
      physical.topLevelFields.emplace_back(static_cast<int32_t>(i));
    }
  }
  if (!required.empty()) {
    std::vector<std::string> missing(required.begin(), required.end());
    std::sort(missing.begin(), missing.end());
    std::string message = "Parquet columns not found:";
    for (size_t i = 0; i < missing.size(); ++i) {
      message += (i == 0 ? " " : ", ") + missing[i];
    }
    THROW_INVALID_ARGUMENT_EXCEPTION(message);
  }
  if (physical.topLevelFields.size() == schema.columnNames.size()) {
    physical.topLevelFields.clear();
  }
  return physical;
}

CarquetReaderOptions supplierOptions(
    const ScanOptions& options, const PhysicalProjection& projection,
    std::shared_ptr<::common::Expression> rowGroupFilter = nullptr) {
  return {
      .batchSize = options.rowBatchSize,
      .numThreads = options.parallel ? 0 : 1,
      .bufferSize = options.bufferSize,
      .bufferedStream = options.bufferedStream,
      .preBuffer = options.preBuffer || !options.enableIoCoalescing,
      .topLevelFields = projection.topLevelFields,
      .rowGroupFilter = std::move(rowGroupFilter),
  };
}

std::shared_ptr<CarquetChunkSupplier> openSupplier(
    const reader::ReadSharedState& state, const std::string& path,
    CarquetReaderOptions options) {
  auto supplier =
      CarquetChunkSupplier::create(openInput(state, path), std::move(options));
  if (!supplier) {
    THROW_IO_EXCEPTION("Failed to read Parquet input " + path + ": " +
                       supplier.error().ToString());
  }
  return std::move(*supplier);
}

std::shared_ptr<DataChunk> transformChunk(
    const std::shared_ptr<DataChunk>& chunk,
    const reader::ReadSharedState& state, const PhysicalProjection& physical) {
  auto transformed = reader::filter_chunk(
      *chunk, state.skipRows, physical.columnNames, state.parameters);
  transformed = reader::project_chunk(transformed, physical.columnNames,
                                      state.projectColumns);
  return std::make_shared<DataChunk>(std::move(transformed));
}

std::vector<std::shared_ptr<DataChunk>> readSupplier(
    const std::shared_ptr<CarquetChunkSupplier>& supplier,
    const reader::ReadSharedState& state, const PhysicalProjection& physical) {
  std::vector<std::shared_ptr<DataChunk>> chunks;
  while (auto chunk = supplier->GetNextChunk()) {
    chunks.emplace_back(transformChunk(chunk, state, physical));
  }
  return chunks;
}

void readSequential(const reader::ReadSharedState& state,
                    const ScanOptions& options,
                    const PhysicalProjection& physical,
                    const ChunkConsumer& consume) {
  for (const auto& path : state.schema.file.paths) {
    auto supplier = openSupplier(
        state, path, supplierOptions(options, physical, state.skipRows));
    if (!sameSchema(*state.schema.entry, *supplier->schema())) {
      THROW_SCHEMA_MISMATCH("Parquet schema does not match declared schema: " +
                            path);
    }
    while (auto chunk = supplier->GetNextChunk()) {
      consume(transformChunk(chunk, state, physical));
    }
  }
}

std::vector<RowGroupTask> inspectParallelTasks(
    const reader::ReadSharedState& state, const ScanOptions& options,
    const PhysicalProjection& physical) {
  std::vector<RowGroupTask> tasks;
  for (const auto& path : state.schema.file.paths) {
    auto probe = openSupplier(
        state, path, supplierOptions(options, physical, state.skipRows));
    if (!sameSchema(*state.schema.entry, *probe->schema())) {
      THROW_SCHEMA_MISMATCH("Parquet schema does not match declared schema: " +
                            path);
    }
    if (probe->rowGroupCount() >
        static_cast<size_t>(std::numeric_limits<int32_t>::max())) {
      THROW_IO_EXCEPTION("Parquet row-group count exceeds INT32_MAX");
    }
    for (const int32_t group : probe->selectedRowGroups()) {
      tasks.push_back({path, group});
    }
  }
  return tasks;
}

void readParallel(const reader::ReadSharedState& state,
                  const ScanOptions& options,
                  const PhysicalProjection& physical,
                  const ChunkConsumer& consume) {
  const auto tasks = inspectParallelTasks(state, options, physical);
  if (tasks.empty()) {
    return;
  }
  const size_t concurrency = std::max<size_t>(
      1, std::min<size_t>(tasks.size(), std::thread::hardware_concurrency()));
  for (size_t begin = 0; begin < tasks.size(); begin += concurrency) {
    const size_t end = std::min(tasks.size(), begin + concurrency);
    std::vector<std::future<std::vector<std::shared_ptr<DataChunk>>>> futures;
    futures.reserve(end - begin);
    for (size_t i = begin; i < end; ++i) {
      futures.emplace_back(std::async(std::launch::async, [&, task = tasks[i]] {
        auto taskOptions = supplierOptions(options, physical);
        taskOptions.numThreads = 1;
        taskOptions.rowGroups = {task.rowGroup};
        auto supplier = openSupplier(state, task.path, std::move(taskOptions));
        return readSupplier(supplier, state, physical);
      }));
    }
    for (auto& future : futures) {
      auto taskChunks = future.get();
      for (auto& chunk : taskChunks) {
        consume(std::move(chunk));
      }
    }
  }
}

}  // namespace

result<std::shared_ptr<reader::EntrySchema>> sniffCarquet(
    io::InputStreamFactory inputFactory) {
  return CarquetSniffer(std::move(inputFactory)).sniff();
}

void scanCarquet(const std::shared_ptr<reader::ReadSharedState>& state,
                 execution::Context& output) {
  if (!state || !state->schema.entry) {
    THROW_INVALID_ARGUMENT_EXCEPTION(
        "Carquet scan state and entry schema must be set");
  }
  if (state->schema.file.paths.empty()) {
    THROW_INVALID_ARGUMENT_EXCEPTION("No Parquet input paths provided");
  }

  const auto options = parseOptions(state->schema.file.options);
  const auto physical = buildPhysicalProjection(
      *state->schema.entry, state->projectColumns, state->skipRows);
  output.clear();
  std::vector<std::shared_ptr<DataChunk>> chunks;
  const ChunkConsumer consume = [&](std::shared_ptr<DataChunk> chunk) {
    if (options.batchRead) {
      output.append_chunk(std::move(*chunk));
    } else {
      chunks.emplace_back(std::move(chunk));
    }
  };
  if (options.parallel) {
    readParallel(*state, options, physical, consume);
  } else {
    readSequential(*state, options, physical, consume);
  }
  if (!options.batchRead && !chunks.empty()) {
    output.append_chunk(reader::merge_chunks(std::move(chunks)));
  }
}

}  // namespace neug::parquet
