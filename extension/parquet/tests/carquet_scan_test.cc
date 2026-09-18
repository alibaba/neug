/** Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */

#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <barrier>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <limits>
#include <map>
#include <memory>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "carquet/chunk_supplier.h"
#include "carquet/output_adapter.h"
#include "carquet/row_group_pruner.h"
#include "carquet/scan.h"
#include "carquet_test_data.h"
#include "neug/generated/proto/plan/expr.pb.h"
#include "neug/utils/exception/exception.h"
#include "neug/utils/io/read/common/row_expression_filter.h"

namespace neug::parquet {
namespace {

struct StreamStats {
  std::atomic<int> opens = 0;
  std::atomic<int> closes = 0;
  std::atomic<int> readCalls = 0;
  std::atomic<int64_t> requestedBytes = 0;
  std::atomic<int> activeReads = 0;
  std::atomic<int> maxActiveReads = 0;
  std::shared_ptr<std::barrier<>> firstReadBarrier;

  void reset() {
    opens = 0;
    closes = 0;
    readCalls = 0;
    requestedBytes = 0;
    activeReads = 0;
    maxActiveReads = 0;
  }
};

class MemoryInput final : public io::InputStream {
 public:
  MemoryInput(std::shared_ptr<std::vector<uint8_t>> bytes,
              std::shared_ptr<StreamStats> stats, bool synchronizeFirstRead)
      : bytes_(std::move(bytes)),
        stats_(std::move(stats)),
        synchronizeFirstRead_(synchronizeFirstRead) {}

  result<int64_t> Read(void* out, int64_t nbytes) override {
    auto read = ReadAt(position_, nbytes, out);
    if (read) {
      position_ += *read;
    }
    return read;
  }

  result<int64_t> ReadAt(int64_t position, int64_t nbytes, void* out) override {
    ++stats_->readCalls;
    stats_->requestedBytes += nbytes;
    if (synchronizeFirstRead_ && !synchronized_) {
      synchronized_ = true;
      const int active = ++stats_->activeReads;
      int maximum = stats_->maxActiveReads.load();
      while (active > maximum &&
             !stats_->maxActiveReads.compare_exchange_weak(maximum, active)) {}
      stats_->firstReadBarrier->arrive_and_wait();
      --stats_->activeReads;
    }
    if (position < 0 || nbytes < 0 || (nbytes > 0 && !out)) {
      RETURN_STATUS_ERROR(StatusCode::ERR_INVALID_ARGUMENT,
                          "invalid memory read");
    }
    if (position >= static_cast<int64_t>(bytes_->size())) {
      return int64_t{0};
    }
    const int64_t count =
        std::min(nbytes, static_cast<int64_t>(bytes_->size()) - position);
    if (count > 0) {
      std::memcpy(out, bytes_->data() + position, static_cast<size_t>(count));
    }
    return count;
  }

  result<int64_t> GetSize() override {
    return static_cast<int64_t>(bytes_->size());
  }

  void Close() override { ++stats_->closes; }

 private:
  std::shared_ptr<std::vector<uint8_t>> bytes_;
  std::shared_ptr<StreamStats> stats_;
  bool synchronizeFirstRead_;
  bool synchronized_ = false;
  int64_t position_ = 0;
};

class MemoryOutput final : public io::OutputStream {
 public:
  explicit MemoryOutput(std::shared_ptr<std::vector<uint8_t>> bytes)
      : bytes_(std::move(bytes)) {}

  Status Write(const uint8_t* data, int64_t nbytes) override {
    bytes_->insert(bytes_->end(), data, data + nbytes);
    return Status::OK();
  }
  Status Close() override { return Status::OK(); }
  void Abort() override { bytes_->clear(); }

 private:
  std::shared_ptr<std::vector<uint8_t>> bytes_;
};

class MemoryFiles {
 public:
  void add(std::string path, bool idOnly = false) {
    files_.emplace(std::move(path), test::scanFile(idOnly));
  }

  void addBytes(std::string path, std::shared_ptr<std::vector<uint8_t>> bytes) {
    files_.emplace(std::move(path), std::move(bytes));
  }

  io::InputStreamOpener opener(bool parallelBarrier = false) {
    return [this, parallelBarrier](const std::string& path) {
      const auto iter = files_.find(path);
      if (iter == files_.end()) {
        return std::unique_ptr<io::InputStream>{};
      }
      const int open = ++stats_->opens;
      const bool synchronize = parallelBarrier && open > 1;
      return std::unique_ptr<io::InputStream>(
          new MemoryInput(iter->second, stats_, synchronize));
    };
  }

  std::shared_ptr<StreamStats> stats() const { return stats_; }

 private:
  std::map<std::string, std::shared_ptr<std::vector<uint8_t>>> files_;
  std::shared_ptr<StreamStats> stats_ = std::make_shared<StreamStats>();
};

std::shared_ptr<reader::ReadSharedState> makeState(
    MemoryFiles& files, std::vector<std::string> paths,
    const std::string& firstPath) {
  auto state = std::make_shared<reader::ReadSharedState>();
  state->schema.file.paths = std::move(paths);
  state->schema.file.format = "parquet";
  state->stream_opener = files.opener();
  auto schema =
      sniffCarquet(io::bindInputStream(state->stream_opener, firstPath));
  EXPECT_TRUE(schema) << schema.error().ToString();
  if (schema) {
    state->schema.entry = std::move(*schema);
  }
  return state;
}

std::shared_ptr<::common::Expression> compareDouble(const std::string& column,
                                                    ::common::Logical op,
                                                    double value) {
  auto expression = std::make_shared<::common::Expression>();
  expression->add_operators()->mutable_var()->mutable_tag()->set_name(column);
  expression->add_operators()->set_logical(op);
  expression->add_operators()->mutable_const_()->set_f64(value);
  return expression;
}

std::shared_ptr<::common::Expression> compareDoubleFromLeft(
    double value, ::common::Logical op, const std::string& column) {
  auto expression = std::make_shared<::common::Expression>();
  expression->add_operators()->mutable_const_()->set_f64(value);
  expression->add_operators()->set_logical(op);
  expression->add_operators()->mutable_var()->mutable_tag()->set_name(column);
  return expression;
}

std::shared_ptr<::common::Expression> compareString(const std::string& column,
                                                    ::common::Logical op,
                                                    std::string value) {
  auto expression = std::make_shared<::common::Expression>();
  expression->add_operators()->mutable_var()->mutable_tag()->set_name(column);
  expression->add_operators()->set_logical(op);
  expression->add_operators()->mutable_const_()->set_str(std::move(value));
  return expression;
}

std::shared_ptr<::common::Expression> isNull(const std::string& column) {
  auto expression = std::make_shared<::common::Expression>();
  expression->add_operators()->set_logical(::common::Logical::ISNULL);
  expression->add_operators()->mutable_var()->mutable_tag()->set_name(column);
  return expression;
}

void appendExpression(::common::Expression& target,
                      const ::common::Expression& source) {
  for (const auto& operation : source.operators()) {
    *target.add_operators() = operation;
  }
}

std::shared_ptr<::common::Expression> combineExpressions(
    const ::common::Expression& left, ::common::Logical op,
    const ::common::Expression& right) {
  auto expression = std::make_shared<::common::Expression>();
  expression->add_operators()->set_brace(::common::ExprOpr::LEFT_BRACE);
  appendExpression(*expression, left);
  expression->add_operators()->set_brace(::common::ExprOpr::RIGHT_BRACE);
  expression->add_operators()->set_logical(op);
  expression->add_operators()->set_brace(::common::ExprOpr::LEFT_BRACE);
  appendExpression(*expression, right);
  expression->add_operators()->set_brace(::common::ExprOpr::RIGHT_BRACE);
  return expression;
}

std::shared_ptr<::common::Expression> negateExpression(
    const ::common::Expression& operand) {
  auto expression = std::make_shared<::common::Expression>();
  expression->add_operators()->set_logical(::common::Logical::NOT);
  expression->add_operators()->set_brace(::common::ExprOpr::LEFT_BRACE);
  appendExpression(*expression, operand);
  expression->add_operators()->set_brace(::common::ExprOpr::RIGHT_BRACE);
  return expression;
}

std::shared_ptr<std::vector<uint8_t>> makeDoubleFile(
    const std::vector<std::vector<double>>& rowGroups, bool writeStatistics) {
  carquet_error_t error = CARQUET_ERROR_INIT;
  std::unique_ptr<carquet_schema_t, decltype(&carquet_schema_free)> schema(
      carquet_schema_create(&error), carquet_schema_free);
  EXPECT_NE(schema, nullptr) << error.message;
  EXPECT_EQ(
      carquet_schema_add_column(schema.get(), "score", CARQUET_PHYSICAL_DOUBLE,
                                nullptr, CARQUET_REPETITION_REQUIRED, 0, 0),
      CARQUET_OK);
  carquet_writer_options_t options;
  carquet_writer_options_init(&options);
  options.write_statistics = writeStatistics;
  auto bytes = std::make_shared<std::vector<uint8_t>>();
  auto* writer = createCarquetWriter(std::make_unique<MemoryOutput>(bytes),
                                     schema.get(), &options, &error);
  EXPECT_NE(writer, nullptr) << error.message;
  for (size_t group = 0; writer && group < rowGroups.size(); ++group) {
    EXPECT_EQ(
        carquet_writer_write_batch(
            writer, 0, rowGroups[group].data(),
            static_cast<int64_t>(rowGroups[group].size()), nullptr, nullptr),
        CARQUET_OK);
    if (group + 1 < rowGroups.size()) {
      EXPECT_EQ(carquet_writer_new_row_group(writer), CARQUET_OK);
    }
  }
  if (writer) {
    EXPECT_EQ(carquet_writer_close(writer), CARQUET_OK);
  }
  return bytes;
}

std::shared_ptr<std::vector<uint8_t>> makeStringFile(
    const std::vector<std::vector<std::string>>& rowGroups) {
  carquet_error_t error = CARQUET_ERROR_INIT;
  std::unique_ptr<carquet_schema_t, decltype(&carquet_schema_free)> schema(
      carquet_schema_create(&error), carquet_schema_free);
  carquet_logical_type_t stringType{};
  stringType.id = CARQUET_LOGICAL_STRING;
  EXPECT_EQ(carquet_schema_add_column(schema.get(), "label",
                                      CARQUET_PHYSICAL_BYTE_ARRAY, &stringType,
                                      CARQUET_REPETITION_REQUIRED, 0, 0),
            CARQUET_OK);
  carquet_writer_options_t options;
  carquet_writer_options_init(&options);
  auto bytes = std::make_shared<std::vector<uint8_t>>();
  auto* writer = createCarquetWriter(std::make_unique<MemoryOutput>(bytes),
                                     schema.get(), &options, &error);
  EXPECT_NE(writer, nullptr) << error.message;
  for (size_t group = 0; writer && group < rowGroups.size(); ++group) {
    std::vector<carquet_byte_array_t> values;
    values.reserve(rowGroups[group].size());
    for (const auto& value : rowGroups[group]) {
      values.push_back(
          {reinterpret_cast<uint8_t*>(const_cast<char*>(value.data())),
           static_cast<int32_t>(value.size())});
    }
    EXPECT_EQ(carquet_writer_write_batch(writer, 0, values.data(),
                                         static_cast<int64_t>(values.size()),
                                         nullptr, nullptr),
              CARQUET_OK);
    if (group + 1 < rowGroups.size()) {
      EXPECT_EQ(carquet_writer_new_row_group(writer), CARQUET_OK);
    }
  }
  if (writer) {
    EXPECT_EQ(carquet_writer_close(writer), CARQUET_OK);
  }
  return bytes;
}

std::vector<int64_t> collectIds(const execution::Context& context) {
  std::vector<int64_t> ids;
  for (const auto& chunk : context.chunks()) {
    for (size_t row = 0; row < chunk.row_num(); ++row) {
      ids.emplace_back(chunk.get(0)->get_elem(row).GetValue<int64_t>());
    }
  }
  return ids;
}

TEST(CarquetScanTest, ReadsProjectedMultiFileBatchesAndMergesFullResults) {
  MemoryFiles files;
  files.add("first");
  files.add("second");
  auto state = makeState(files, {"first", "second"}, "first");
  ASSERT_NE(state->schema.entry, nullptr);
  state->projectColumns = {"id", "label"};
  state->schema.file.options["PARQUET_BATCH_ROWS"] = "1";

  execution::Context batches;
  scanCarquet(state, batches);
  EXPECT_EQ(batches.chunk_num(), 8u);
  EXPECT_EQ(batches.col_num(), 2u);
  EXPECT_EQ(collectIds(batches),
            (std::vector<int64_t>{1, 2, 3, 4, 1, 2, 3, 4}));

  state->schema.file.options["batch_read"] = "false";
  execution::Context full;
  scanCarquet(state, full);
  ASSERT_EQ(full.chunk_num(), 1u);
  EXPECT_EQ(full.col_num(), 2u);
  EXPECT_EQ(collectIds(full), collectIds(batches));
}

TEST(CarquetScanTest, ReadsFilterColumnsBeforeApplyingOutputProjection) {
  MemoryFiles files;
  files.add("types");
  auto state = makeState(files, {"types"}, "types");
  ASSERT_NE(state->schema.entry, nullptr);
  state->projectColumns = {"id", "label"};
  state->skipRows = compareDouble("score", ::common::Logical::GE, 0.0);
  state->schema.file.options["batch_read"] = "false";

  execution::Context output;
  scanCarquet(state, output);
  ASSERT_EQ(output.chunk_num(), 1u);
  EXPECT_EQ(output.col_num(), 2u);
  EXPECT_EQ(collectIds(output), (std::vector<int64_t>{1, 4}));
}

TEST(CarquetScanTest, KeepsIntervalStorageAsStringForDownstreamCast) {
  MemoryFiles files;
  files.add("types");
  auto state = makeState(files, {"types"}, "types");
  ASSERT_NE(state->schema.entry, nullptr);
  ASSERT_EQ(state->schema.entry->columnNames[5], "label");
  state->schema.entry->columnTypes[5]->Clear();
  state->schema.entry->columnTypes[5]->mutable_temporal()->mutable_interval();
  state->projectColumns = {"label"};
  state->schema.file.options["batch_read"] = "false";

  execution::Context output;
  scanCarquet(state, output);
  ASSERT_EQ(output.chunk_num(), 1u);
  ASSERT_EQ(output.col_num(), 1u);
  const auto& column = output.chunk(0).get(0);
  ASSERT_EQ(column->size(), 4u);
  EXPECT_EQ(column->get_elem(0).GetValue<std::string>(), "");
  EXPECT_EQ(column->get_elem(1).GetValue<std::string>(), "hello");
  EXPECT_TRUE(column->get_elem(2).IsNull());
  EXPECT_EQ(column->get_elem(3).GetValue<std::string>(), "中文");
}

TEST(CarquetScanTest, ParallelRowGroupsUseIndependentStreamsInStableOrder) {
  if (std::thread::hardware_concurrency() < 2) {
    GTEST_SKIP() << "parallel overlap requires two hardware threads";
  }
  MemoryFiles files;
  files.add("types");
  auto state = makeState(files, {"types"}, "types");
  ASSERT_NE(state->schema.entry, nullptr);
  files.stats()->reset();
  files.stats()->firstReadBarrier = std::make_shared<std::barrier<>>(2);
  state->stream_opener = files.opener(true);
  state->projectColumns = {"id"};
  state->schema.file.options["parallel"] = "true";
  state->schema.file.options["PARQUET_BATCH_ROWS"] = "1";

  execution::Context output;
  scanCarquet(state, output);
  EXPECT_EQ(collectIds(output), (std::vector<int64_t>{1, 2, 3, 4}));
  EXPECT_EQ(output.chunk_num(), 4u);
  EXPECT_GE(files.stats()->maxActiveReads.load(), 2);
  EXPECT_EQ(files.stats()->opens.load(), 3);
  EXPECT_EQ(files.stats()->closes.load(), 3);
}

TEST(CarquetScanTest, ProjectionReducesPhysicalRangeReads) {
  const auto bytes = test::scanFile();
  auto fullStats = std::make_shared<StreamStats>();
  auto projectedStats = std::make_shared<StreamStats>();

  CarquetReaderOptions options;
  options.bufferedStream = false;
  auto full = CarquetChunkSupplier::create(
      std::make_unique<MemoryInput>(bytes, fullStats, false), options);
  ASSERT_TRUE(full) << full.error().ToString();
  while ((*full)->GetNextChunk()) {}

  options.topLevelFields = {0};
  auto projected = CarquetChunkSupplier::create(
      std::make_unique<MemoryInput>(bytes, projectedStats, false), options);
  ASSERT_TRUE(projected) << projected.error().ToString();
  while (auto chunk = (*projected)->GetNextChunk()) {
    EXPECT_EQ(chunk->col_num(), 1u);
  }
  EXPECT_LT(projectedStats->requestedBytes.load(),
            fullStats->requestedBytes.load());
  EXPECT_LT(projectedStats->readCalls.load(), fullStats->readCalls.load());
}

TEST(CarquetScanTest, PreservesResultsAcrossBufferAndPrebufferModes) {
  MemoryFiles files;
  files.add("types");
  auto state = makeState(files, {"types"}, "types");
  ASSERT_NE(state->schema.entry, nullptr);
  state->projectColumns = {"id"};
  state->schema.file.options["BATCH_SIZE"] = "32";
  state->schema.file.options["BUFFERED_STREAM"] = "true";
  state->schema.file.options["PRE_BUFFER"] = "true";

  execution::Context explicitPrebuffer;
  scanCarquet(state, explicitPrebuffer);
  EXPECT_EQ(collectIds(explicitPrebuffer), (std::vector<int64_t>{1, 2, 3, 4}));

  state->schema.file.options["BUFFERED_STREAM"] = "false";
  state->schema.file.options["PRE_BUFFER"] = "false";
  state->schema.file.options["ENABLE_IO_COALESCING"] = "false";
  execution::Context eagerCoalescing;
  scanCarquet(state, eagerCoalescing);
  EXPECT_EQ(collectIds(eagerCoalescing), collectIds(explicitPrebuffer));
}

TEST(CarquetScanTest, PrunesOnlyProvenNonMatchingGroupsBeforeReadingData) {
  const auto bytes = test::scanFile();
  auto fullStats = std::make_shared<StreamStats>();
  auto prunedStats = std::make_shared<StreamStats>();
  CarquetReaderOptions options;
  options.bufferedStream = false;
  options.preBuffer = true;
  options.topLevelFields = {0, 4};

  auto full = CarquetChunkSupplier::create(
      std::make_unique<MemoryInput>(bytes, fullStats, false), options);
  ASSERT_TRUE(full) << full.error().ToString();
  while ((*full)->GetNextChunk()) {}
  EXPECT_EQ((*full)->rowGroupsRead(), 2u);

  options.rowGroupFilter = compareDouble("score", ::common::Logical::GT, 2.0);
  auto pruned = CarquetChunkSupplier::create(
      std::make_unique<MemoryInput>(bytes, prunedStats, false), options);
  ASSERT_TRUE(pruned) << pruned.error().ToString();
  EXPECT_EQ((*pruned)->selectedRowGroups(), (std::vector<int32_t>{1}));
  while ((*pruned)->GetNextChunk()) {}
  EXPECT_EQ((*pruned)->rowGroupsRead(), 1u);
  EXPECT_EQ((*pruned)->rowGroupsSkipped(), 1u);
  EXPECT_LT(prunedStats->requestedBytes.load(),
            fullStats->requestedBytes.load());
  EXPECT_LT(prunedStats->readCalls.load(), fullStats->readCalls.load());

  MemoryFiles files;
  files.addBytes("types", bytes);
  auto state = makeState(files, {"types"}, "types");
  state->projectColumns = {"id"};
  state->skipRows = options.rowGroupFilter;
  state->schema.file.options["parallel"] = "true";
  state->schema.file.options["PRE_BUFFER"] = "true";
  files.stats()->reset();
  state->stream_opener = files.opener();
  execution::Context output;
  scanCarquet(state, output);
  EXPECT_EQ(collectIds(output), (std::vector<int64_t>{4}));
  EXPECT_EQ(files.stats()->opens.load(), 2);
}

TEST(CarquetScanTest, CombinesReversedAndThreeValuedPredicatesSafely) {
  MemoryFiles files;
  files.add("types");
  auto state = makeState(files, {"types"}, "types");
  state->projectColumns = {"id"};
  execution::Context output;

  state->skipRows = compareDoubleFromLeft(2.0, ::common::Logical::LT, "score");
  scanCarquet(state, output);
  EXPECT_EQ(collectIds(output), (std::vector<int64_t>{4}));

  const auto aboveZero = compareDouble("score", ::common::Logical::GT, 0.0);
  const auto belowTwo = compareDouble("score", ::common::Logical::LT, 2.0);
  state->skipRows =
      combineExpressions(*aboveZero, ::common::Logical::AND, *belowTwo);
  scanCarquet(state, output);
  EXPECT_EQ(collectIds(output), (std::vector<int64_t>{1}));

  const auto belowMinusThree =
      compareDouble("score", ::common::Logical::LT, -3.0);
  state->skipRows =
      combineExpressions(*compareDouble("score", ::common::Logical::GT, 2.0),
                         ::common::Logical::OR, *belowMinusThree);
  scanCarquet(state, output);
  EXPECT_EQ(collectIds(output), (std::vector<int64_t>{4}));

  state->skipRows = isNull("score");
  scanCarquet(state, output);
  EXPECT_EQ(collectIds(output), (std::vector<int64_t>{3}));

  state->skipRows =
      negateExpression(*compareDouble("score", ::common::Logical::LE, 2.0));
  scanCarquet(state, output);
  EXPECT_EQ(collectIds(output), (std::vector<int64_t>{4}));
}

TEST(CarquetScanTest, KeepsGroupsWithMissingOrNanStatistics) {
  auto missingBytes = makeDoubleFile({{-2.0, -1.0}, {1.0, 2.0}}, false);
  auto missingStats = std::make_shared<StreamStats>();
  CarquetReaderOptions options;
  options.bufferedStream = false;
  options.rowGroupFilter = compareDouble("score", ::common::Logical::GT, 0.0);
  auto missing = CarquetChunkSupplier::create(
      std::make_unique<MemoryInput>(missingBytes, missingStats, false),
      options);
  ASSERT_TRUE(missing) << missing.error().ToString();
  while ((*missing)->GetNextChunk()) {}
  EXPECT_EQ((*missing)->rowGroupsRead(), 2u);
  EXPECT_EQ((*missing)->rowGroupsSkipped(), 0u);

  const double nan = std::numeric_limits<double>::quiet_NaN();
  auto onlyNanBytes = makeDoubleFile({{nan, nan}}, true);
  auto onlyNan = CarquetChunkSupplier::create(
      std::make_unique<MemoryInput>(onlyNanBytes,
                                    std::make_shared<StreamStats>(), false),
      options);
  ASSERT_TRUE(onlyNan) << onlyNan.error().ToString();
  while ((*onlyNan)->GetNextChunk()) {}
  EXPECT_EQ((*onlyNan)->rowGroupsRead(), 1u);
  EXPECT_EQ((*onlyNan)->rowGroupsSkipped(), 0u);

  auto finiteAndNanBytes = makeDoubleFile({{0.0, nan, 0.0}}, true);
  for (const auto op :
       {::common::Logical::NE, ::common::Logical::LE, ::common::Logical::GE}) {
    options.rowGroupFilter = compareDouble(
        "score", op,
        op == ::common::Logical::LE ? -1.0
                                    : op == ::common::Logical::GE ? 1.0 : 0.0);
    auto supplier = CarquetChunkSupplier::create(
        std::make_unique<MemoryInput>(finiteAndNanBytes,
                                      std::make_shared<StreamStats>(), false),
        options);
    ASSERT_TRUE(supplier) << supplier.error().ToString();
    while ((*supplier)->GetNextChunk()) {}
    EXPECT_EQ((*supplier)->rowGroupsSkipped(),
              op == ::common::Logical::NE ? 0u : 1u)
        << op;
  }

  auto stringBytes = makeStringFile({{"", ""}, {"b", "c"}});
  options.rowGroupFilter = compareString("label", ::common::Logical::GT, "a");
  auto strings = CarquetChunkSupplier::create(
      std::make_unique<MemoryInput>(stringBytes,
                                    std::make_shared<StreamStats>(), false),
      options);
  ASSERT_TRUE(strings) << strings.error().ToString();
  EXPECT_EQ((*strings)->selectedRowGroups(), (std::vector<int32_t>{0, 1}));
  while ((*strings)->GetNextChunk()) {}
  EXPECT_EQ((*strings)->rowGroupsSkipped(), 0u);
}

TEST(CarquetScanTest, RejectsInvalidOptionsColumnsAndCrossFileSchema) {
  MemoryFiles files;
  files.add("types");
  files.add("flat", true);
  auto state = makeState(files, {"types"}, "types");
  ASSERT_NE(state->schema.entry, nullptr);
  execution::Context output;

  state->schema.file.options["PARQUET_BATCH_ROWS"] = "0";
  EXPECT_THROW(scanCarquet(state, output), exception::InvalidArgumentException);
  state->schema.file.options["PARQUET_BATCH_ROWS"] = "1";
  state->schema.file.options["BATCH_SIZE"] = "0";
  EXPECT_THROW(scanCarquet(state, output), exception::InvalidArgumentException);

  state->schema.file.options["BATCH_SIZE"] = "1024";
  state->projectColumns = {"missing"};
  EXPECT_THROW(scanCarquet(state, output), exception::InvalidArgumentException);

  state->projectColumns = {"z_missing", "id", "a_missing", "z_missing"};
  state->skipRows = isNull("m_missing");
  for (int attempt = 0; attempt < 2; ++attempt) {
    try {
      scanCarquet(state, output);
      FAIL() << "Expected missing projection and filter columns to be rejected";
    } catch (const exception::InvalidArgumentException& error) {
      EXPECT_NE(
          std::string(error.what())
              .find(
                  "Parquet columns not found: a_missing, m_missing, z_missing"),
          std::string::npos);
    }
    std::reverse(state->projectColumns.begin(), state->projectColumns.end());
  }
  state->skipRows.reset();
  state->projectColumns.clear();
  state->schema.file.paths = {"types", "flat"};
  EXPECT_THROW(scanCarquet(state, output), exception::SchemaMismatchException);
}

TEST(CarquetScanTest, RebindsNestedParametersBeforeFinalProjection) {
  MemoryFiles files;
  files.add("first");
  files.add("second");
  auto state = makeState(files, {"first", "second"}, "first");
  // CASE WHEN score > $minimum THEN true ELSE false END
  auto predicate = std::make_shared<::common::Expression>();
  auto* cases = predicate->add_operators()->mutable_case_();
  auto* branch = cases->add_when_then_expressions();
  auto* condition = branch->mutable_when_expression();
  condition->add_operators()->mutable_var()->mutable_tag()->set_name("score");
  condition->add_operators()->set_logical(::common::Logical::GT);
  auto* parameter = condition->add_operators()->mutable_param();
  parameter->set_name("minimum");
  parameter->mutable_data_type()->mutable_data_type()->set_primitive_type(
      ::common::PrimitiveType::DT_DOUBLE);
  branch->mutable_then_result_expression()
      ->add_operators()
      ->mutable_const_()
      ->set_boolean(true);
  cases->mutable_else_result_expression()
      ->add_operators()
      ->mutable_const_()
      ->set_boolean(false);
  state->skipRows = predicate;
  state->projectColumns = {"id", "label", "id"};
  const auto original = predicate->SerializeAsString();
  for (const auto* parallel : {"false", "true"}) {
    for (const auto* batch : {"false", "true"}) {
      state->schema.file.options = {{"parallel", parallel},
                                    {"batch_read", batch},
                                    {"PARQUET_BATCH_ROWS", "1"}};
      for (const double minimum : {0.0, 3.0, 10.0, -3.0}) {
        SCOPED_TRACE(std::string(parallel) + "/" + batch + "/" +
                     std::to_string(minimum));
        state->parameters = {{"minimum", Value::DOUBLE(minimum)}};
        execution::Context output;
        scanCarquet(state, output);
        std::vector<int64_t> expected;
        for (int file = 0; file < 2; ++file) {
          if (1.25 > minimum)
            expected.push_back(1);
          if (-2.5 > minimum)
            expected.push_back(2);
          if (4.5 > minimum)
            expected.push_back(4);
        }
        EXPECT_EQ(collectIds(output), expected);
        EXPECT_EQ(output.col_num(), 3u);
        for (const auto& chunk : output.chunks()) {
          for (size_t row = 0; row < chunk.row_num(); ++row) {
            EXPECT_EQ(chunk.get(0)->get_elem(row), chunk.get(2)->get_elem(row));
          }
        }
        EXPECT_EQ(predicate->SerializeAsString(), original);
      }
    }
  }
  state->parameters.clear();
  execution::Context output;
  EXPECT_THROW(scanCarquet(state, output), exception::InvalidArgumentException);
  EXPECT_EQ(predicate->SerializeAsString(), original);
}

TEST(CarquetScanTest, PruningNeverDropsRowsAcceptedByCompleteEvaluation) {
  const auto bytes = test::scanFile();
  carquet_error_t error = CARQUET_ERROR_INIT;
  std::unique_ptr<carquet_reader_t, decltype(&carquet_reader_close)> reader(
      carquet_reader_open_buffer(bytes->data(), bytes->size(), nullptr, &error),
      carquet_reader_close);
  ASSERT_NE(reader, nullptr) << error.message;
  MemoryFiles files;
  files.addBytes("types", bytes);
  auto state = makeState(files, {"types"}, "types");
  std::vector<std::shared_ptr<::common::Expression>> predicates;
  for (const auto op :
       {::common::Logical::EQ, ::common::Logical::NE, ::common::Logical::LT,
        ::common::Logical::LE, ::common::Logical::GT, ::common::Logical::GE}) {
    for (const double threshold : {-3.0, -2.5, 0.0, 1.25, 4.5, 5.0}) {
      predicates.push_back(compareDouble("score", op, threshold));
      predicates.push_back(compareDoubleFromLeft(threshold, op, "score"));
    }
  }
  predicates.push_back(isNull("score"));
  auto prefix = std::make_shared<::common::Expression>();
  prefix->add_operators()->set_logical(::common::Logical::NOT);
  appendExpression(*prefix, *isNull("score"));
  predicates.push_back(prefix);
  auto doublePrefix = std::make_shared<::common::Expression>();
  doublePrefix->add_operators()->set_logical(::common::Logical::NOT);
  appendExpression(*doublePrefix, *prefix);
  predicates.push_back(doublePrefix);
  const size_t simpleCount = predicates.size();
  for (size_t i = 0; i < simpleCount; ++i) {
    predicates.push_back(negateExpression(*predicates[i]));
    for (const auto op : {::common::Logical::AND, ::common::Logical::OR}) {
      // No additional parentheses around NOT/ISNULL: exercise precedence.
      auto combined = std::make_shared<::common::Expression>(*prefix);
      combined->add_operators()->set_logical(op);
      appendExpression(*combined, *predicates[i]);
      predicates.push_back(std::move(combined));
    }
  }
  for (int32_t group = 0; group < 2; ++group) {
    auto supplier = CarquetChunkSupplier::create(
        std::make_unique<MemoryInput>(bytes, std::make_shared<StreamStats>(),
                                      false),
        {.rowGroups = {group}});
    ASSERT_TRUE(supplier) << supplier.error().ToString();
    const auto chunk = (*supplier)->GetNextChunk();
    ASSERT_NE(chunk, nullptr);
    for (size_t index = 0; index < predicates.size(); ++index) {
      SCOPED_TRACE(index);
      const auto& predicate = predicates[index];
      auto exact = reader::filter_chunk(*chunk, predicate,
                                        state->schema.entry->columnNames);
      auto pruner = CarquetRowGroupPruner::create(
          reader.get(), *state->schema.entry, predicate.get());
      if (exact.row_num() > 0) {
        EXPECT_TRUE(pruner->mightMatch(group)) << "row group " << group;
      }
    }
  }
}

TEST(CarquetScanTest, PrunesNullGroupsWithAdjacentUnaryOperators) {
  carquet_error_t error = CARQUET_ERROR_INIT;
  std::unique_ptr<carquet_schema_t, decltype(&carquet_schema_free)> schema(
      carquet_schema_create(&error), carquet_schema_free);
  ASSERT_NE(schema, nullptr);
  test::check(carquet_schema_add_column(schema.get(), "score",
                                        CARQUET_PHYSICAL_DOUBLE, nullptr,
                                        CARQUET_REPETITION_OPTIONAL, 0, 0));
  auto writer = test::writerFor(schema.get());
  const double value = 2.0;
  const int16_t nulls[] = {0, 0}, mixed[] = {1, 0};
  test::check(
      carquet_writer_write_batch(writer.get(), 0, &value, 2, nulls, nullptr));
  test::check(carquet_writer_new_row_group(writer.get()));
  test::check(
      carquet_writer_write_batch(writer.get(), 0, &value, 2, mixed, nullptr));
  const auto bytes = test::finish(std::move(writer));
  auto predicate = std::make_shared<::common::Expression>();
  predicate->add_operators()->set_logical(::common::Logical::NOT);
  appendExpression(*predicate, *isNull("score"));
  CarquetReaderOptions options{.rowGroupFilter = predicate};
  auto supplier = CarquetChunkSupplier::create(
      std::make_unique<MemoryInput>(bytes, std::make_shared<StreamStats>(),
                                    false),
      options);
  ASSERT_TRUE(supplier) << supplier.error().ToString();
  EXPECT_EQ((*supplier)->selectedRowGroups(), (std::vector<int32_t>{1}));
  MemoryFiles files;
  files.addBytes("nulls", bytes);
  auto state = makeState(files, {"nulls"}, "nulls");
  state->skipRows = predicate;
  execution::Context output;
  scanCarquet(state, output);
  ASSERT_EQ(output.row_num(), 1u);
  EXPECT_DOUBLE_EQ(output.chunk(0).get(0)->get_elem(0).GetValue<double>(), 2.0);
}

TEST(CarquetScanTest, PreservesParametersNestedInsideListAndArrayPredicates) {
  MemoryFiles files;
  files.add("types");
  auto state = makeState(files, {"types"}, "types");
  state->projectColumns = {"label"};
  for (const bool asArray : {false, true}) {
    auto predicate = std::make_shared<::common::Expression>();
    predicate->add_operators()->mutable_var()->mutable_tag()->set_name("id");
    predicate->add_operators()->set_logical(::common::Logical::WITHIN);
    auto* container = predicate->add_operators();
    auto* fields = asArray ? container->mutable_to_array()->mutable_fields()
                           : container->mutable_to_list()->mutable_fields();
    if (asArray) {
      auto* type =
          container->mutable_node_type()->mutable_data_type()->mutable_array();
      type->set_fixed_length(1);
      type->mutable_component_type()->set_primitive_type(
          ::common::PrimitiveType::DT_SIGNED_INT64);
    }
    auto* parameter = fields->Add()->add_operators()->mutable_param();
    parameter->set_name("selected");
    parameter->mutable_data_type()->mutable_data_type()->set_primitive_type(
        ::common::PrimitiveType::DT_SIGNED_INT64);
    state->skipRows = predicate;
    const auto original = predicate->SerializeAsString();
    for (const auto* parallel : {"false", "true"}) {
      for (const auto* batch : {"false", "true"}) {
        state->schema.file.options = {{"parallel", parallel},
                                      {"batch_read", batch},
                                      {"PARQUET_BATCH_ROWS", "1"}};
        for (const int64_t selected : {2, 4}) {
          state->parameters = {{"selected", Value::INT64(selected)}};
          execution::Context output;
          scanCarquet(state, output);
          ASSERT_EQ(output.row_num(), 1u);
          ASSERT_EQ(output.col_num(), 1u);
          for (const auto& chunk : output.chunks()) {
            if (chunk.row_num())
              EXPECT_EQ(chunk.get(0)->get_elem(0).GetValue<std::string>(),
                        selected == 2 ? "hello" : "中文");
          }
          EXPECT_EQ(predicate->SerializeAsString(), original);
        }
      }
    }
  }
}

}  // namespace
}  // namespace neug::parquet
