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

#include "test_reader.h"

#include <algorithm>
#include <initializer_list>
#include <sstream>
#include <utility>

#include "neug/storages/loader/chunk_pipeline_utils.h"
#include "neug/storages/loader/loader_utils.h"

namespace neug {
namespace test {

namespace {

ChunkSourceOptions parallel_source_options(
    int32_t producer_count = 4, size_t queue_capacity = 8,
    std::vector<int32_t> projected_columns = {}) {
  return {
      .producer_count = producer_count,
      .worker_budget = std::max<int32_t>(1, producer_count + 1),
      .queue_capacity = queue_capacity,
      .preserve_order = false,
      .projected_columns = std::move(projected_columns),
  };
}

CsvReadConfig csv_config(
    std::initializer_list<std::pair<std::string, DataTypeId>> columns,
    size_t skip_rows = 0) {
  CsvReadConfig config;
  config.delimiter = '|';
  config.skip_rows = skip_rows;
  for (const auto& [name, type] : columns) {
    config.column_names.push_back(name);
    config.column_types.emplace(name, DataType(type));
  }
  config.include_columns = config.column_names;
  return config;
}

template <typename T>
std::vector<T> read_sorted_column(
    const std::shared_ptr<IDataChunkSupplier>& supplier, size_t column = 0) {
  std::vector<T> values;
  while (auto chunk = supplier->GetNextChunk()) {
    for (size_t row = 0; row < chunk->row_num(); ++row) {
      values.push_back(chunk->get(column)->get_elem(row).GetValue<T>());
    }
  }
  std::sort(values.begin(), values.end());
  return values;
}

}  // namespace

TEST(ChunkSourceOptionsTest, BulkBuildPlannerHonorsWorkerBudget) {
  constexpr int64_t kLargeInput = 1024LL * 1024 * 1024;

  const auto serial = ResolveBulkBuildSourceOptions(
      kLargeInput, true, 1, BulkBuildWorkerStrategy::kBalancedProducerConsumer);
  EXPECT_EQ(serial.worker_budget, 1);
  EXPECT_EQ(serial.producer_count, 0);
  EXPECT_EQ(serial.consumer_count, 1);

  const auto balanced = ResolveBulkBuildSourceOptions(
      kLargeInput, true, 4, BulkBuildWorkerStrategy::kBalancedProducerConsumer);
  EXPECT_EQ(balanced.worker_budget, 4);
  EXPECT_GT(balanced.producer_count, 0);
  EXPECT_GE(balanced.consumer_count, 1);
  EXPECT_LE(balanced.producer_count + balanced.consumer_count,
            balanced.worker_budget);

  const auto clamped = ResolveBulkBuildSourceOptions(
      kLargeInput, true, 0, BulkBuildWorkerStrategy::kMaxProducers);
  EXPECT_EQ(clamped.worker_budget, 1);
  EXPECT_EQ(clamped.producer_count, 0);
}

TEST(ChunkSourceOptionsTest, ExecutionBoundaryClampsMalformedWorkerCounts) {
  ChunkSourceOptions malformed{
      .producer_count = 8,
      .consumer_count = 7,
      .worker_budget = 4,
      .queue_capacity = 0,
      .preserve_order = false,
  };
  const auto normalized = NormalizeChunkSourceOptions(malformed);
  EXPECT_EQ(normalized.worker_budget, 4);
  EXPECT_EQ(normalized.producer_count, 3);
  EXPECT_EQ(normalized.consumer_count, 1);
  EXPECT_EQ(normalized.queue_capacity, 1);
  EXPECT_LE(normalized.producer_count + normalized.consumer_count,
            normalized.worker_budget);

  malformed.worker_budget = 0;
  const auto serial = NormalizeChunkSourceOptions(malformed);
  EXPECT_EQ(serial.worker_budget, 1);
  EXPECT_EQ(serial.producer_count, 0);
  EXPECT_EQ(serial.consumer_count, 1);

  malformed.worker_budget = 8;
  malformed.preserve_order = true;
  const auto ordered = NormalizeChunkSourceOptions(malformed);
  EXPECT_EQ(ordered.producer_count, 0);
  EXPECT_EQ(ordered.consumer_count, 1);
}

// Test 1: Basic CSV reading with default options
TEST_F(ReaderTest, TestBasicCsvRead) {
  // Create test CSV file
  createCsvFile("test1.csv",
                "id|name|score\n1|Alice|95.5\n2|Bob|87.0\n3|Charlie|92.5\n");

  // Create schema
  std::vector<std::string> columnNames = {"id", "name", "score"};
  std::vector<std::shared_ptr<::common::DataType>> columnTypes = {
      createInt32Type(), createStringType(), createDoubleType()};

  auto sharedState =
      createSharedState("test1.csv", columnNames, columnTypes,
                        {{"skip_rows", "1"}, {"batch_read", "false"}});
  auto reader = createCsvReader(sharedState);

  auto localState = std::make_shared<reader::ReadLocalState>();
  execution::Context ctx;

  reader->read(localState, ctx);

  // Verify data: should have 3 columns
  EXPECT_EQ(ctx.col_num(), 3);
  // Verify rows: should have 3 rows
  EXPECT_EQ(ctx.row_num(), 3);
}

TEST_F(ReaderTest, CsvChunkSourceReusesPartitionPlanAcrossParallelOpens) {
  createCsvFile("cached-plan.csv", "id|name\n1|Alice\n2|Bob\n3|Carol\n");
  auto config = csv_config(
      {{"id", DataTypeId::kInt32}, {"name", DataTypeId::kVarchar}}, 1);
  const auto csv_path =
      std::filesystem::path(ARROW_READER_TEST_DIR) / "cached-plan.csv";
  const auto moved_path =
      std::filesystem::path(ARROW_READER_TEST_DIR) / "cached-plan.moved";
  CSVChunkSource source({csv_path.string()}, std::move(config));
  const auto options = parallel_source_options(4, 4);

  auto first = source.Open(options);
  ASSERT_NE(first, nullptr);
  EXPECT_EQ(first->RowNum(), 4);

  std::filesystem::rename(csv_path, moved_path);
  auto second = source.Open(options);
  ASSERT_NE(second, nullptr);
  EXPECT_EQ(second->RowNum(), 4);
  std::filesystem::rename(moved_path, csv_path);
}

TEST_F(ReaderTest, CsvChunkSourceSeparatesPartitionPlansByWorkerBudget) {
  createCsvFile("budgeted-plan.csv", "id|name\n1|Alice\n2|Bob\n3|Carol\n");
  auto config = csv_config(
      {{"id", DataTypeId::kInt32}, {"name", DataTypeId::kVarchar}}, 1);
  const auto csv_path =
      std::filesystem::path(ARROW_READER_TEST_DIR) / "budgeted-plan.csv";
  const auto moved_path =
      std::filesystem::path(ARROW_READER_TEST_DIR) / "budgeted-plan.moved";
  CSVChunkSource source({csv_path.string()}, std::move(config));
  const auto first_options = parallel_source_options(4, 4);

  auto first = source.Open(first_options);
  ASSERT_NE(first, nullptr);
  EXPECT_EQ(first->RowNum(), 4);

  std::filesystem::rename(csv_path, moved_path);
  auto second_options = first_options;
  second_options.worker_budget = 8;
  auto second = source.Open(second_options);
  ASSERT_NE(second, nullptr);
  EXPECT_ANY_THROW(second->RowNum());
  std::filesystem::rename(moved_path, csv_path);
}

TEST_F(ReaderTest, PartitionedCsvProvidesStableRowOrdinals) {
  constexpr size_t kRowCount = 257;
  std::ostringstream csv;
  csv << "id|value\n";
  for (size_t row = 0; row < kRowCount; ++row) {
    csv << row << '|' << row * 7 << '\n';
  }
  createCsvFile("stable-ordinals.csv", csv.str());

  auto config = csv_config(
      {{"id", DataTypeId::kInt64}, {"value", DataTypeId::kInt64}}, 1);
  config.chunk_size = 3;
  const auto csv_path =
      std::filesystem::path(ARROW_READER_TEST_DIR) / "stable-ordinals.csv";
  CSVChunkSource source({csv_path.string()}, std::move(config));
  auto supplier = source.Open(parallel_source_options(4, 8));
  ASSERT_NE(supplier, nullptr);
  ASSERT_TRUE(supplier->SupportsConcurrentGetNext());

  std::vector<SequencedDataChunk> chunks;
  while (true) {
    auto chunk = supplier->GetNextChunkWithOrdinal();
    if (!chunk.chunk) {
      break;
    }
    chunks.push_back(std::move(chunk));
  }
  std::sort(chunks.begin(), chunks.end(), [](const auto& lhs, const auto& rhs) {
    return lhs.first_row_ordinal < rhs.first_row_ordinal;
  });

  uint64_t expected_ordinal = 0;
  for (const auto& sequenced : chunks) {
    EXPECT_EQ(sequenced.first_row_ordinal, expected_ordinal);
    expected_ordinal += sequenced.chunk->row_num();
  }
  EXPECT_EQ(expected_ordinal, kRowCount);
}

TEST_F(ReaderTest, PartitionedCsvHandlesQuotedRecordBoundaries) {
  {
    SCOPED_TRACE("quoted records");
    createCsvFile(
        "partitioned.csv",
        "id|name\r\n\r\n1|\"Alice|Smith\"\r\n2|\"line one\nline two\"\r\n"
        "3|Carol\r\n4|\"D\"\"Angelo\"\r\n5|Last");
    std::vector<std::string> column_names = {"id", "name"};
    std::vector<std::shared_ptr<::common::DataType>> column_types = {
        createInt32Type(), createStringType()};
    auto shared_state = createSharedState(
        "partitioned.csv", column_names, column_types,
        {{"skip_rows", "1"}, {"batch_read", "true"}, {"batch_size", "1"}});
    auto reader = createCsvReader(shared_state);
    auto source = reader->createChunkSource();
    ASSERT_NE(source, nullptr);

    auto read_rows = [](const std::shared_ptr<IDataChunkSupplier>& supplier) {
      std::vector<std::pair<int32_t, std::string>> rows;
      while (auto chunk = supplier->GetNextChunk()) {
        EXPECT_EQ(chunk->col_num(), 2);
        for (size_t row = 0; row < chunk->row_num(); ++row) {
          rows.emplace_back(
              chunk->get(0)->get_elem(row).GetValue<int32_t>(),
              chunk->get(1)->get_elem(row).GetValue<std::string>());
        }
      }
      std::sort(rows.begin(), rows.end());
      return rows;
    };

    auto expected = read_rows(source->Open());
    auto partitioned = source->Open(parallel_source_options());
    ASSERT_NE(partitioned, nullptr);
    EXPECT_TRUE(partitioned->SupportsConcurrentGetNext());
    EXPECT_EQ(partitioned->RowNum(), 6);  // Header is an intentional overcount.
    auto actual = read_rows(partitioned);

    ASSERT_EQ(actual, expected);
    ASSERT_EQ(actual.size(), 5);
    EXPECT_EQ(actual[0], std::make_pair(1, std::string("Alice|Smith")));
    EXPECT_EQ(actual[1], std::make_pair(2, std::string("line one\nline two")));
    EXPECT_EQ(actual[3], std::make_pair(4, std::string("D\"Angelo")));
    EXPECT_EQ(actual[4], std::make_pair(5, std::string("Last")));
  }

  {
    SCOPED_TRACE("skip rows across ranges");
    std::string long_quoted_field(64 * 1024, 'x');
    for (size_t i = 32; i < long_quoted_field.size(); i += 64) {
      long_quoted_field[i] = '\n';
    }
    createCsvFile("partition-skip.csv",
                  "0|\"" + long_quoted_field +
                      "\"\r\n1|also-skipped\r\n2|kept\r\n3|last\r\n");

    auto config = csv_config(
        {{"id", DataTypeId::kInt32}, {"name", DataTypeId::kVarchar}}, 2);
    config.quoting = true;
    config.chunk_size = 1;
    CSVChunkSource source(
        {std::string(ARROW_READER_TEST_DIR) + "/partition-skip.csv"}, config);

    auto supplier = source.Open(parallel_source_options(4, 4));
    ASSERT_NE(supplier, nullptr);

    EXPECT_EQ(read_sorted_column<int32_t>(supplier),
              (std::vector<int32_t>{2, 3}));
  }

  {
    SCOPED_TRACE("double quote disabled");
    std::string multiline = "first \"\"quoted\n";
    multiline.append(64 * 1024, 'z');
    multiline += "\ncontinued\"\" tail";
    createCsvFile("partition-double-quote.csv",
                  "0|\"" + multiline + "\"\n1|one\n2|two\n");

    auto config = csv_config(
        {{"id", DataTypeId::kInt32}, {"name", DataTypeId::kVarchar}});
    config.quoting = true;
    config.double_quote = false;
    config.chunk_size = 1;
    CSVChunkSource source(
        {std::string(ARROW_READER_TEST_DIR) + "/partition-double-quote.csv"},
        config);

    const auto expected = read_sorted_column<int32_t>(source.Open());

    EXPECT_EQ(
        read_sorted_column<int32_t>(source.Open(parallel_source_options(4, 4))),
        expected);
    EXPECT_EQ(expected, (std::vector<int32_t>{0, 1, 2}));
  }
}

TEST_F(ReaderTest, CsvChunkSourceHonorsProjectionAndParallelOptions) {
  {
    SCOPED_TRACE("partitioned projection");
    createCsvFile("partition-projection.csv",
                  "id|ignored|score\n1|Alice|10\n2|Bob|20\n");
    auto config = csv_config({{"id", DataTypeId::kInt32},
                              {"ignored", DataTypeId::kVarchar},
                              {"score", DataTypeId::kInt32}},
                             1);
    config.chunk_size = 1;
    CSVChunkSource source(
        {std::string(ARROW_READER_TEST_DIR) + "/partition-projection.csv"},
        config);
    auto supplier = source.Open(parallel_source_options(2, 2, {2, 0}));
    ASSERT_NE(supplier, nullptr);

    std::vector<std::pair<int32_t, int32_t>> rows;
    while (auto chunk = supplier->GetNextChunk()) {
      ASSERT_EQ(chunk->col_num(), 2);
      for (size_t row = 0; row < chunk->row_num(); ++row) {
        rows.emplace_back(chunk->get(0)->get_elem(row).GetValue<int32_t>(),
                          chunk->get(1)->get_elem(row).GetValue<int32_t>());
      }
    }
    std::sort(rows.begin(), rows.end());
    EXPECT_EQ(rows,
              (std::vector<std::pair<int32_t, int32_t>>{{10, 1}, {20, 2}}));
  }

  {
    SCOPED_TRACE("parallel disabled");
    createCsvFile("serial.csv", "id|name\n1|Alice\n2|Bob\n");
    std::vector<std::string> column_names = {"id", "name"};
    std::vector<std::shared_ptr<::common::DataType>> column_types = {
        createInt32Type(), createStringType()};
    auto shared_state = createSharedState(
        "serial.csv", column_names, column_types,
        {{"skip_rows", "1"}, {"batch_read", "true"}, {"parallel", "false"}});
    auto source = createCsvReader(shared_state)->createChunkSource();
    ASSERT_NE(source, nullptr);
    EXPECT_FALSE(source->ParallelEnabled());

    auto supplier = source->Open(parallel_source_options());
    ASSERT_NE(supplier, nullptr);
    EXPECT_FALSE(supplier->SupportsConcurrentGetNext());
  }

  {
    SCOPED_TRACE("post-read projection rejected");
    createCsvFile("projected.csv", "id|name\n1|Alice\n");
    std::vector<std::string> column_names = {"id", "name"};
    std::vector<std::shared_ptr<::common::DataType>> column_types = {
        createInt32Type(), createStringType()};
    auto shared_state =
        createSharedState("projected.csv", column_names, column_types, {});
    shared_state->projectColumns = {"id"};
    EXPECT_EQ(createCsvReader(shared_state)->createChunkSource(), nullptr);
  }
}

TEST_F(ReaderTest, PartitionedCsvSkipsHeaderForEveryFile) {
  createCsvFile("part-a.csv", "id|name\n1|Alice\n2|Bob\n");
  createCsvFile("part-b.csv", "id|name\n3|Carol\n4|Dave");
  auto config = csv_config(
      {{"id", DataTypeId::kInt32}, {"name", DataTypeId::kVarchar}}, 1);
  config.quoting = true;
  config.escaping = false;
  config.chunk_size = 1;
  auto path = [](const char* file) {
    return std::string(ARROW_READER_TEST_DIR) + "/" + file;
  };
  CSVChunkSource source({path("part-a.csv"), path("part-b.csv")}, config);

  auto supplier = source.Open(parallel_source_options());
  ASSERT_NE(supplier, nullptr);
  EXPECT_EQ(supplier->RowNum(),
            6);  // Two headers are counted as reserve hints.

  EXPECT_EQ(read_sorted_column<int32_t>(supplier),
            (std::vector<int32_t>{1, 2, 3, 4}));
}

TEST_F(ReaderTest, PartitionedCsvPropagatesProducerErrors) {
  createCsvFile("partition-error.csv",
                "id|name\n1|Alice\nnot-an-int|Broken\n3|Carol\n4|Dave");
  auto config = csv_config(
      {{"id", DataTypeId::kInt32}, {"name", DataTypeId::kVarchar}}, 1);
  config.quoting = true;
  config.escaping = false;
  config.chunk_size = 1;
  CSVChunkSource source(
      {std::string(ARROW_READER_TEST_DIR) + "/partition-error.csv"}, config);

  auto supplier = source.Open(parallel_source_options(4, 2));
  ASSERT_NE(supplier, nullptr);
  EXPECT_ANY_THROW({
    while (supplier->GetNextChunk()) {}
  });
}

// Test 2: CSV with different delimiter (tab)
TEST_F(ReaderTest, TestCsvWithTabDelimiter) {
  createCsvFile("test2.csv", "id\tname\tage\n1\tAlice\t95.5\n2\tBob\t87.0\n");

  std::vector<std::string> columnNames = {"id", "name", "age"};
  std::vector<std::shared_ptr<::common::DataType>> columnTypes = {
      createInt32Type(), createStringType(), createDoubleType()};

  auto sharedState = createSharedState(
      "test2.csv", columnNames, columnTypes,
      {{"skip_rows", "1"}, {"delim", "\t"}, {"batch_read", "false"}});

  auto reader = createCsvReader(sharedState);

  auto localState = std::make_shared<reader::ReadLocalState>();
  execution::Context ctx;

  reader->read(localState, ctx);

  EXPECT_EQ(ctx.col_num(), 3);
  EXPECT_EQ(ctx.row_num(), 2);
}

// Test 3: CSV with custom quoting
TEST_F(ReaderTest, TestCsvWithCustomQuoting) {
  createCsvFile("test3.csv",
                "id,name,score\n1,'Alice,Smith',95.5\n2,\"Bob\",87.0\n");

  std::vector<std::string> columnNames = {"id", "name", "score"};
  std::vector<std::shared_ptr<::common::DataType>> columnTypes = {
      createInt32Type(), createStringType(), createDoubleType()};

  auto sharedState = createSharedState("test3.csv", columnNames, columnTypes,
                                       {{"quote", "'"},
                                        {"delim", ","},
                                        {"skip_rows", "1"},
                                        {"batch_read", "false"}});
  auto reader = createCsvReader(sharedState);

  auto localState = std::make_shared<reader::ReadLocalState>();
  execution::Context ctx;

  reader->read(localState, ctx);

  EXPECT_EQ(ctx.col_num(), 3);
  EXPECT_EQ(ctx.row_num(), 2);
}

// Test 4: CSV with header row
TEST_F(ReaderTest, TestCsvWithNoHeader) {
  createCsvFile("test4.csv", "1|Alice|95.5\n2|Bob|87.0\n");

  std::vector<std::string> columnNames = {"id", "name", "score"};
  std::vector<std::shared_ptr<::common::DataType>> columnTypes = {
      createInt32Type(), createStringType(), createDoubleType()};

  auto sharedState = createSharedState("test4.csv", columnNames, columnTypes,
                                       {{"batch_read", "false"}});
  auto reader = createCsvReader(sharedState);

  auto localState = std::make_shared<reader::ReadLocalState>();
  execution::Context ctx;

  reader->read(localState, ctx);

  EXPECT_EQ(ctx.col_num(), 3);
  EXPECT_EQ(ctx.row_num(), 2);
}

// Test 5: Batch read mode
TEST_F(ReaderTest, TestBatchRead) {
  // Create a larger CSV file for batch reading
  std::string content = "id|name|score\n";
  for (int i = 1; i <= 100; ++i) {
    content += std::to_string(i) + "|User" + std::to_string(i) + "|" +
               std::to_string(50.0 + i) + "\n";
  }
  createCsvFile("test5.csv", content);

  std::vector<std::string> columnNames = {"id", "name", "score"};
  std::vector<std::shared_ptr<::common::DataType>> columnTypes = {
      createInt32Type(), createStringType(), createDoubleType()};

  auto sharedState = createSharedState(
      "test5.csv", columnNames, columnTypes,
      {{"batch_read", "true"}, {"batch_size", "1024"}, {"skip_rows", "1"}});
  auto reader = createCsvReader(sharedState);

  auto localState = std::make_shared<reader::ReadLocalState>();
  execution::Context ctx;

  reader->read(localState, ctx);

  // Batch mode: data is materialized into Context chunks
  EXPECT_GT(ctx.chunk_num(), 0);
  EXPECT_EQ(ctx.col_num(), 3);

  // Count rows using helper function
  int64_t totalRows = count_batch_row_num(ctx);
  EXPECT_EQ(totalRows, 100);  // All 100 rows should be read
}

// Test 6: Column pruning (skip columns)
TEST_F(ReaderTest, TestColumnPruning) {
  createCsvFile("test6.csv",
                "id|name|score\n1|Alice|95.5\n2|Bob|87.0\n3|Charlie|92.5\n");

  std::vector<std::string> columnNames = {"id", "name", "score"};
  std::vector<std::shared_ptr<::common::DataType>> columnTypes = {
      createInt32Type(), createStringType(), createDoubleType()};

  // Project only "id" and "score" columns (exclude "name")
  std::vector<std::string> projectColumns = {"id", "score"};
  auto sharedState = createSharedState(
      "test6.csv", columnNames, columnTypes,
      {{"skip_rows", "1"}, {"batch_read", "false"}}, projectColumns);

  auto reader = createCsvReader(sharedState);

  auto localState = std::make_shared<reader::ReadLocalState>();
  execution::Context ctx;

  reader->read(localState, ctx);

  // Should only have 2 columns (id and score)
  EXPECT_EQ(ctx.col_num(), 2);
  EXPECT_EQ(sharedState->columnNum(), 2);
  EXPECT_EQ(ctx.row_num(), 3);
}

// Test 7: Filter pushdown (row filtering)
TEST_F(ReaderTest, TestFilterPushdown) {
  createCsvFile("test7.csv",
                "id|name|score\n1|Alice|95.5\n2|Bob|87.0\n3|Charlie|92.5\n4|"
                "David|88.0\n");

  std::vector<std::string> columnNames = {"id", "name", "score"};
  std::vector<std::shared_ptr<::common::DataType>> columnTypes = {
      createInt32Type(), createStringType(), createDoubleType()};

  // Filter: score > 90.0
  auto filterExpr =
      createFilterExpression("score", ValueConverter::fromDouble(90.0));
  auto sharedState = createSharedState(
      "test7.csv", columnNames, columnTypes,
      {{"skip_rows", "1"}, {"batch_read", "false"}}, {}, filterExpr);

  auto reader = createCsvReader(sharedState);

  auto localState = std::make_shared<reader::ReadLocalState>();
  execution::Context ctx;

  reader->read(localState, ctx);

  // Should filter out rows with score <= 90.0
  // Expected: Alice (95.5) and Charlie (92.5) - 2 rows
  EXPECT_EQ(ctx.col_num(), 3);
  EXPECT_EQ(ctx.row_num(), 2);
}

// Test 8: Combined column pruning and filter pushdown
TEST_F(ReaderTest, TestColumnPruningAndFilterPushdown) {
  createCsvFile("test8.csv",
                "id|name|score\n1|Alice|95.5\n2|Bob|87.0\n3|Charlie|92.5\n4|"
                "David|88.0\n");

  std::vector<std::string> columnNames = {"id", "name", "score"};
  std::vector<std::shared_ptr<::common::DataType>> columnTypes = {
      createInt32Type(), createStringType(), createDoubleType()};

  // Project only "id" and "score" columns (exclude "name"), filter: score
  // > 90.0
  std::vector<std::string> projectColumns = {"id", "score"};
  auto filterExpr =
      createFilterExpression("score", ValueConverter::fromDouble(90.0));
  auto sharedState =
      createSharedState("test8.csv", columnNames, columnTypes,
                        {{"skip_rows", "1"}, {"batch_read", "false"}},
                        projectColumns, filterExpr);

  auto reader = createCsvReader(sharedState);

  auto localState = std::make_shared<reader::ReadLocalState>();
  execution::Context ctx;

  reader->read(localState, ctx);

  // Should have 2 columns (id, score) and filtered rows (score > 90.0)
  EXPECT_EQ(ctx.col_num(), 2);
  EXPECT_EQ(sharedState->columnNum(), 2);
  EXPECT_EQ(ctx.row_num(), 2);  // Alice and Charlie
}

// Test 9: Multiple files reading
TEST_F(ReaderTest, TestMultipleFiles) {
  createCsvFile("test9a.csv", "id|name|score\n1|Alice|95.5\n2|Bob|87.0\n");
  createCsvFile("test9b.csv", "id|name|score\n3|Charlie|92.5\n4|David|88.0\n");

  std::vector<std::string> columnNames = {"id", "name", "score"};
  std::vector<std::shared_ptr<::common::DataType>> columnTypes = {
      createInt32Type(), createStringType(), createDoubleType()};

  auto sharedState =
      createSharedState("test9a.csv", columnNames, columnTypes,
                        {{"skip_rows", "1"}, {"batch_read", "false"}});
  // Add second file
  sharedState->schema.file.paths.push_back(std::string(ARROW_READER_TEST_DIR) +
                                           "/test9b.csv");

  auto reader = createCsvReader(sharedState);

  auto localState = std::make_shared<reader::ReadLocalState>();
  execution::Context ctx;

  reader->read(localState, ctx);

  // Should read all rows from both files (4 rows total)
  EXPECT_EQ(ctx.col_num(), 3);
  EXPECT_EQ(ctx.row_num(), 4);
}

// Test 10: Force column type conversion (int64 -> int32)
TEST_F(ReaderTest, TestForceColumnTypeConversion) {
  // Create CSV file with numeric values that Arrow would default to int64
  createCsvFile("test10.csv",
                "id|name|value\n1|Alice|100\n2|Bob|200\n3|Charlie|300\n");

  // Define schema with int32 instead of int64 to force type conversion
  std::vector<std::string> columnNames = {"id", "name", "value"};
  std::vector<std::shared_ptr<::common::DataType>> columnTypes = {
      createInt32Type(), createStringType(), createInt64Type()};

  auto sharedState =
      createSharedState("test10.csv", columnNames, columnTypes,
                        {{"skip_rows", "1"}, {"batch_read", "false"}});
  auto reader = createCsvReader(sharedState);

  auto localState = std::make_shared<reader::ReadLocalState>();
  execution::Context ctx;

  reader->read(localState, ctx);

  EXPECT_EQ(ctx.col_num(), 3);
  EXPECT_EQ(ctx.row_num(), 3);

  // Verify the first column (id) is int32 ValueColumn
  auto column0 = ctx.chunk(0).columns()[0];
  ASSERT_EQ(column0->column_type(), ContextColumnType::kValue);
  EXPECT_EQ(column0->elem_type().id(), DataTypeId::kInt32);

  // Verify the third column (value) is int64 ValueColumn
  auto column2 = ctx.chunk(0).columns()[2];
  ASSERT_EQ(column2->column_type(), ContextColumnType::kValue);
  EXPECT_EQ(column2->elem_type().id(), DataTypeId::kInt64);
}

// Test 11: Multi-column AND expression filter pushdown
TEST_F(ReaderTest, TestMultiColumnAndFilterPushdown) {
  // Create CSV file with multiple columns
  createCsvFile("test11.csv",
                "id|name|score\n1|Alice|95.5\n2|Bob|87.0\n3|Charlie|92.5\n4|"
                "David|88.0\n5|Eve|96.0\n");

  std::vector<std::string> columnNames = {"id", "name", "score"};
  std::vector<std::shared_ptr<::common::DataType>> columnTypes = {
      createInt32Type(), createStringType(), createDoubleType()};

  // Create AND expression: (id > 2) AND (score > 90.0)
  // This should filter to rows: Charlie (id=3, score=92.5) and Eve (id=5,
  // score=96.0)
  auto leftExpr = createFilterExpression("id", ValueConverter::fromInt32(2));
  auto rightExpr =
      createFilterExpression("score", ValueConverter::fromDouble(90.0));
  auto andExpr = createAndExpression(leftExpr, rightExpr);

  auto sharedState = createSharedState(
      "test11.csv", columnNames, columnTypes,
      {{"skip_rows", "1"}, {"batch_read", "false"}}, {}, andExpr);

  auto reader = createCsvReader(sharedState);

  auto localState = std::make_shared<reader::ReadLocalState>();
  execution::Context ctx;

  reader->read(localState, ctx);

  // Should have 3 columns
  EXPECT_EQ(ctx.col_num(), 3);
  // Should filter to 2 rows: Charlie (id=3, score=92.5) and Eve (id=5,
  // score=96.0)
  EXPECT_EQ(ctx.row_num(), 2);
}

// Test 12: batch_read=true with filter (skipRows) should fallback to full_read
TEST_F(ReaderTest, TestBatchReadWithFilter) {
  createCsvFile("test12.csv",
                "id|name|score\n1|Alice|95.5\n2|Bob|87.0\n3|Charlie|92.5\n4|"
                "David|88.0\n");

  std::vector<std::string> columnNames = {"id", "name", "score"};
  std::vector<std::shared_ptr<::common::DataType>> columnTypes = {
      createInt32Type(), createStringType(), createDoubleType()};

  // Filter: score > 90.0 with batch_read=true
  auto filterExpr =
      createFilterExpression("score", ValueConverter::fromDouble(90.0));
  auto sharedState = createSharedState(
      "test12.csv", columnNames, columnTypes,
      {{"skip_rows", "1"}, {"batch_read", "true"}}, {}, filterExpr);

  auto reader = createCsvReader(sharedState);

  auto localState = std::make_shared<reader::ReadLocalState>();
  execution::Context ctx;

  reader->read(localState, ctx);

  // Should filter out rows with score <= 90.0
  // Expected: Alice (95.5) and Charlie (92.5) - 2 rows
  EXPECT_EQ(ctx.col_num(), 3);
  EXPECT_EQ(ctx.row_num(), 2);
}

// Test 13: batch_read=true with filter AND column projection
TEST_F(ReaderTest, TestBatchReadWithFilterAndProjection) {
  createCsvFile("test13.csv",
                "id|name|score\n1|Alice|95.5\n2|Bob|87.0\n3|Charlie|92.5\n4|"
                "David|88.0\n");

  std::vector<std::string> columnNames = {"id", "name", "score"};
  std::vector<std::shared_ptr<::common::DataType>> columnTypes = {
      createInt32Type(), createStringType(), createDoubleType()};

  // Project only "id" and "score" columns, filter: score > 90.0,
  // batch_read=true
  std::vector<std::string> projectColumns = {"id", "score"};
  auto filterExpr =
      createFilterExpression("score", ValueConverter::fromDouble(90.0));
  auto sharedState = createSharedState(
      "test13.csv", columnNames, columnTypes,
      {{"skip_rows", "1"}, {"batch_read", "true"}}, projectColumns, filterExpr);

  auto reader = createCsvReader(sharedState);

  auto localState = std::make_shared<reader::ReadLocalState>();
  execution::Context ctx;

  reader->read(localState, ctx);

  // Should have 2 columns (id, score) and filtered rows (score > 90.0)
  EXPECT_EQ(ctx.col_num(), 2);
  EXPECT_EQ(sharedState->columnNum(), 2);
  EXPECT_EQ(ctx.row_num(), 2);  // Alice and Charlie
}

// =============== JSON Reader ===============

TEST_F(ReaderTest, TestBasicJsonRead) {
  createJsonFile("test_json_basic.json",
                 "{\"id\":1,\"name\":\"Alice\",\"score\":95.5}\n"
                 "{\"id\":2,\"name\":\"Bob\",\"score\":87.0}\n");

  std::vector<std::string> columnNames = {"id", "name", "score"};
  std::vector<std::shared_ptr<::common::DataType>> columnTypes = {
      createInt64Type(), createStringType(), createDoubleType()};

  auto sharedState =
      createJsonSharedState("test_json_basic.json", columnNames, columnTypes,
                            {{"batch_read", "false"}});
  auto reader = createJsonReader(sharedState, false);

  auto localState = std::make_shared<reader::ReadLocalState>();
  execution::Context ctx;

  reader->read(localState, ctx);

  EXPECT_EQ(ctx.col_num(), 3);
  EXPECT_EQ(ctx.row_num(), 2);
}

TEST_F(ReaderTest, TestJsonNonExistentColumnThrows) {
  createJsonFile("test_json_nonexist.json",
                 "{\"id\":1,\"name\":\"Alice\",\"score\":95.5}\n");

  std::vector<std::string> columnNames = {"id", "name", "wrong_col"};
  std::vector<std::shared_ptr<::common::DataType>> columnTypes = {
      createInt64Type(), createStringType(), createDoubleType()};

  auto sharedState =
      createJsonSharedState("test_json_nonexist.json", columnNames, columnTypes,
                            {{"batch_read", "false"}});
  auto reader = createJsonReader(sharedState, false);

  auto localState = std::make_shared<reader::ReadLocalState>();
  execution::Context ctx;

  EXPECT_THROW(reader->read(localState, ctx),
               exception::SchemaMismatchException);
}

// Test: JSON batch_read=true with filter should fallback to full_read
TEST_F(ReaderTest, TestJsonBatchReadWithFilter) {
  createJsonFile("test_json_filter.json",
                 "{\"id\":1,\"name\":\"Alice\",\"score\":95.5}\n"
                 "{\"id\":2,\"name\":\"Bob\",\"score\":87.0}\n"
                 "{\"id\":3,\"name\":\"Charlie\",\"score\":92.5}\n"
                 "{\"id\":4,\"name\":\"David\",\"score\":88.0}\n");

  std::vector<std::string> columnNames = {"id", "name", "score"};
  std::vector<std::shared_ptr<::common::DataType>> columnTypes = {
      createInt64Type(), createStringType(), createDoubleType()};

  // Filter: score > 90.0 with batch_read=true
  auto filterExpr =
      createFilterExpression("score", ValueConverter::fromDouble(90.0));
  auto sharedState =
      createJsonSharedState("test_json_filter.json", columnNames, columnTypes,
                            {{"batch_read", "true"}});
  sharedState->skipRows = filterExpr;

  auto reader = createJsonReader(sharedState, false);

  auto localState = std::make_shared<reader::ReadLocalState>();
  execution::Context ctx;

  reader->read(localState, ctx);

  // Should filter out rows with score <= 90.0
  // Expected: Alice (95.5) and Charlie (92.5) - 2 rows
  EXPECT_EQ(ctx.col_num(), 3);
  EXPECT_EQ(ctx.row_num(), 2);
}

// Test: JSON batch_read=true with filter AND column projection
TEST_F(ReaderTest, TestJsonBatchReadWithFilterAndProjection) {
  createJsonFile("test_json_filter_proj.json",
                 "{\"id\":1,\"name\":\"Alice\",\"score\":95.5}\n"
                 "{\"id\":2,\"name\":\"Bob\",\"score\":87.0}\n"
                 "{\"id\":3,\"name\":\"Charlie\",\"score\":92.5}\n"
                 "{\"id\":4,\"name\":\"David\",\"score\":88.0}\n");

  std::vector<std::string> columnNames = {"id", "name", "score"};
  std::vector<std::shared_ptr<::common::DataType>> columnTypes = {
      createInt64Type(), createStringType(), createDoubleType()};

  // Project only "id" and "score", filter: score > 90.0, batch_read=true
  auto filterExpr =
      createFilterExpression("score", ValueConverter::fromDouble(90.0));
  auto sharedState =
      createJsonSharedState("test_json_filter_proj.json", columnNames,
                            columnTypes, {{"batch_read", "true"}});
  sharedState->skipRows = filterExpr;
  sharedState->projectColumns = {"id", "score"};

  auto reader = createJsonReader(sharedState, false);

  auto localState = std::make_shared<reader::ReadLocalState>();
  execution::Context ctx;

  reader->read(localState, ctx);

  // Should have 2 columns (id, score) and filtered rows (score > 90.0)
  EXPECT_EQ(ctx.col_num(), 2);
  EXPECT_EQ(sharedState->columnNum(), 2);
  EXPECT_EQ(ctx.row_num(), 2);  // Alice and Charlie
}

}  // namespace test
}  // namespace neug
