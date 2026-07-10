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
#include <limits>

#include "neug/storages/loader/chunk_pipeline_utils.h"
#include "neug/storages/loader/loader_utils.h"

namespace neug {
namespace test {

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

TEST_F(ReaderTest, CsvChunkSourceCanBeReopened) {
  createCsvFile("repeatable.csv", "id|name\n1|Alice\n2|Bob\n3|Carol\n");
  std::vector<std::string> column_names = {"id", "name"};
  std::vector<std::shared_ptr<::common::DataType>> column_types = {
      createInt32Type(), createStringType()};
  auto shared_state =
      createSharedState("repeatable.csv", column_names, column_types,
                        {{"skip_rows", "1"}, {"batch_read", "true"}});
  auto reader = createCsvReader(shared_state);
  auto source = reader->createChunkSource();

  ASSERT_NE(source, nullptr);
  EXPECT_TRUE(source->rewindable());
  for (int pass = 0; pass < 2; ++pass) {
    auto supplier = source->Open();
    ASSERT_NE(supplier, nullptr);
    size_t rows = 0;
    while (auto chunk = supplier->GetNextChunk()) {
      EXPECT_EQ(chunk->col_num(), 2);
      rows += chunk->row_num();
    }
    EXPECT_EQ(rows, 3);
  }
}

TEST_F(ReaderTest, CsvChunkSourcePartitionsQuotedRecords) {
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
        rows.emplace_back(chunk->get(0)->get_elem(row).GetValue<int32_t>(),
                          chunk->get(1)->get_elem(row).GetValue<std::string>());
      }
    }
    std::sort(rows.begin(), rows.end());
    return rows;
  };

  auto expected = read_rows(source->Open());
  ChunkSourceOptions options;
  options.parallel_enabled = true;
  options.producer_count = 4;
  options.queue_capacity = 8;
  options.preserve_order = false;
  auto partitioned = source->Open(options);
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

TEST_F(ReaderTest, PartitionedCsvCarriesSkipAcrossRanges) {
  std::string long_quoted_field(64 * 1024, 'x');
  for (size_t i = 32; i < long_quoted_field.size(); i += 64) {
    long_quoted_field[i] = '\n';
  }
  createCsvFile("partition-skip.csv",
                "0|\"" + long_quoted_field +
                    "\"\r\n1|also-skipped\r\n2|kept\r\n3|last\r\n");

  CsvReadConfig config;
  config.delimiter = '|';
  config.quoting = true;
  config.skip_rows = 2;
  config.chunk_size = 1;
  config.column_names = {"id", "name"};
  config.include_columns = config.column_names;
  config.column_types.emplace("id", DataType(DataTypeId::kInt32));
  config.column_types.emplace("name", DataType(DataTypeId::kVarchar));
  CSVChunkSource source(
      {std::string(ARROW_READER_TEST_DIR) + "/partition-skip.csv"}, config);

  ChunkSourceOptions options;
  options.parallel_enabled = true;
  options.producer_count = 4;
  options.queue_capacity = 4;
  options.preserve_order = false;
  auto supplier = source.Open(options);
  ASSERT_NE(supplier, nullptr);

  std::vector<int32_t> ids;
  while (auto chunk = supplier->GetNextChunk()) {
    for (size_t row = 0; row < chunk->row_num(); ++row) {
      ids.push_back(chunk->get(0)->get_elem(row).GetValue<int32_t>());
    }
  }
  std::sort(ids.begin(), ids.end());
  EXPECT_EQ(ids, (std::vector<int32_t>{2, 3}));
}

TEST_F(ReaderTest, PartitionPlannerMatchesParserWhenDoubleQuoteIsFalse) {
  std::string multiline = "first \"\"quoted\n";
  multiline.append(64 * 1024, 'z');
  multiline += "\ncontinued\"\" tail";
  createCsvFile("partition-double-quote.csv",
                "0|\"" + multiline + "\"\n1|one\n2|two\n");

  CsvReadConfig config;
  config.delimiter = '|';
  config.quoting = true;
  config.double_quote = false;
  config.chunk_size = 1;
  config.column_names = {"id", "name"};
  config.include_columns = config.column_names;
  config.column_types.emplace("id", DataType(DataTypeId::kInt32));
  config.column_types.emplace("name", DataType(DataTypeId::kVarchar));
  CSVChunkSource source(
      {std::string(ARROW_READER_TEST_DIR) + "/partition-double-quote.csv"},
      config);

  auto read_ids = [](std::shared_ptr<IDataChunkSupplier> supplier) {
    std::vector<int32_t> ids;
    while (auto chunk = supplier->GetNextChunk()) {
      for (size_t row = 0; row < chunk->row_num(); ++row) {
        ids.push_back(chunk->get(0)->get_elem(row).GetValue<int32_t>());
      }
    }
    std::sort(ids.begin(), ids.end());
    return ids;
  };
  const auto expected = read_ids(source.Open());

  ChunkSourceOptions options;
  options.parallel_enabled = true;
  options.producer_count = 4;
  options.queue_capacity = 4;
  options.preserve_order = false;
  EXPECT_EQ(read_ids(source.Open(options)), expected);
  EXPECT_EQ(expected, (std::vector<int32_t>{0, 1, 2}));
}

TEST_F(ReaderTest, CsvChunkSourcePushesProjectionIntoPartitionedParsing) {
  createCsvFile("partition-projection.csv",
                "id|ignored|score\n1|Alice|10\n2|Bob|20\n");
  CsvReadConfig config;
  config.delimiter = '|';
  config.skip_rows = 1;
  config.chunk_size = 1;
  config.column_names = {"id", "ignored", "score"};
  config.include_columns = config.column_names;
  config.column_types.emplace("id", DataType(DataTypeId::kInt32));
  config.column_types.emplace("ignored", DataType(DataTypeId::kVarchar));
  config.column_types.emplace("score", DataType(DataTypeId::kInt32));
  CSVChunkSource source(
      {std::string(ARROW_READER_TEST_DIR) + "/partition-projection.csv"},
      config);

  ChunkSourceOptions options;
  options.parallel_enabled = true;
  options.producer_count = 2;
  options.queue_capacity = 2;
  options.preserve_order = false;
  options.projected_columns = {2, 0};
  auto supplier = source.Open(options);
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
  EXPECT_EQ(rows, (std::vector<std::pair<int32_t, int32_t>>{{10, 1}, {20, 2}}));
}

TEST_F(ReaderTest, ChunkPipelineAllocationSharesHardwareBudget) {
  constexpr int64_t kGiB = 1024LL * 1024 * 1024;
  auto allocation = resolve_chunk_pipeline_allocation(kGiB, true, false, 16);
  EXPECT_TRUE(allocation.parallel_enabled);
  EXPECT_EQ(allocation.producer_count, 8);
  EXPECT_EQ(allocation.consumer_count, 8);
  EXPECT_LE(allocation.producer_count + allocation.consumer_count, 16);
  EXPECT_EQ(allocation.queue_capacity, 16);

  auto ordered = resolve_chunk_pipeline_allocation(kGiB, true, true, 16);
  EXPECT_FALSE(ordered.parallel_enabled);
  EXPECT_EQ(ordered.producer_count, 1);
  EXPECT_EQ(ordered.consumer_count, 1);

  auto disabled = resolve_chunk_pipeline_allocation(kGiB, false, false, 16);
  EXPECT_FALSE(disabled.parallel_enabled);
  EXPECT_EQ(disabled.producer_count, 1);
  EXPECT_EQ(disabled.consumer_count, 1);

  auto small =
      resolve_chunk_pipeline_allocation(128LL * 1024 * 1024, true, false, 16);
  EXPECT_FALSE(small.parallel_enabled);
  EXPECT_EQ(small.producer_count, 1);
  EXPECT_EQ(small.consumer_count, 1);

  auto two_workers = resolve_chunk_pipeline_allocation(kGiB, true, false, 2);
  EXPECT_TRUE(two_workers.parallel_enabled);
  EXPECT_EQ(two_workers.producer_count, 1);
  EXPECT_EQ(two_workers.consumer_count, 1);

  auto maximum_size = resolve_chunk_pipeline_allocation(
      std::numeric_limits<int64_t>::max(), true, false, 16);
  EXPECT_TRUE(maximum_size.parallel_enabled);
  EXPECT_EQ(maximum_size.producer_count, 8);
  EXPECT_EQ(maximum_size.consumer_count, 8);
}

TEST_F(ReaderTest, CsvChunkSourceHonorsParallelFalse) {
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

  ChunkSourceOptions options;
  options.parallel_enabled = true;
  options.producer_count = 4;
  options.queue_capacity = 8;
  options.preserve_order = false;
  auto supplier = source->Open(options);
  ASSERT_NE(supplier, nullptr);
  EXPECT_FALSE(supplier->SupportsConcurrentGetNext());
}

TEST_F(ReaderTest, PartitionedCsvSkipsHeaderForEveryFile) {
  createCsvFile("part-a.csv", "id|name\n1|Alice\n2|Bob\n");
  createCsvFile("part-b.csv", "id|name\n3|Carol\n4|Dave");
  CsvReadConfig config;
  config.delimiter = '|';
  config.quoting = true;
  config.escaping = false;
  config.skip_rows = 1;
  config.chunk_size = 1;
  config.column_names = {"id", "name"};
  config.include_columns = config.column_names;
  config.column_types.emplace("id", DataType(DataTypeId::kInt32));
  config.column_types.emplace("name", DataType(DataTypeId::kVarchar));
  auto path = [](const char* file) {
    return std::string(ARROW_READER_TEST_DIR) + "/" + file;
  };
  CSVChunkSource source({path("part-a.csv"), path("part-b.csv")}, config);

  ChunkSourceOptions options;
  options.parallel_enabled = true;
  options.producer_count = 4;
  options.queue_capacity = 8;
  options.preserve_order = false;
  auto supplier = source.Open(options);
  ASSERT_NE(supplier, nullptr);
  EXPECT_EQ(supplier->RowNum(),
            6);  // Two headers are counted as reserve hints.

  std::vector<int32_t> ids;
  while (auto chunk = supplier->GetNextChunk()) {
    for (size_t row = 0; row < chunk->row_num(); ++row) {
      ids.push_back(chunk->get(0)->get_elem(row).GetValue<int32_t>());
    }
  }
  std::sort(ids.begin(), ids.end());
  EXPECT_EQ(ids, (std::vector<int32_t>{1, 2, 3, 4}));
}

TEST_F(ReaderTest, PartitionedCsvPropagatesProducerErrors) {
  createCsvFile("partition-error.csv",
                "id|name\n1|Alice\nnot-an-int|Broken\n3|Carol\n4|Dave");
  CsvReadConfig config;
  config.delimiter = '|';
  config.quoting = true;
  config.escaping = false;
  config.skip_rows = 1;
  config.chunk_size = 1;
  config.column_names = {"id", "name"};
  config.include_columns = config.column_names;
  config.column_types.emplace("id", DataType(DataTypeId::kInt32));
  config.column_types.emplace("name", DataType(DataTypeId::kVarchar));
  CSVChunkSource source(
      {std::string(ARROW_READER_TEST_DIR) + "/partition-error.csv"}, config);

  ChunkSourceOptions options;
  options.parallel_enabled = true;
  options.producer_count = 4;
  options.queue_capacity = 2;
  options.preserve_order = false;
  auto supplier = source.Open(options);
  ASSERT_NE(supplier, nullptr);
  EXPECT_ANY_THROW({
    while (supplier->GetNextChunk()) {}
  });
}

TEST_F(ReaderTest, CsvChunkSourceRejectsPostReadProjection) {
  createCsvFile("projected.csv", "id|name\n1|Alice\n");
  std::vector<std::string> column_names = {"id", "name"};
  std::vector<std::shared_ptr<::common::DataType>> column_types = {
      createInt32Type(), createStringType()};
  auto shared_state =
      createSharedState("projected.csv", column_names, column_types, {});
  shared_state->projectColumns = {"id"};

  auto reader = createCsvReader(shared_state);
  EXPECT_EQ(reader->createChunkSource(), nullptr);
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
