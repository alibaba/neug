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

namespace gs {
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
  auto reader = createArrowReader(sharedState);

  auto localState = std::make_shared<reader::ReadLocalState>();
  runtime::Context ctx;

  reader->read(localState, ctx);

  // Verify data: should have 3 columns
  EXPECT_EQ(ctx.col_num(), 3);
  // Verify rows: should have 3 rows
  EXPECT_EQ(ctx.row_num(), 3);
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

  auto reader = createArrowReader(sharedState);

  auto localState = std::make_shared<reader::ReadLocalState>();
  runtime::Context ctx;

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
  auto reader = createArrowReader(sharedState);

  auto localState = std::make_shared<reader::ReadLocalState>();
  runtime::Context ctx;

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
  auto reader = createArrowReader(sharedState);

  auto localState = std::make_shared<reader::ReadLocalState>();
  runtime::Context ctx;

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
  auto reader = createArrowReader(sharedState);

  auto localState = std::make_shared<reader::ReadLocalState>();
  runtime::Context ctx;

  reader->read(localState, ctx);

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

  // Skip "name" column
  std::vector<std::string> skipColumns = {"name"};
  auto sharedState = createSharedState(
      "test6.csv", columnNames, columnTypes,
      {{"skip_rows", "1"}, {"batch_read", "false"}}, skipColumns);

  auto reader = createArrowReader(sharedState);

  auto localState = std::make_shared<reader::ReadLocalState>();
  runtime::Context ctx;

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

  auto reader = createArrowReader(sharedState);

  auto localState = std::make_shared<reader::ReadLocalState>();
  runtime::Context ctx;

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

  // Skip "name" column and filter: score > 90.0
  std::vector<std::string> skipColumns = {"name"};
  auto filterExpr =
      createFilterExpression("score", ValueConverter::fromDouble(90.0));
  auto sharedState = createSharedState(
      "test8.csv", columnNames, columnTypes,
      {{"skip_rows", "1"}, {"batch_read", "false"}}, skipColumns, filterExpr);

  auto reader = createArrowReader(sharedState);

  auto localState = std::make_shared<reader::ReadLocalState>();
  runtime::Context ctx;

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

  auto reader = createArrowReader(sharedState);

  auto localState = std::make_shared<reader::ReadLocalState>();
  runtime::Context ctx;

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
  auto reader = createArrowReader(sharedState);

  auto localState = std::make_shared<reader::ReadLocalState>();
  runtime::Context ctx;

  reader->read(localState, ctx);

  EXPECT_EQ(ctx.col_num(), 3);
  EXPECT_EQ(ctx.row_num(), 3);

  // Verify the first column (id) is int32
  auto column0 = ctx.columns[0];
  ASSERT_EQ(column0->column_type(), runtime::ContextColumnType::kArrowArray);
  auto arrayColumn0 =
      std::dynamic_pointer_cast<runtime::ArrowArrayContextColumn>(column0);
  auto arrowType0 = arrayColumn0->GetArrowType();
  EXPECT_TRUE(arrowType0->Equals(arrow::int32()))
      << "Expected int32, but got: " << arrowType0->ToString();

  // Verify the third column (value) is int64
  auto column2 = ctx.columns[2];
  ASSERT_EQ(column2->column_type(), runtime::ContextColumnType::kArrowArray);
  auto arrayColumn2 =
      std::dynamic_pointer_cast<runtime::ArrowArrayContextColumn>(column2);
  auto arrowType2 = arrayColumn2->GetArrowType();
  EXPECT_TRUE(arrowType2->Equals(arrow::int64()))
      << "Expected int64 for value column, but got: " << arrowType2->ToString();
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

  auto reader = createArrowReader(sharedState);

  auto localState = std::make_shared<reader::ReadLocalState>();
  runtime::Context ctx;

  reader->read(localState, ctx);

  // Should have 3 columns
  EXPECT_EQ(ctx.col_num(), 3);
  // Should filter to 2 rows: Charlie (id=3, score=92.5) and Eve (id=5,
  // score=96.0)
  EXPECT_EQ(ctx.row_num(), 2);
}

}  // namespace test
}  // namespace gs
