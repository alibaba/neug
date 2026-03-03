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
  auto reader = createArrowReader(sharedState);

  auto localState = std::make_shared<reader::ReadLocalState>();
  execution::Context ctx;

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
  auto reader = createArrowReader(sharedState);

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
  auto reader = createArrowReader(sharedState);

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
  auto reader = createArrowReader(sharedState);

  auto localState = std::make_shared<reader::ReadLocalState>();
  execution::Context ctx;

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

  auto reader = createArrowReader(sharedState);

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

  // Skip "name" column and filter: score > 90.0
  std::vector<std::string> skipColumns = {"name"};
  auto filterExpr =
      createFilterExpression("score", ValueConverter::fromDouble(90.0));
  auto sharedState = createSharedState(
      "test8.csv", columnNames, columnTypes,
      {{"skip_rows", "1"}, {"batch_read", "false"}}, skipColumns, filterExpr);

  auto reader = createArrowReader(sharedState);

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

  auto reader = createArrowReader(sharedState);

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
  auto reader = createArrowReader(sharedState);

  auto localState = std::make_shared<reader::ReadLocalState>();
  execution::Context ctx;

  reader->read(localState, ctx);

  EXPECT_EQ(ctx.col_num(), 3);
  EXPECT_EQ(ctx.row_num(), 3);

  // Verify the first column (id) is int32
  auto column0 = ctx.columns[0];
  ASSERT_EQ(column0->column_type(), execution::ContextColumnType::kArrowArray);
  auto arrayColumn0 =
      std::dynamic_pointer_cast<execution::ArrowArrayContextColumn>(column0);
  auto arrowType0 = arrayColumn0->GetArrowType();
  EXPECT_TRUE(arrowType0->Equals(arrow::int32()))
      << "Expected int32, but got: " << arrowType0->ToString();

  // Verify the third column (value) is int64
  auto column2 = ctx.columns[2];
  ASSERT_EQ(column2->column_type(), execution::ContextColumnType::kArrowArray);
  auto arrayColumn2 =
      std::dynamic_pointer_cast<execution::ArrowArrayContextColumn>(column2);
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
  execution::Context ctx;

  reader->read(localState, ctx);

  // Should have 3 columns
  EXPECT_EQ(ctx.col_num(), 3);
  // Should filter to 2 rows: Charlie (id=3, score=92.5) and Eve (id=5,
  // score=96.0)
  EXPECT_EQ(ctx.row_num(), 2);
}

// =============== Type Converter ===============
class ArrowTypeConverterTest : public ::testing::Test {
 public:
  reader::ArrowTypeConverter converter_;
};

::common::DataType MakePrimitiveType(::common::PrimitiveType type) {
  ::common::DataType dt;
  dt.set_primitive_type(type);
  return dt;
}

::common::DataType MakeStringType() {
  ::common::DataType dt;
  dt.mutable_string()->mutable_var_char();
  return dt;
}

::common::DataType MakeDateType() {
  ::common::DataType dt;
  dt.mutable_temporal()->mutable_date();
  return dt;
}

::common::DataType MakeTimestampType() {
  ::common::DataType dt;
  dt.mutable_temporal()->mutable_timestamp();
  return dt;
}

::common::DataType MakeIntervalType() {
  ::common::DataType dt;
  dt.mutable_temporal()->mutable_interval();
  return dt;
}

::common::DataType MakeArrayType(const ::common::DataType& component,
                                 int64_t max_length = -1) {
  ::common::DataType dt;
  auto* array = dt.mutable_array();
  *array->mutable_component_type() = component;
  if (max_length > 0) {
    array->set_max_length(max_length);
  }
  return dt;
}

::common::DataType MakeMapType(const ::common::DataType& key,
                               const ::common::DataType& value) {
  ::common::DataType dt;
  auto* map = dt.mutable_map();
  *map->mutable_key_type() = key;
  *map->mutable_value_type() = value;
  return dt;
}

TEST_F(ArrowTypeConverterTest, Convert_PrimitiveTypes) {
  struct TestCase {
    ::common::PrimitiveType input;
    std::shared_ptr<arrow::DataType> expected;
  };

  std::vector<TestCase> cases = {
      {::common::PrimitiveType::DT_BOOL, arrow::boolean()},
      {::common::PrimitiveType::DT_SIGNED_INT32, arrow::int32()},
      {::common::PrimitiveType::DT_UNSIGNED_INT32, arrow::uint32()},
      {::common::PrimitiveType::DT_SIGNED_INT64, arrow::int64()},
      {::common::PrimitiveType::DT_UNSIGNED_INT64, arrow::uint64()},
      {::common::PrimitiveType::DT_FLOAT, arrow::float32()},
      {::common::PrimitiveType::DT_DOUBLE, arrow::float64()},
  };

  for (const auto& tc : cases) {
    auto input = MakePrimitiveType(tc.input);
    auto result = converter_.convert(input);  // ← 使用实例方法
    ASSERT_NE(result, nullptr);
    EXPECT_TRUE(result->Equals(tc.expected))
        << "Failed for " << static_cast<int>(tc.input);
  }
}

TEST_F(ArrowTypeConverterTest, Convert_StringType) {
  auto input = MakeStringType();
  auto result = converter_.convert(input);
  ASSERT_NE(result, nullptr);
  EXPECT_TRUE(result->Equals(arrow::large_utf8()));
}

TEST_F(ArrowTypeConverterTest, Convert_TemporalTypes) {
  {
    auto input = MakeDateType();
    auto result = converter_.convert(input);
    ASSERT_NE(result, nullptr);
    EXPECT_TRUE(result->Equals(arrow::date64()));
  }

  {
    auto input = MakeTimestampType();
    auto result = converter_.convert(input);
    ASSERT_NE(result, nullptr);
    EXPECT_TRUE(result->Equals(arrow::timestamp(arrow::TimeUnit::MILLI)));
  }

  {
    auto input = MakeIntervalType();
    auto result = converter_.convert(input);
    ASSERT_NE(result, nullptr);
    EXPECT_TRUE(result->Equals(arrow::large_utf8()));
  }
}

TEST_F(ArrowTypeConverterTest, Convert_ArrayTypes) {
  {
    auto component =
        MakePrimitiveType(::common::PrimitiveType::DT_SIGNED_INT32);
    auto input = MakeArrayType(component);
    auto result = converter_.convert(input);
    auto expected = arrow::list(arrow::int32());
    ASSERT_NE(result, nullptr);
    EXPECT_TRUE(result->Equals(expected));
  }

  {
    auto component = MakePrimitiveType(::common::PrimitiveType::DT_BOOL);
    auto input = MakeArrayType(component, 10);
    auto result = converter_.convert(input);
    auto expected = arrow::fixed_size_list(arrow::boolean(), 10);
    ASSERT_NE(result, nullptr);
    EXPECT_TRUE(result->Equals(expected));
  }
}

TEST_F(ArrowTypeConverterTest, Convert_MapType) {
  auto key = MakeStringType();
  auto value = MakePrimitiveType(::common::PrimitiveType::DT_SIGNED_INT64);
  auto input = MakeMapType(key, value);
  auto result = converter_.convert(input);
  auto expected = arrow::map(arrow::large_utf8(), arrow::int64());
  ASSERT_NE(result, nullptr);
  EXPECT_TRUE(result->Equals(expected));
}

TEST_F(ArrowTypeConverterTest, ReverseConvert_PrimitiveTypes) {
  struct TestCase {
    std::shared_ptr<arrow::DataType> input;
    ::common::PrimitiveType expected;
  };

  std::vector<TestCase> cases = {
      {arrow::boolean(), ::common::PrimitiveType::DT_BOOL},
      {arrow::int8(), ::common::PrimitiveType::DT_SIGNED_INT32},
      {arrow::int16(), ::common::PrimitiveType::DT_SIGNED_INT32},
      {arrow::int32(), ::common::PrimitiveType::DT_SIGNED_INT32},
      {arrow::uint8(), ::common::PrimitiveType::DT_UNSIGNED_INT32},
      {arrow::uint16(), ::common::PrimitiveType::DT_UNSIGNED_INT32},
      {arrow::uint32(), ::common::PrimitiveType::DT_UNSIGNED_INT32},
      {arrow::int64(), ::common::PrimitiveType::DT_SIGNED_INT64},
      {arrow::uint64(), ::common::PrimitiveType::DT_UNSIGNED_INT64},
      {arrow::float32(), ::common::PrimitiveType::DT_FLOAT},
      {arrow::float64(), ::common::PrimitiveType::DT_DOUBLE},
  };

  for (const auto& tc : cases) {
    auto result = converter_.convert(*tc.input);
    ASSERT_NE(result, nullptr);
    ASSERT_TRUE(result->has_primitive_type());
    EXPECT_EQ(result->primitive_type(), tc.expected);
  }
}

TEST_F(ArrowTypeConverterTest, ReverseConvert_StringTypes) {
  std::vector<std::shared_ptr<arrow::DataType>> cases = {
      arrow::utf8(),
      arrow::large_utf8(),
  };

  for (const auto& input : cases) {
    auto result = converter_.convert(*input);
    ASSERT_NE(result, nullptr);
    EXPECT_TRUE(result->has_string());
  }
}

TEST_F(ArrowTypeConverterTest, ReverseConvert_TemporalTypes) {
  {
    auto result = converter_.convert(*arrow::date32());
    ASSERT_NE(result, nullptr);
    EXPECT_TRUE(result->has_temporal());
    EXPECT_TRUE(result->temporal().has_date());
  }

  {
    auto result = converter_.convert(*arrow::date64());
    ASSERT_NE(result, nullptr);
    EXPECT_TRUE(result->has_temporal());
    EXPECT_TRUE(result->temporal().has_date());
  }

  {
    auto result =
        converter_.convert(*arrow::timestamp(arrow::TimeUnit::SECOND));
    ASSERT_NE(result, nullptr);
    EXPECT_TRUE(result->has_temporal());
    EXPECT_TRUE(result->temporal().has_timestamp());
  }

  {
    auto result = converter_.convert(*arrow::duration(arrow::TimeUnit::MILLI));
    ASSERT_NE(result, nullptr);
    EXPECT_TRUE(result->has_temporal());
    EXPECT_TRUE(result->temporal().has_interval());
  }
}

TEST_F(ArrowTypeConverterTest, ReverseConvert_ListTypes) {
  {
    auto input = arrow::list(arrow::int32());
    auto result = converter_.convert(*input);
    ASSERT_NE(result, nullptr);
    EXPECT_TRUE(result->has_array());
    EXPECT_EQ(result->array().max_length(), 0);
    EXPECT_EQ(result->array().component_type().primitive_type(),
              ::common::PrimitiveType::DT_SIGNED_INT32);
  }

  {
    auto input = arrow::fixed_size_list(arrow::float64(), 5);
    auto result = converter_.convert(*input);
    ASSERT_NE(result, nullptr);
    EXPECT_TRUE(result->has_array());
    EXPECT_EQ(result->array().max_length(), 5);
    EXPECT_EQ(result->array().component_type().primitive_type(),
              ::common::PrimitiveType::DT_DOUBLE);
  }
}

TEST_F(ArrowTypeConverterTest, ReverseConvert_MapType) {
  auto input = arrow::map(arrow::utf8(), arrow::int64());
  auto result = converter_.convert(*input);
  ASSERT_NE(result, nullptr);

  EXPECT_TRUE(result->has_map());
  EXPECT_EQ(result->map().key_type().item_case(), ::common::DataType::kString);
  EXPECT_EQ(result->map().value_type().primitive_type(),
            ::common::PrimitiveType::DT_SIGNED_INT64);
}

TEST_F(ArrowTypeConverterTest,
       ReverseConvert_UnsupportedType_DefaultsToString) {
  auto input = arrow::binary();  // unsupported
  auto result = converter_.convert(*input);
  ASSERT_NE(result, nullptr);
  EXPECT_TRUE(result->has_string());  // fallback to string
}

class ArrowExpressionConverterTest : public ::testing::Test {
 public:
  reader::ArrowExpressionConverter converter_;

  // Helper: Create constant value
  ::common::ExprOpr MakeConstBool(bool value) {
    ::common::ExprOpr opr;
    opr.mutable_const_()->set_boolean(value);
    return opr;
  }

  ::common::ExprOpr MakeConstInt(int64_t value) {
    ::common::ExprOpr opr;
    opr.mutable_const_()->set_i64(value);
    return opr;
  }

  ::common::ExprOpr MakeConstFloat(double value) {
    ::common::ExprOpr opr;
    opr.mutable_const_()->set_f64(value);
    return opr;
  }

  ::common::ExprOpr MakeConstString(const std::string& value) {
    ::common::ExprOpr opr;
    opr.mutable_const_()->set_str(value);
    return opr;
  }

  ::common::ExprOpr MakeVar(const std::string& name) {
    ::common::ExprOpr opr;
    auto* var = opr.mutable_var();
    var->mutable_tag()->set_name(name);
    return opr;
  }

  ::common::ExprOpr MakeLogical(::common::Logical op) {
    ::common::ExprOpr opr;
    opr.set_logical(op);
    return opr;
  }

  ::common::ExprOpr MakeArith(::common::Arithmetic op) {
    ::common::ExprOpr opr;
    opr.set_arith(op);
    return opr;
  }

  ::common::ExprOpr MakeBrace(::common::ExprOpr::Brace brace) {
    ::common::ExprOpr opr;
    opr.set_brace(brace);
    return opr;
  }

  ::common::Expression MakeExpression(
      const std::vector<::common::ExprOpr>& ops) {
    ::common::Expression expr;
    for (const auto& op : ops) {
      *expr.add_operators() = op;
    }
    return expr;
  }

  void ExpectEqual(const arrow::compute::Expression& actual,
                   const arrow::compute::Expression& expected) {
    EXPECT_EQ(actual.ToString(), expected.ToString());
  }
};

TEST_F(ArrowExpressionConverterTest, Convert_ConstBool) {
  auto expr = MakeExpression({MakeConstBool(true)});
  auto result = converter_.convert(expr);
  ExpectEqual(result, arrow::compute::literal(true));
}

TEST_F(ArrowExpressionConverterTest, Convert_ConstInt) {
  auto expr = MakeExpression({MakeConstInt(42)});
  auto result = converter_.convert(expr);
  ExpectEqual(result, arrow::compute::literal(int64_t{42}));
}

TEST_F(ArrowExpressionConverterTest, Convert_ConstFloat) {
  auto expr = MakeExpression({MakeConstFloat(3.14)});
  auto result = converter_.convert(expr);
  ExpectEqual(result, arrow::compute::literal(3.14));
}

TEST_F(ArrowExpressionConverterTest, Convert_ConstString) {
  auto expr = MakeExpression({MakeConstString("hello")});
  auto result = converter_.convert(expr);
  ExpectEqual(result, arrow::compute::literal("hello"));
}

TEST_F(ArrowExpressionConverterTest, Convert_Variable) {
  auto expr = MakeExpression({MakeVar("age")});
  auto result = converter_.convert(expr);
  ExpectEqual(result, arrow::compute::field_ref("age"));
}

TEST_F(ArrowExpressionConverterTest, Convert_VariableWithProperty_Throws) {
  ::common::ExprOpr opr;
  auto* var = opr.mutable_var();
  var->mutable_tag()->set_name("person");
  var->mutable_property()->mutable_key()->set_name("name");

  auto expr = MakeExpression({opr});
  EXPECT_THROW(converter_.convert(expr), exception::ConversionException);
}

TEST_F(ArrowExpressionConverterTest, Convert_Addition) {
  auto expr = MakeExpression(
      {MakeVar("a"), MakeVar("b"), MakeArith(::common::Arithmetic::ADD)});
  auto result = converter_.convert(expr);
  auto expected = arrow::compute::call(
      "add", {arrow::compute::field_ref("a"), arrow::compute::field_ref("b")});
  ExpectEqual(result, expected);
}

TEST_F(ArrowExpressionConverterTest, Convert_Multiplication) {
  auto expr = MakeExpression(
      {MakeVar("x"), MakeVar("y"), MakeArith(::common::Arithmetic::MUL)});
  auto result = converter_.convert(expr);
  auto expected = arrow::compute::call(
      "multiply",
      {arrow::compute::field_ref("x"), arrow::compute::field_ref("y")});
  ExpectEqual(result, expected);
}

TEST_F(ArrowExpressionConverterTest, Convert_Equality) {
  auto expr = MakeExpression(
      {MakeVar("a"), MakeConstInt(10), MakeLogical(::common::Logical::EQ)});
  auto result = converter_.convert(expr);
  auto expected = arrow::compute::equal(arrow::compute::field_ref("a"),
                                        arrow::compute::literal(int64_t{10}));
  ExpectEqual(result, expected);
}

TEST_F(ArrowExpressionConverterTest, Convert_AND) {
  auto expr = MakeExpression(
      {MakeVar("a"), MakeLogical(::common::Logical::GT), MakeConstInt(5),
       MakeLogical(::common::Logical::AND), MakeVar("b"),
       MakeLogical(::common::Logical::LT), MakeConstInt(10)

      });
  auto result = converter_.convert(expr);
  auto expected = arrow::compute::call(
      "and_kleene",
      {arrow::compute::greater(arrow::compute::field_ref("a"),
                               arrow::compute::literal(int64_t{5})),
       arrow::compute::less(arrow::compute::field_ref("b"),
                            arrow::compute::literal(int64_t{10}))});
  ExpectEqual(result, expected);
}

TEST_F(ArrowExpressionConverterTest, Convert_NOT) {
  auto expr = MakeExpression(
      {MakeLogical(::common::Logical::NOT),
       MakeBrace(::common::ExprOpr::Brace::ExprOpr_Brace_LEFT_BRACE),
       MakeVar("flag"), MakeLogical(::common::Logical::EQ), MakeConstBool(true),
       MakeBrace(::common::ExprOpr::Brace::ExprOpr_Brace_RIGHT_BRACE)});
  auto result = converter_.convert(expr);
  auto expected = arrow::compute::call(
      "invert", {arrow::compute::equal(arrow::compute::field_ref("flag"),
                                       arrow::compute::literal(true))});
  ExpectEqual(result, expected);
}

TEST_F(ArrowExpressionConverterTest, Precedence_Arithmetic) {
  auto expr = MakeExpression({MakeVar("a"), MakeVar("b"), MakeVar("c"),
                              MakeArith(::common::Arithmetic::MUL),
                              MakeArith(::common::Arithmetic::ADD)});
  auto result = converter_.convert(expr);
  auto expected = arrow::compute::call(
      "add",
      {arrow::compute::field_ref("a"),
       arrow::compute::call("multiply", {arrow::compute::field_ref("b"),
                                         arrow::compute::field_ref("c")})});
  ExpectEqual(result, expected);
}

TEST_F(ArrowExpressionConverterTest, Precedence_Mixed) {
  auto expr = MakeExpression(
      {MakeVar("a"), MakeArith(::common::Arithmetic::MUL), MakeVar("b"),
       MakeArith(::common::Arithmetic::ADD), MakeVar("c"),
       MakeArith(::common::Arithmetic::MUL), MakeVar("d")});
  auto result = converter_.convert(expr);
  auto expected = arrow::compute::call(
      "add",
      {arrow::compute::call("multiply", {arrow::compute::field_ref("a"),
                                         arrow::compute::field_ref("b")}),
       arrow::compute::call("multiply", {arrow::compute::field_ref("c"),
                                         arrow::compute::field_ref("d")})});
  ExpectEqual(result, expected);
}

TEST_F(ArrowExpressionConverterTest, Parentheses_Simple) {
  auto expr = MakeExpression(
      {MakeBrace(::common::ExprOpr::Brace::ExprOpr_Brace_LEFT_BRACE),
       MakeVar("a"), MakeVar("b"), MakeArith(::common::Arithmetic::ADD),
       MakeBrace(::common::ExprOpr::Brace::ExprOpr_Brace_RIGHT_BRACE),
       MakeVar("c"), MakeArith(::common::Arithmetic::MUL)});
  auto result = converter_.convert(expr);
  auto expected = arrow::compute::call(
      "multiply",
      {arrow::compute::call("add", {arrow::compute::field_ref("a"),
                                    arrow::compute::field_ref("b")}),
       arrow::compute::field_ref("c")});
  ExpectEqual(result, expected);
}

TEST_F(ArrowExpressionConverterTest, Parentheses_Nested) {
  auto expr = MakeExpression(
      {MakeBrace(::common::ExprOpr::Brace::ExprOpr_Brace_LEFT_BRACE),
       MakeBrace(::common::ExprOpr::Brace::ExprOpr_Brace_LEFT_BRACE),
       MakeVar("a"), MakeVar("b"), MakeArith(::common::Arithmetic::ADD),
       MakeBrace(::common::ExprOpr::Brace::ExprOpr_Brace_RIGHT_BRACE),
       MakeVar("c"), MakeArith(::common::Arithmetic::MUL),
       MakeBrace(::common::ExprOpr::Brace::ExprOpr_Brace_RIGHT_BRACE),
       MakeVar("d"), MakeArith(::common::Arithmetic::ADD)});
  auto result = converter_.convert(expr);
  auto expected = arrow::compute::call(
      "add", {arrow::compute::call(
                  "multiply", {arrow::compute::call(
                                   "add", {arrow::compute::field_ref("a"),
                                           arrow::compute::field_ref("b")}),
                               arrow::compute::field_ref("c")}),
              arrow::compute::field_ref("d")});
  ExpectEqual(result, expected);
}

TEST_F(ArrowExpressionConverterTest, Invalid_UnmatchedLeftParen) {
  auto expr = MakeExpression(
      {MakeBrace(::common::ExprOpr::Brace::ExprOpr_Brace_LEFT_BRACE),
       MakeVar("a"), MakeVar("b"), MakeArith(::common::Arithmetic::ADD)});
  EXPECT_THROW(converter_.convert(expr), exception::ConversionException);
}

TEST_F(ArrowExpressionConverterTest, Invalid_UnmatchedRightParen) {
  auto expr = MakeExpression(
      {MakeVar("a"), MakeVar("b"), MakeArith(::common::Arithmetic::ADD),
       MakeBrace(::common::ExprOpr::Brace::ExprOpr_Brace_RIGHT_BRACE)});
  EXPECT_THROW(converter_.convert(expr), exception::ConversionException);
}

TEST_F(ArrowExpressionConverterTest, Invalid_NotEnoughOperands) {
  auto expr = MakeExpression({MakeArith(::common::Arithmetic::ADD)});
  EXPECT_THROW(converter_.convert(expr), exception::ConversionException);
}

TEST_F(ArrowExpressionConverterTest, Invalid_TooManyValues) {
  auto expr =
      MakeExpression({MakeVar("a"), MakeVar("b"),
                      MakeArith(::common::Arithmetic::ADD), MakeVar("c")});
  EXPECT_THROW(converter_.convert(expr), exception::ConversionException);
}

}  // namespace test
}  // namespace neug
