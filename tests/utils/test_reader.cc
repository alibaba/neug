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

#include <limits>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "neug/common/columns/array_columns.h"
#include "neug/common/columns/list_columns.h"
#include "neug/common/columns/value_columns.h"
#include "neug/storages/loader/loader_utils.h"
#include "neug/utils/io/read/common/row_expression_filter.h"
#include "neug/utils/io/read/common/type_converter.h"

namespace neug {
namespace test {

namespace {

class VectorChunkSupplier final : public IDataChunkSupplier {
 public:
  explicit VectorChunkSupplier(std::vector<DataChunk> chunks)
      : chunks_(std::move(chunks)) {
    for (const auto& chunk : chunks_) {
      row_count_ += static_cast<int64_t>(chunk.row_num());
    }
  }

  std::shared_ptr<DataChunk> GetNextChunk() override {
    if (next_ == chunks_.size()) {
      return nullptr;
    }
    return std::make_shared<DataChunk>(chunks_[next_++]);
  }

  int64_t RowNum() const override { return row_count_; }

 private:
  std::vector<DataChunk> chunks_;
  size_t next_ = 0;
  int64_t row_count_ = 0;
};

std::shared_ptr<::common::Expression> comparison(const std::string& column,
                                                 ::common::Logical logical,
                                                 int32_t value) {
  auto expression = std::make_shared<::common::Expression>();
  expression->add_operators()->mutable_var()->mutable_tag()->set_name(column);
  expression->add_operators()->set_logical(logical);
  expression->add_operators()->mutable_const_()->set_i32(value);
  return expression;
}

std::shared_ptr<::common::Expression> isNull(const std::string& column) {
  auto expression = std::make_shared<::common::Expression>();
  expression->add_operators()->set_logical(::common::Logical::ISNULL);
  expression->add_operators()->mutable_var()->mutable_tag()->set_name(column);
  return expression;
}

void appendExpression(::common::Expression& output,
                      const ::common::Expression& input) {
  for (const auto& op : input.operators()) {
    *output.add_operators() = op;
  }
}

std::shared_ptr<::common::Expression> binaryExpression(
    const ::common::Expression& left, ::common::Logical logical,
    const ::common::Expression& right) {
  auto expression = std::make_shared<::common::Expression>();
  appendExpression(*expression, left);
  expression->add_operators()->set_logical(logical);
  appendExpression(*expression, right);
  return expression;
}

std::shared_ptr<::common::Expression> negated(
    const ::common::Expression& operand) {
  auto expression = std::make_shared<::common::Expression>();
  expression->add_operators()->set_logical(::common::Logical::NOT);
  expression->add_operators()->set_brace(::common::ExprOpr::LEFT_BRACE);
  appendExpression(*expression, operand);
  expression->add_operators()->set_brace(::common::ExprOpr::RIGHT_BRACE);
  return expression;
}

DataChunk nullableIntChunk() {
  ValueColumnBuilder<int32_t> builder;
  builder.push_back_opt(1);
  builder.push_back_null();
  builder.push_back_opt(-1);
  DataChunk chunk;
  chunk.set(0, builder.finish());
  return chunk;
}

DataChunk nestedChunk(int32_t first) {
  ValueColumnBuilder<int32_t> scalar_builder;
  scalar_builder.push_back_opt(first);
  scalar_builder.push_back_opt(first + 1);

  const auto int_type = DataType::INT32;
  ListColumnBuilder list_builder(int_type);
  list_builder.push_back_elem(Value::LIST(int_type, {Value::INT32(first)}));
  list_builder.push_back_null();

  const auto array_type = DataType::Array(int_type, 2);
  ContextArrayColumnBuilder array_builder(array_type);
  array_builder.push_back_elem(Value::ARRAY(
      array_type, {Value::INT32(first), Value::INT32(first + 10)}));
  array_builder.push_back_null();

  DataChunk chunk;
  chunk.set(0, scalar_builder.finish());
  chunk.set(1, list_builder.finish());
  chunk.set(2, array_builder.finish());
  return chunk;
}

DataChunk emptyNestedChunk() {
  ValueColumnBuilder<int32_t> scalar_builder;
  ListColumnBuilder list_builder(DataType::INT32);
  ContextArrayColumnBuilder array_builder(DataType::Array(DataType::INT32, 2));
  DataChunk chunk;
  chunk.set(0, scalar_builder.finish());
  chunk.set(1, list_builder.finish());
  chunk.set(2, array_builder.finish());
  return chunk;
}

}  // namespace

TEST_F(ReaderTest, TestReadAllChunksMergesNestedValuesAndArrayNulls) {
  auto first = std::make_shared<VectorChunkSupplier>(
      std::vector<DataChunk>{DataChunk(), emptyNestedChunk(), nestedChunk(1)});
  auto second = std::make_shared<VectorChunkSupplier>(
      std::vector<DataChunk>{nestedChunk(3)});

  auto merged = reader::read_all_chunks({first, second});
  ASSERT_EQ(merged.col_num(), 3);
  ASSERT_EQ(merged.row_num(), 4);
  EXPECT_EQ(merged.get(0)->get_elem(0).GetValue<int32_t>(), 1);
  EXPECT_EQ(merged.get(0)->get_elem(2).GetValue<int32_t>(), 3);

  const auto first_list = ListValue::GetChildren(merged.get(1)->get_elem(0));
  ASSERT_EQ(first_list.size(), 1);
  EXPECT_EQ(first_list[0].GetValue<int32_t>(), 1);
  EXPECT_TRUE(merged.get(1)->get_elem(1).IsNull());
  EXPECT_TRUE(merged.get(1)->get_elem(3).IsNull());

  const auto first_array = ArrayValue::GetChildren(merged.get(2)->get_elem(0));
  ASSERT_EQ(first_array.size(), 2);
  EXPECT_EQ(first_array[0].GetValue<int32_t>(), 1);
  EXPECT_EQ(first_array[1].GetValue<int32_t>(), 11);
  EXPECT_TRUE(merged.get(2)->get_elem(1).IsNull());
  EXPECT_TRUE(merged.get(2)->get_elem(3).IsNull());
}

TEST_F(ReaderTest, TestReadAllChunksRejectsIncompatibleChunks) {
  DataChunk one_column;
  ValueColumnBuilder<int32_t> int_builder;
  int_builder.push_back_opt(1);
  one_column.set(0, int_builder.finish());

  DataChunk two_columns;
  ValueColumnBuilder<int32_t> first_builder;
  ValueColumnBuilder<int32_t> second_builder;
  first_builder.push_back_opt(1);
  second_builder.push_back_opt(2);
  two_columns.set(0, first_builder.finish());
  two_columns.set(1, second_builder.finish());

  auto supplier = std::make_shared<VectorChunkSupplier>(
      std::vector<DataChunk>{one_column, two_columns});
  EXPECT_THROW(reader::read_all_chunks({supplier}),
               exception::InvalidArgumentException);
  EXPECT_THROW(reader::read_all_chunks({nullptr}),
               exception::InvalidArgumentException);
}

TEST_F(ReaderTest, TestCommonRowFilterUsesSqlNullSemantics) {
  auto chunk = nullableIntChunk();

  auto nonnegative = comparison("value", ::common::Logical::GE, 0);
  auto filtered = reader::filter_chunk(chunk, nonnegative, {"value"});
  ASSERT_EQ(filtered.row_num(), 1);
  EXPECT_EQ(filtered.get(0)->get_elem(0).GetValue<int32_t>(), 1);

  auto nulls = reader::filter_chunk(chunk, isNull("value"), {"value"});
  ASSERT_EQ(nulls.row_num(), 1);
  EXPECT_TRUE(nulls.get(0)->get_elem(0).IsNull());

  auto present =
      reader::filter_chunk(chunk, negated(*isNull("value")), {"value"});
  ASSERT_EQ(present.row_num(), 2);
  EXPECT_EQ(present.get(0)->get_elem(0).GetValue<int32_t>(), 1);
  EXPECT_EQ(present.get(0)->get_elem(1).GetValue<int32_t>(), -1);

  auto not_positive = reader::filter_chunk(
      chunk, negated(*comparison("value", ::common::Logical::GT, 0)),
      {"value"});
  ASSERT_EQ(not_positive.row_num(), 1);
  EXPECT_EQ(not_positive.get(0)->get_elem(0).GetValue<int32_t>(), -1);
}

TEST_F(ReaderTest, TestCommonRowFilterPreservesThreeValuedAndOr) {
  auto chunk = nullableIntChunk();
  auto positive = comparison("value", ::common::Logical::GT, 0);

  ::common::Expression true_constant;
  true_constant.add_operators()->mutable_const_()->set_boolean(true);
  ::common::Expression false_constant;
  false_constant.add_operators()->mutable_const_()->set_boolean(false);

  auto or_true =
      binaryExpression(*positive, ::common::Logical::OR, true_constant);
  EXPECT_EQ(reader::filter_chunk(chunk, or_true, {"value"}).row_num(), 3);

  auto and_false =
      binaryExpression(*positive, ::common::Logical::AND, false_constant);
  EXPECT_EQ(reader::filter_chunk(chunk, and_false, {"value"}).row_num(), 0);

  auto or_false =
      binaryExpression(*positive, ::common::Logical::OR, false_constant);
  EXPECT_EQ(reader::filter_chunk(chunk, or_false, {"value"}).row_num(), 1);

  auto and_true =
      binaryExpression(*positive, ::common::Logical::AND, true_constant);
  EXPECT_EQ(reader::filter_chunk(chunk, and_true, {"value"}).row_num(), 1);
}

TEST_F(ReaderTest, TestCommonRowFilterKeepsNanUnordered) {
  ValueColumnBuilder<double> values;
  values.push_back_opt(std::numeric_limits<double>::quiet_NaN());
  values.push_back_opt(0.0);
  values.push_back_null();
  DataChunk chunk;
  chunk.set(0, values.finish());
  for (auto op : {::common::Logical::LE, ::common::Logical::GE}) {
    auto predicate = comparison("value", op, 0);
    predicate->mutable_operators(2)->mutable_const_()->set_f64(0.0);
    auto filtered = reader::filter_chunk(chunk, predicate, {"value"});
    ASSERT_EQ(filtered.row_num(), 1);
    EXPECT_EQ(filtered.get(0)->get_elem(0).GetValue<double>(), 0.0);
  }
}

TEST_F(ReaderTest, TestCommonFilterThenProjectionUsesSharedHelpers) {
  auto chunk = nullableIntChunk();
  ValueColumnBuilder<std::string> names;
  names.push_back_opt("positive");
  names.push_back_opt("null");
  names.push_back_opt("negative");
  chunk.set(1, names.finish());

  auto filtered = reader::filter_chunk(
      chunk, comparison("value", ::common::Logical::GT, 0), {"value", "name"});
  auto projected = reader::project_chunk(filtered, {"value", "name"}, {"name"});
  ASSERT_EQ(projected.col_num(), 1);
  ASSERT_EQ(projected.row_num(), 1);
  EXPECT_EQ(projected.get(0)->get_elem(0).GetValue<std::string>(), "positive");
}

TEST_F(ReaderTest,
       TestCommonRowFilterValidatesBindingsWithoutMutatingPredicate) {
  auto chunk = nullableIntChunk();
  auto predicate = comparison("value", ::common::Logical::GT, 0);
  auto* parameter = predicate->mutable_operators(2)->mutable_param();
  parameter->set_name("minimum");
  parameter->mutable_data_type()->mutable_data_type()->set_primitive_type(
      ::common::PrimitiveType::DT_SIGNED_INT32);
  const auto original = predicate->SerializeAsString();
  EXPECT_THROW(reader::filter_chunk(chunk, predicate, {"value"}),
               exception::InvalidArgumentException);
  for (const int32_t minimum : {0, 2, -2}) {
    const auto filtered = reader::filter_chunk(
        chunk, predicate, {"value"}, {{"minimum", Value::INT32(minimum)}});
    EXPECT_EQ(filtered.row_num(), minimum == 0 ? 1 : (minimum == 2 ? 0 : 2));
    EXPECT_EQ(predicate->SerializeAsString(), original);
  }
  EXPECT_THROW(reader::filter_chunk(chunk, predicate, {"other"},
                                    {{"minimum", Value::INT32(0)}}),
               exception::InvalidArgumentException);
}

TEST_F(ReaderTest, TestCommonRowFilterRebindsNestedPredicateWithoutMutation) {
  // value IN [CASE WHEN value > 0 THEN value ELSE $fallback END]
  // Exercise both list and fixed-size array representations of the RHS.
  for (const bool as_array : {false, true}) {
    auto predicate = std::make_shared<::common::Expression>();
    predicate->add_operators()->mutable_var()->mutable_tag()->set_name("value");
    predicate->add_operators()->set_logical(::common::Logical::WITHIN);
    auto* container = predicate->add_operators();
    auto* fields = as_array ? container->mutable_to_array()->mutable_fields()
                            : container->mutable_to_list()->mutable_fields();
    if (as_array) {
      *container->mutable_node_type()->mutable_data_type() =
          *reader::NeuGTypeConverter().convert(
              DataType::Array(DataType::INT32, 1));
    }
    auto* cases = fields->Add()->add_operators()->mutable_case_();
    auto* when = cases->add_when_then_expressions();
    *when->mutable_when_expression() =
        *comparison("value", ::common::Logical::GT, 0);
    when->mutable_then_result_expression()
        ->add_operators()
        ->mutable_var()
        ->mutable_tag()
        ->set_name("value");
    auto* parameter = cases->mutable_else_result_expression()
                          ->add_operators()
                          ->mutable_param();
    parameter->set_name("fallback");
    parameter->mutable_data_type()->mutable_data_type()->set_primitive_type(
        ::common::DT_SIGNED_INT32);
    const auto original = predicate->SerializeAsString();

    auto values = nullableIntChunk();
    ValueColumnBuilder<int32_t> other;
    for (size_t row = 0; row < values.row_num(); ++row) {
      other.push_back_opt(99);
    }
    values.set(1, other.finish());
    for (const int value_index : {0, 1}) {
      SCOPED_TRACE(as_array ? "array" : "list");
      SCOPED_TRACE(value_index);
      DataChunk input;
      input.set(value_index, values.get(0));
      input.set(1 - value_index, values.get(1));
      std::vector<std::string> names{"other", "other"};
      names[value_index] = "value";
      EXPECT_THROW(reader::filter_chunk(input, predicate, names),
                   exception::InvalidArgumentException);
      EXPECT_EQ(predicate->SerializeAsString(), original);
      for (const int32_t fallback : {-1, 0}) {
        const auto filtered = reader::filter_chunk(
            input, predicate, names, {{"fallback", Value::INT32(fallback)}});
        ASSERT_EQ(filtered.row_num(), fallback == -1 ? 2 : 1);
        EXPECT_EQ(filtered.get(value_index)->get_elem(0).GetValue<int32_t>(),
                  1);
        if (fallback == -1) {
          EXPECT_EQ(filtered.get(value_index)->get_elem(1).GetValue<int32_t>(),
                    -1);
        }
        EXPECT_EQ(predicate->SerializeAsString(), original);
      }
    }
  }
}

TEST_F(ReaderTest, TestCommonRowFilterRetainsBoundInput) {
  const auto filter = [] {
    auto chunk = nullableIntChunk();
    reader::RowExpressionFilter filter(
        *comparison("value", ::common::Logical::GT, 0), {{"value", 0}}, chunk);
    // Reusing the source chunk must not change the filter's bound columns.
    ValueColumnBuilder<std::string> replacement;
    replacement.push_back_opt("different layout");
    chunk.clear();
    chunk.set(0, replacement.finish());
    return filter;
  }();

  // The source chunk and predicate have both gone out of scope.
  EXPECT_TRUE(filter.eval(0));
  EXPECT_FALSE(filter.eval(1));
  EXPECT_FALSE(filter.eval(2));
}

TEST_F(ReaderTest, TestCommonRowFilterHandlesAdjacentUnaryOperators) {
  const auto chunk = nullableIntChunk();
  auto present = std::make_shared<::common::Expression>();
  present->add_operators()->set_logical(::common::Logical::NOT);
  appendExpression(*present, *isNull("value"));
  auto filtered = reader::filter_chunk(chunk, present, {"value"});
  EXPECT_EQ(filtered.row_num(), 2);

  auto combined =
      binaryExpression(*present, ::common::Logical::AND,
                       *comparison("value", ::common::Logical::GT, 0));
  filtered = reader::filter_chunk(chunk, combined, {"value"});
  ASSERT_EQ(filtered.row_num(), 1);
  EXPECT_EQ(filtered.get(0)->get_elem(0).GetValue<int32_t>(), 1);
}

TEST_F(ReaderTest, TestMergeChunksValidatesEmptySchemasTypesAndLengths) {
  EXPECT_EQ(
      reader::merge_chunks({nullptr, std::make_shared<DataChunk>()}).col_num(),
      0);
  auto empty = std::make_shared<DataChunk>(emptyNestedChunk());
  auto merged = reader::merge_chunks({empty, empty});
  ASSERT_EQ(merged.col_num(), 3);
  EXPECT_EQ(merged.row_num(), 0);
  EXPECT_EQ(merged.get(2)->elem_type(), DataType::Array(DataType::INT32, 2));

  auto integers = std::make_shared<DataChunk>(nullableIntChunk());
  auto strings = std::make_shared<DataChunk>();
  ValueColumnBuilder<std::string> builder;
  builder.push_back_opt("value");
  strings->set(0, builder.finish());
  EXPECT_THROW(reader::merge_chunks({integers, strings}),
               exception::InvalidArgumentException);

  auto uneven = std::make_shared<DataChunk>(*integers);
  uneven->set(1, strings->get(0));
  EXPECT_THROW(reader::merge_chunks({uneven}),
               exception::InvalidArgumentException);
  auto missing = std::make_shared<DataChunk>();
  missing->set(1, integers->get(0));
  EXPECT_THROW(reader::merge_chunks({missing}),
               exception::InvalidArgumentException);
}

TEST_F(ReaderTest, TestCsvParallelOptionPropagation) {
  createCsvFile("parallel_options.csv", "id\n1\n");

  std::vector<std::string> columnNames = {"id"};
  std::vector<std::shared_ptr<::common::DataType>> columnTypes = {
      createInt64Type()};

  auto defaultState =
      createSharedState("parallel_options.csv", columnNames, columnTypes);
  reader::CsvOptionsBuilder defaultBuilder(defaultState);
  EXPECT_TRUE(defaultBuilder.build().use_threads);

  auto singleThreadedState =
      createSharedState("parallel_options.csv", columnNames, columnTypes,
                        {{"parallel", "false"}});
  reader::CsvOptionsBuilder singleThreadedBuilder(singleThreadedState);
  EXPECT_FALSE(singleThreadedBuilder.build().use_threads);

  const auto filePath =
      std::string(ARROW_READER_TEST_DIR) + "/parallel_options.csv";
  const std::vector<DataType> nativeColumnTypes = {
      DataType(DataTypeId::kInt64)};
  EXPECT_TRUE(
      build_csv_read_config(filePath, {}, nativeColumnTypes).use_threads);
  EXPECT_FALSE(build_csv_read_config(filePath,
                                     {{reader_options::PARALLEL, "false"}},
                                     nativeColumnTypes)
                   .use_threads);
  EXPECT_THROW(
      build_csv_read_config(filePath, {{reader_options::PARALLEL, "invalid"}},
                            nativeColumnTypes),
      exception::InvalidArgumentException);
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

// =============== Streaming (stream_opener) reads ===============

// Same setup as TestBasicCsvRead, but with a stream opener injected so
// the read goes through InputStream/IoStreamBuf instead of the local
// mmap path.
TEST_F(ReaderTest, TestCsvStreamingRead) {
  createCsvFile("stream1.csv",
                "id|name|score\n1|Alice|95.5\n2|Bob|87.0\n3|Charlie|92.5\n");

  std::vector<std::string> columnNames = {"id", "name", "score"};
  std::vector<std::shared_ptr<::common::DataType>> columnTypes = {
      createInt32Type(), createStringType(), createDoubleType()};

  auto sharedState =
      createSharedState("stream1.csv", columnNames, columnTypes,
                        {{"skip_rows", "1"}, {"batch_read", "false"}});
  sharedState->stream_opener = localStreamOpener();
  auto reader = createCsvReader(sharedState);

  auto localState = std::make_shared<reader::ReadLocalState>();
  execution::Context ctx;

  reader->read(localState, ctx);

  EXPECT_EQ(ctx.col_num(), 3);
  EXPECT_EQ(ctx.row_num(), 3);
}

// Streaming batch read: drives the supplier's streamed row counting and
// the istream-based CSV parser over multiple rows.
TEST_F(ReaderTest, TestCsvStreamingBatchRead) {
  std::string content = "id|name|score\n";
  for (int i = 0; i < 100; ++i) {
    content += std::to_string(i) + "|name" + std::to_string(i) + "|" +
               std::to_string(i) + ".5\n";
  }
  createCsvFile("stream_batch.csv", content);

  std::vector<std::string> columnNames = {"id", "name", "score"};
  std::vector<std::shared_ptr<::common::DataType>> columnTypes = {
      createInt32Type(), createStringType(), createDoubleType()};

  auto sharedState = createSharedState(
      "stream_batch.csv", columnNames, columnTypes,
      {{"skip_rows", "1"}, {"batch_read", "true"}, {"batch_size", "32"}});
  sharedState->stream_opener = localStreamOpener();
  auto reader = createCsvReader(sharedState);

  auto localState = std::make_shared<reader::ReadLocalState>();
  execution::Context ctx;

  reader->read(localState, ctx);

  EXPECT_EQ(ctx.col_num(), 3);
  EXPECT_EQ(count_batch_row_num(ctx), 100);
}

// Quoted fields through the streaming parser must parse identically to
// the local path.
TEST_F(ReaderTest, TestCsvStreamingReadWithQuoting) {
  createCsvFile("stream_quote.csv",
                "id,name,score\n1,'Alice,Smith',95.5\n2,\"Bob\",87.0\n");

  std::vector<std::string> columnNames = {"id", "name", "score"};
  std::vector<std::shared_ptr<::common::DataType>> columnTypes = {
      createInt32Type(), createStringType(), createDoubleType()};

  auto sharedState =
      createSharedState("stream_quote.csv", columnNames, columnTypes,
                        {{"quote", "'"},
                         {"delim", ","},
                         {"skip_rows", "1"},
                         {"batch_read", "false"}});
  sharedState->stream_opener = localStreamOpener();
  auto reader = createCsvReader(sharedState);

  auto localState = std::make_shared<reader::ReadLocalState>();
  execution::Context ctx;

  reader->read(localState, ctx);

  EXPECT_EQ(ctx.col_num(), 3);
  EXPECT_EQ(ctx.row_num(), 2);
}

// JSONL through the streaming path: counting pass and buffered line
// reads over an InputStream.
TEST_F(ReaderTest, TestJsonStreamingRead) {
  createJsonFile("test_json_stream.json",
                 "{\"id\":1,\"name\":\"Alice\",\"score\":95.5}\n"
                 "{\"id\":2,\"name\":\"Bob\",\"score\":87.0}\n"
                 "{\"id\":3,\"name\":\"Charlie\",\"score\":92.5}\n");

  std::vector<std::string> columnNames = {"id", "name", "score"};
  std::vector<std::shared_ptr<::common::DataType>> columnTypes = {
      createInt64Type(), createStringType(), createDoubleType()};

  auto sharedState =
      createJsonSharedState("test_json_stream.json", columnNames, columnTypes,
                            {{"batch_read", "false"}});
  sharedState->stream_opener = localStreamOpener();
  auto reader = createJsonReader(sharedState, false);

  auto localState = std::make_shared<reader::ReadLocalState>();
  execution::Context ctx;

  reader->read(localState, ctx);

  EXPECT_EQ(ctx.col_num(), 3);
  EXPECT_EQ(ctx.row_num(), 3);
}

// A header-only CSV has no data rows: full_read must return an empty
// result instead of failing the column-count validation ("Column number
// mismatch between schema and CSV data").
TEST_F(ReaderTest, TestCsvHeaderOnlyFileReturnsEmpty) {
  createCsvFile("header_only.csv", "id|name|score\n");

  std::vector<std::string> columnNames = {"id", "name", "score"};
  std::vector<std::shared_ptr<::common::DataType>> columnTypes = {
      createInt32Type(), createStringType(), createDoubleType()};

  auto sharedState =
      createSharedState("header_only.csv", columnNames, columnTypes,
                        {{"skip_rows", "1"}, {"batch_read", "false"}});
  auto reader = createCsvReader(sharedState);

  auto localState = std::make_shared<reader::ReadLocalState>();
  execution::Context ctx;

  EXPECT_NO_THROW(reader->read(localState, ctx));
  EXPECT_EQ(ctx.col_num(), 0);
  EXPECT_EQ(ctx.row_num(), 0);
}

}  // namespace test
}  // namespace neug
