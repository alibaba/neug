/**
 * Copyright 2020 Alibaba Group Holding Limited.
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

#include <gtest/gtest.h>
#include <arrow/api.h>
#include <arrow/filesystem/localfs.h>
#include <arrow/io/file.h>
#include <arrow/io/caching.h>
#include <parquet/arrow/writer.h>
#include <filesystem>
#include <memory>
#include <vector>

#include "neug/compiler/common/case_insensitive_map.h"
#include "neug/execution/common/columns/arrow_context_column.h"
#include "neug/execution/common/context.h"
#include "neug/generated/proto/plan/basic_type.pb.h"
#include "neug/utils/reader/options.h"
#include "neug/utils/reader/reader.h"
#include "neug/utils/reader/schema.h"

#include "../../extension/parquet/include/parquet_options.h"

namespace neug {
namespace test {

static constexpr const char* PARQUET_TEST_DIR = "/tmp/parquet_test";

class ParquetTest : public ::testing::Test {
 public:
  void SetUp() override {
    if (std::filesystem::exists(PARQUET_TEST_DIR)) {
      std::filesystem::remove_all(PARQUET_TEST_DIR);
    }
    std::filesystem::create_directories(PARQUET_TEST_DIR);
  }

  void TearDown() override {
    if (std::filesystem::exists(PARQUET_TEST_DIR)) {
      std::filesystem::remove_all(PARQUET_TEST_DIR);
    }
  }

  // Helper function to create a simple Parquet file
  void createSimpleParquetFile(const std::string& filename) {
    // Create Arrow schema
    auto schema = arrow::schema({
        arrow::field("id", arrow::int64()),
        arrow::field("name", arrow::utf8()),
        arrow::field("value", arrow::float64())
    });

    // Create data
    arrow::Int64Builder id_builder;
    arrow::StringBuilder name_builder;
    arrow::DoubleBuilder value_builder;

    ASSERT_TRUE(id_builder.Append(1).ok());
    ASSERT_TRUE(id_builder.Append(2).ok());
    ASSERT_TRUE(id_builder.Append(3).ok());

    ASSERT_TRUE(name_builder.Append("Alice").ok());
    ASSERT_TRUE(name_builder.Append("Bob").ok());
    ASSERT_TRUE(name_builder.Append("Charlie").ok());

    ASSERT_TRUE(value_builder.Append(10.5).ok());
    ASSERT_TRUE(value_builder.Append(20.3).ok());
    ASSERT_TRUE(value_builder.Append(30.7).ok());

    std::shared_ptr<arrow::Array> id_array;
    std::shared_ptr<arrow::Array> name_array;
    std::shared_ptr<arrow::Array> value_array;

    ASSERT_TRUE(id_builder.Finish(&id_array).ok());
    ASSERT_TRUE(name_builder.Finish(&name_array).ok());
    ASSERT_TRUE(value_builder.Finish(&value_array).ok());

    // Create table
    auto table = arrow::Table::Make(schema, {id_array, name_array, value_array});

    // Write to Parquet file
    std::string filepath = std::string(PARQUET_TEST_DIR) + "/" + filename;
    std::shared_ptr<arrow::io::FileOutputStream> outfile;
    PARQUET_ASSIGN_OR_THROW(outfile, arrow::io::FileOutputStream::Open(filepath));

    PARQUET_THROW_NOT_OK(
        parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), outfile, 3));
  }

  // Helper function to create DataType as shared_ptr
  std::shared_ptr<::common::DataType> createInt64Type() {
    auto type = std::make_shared<::common::DataType>();
    type->set_primitive_type(::common::PrimitiveType::DT_SIGNED_INT64);
    return type;
  }

  std::shared_ptr<::common::DataType> createStringType() {
    auto type = std::make_shared<::common::DataType>();
    auto strType = std::make_unique<::common::String>();
    auto varChar = std::make_unique<::common::String::VarChar>();
    strType->set_allocated_var_char(varChar.release());
    type->set_allocated_string(strType.release());
    return type;
  }

  std::shared_ptr<::common::DataType> createDoubleType() {
    auto type = std::make_shared<::common::DataType>();
    type->set_primitive_type(::common::PrimitiveType::DT_DOUBLE);
    return type;
  }

  std::shared_ptr<::common::DataType> createInt32Type() {
    auto type = std::make_shared<::common::DataType>();
    type->set_primitive_type(::common::PrimitiveType::DT_SIGNED_INT32);
    return type;
  }

  std::shared_ptr<::common::DataType> createBoolType() {
    auto type = std::make_shared<::common::DataType>();
    type->set_primitive_type(::common::PrimitiveType::DT_BOOL);
    return type;
  }

  // Helper function to create ReadSharedState
  std::shared_ptr<reader::ReadSharedState> createSharedState(
      const std::string& parquetFile,
      const std::vector<std::string>& columnNames,
      const std::vector<std::shared_ptr<::common::DataType>>& columnTypes,
      const common::case_insensitive_map_t<std::string>& options = {}) {
    auto sharedState = std::make_shared<reader::ReadSharedState>();

    auto entrySchema = std::make_shared<reader::TableEntrySchema>();
    entrySchema->columnNames = columnNames;
    entrySchema->columnTypes = columnTypes;

    // Create FileSchema
    reader::FileSchema fileSchema;
    fileSchema.paths = {std::string(PARQUET_TEST_DIR) + "/" + parquetFile};
    fileSchema.format = "parquet";
    fileSchema.options = options;

    // Create ExternalSchema
    reader::ExternalSchema externalSchema;
    externalSchema.entry = entrySchema;
    externalSchema.file = fileSchema;

    sharedState->schema = std::move(externalSchema);

    return sharedState;
  }

  std::shared_ptr<reader::ArrowReader> createParquetReader(
      const std::shared_ptr<reader::ReadSharedState>& sharedState) {
    auto fileSystem = std::make_shared<arrow::fs::LocalFileSystem>();
    auto optionsBuilder =
        std::make_unique<reader::ArrowParquetOptionsBuilder>(sharedState);
    return std::make_shared<reader::ArrowReader>(
        sharedState, std::move(optionsBuilder), std::move(fileSystem));
  }
};

// =============================================================================
// Test Suite 1: Options Translation Tests
// Verify that Neug options are correctly translated to Arrow Parquet configuration
// =============================================================================

TEST_F(ParquetTest, TestOptionsBuilder_BuildsValidParquetFragmentScanOptions) {
  createSimpleParquetFile("test_options.parquet");
  
  auto sharedState = createSharedState(
      "test_options.parquet", 
      {"id", "name", "value"},
      {createInt64Type(), createStringType(), createDoubleType()},
      {});
  
  reader::ArrowParquetOptionsBuilder optionsBuilder(sharedState);
  auto options = optionsBuilder.build();
  
  // Verify the builder creates ParquetFragmentScanOptions (not generic FragmentScanOptions)
  ASSERT_NE(options.scanOptions, nullptr);
  ASSERT_NE(options.scanOptions->fragment_scan_options, nullptr);
  
  auto parquetFragmentOpts = std::dynamic_pointer_cast<arrow::dataset::ParquetFragmentScanOptions>(
      options.scanOptions->fragment_scan_options);
  ASSERT_NE(parquetFragmentOpts, nullptr) 
      << "Extension should create ParquetFragmentScanOptions, not generic FragmentScanOptions";
  
  // Verify reader_properties and arrow_reader_properties are initialized
  EXPECT_NE(parquetFragmentOpts->reader_properties, nullptr)
      << "Extension should initialize reader_properties";
  EXPECT_NE(parquetFragmentOpts->arrow_reader_properties, nullptr)
      << "Extension should initialize arrow_reader_properties";
}

TEST_F(ParquetTest, TestOptionsTranslation_BufferSize) {
  createSimpleParquetFile("test_buffer.parquet");
  
  // Test custom buffer_size option
  const int64_t custom_buffer_size = 2048;
  auto sharedState = createSharedState(
      "test_buffer.parquet",
      {"id", "name", "value"},
      {createInt64Type(), createStringType(), createDoubleType()},
      {{"batch_size", std::to_string(custom_buffer_size)}});
  
  reader::ArrowParquetOptionsBuilder optionsBuilder(sharedState);
  auto options = optionsBuilder.build();
  
  auto parquetFragmentOpts = std::dynamic_pointer_cast<arrow::dataset::ParquetFragmentScanOptions>(
      options.scanOptions->fragment_scan_options);
  ASSERT_NE(parquetFragmentOpts, nullptr);
  ASSERT_NE(parquetFragmentOpts->reader_properties, nullptr);
  
  // Verify the Neug batch_size option is correctly translated to Arrow buffer_size
  EXPECT_EQ(parquetFragmentOpts->reader_properties->buffer_size(), custom_buffer_size)
      << "Extension should translate batch_size option to Arrow buffer_size";
}

TEST_F(ParquetTest, TestOptionsTranslation_ParquetBatchRows) {
  createSimpleParquetFile("test_batch_rows.parquet");
  
  // Test PARQUET_BATCH_ROWS option
  const int64_t custom_batch_rows = 4096;
  auto sharedState = createSharedState(
      "test_batch_rows.parquet",
      {"id", "name", "value"},
      {createInt64Type(), createStringType(), createDoubleType()},
      {{"PARQUET_BATCH_ROWS", std::to_string(custom_batch_rows)}});
  
  reader::ArrowParquetOptionsBuilder optionsBuilder(sharedState);
  auto options = optionsBuilder.build();
  
  auto parquetFragmentOpts = std::dynamic_pointer_cast<arrow::dataset::ParquetFragmentScanOptions>(
      options.scanOptions->fragment_scan_options);
  ASSERT_NE(parquetFragmentOpts, nullptr);
  ASSERT_NE(parquetFragmentOpts->arrow_reader_properties, nullptr);
  
  // Verify PARQUET_BATCH_ROWS is translated to Arrow batch_size
  EXPECT_EQ(parquetFragmentOpts->arrow_reader_properties->batch_size(), custom_batch_rows)
      << "Extension should translate PARQUET_BATCH_ROWS to Arrow batch_size";
}

TEST_F(ParquetTest, TestOptionsTranslation_PreBuffer) {
  createSimpleParquetFile("test_prebuffer.parquet");
  
  // Test PRE_BUFFER=true (default is false)
  auto sharedState = createSharedState(
      "test_prebuffer.parquet",
      {"id", "name", "value"},
      {createInt64Type(), createStringType(), createDoubleType()},
      {{"PRE_BUFFER", "true"}});
  
  reader::ArrowParquetOptionsBuilder optionsBuilder(sharedState);
  auto options = optionsBuilder.build();
  
  auto parquetFragmentOpts = std::dynamic_pointer_cast<arrow::dataset::ParquetFragmentScanOptions>(
      options.scanOptions->fragment_scan_options);
  ASSERT_NE(parquetFragmentOpts, nullptr);
  ASSERT_NE(parquetFragmentOpts->arrow_reader_properties, nullptr);
  
  // Verify PRE_BUFFER option is translated
  EXPECT_TRUE(parquetFragmentOpts->arrow_reader_properties->pre_buffer())
      << "Extension should translate PRE_BUFFER=true to Arrow pre_buffer setting";
}

TEST_F(ParquetTest, TestOptionsTranslation_UseThreads) {
  createSimpleParquetFile("test_threads.parquet");
  
  // Test parallel=false (use_threads)
  auto sharedState = createSharedState(
      "test_threads.parquet",
      {"id", "name", "value"},
      {createInt64Type(), createStringType(), createDoubleType()},
      {{"parallel", "false"}});
  
  reader::ArrowParquetOptionsBuilder optionsBuilder(sharedState);
  auto options = optionsBuilder.build();
  
  auto parquetFragmentOpts = std::dynamic_pointer_cast<arrow::dataset::ParquetFragmentScanOptions>(
      options.scanOptions->fragment_scan_options);
  ASSERT_NE(parquetFragmentOpts, nullptr);
  ASSERT_NE(parquetFragmentOpts->arrow_reader_properties, nullptr);
  
  // Verify parallel option is translated to use_threads
  EXPECT_FALSE(parquetFragmentOpts->arrow_reader_properties->use_threads())
      << "Extension should translate parallel=false to use_threads=false";
}

TEST_F(ParquetTest, TestOptionsTranslation_IoCoalescing) {
  createSimpleParquetFile("test_cache.parquet");
  
  // Test ENABLE_IO_COALESCING=true (default) — should use LazyDefaults (lazy=true)
  auto sharedState1 = createSharedState(
      "test_cache.parquet",
      {"id", "name", "value"},
      {createInt64Type(), createStringType(), createDoubleType()},
      {{"ENABLE_IO_COALESCING", "true"}});
  
  reader::ArrowParquetOptionsBuilder optionsBuilder1(sharedState1);
  auto options1 = optionsBuilder1.build();
  
  auto parquetFragmentOpts1 = std::dynamic_pointer_cast<arrow::dataset::ParquetFragmentScanOptions>(
      options1.scanOptions->fragment_scan_options);
  ASSERT_NE(parquetFragmentOpts1, nullptr);
  ASSERT_NE(parquetFragmentOpts1->arrow_reader_properties, nullptr);
  
  // Verify lazy coalescing is enabled when ENABLE_IO_COALESCING=true
  auto cache_opts1 = parquetFragmentOpts1->arrow_reader_properties->cache_options();
  EXPECT_TRUE(cache_opts1.lazy)
      << "Extension should use LazyDefaults (lazy=true) when ENABLE_IO_COALESCING=true";
  
  // Test ENABLE_IO_COALESCING=false — should use Defaults (lazy=false, eager coalescing)
  auto sharedState2 = createSharedState(
      "test_cache.parquet",
      {"id", "name", "value"},
      {createInt64Type(), createStringType(), createDoubleType()},
      {{"ENABLE_IO_COALESCING", "false"}});
  
  reader::ArrowParquetOptionsBuilder optionsBuilder2(sharedState2);
  auto options2 = optionsBuilder2.build();
  
  auto parquetFragmentOpts2 = std::dynamic_pointer_cast<arrow::dataset::ParquetFragmentScanOptions>(
      options2.scanOptions->fragment_scan_options);
  ASSERT_NE(parquetFragmentOpts2, nullptr);
  
  auto cache_opts2 = parquetFragmentOpts2->arrow_reader_properties->cache_options();
  EXPECT_FALSE(cache_opts2.lazy)
      << "Extension should use Defaults (lazy=false) when ENABLE_IO_COALESCING=false";
}

TEST_F(ParquetTest, TestOptionsTranslation_DefaultValues) {
  createSimpleParquetFile("test_defaults.parquet");
  
  // Create state without any options - should use defaults
  auto sharedState = createSharedState(
      "test_defaults.parquet",
      {"id", "name", "value"},
      {createInt64Type(), createStringType(), createDoubleType()},
      {});
  
  reader::ArrowParquetOptionsBuilder optionsBuilder(sharedState);
  auto options = optionsBuilder.build();
  
  auto parquetFragmentOpts = std::dynamic_pointer_cast<arrow::dataset::ParquetFragmentScanOptions>(
      options.scanOptions->fragment_scan_options);
  ASSERT_NE(parquetFragmentOpts, nullptr);
  ASSERT_NE(parquetFragmentOpts->arrow_reader_properties, nullptr);
  
  // Verify default values are applied
  // Default PARQUET_BATCH_ROWS = 65536
  EXPECT_EQ(parquetFragmentOpts->arrow_reader_properties->batch_size(), 65536)
      << "Extension should use default PARQUET_BATCH_ROWS=65536";
  
  // Default PRE_BUFFER = false
  EXPECT_FALSE(parquetFragmentOpts->arrow_reader_properties->pre_buffer())
      << "Extension should use default PRE_BUFFER=false";
  
  // Default parallel/use_threads = true
  EXPECT_TRUE(parquetFragmentOpts->arrow_reader_properties->use_threads())
      << "Extension should use default parallel=true";
}

TEST_F(ParquetTest, TestFileFormatConfiguration_SharesFragmentOptions) {
  createSimpleParquetFile("test_format.parquet");
  
  auto sharedState = createSharedState(
      "test_format.parquet", 
      {"id", "name", "value"},
      {createInt64Type(), createStringType(), createDoubleType()},
      {{"PARQUET_BATCH_ROWS", "2048"}});
  
  reader::ArrowParquetOptionsBuilder optionsBuilder(sharedState);
  auto options = optionsBuilder.build();
  
  // Verify FileFormat is ParquetFileFormat
  ASSERT_NE(options.fileFormat, nullptr);
  auto parquetFileFormat = std::dynamic_pointer_cast<arrow::dataset::ParquetFileFormat>(
      options.fileFormat);
  ASSERT_NE(parquetFileFormat, nullptr)
      << "Extension should create ParquetFileFormat";
  
  // Verify the FileFormat shares the same fragment_scan_options as ScanOptions
  // This ensures consistency in configuration
  EXPECT_EQ(parquetFileFormat->default_fragment_scan_options, 
            options.scanOptions->fragment_scan_options)
      << "Extension should set ParquetFileFormat's default_fragment_scan_options to match ScanOptions";
}

// =============================================================================
// Test Suite 2: Type Mapping Tests
// Verify type conversion between Neug DataType and Arrow types
// =============================================================================

TEST_F(ParquetTest, TestTypeMapping_StringToLargeUtf8) {
  createSimpleParquetFile("test_string_type.parquet");
  
  // Neug uses STRING type, Arrow Parquet may have utf8
  // Extension should convert to large_utf8 for consistency
  auto sharedState = createSharedState(
      "test_string_type.parquet", 
      {"id", "name", "value"},
      {createInt64Type(), createStringType(), createDoubleType()},
      {{"batch_read", "false"}});
  
  auto reader = createParquetReader(sharedState);
  auto localState = std::make_shared<reader::ReadLocalState>();
  execution::Context ctx;
  reader->read(localState, ctx);

  // Verify string column is converted to large_utf8
  auto col1 = ctx.columns[1];
  ASSERT_EQ(col1->column_type(), execution::ContextColumnType::kArrowArray);
  auto arrayColumn1 = std::dynamic_pointer_cast<execution::ArrowArrayContextColumn>(col1);
  auto arrowType1 = arrayColumn1->GetArrowType();
  
  EXPECT_TRUE(arrowType1->Equals(arrow::large_utf8()))
      << "Extension should convert Arrow utf8 to large_utf8 for Neug STRING type. "
      << "Got: " << arrowType1->ToString();
}

TEST_F(ParquetTest, TestTypeMapping_PreserveNumericTypes) {
  // Create Parquet file with various numeric types
  auto schema = arrow::schema({
      arrow::field("int32_col", arrow::int32()),
      arrow::field("int64_col", arrow::int64()),
      arrow::field("double_col", arrow::float64()),
      arrow::field("bool_col", arrow::boolean())
  });

  arrow::Int32Builder int32_builder;
  arrow::Int64Builder int64_builder;
  arrow::DoubleBuilder double_builder;
  arrow::BooleanBuilder bool_builder;

  ASSERT_TRUE(int32_builder.Append(42).ok());
  ASSERT_TRUE(int64_builder.Append(9223372036854775807LL).ok());
  ASSERT_TRUE(double_builder.Append(3.14159).ok());
  ASSERT_TRUE(bool_builder.Append(true).ok());

  std::shared_ptr<arrow::Array> arrays[4];
  ASSERT_TRUE(int32_builder.Finish(&arrays[0]).ok());
  ASSERT_TRUE(int64_builder.Finish(&arrays[1]).ok());
  ASSERT_TRUE(double_builder.Finish(&arrays[2]).ok());
  ASSERT_TRUE(bool_builder.Finish(&arrays[3]).ok());

  auto table = arrow::Table::Make(schema, {arrays[0], arrays[1], arrays[2], arrays[3]});

  std::string filepath = std::string(PARQUET_TEST_DIR) + "/test_numeric_types.parquet";
  std::shared_ptr<arrow::io::FileOutputStream> outfile;
  PARQUET_ASSIGN_OR_THROW(outfile, arrow::io::FileOutputStream::Open(filepath));
  PARQUET_THROW_NOT_OK(
      parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), outfile, 1));

  // Read with Neug types
  auto sharedState = createSharedState(
      "test_numeric_types.parquet",
      {"int32_col", "int64_col", "double_col", "bool_col"},
      {createInt32Type(), createInt64Type(), createDoubleType(), createBoolType()},
      {{"batch_read", "false"}});

  auto reader = createParquetReader(sharedState);
  auto localState = std::make_shared<reader::ReadLocalState>();
  execution::Context ctx;
  reader->read(localState, ctx);

  EXPECT_EQ(ctx.col_num(), 4);
  EXPECT_EQ(ctx.row_num(), 1);

  // Verify types are preserved correctly
  auto col0 = std::dynamic_pointer_cast<execution::ArrowArrayContextColumn>(ctx.columns[0]);
  EXPECT_TRUE(col0->GetArrowType()->Equals(arrow::int32()))
      << "Extension should preserve int32 type mapping";
  
  auto col1 = std::dynamic_pointer_cast<execution::ArrowArrayContextColumn>(ctx.columns[1]);
  EXPECT_TRUE(col1->GetArrowType()->Equals(arrow::int64()))
      << "Extension should preserve int64 type mapping";
  
  auto col2 = std::dynamic_pointer_cast<execution::ArrowArrayContextColumn>(ctx.columns[2]);
  EXPECT_TRUE(col2->GetArrowType()->Equals(arrow::float64()))
      << "Extension should preserve double type mapping";
  
  auto col3 = std::dynamic_pointer_cast<execution::ArrowArrayContextColumn>(ctx.columns[3]);
  EXPECT_TRUE(col3->GetArrowType()->Equals(arrow::boolean()))
      << "Extension should preserve boolean type mapping";
}

// =============================================================================
// Test Suite 3: Integration with Neug Query System
// Verify filter pushdown and column pruning work through the extension
// =============================================================================

TEST_F(ParquetTest, TestIntegration_ColumnPruning) {
  // Create Parquet file with 4 columns
  auto schema = arrow::schema({
      arrow::field("id", arrow::int32()),
      arrow::field("name", arrow::utf8()),
      arrow::field("score", arrow::float64()),
      arrow::field("grade", arrow::utf8())
  });

  arrow::Int32Builder id_builder;
  arrow::StringBuilder name_builder, grade_builder;
  arrow::DoubleBuilder score_builder;
  
  ASSERT_TRUE(id_builder.Append(1).ok());
  ASSERT_TRUE(name_builder.Append("Alice").ok());
  ASSERT_TRUE(score_builder.Append(95.5).ok());
  ASSERT_TRUE(grade_builder.Append("A").ok());

  std::shared_ptr<arrow::Array> id_array, name_array, score_array, grade_array;
  ASSERT_TRUE(id_builder.Finish(&id_array).ok());
  ASSERT_TRUE(name_builder.Finish(&name_array).ok());
  ASSERT_TRUE(score_builder.Finish(&score_array).ok());
  ASSERT_TRUE(grade_builder.Finish(&grade_array).ok());

  auto table = arrow::Table::Make(schema, {id_array, name_array, score_array, grade_array});
  
  std::string filepath = std::string(PARQUET_TEST_DIR) + "/test_pruning.parquet";
  std::shared_ptr<arrow::io::FileOutputStream> outfile;
  PARQUET_ASSIGN_OR_THROW(outfile, arrow::io::FileOutputStream::Open(filepath));
  PARQUET_THROW_NOT_OK(
      parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), outfile, 1));

  // Set up shared state with skipColumns
  auto sharedState = std::make_shared<reader::ReadSharedState>();
  auto entrySchema = std::make_shared<reader::TableEntrySchema>();
  entrySchema->columnNames = {"id", "name", "score", "grade"};
  entrySchema->columnTypes = {createInt32Type(), createStringType(), 
                               createDoubleType(), createStringType()};

  reader::FileSchema fileSchema;
  fileSchema.paths = {filepath};
  fileSchema.format = "parquet";
  fileSchema.options = {{"batch_read", "false"}};

  reader::ExternalSchema externalSchema;
  externalSchema.entry = entrySchema;
  externalSchema.file = fileSchema;
  sharedState->schema = std::move(externalSchema);
  
  // Neug's column pruning: skip "name" column
  sharedState->skipColumns = {"name"};

  auto reader = createParquetReader(sharedState);
  auto localState = std::make_shared<reader::ReadLocalState>();
  execution::Context ctx;
  reader->read(localState, ctx);

  // Verify extension translates skipColumns to Arrow projection
  // Should have 3 columns (id, score, grade - "name" is skipped)
  EXPECT_EQ(ctx.col_num(), 3)
      << "Extension should translate Neug's skipColumns to Arrow column projection";
  EXPECT_EQ(sharedState->columnNum(), 3)
      << "Extension should update columnNum after pruning";
}

TEST_F(ParquetTest, TestIntegration_FilterPushdown) {
  // Create Parquet file with test data
  auto schema = arrow::schema({
      arrow::field("id", arrow::int32()),
      arrow::field("score", arrow::float64())
  });

  arrow::Int32Builder id_builder;
  arrow::DoubleBuilder score_builder;
  
  std::vector<std::pair<int32_t, double>> test_data = {
      {1, 95.5},
      {2, 87.0},
      {3, 92.5},
      {4, 78.0},
      {5, 98.0}
  };
  
  for (const auto& [id, score] : test_data) {
    ASSERT_TRUE(id_builder.Append(id).ok());
    ASSERT_TRUE(score_builder.Append(score).ok());
  }

  std::shared_ptr<arrow::Array> id_array, score_array;
  ASSERT_TRUE(id_builder.Finish(&id_array).ok());
  ASSERT_TRUE(score_builder.Finish(&score_array).ok());

  auto table = arrow::Table::Make(schema, {id_array, score_array});
  
  std::string filepath = std::string(PARQUET_TEST_DIR) + "/test_filter.parquet";
  std::shared_ptr<arrow::io::FileOutputStream> outfile;
  PARQUET_ASSIGN_OR_THROW(outfile, arrow::io::FileOutputStream::Open(filepath));
  PARQUET_THROW_NOT_OK(
      parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), outfile, 5));

  // Create Neug filter expression: score > 90.0
  auto filterExpr = std::make_shared<::common::Expression>();
  
  auto var_opr = filterExpr->add_operators();
  auto var = var_opr->mutable_var();
  var->mutable_tag()->set_name("score");
  
  auto gt_opr = filterExpr->add_operators();
  gt_opr->set_logical(::common::Logical::GT);
  
  auto const_opr = filterExpr->add_operators();
  const_opr->mutable_const_()->set_f64(90.0);

  // Set up shared state with filter
  auto sharedState = std::make_shared<reader::ReadSharedState>();
  auto entrySchema = std::make_shared<reader::TableEntrySchema>();
  entrySchema->columnNames = {"id", "score"};
  entrySchema->columnTypes = {createInt32Type(), createDoubleType()};

  reader::FileSchema fileSchema;
  fileSchema.paths = {filepath};
  fileSchema.format = "parquet";
  fileSchema.options = {{"batch_read", "false"}};

  reader::ExternalSchema externalSchema;
  externalSchema.entry = entrySchema;
  externalSchema.file = fileSchema;
  sharedState->schema = std::move(externalSchema);
  sharedState->skipRows = filterExpr;  // Neug's filter expression

  auto reader = createParquetReader(sharedState);
  auto localState = std::make_shared<reader::ReadLocalState>();
  execution::Context ctx;
  reader->read(localState, ctx);

  // Verify extension translates Neug filter to Arrow filter
  EXPECT_EQ(ctx.col_num(), 2);
  EXPECT_EQ(ctx.row_num(), 3)
      << "Extension should translate Neug's skipRows filter to Arrow filter pushdown. "
      << "Should filter to 3 rows with score > 90.0";
  
  // Verify the filtered data
  auto col1 = std::dynamic_pointer_cast<execution::ArrowArrayContextColumn>(ctx.columns[1]);
  ASSERT_NE(col1, nullptr);
  const auto& columns = col1->GetColumns();
  ASSERT_FALSE(columns.empty());
  auto scoreArray = std::static_pointer_cast<arrow::DoubleArray>(columns[0]);
  
  // All scores should be > 90.0
  for (int64_t i = 0; i < scoreArray->length(); ++i) {
    EXPECT_GT(scoreArray->Value(i), 90.0)
        << "Extension's filter translation should result in all scores > 90.0";
  }
}

TEST_F(ParquetTest, TestIntegration_BatchReadMode) {
  createSimpleParquetFile("test_batch_mode.parquet");
  
  // Test with batch_read=true (streaming mode)
  auto sharedState = createSharedState(
      "test_batch_mode.parquet",
      {"id", "name", "value"},
      {createInt64Type(), createStringType(), createDoubleType()},
      {{"batch_read", "true"}});

  auto reader = createParquetReader(sharedState);
  auto localState = std::make_shared<reader::ReadLocalState>();
  execution::Context ctx;
  reader->read(localState, ctx);

  EXPECT_EQ(ctx.col_num(), 3);
  // Verify extension translates batch_read option to streaming column type
  auto col0 = ctx.columns[0];
  EXPECT_EQ(col0->column_type(), execution::ContextColumnType::kArrowStream)
      << "Extension should use ArrowStream column type when batch_read=true";
  
  // Test with batch_read=false (full read mode)
  auto sharedState2 = createSharedState(
      "test_batch_mode.parquet",
      {"id", "name", "value"},
      {createInt64Type(), createStringType(), createDoubleType()},
      {{"batch_read", "false"}});

  auto reader2 = createParquetReader(sharedState2);
  auto localState2 = std::make_shared<reader::ReadLocalState>();
  execution::Context ctx2;
  reader2->read(localState2, ctx2);

  auto col0_2 = ctx2.columns[0];
  EXPECT_EQ(col0_2->column_type(), execution::ContextColumnType::kArrowArray)
      << "Extension should use ArrowArray column type when batch_read=false";
}

TEST_F(ParquetTest, TestIntegration_CombinedFilterAndProjection) {
  // Create Parquet file
  auto schema = arrow::schema({
      arrow::field("id", arrow::int32()),
      arrow::field("name", arrow::utf8()),
      arrow::field("score", arrow::float64()),
      arrow::field("grade", arrow::utf8())
  });

  arrow::Int32Builder id_builder;
  arrow::StringBuilder name_builder, grade_builder;
  arrow::DoubleBuilder score_builder;
  
  std::vector<std::tuple<int32_t, std::string, double, std::string>> test_data = {
      {1, "Alice", 95.5, "A"},
      {2, "Bob", 87.0, "B"},
      {3, "Charlie", 92.5, "A"},
      {4, "David", 78.0, "C"}
  };
  
  for (const auto& [id, name, score, grade] : test_data) {
    ASSERT_TRUE(id_builder.Append(id).ok());
    ASSERT_TRUE(name_builder.Append(name).ok());
    ASSERT_TRUE(score_builder.Append(score).ok());
    ASSERT_TRUE(grade_builder.Append(grade).ok());
  }

  std::shared_ptr<arrow::Array> id_array, name_array, score_array, grade_array;
  ASSERT_TRUE(id_builder.Finish(&id_array).ok());
  ASSERT_TRUE(name_builder.Finish(&name_array).ok());
  ASSERT_TRUE(score_builder.Finish(&score_array).ok());
  ASSERT_TRUE(grade_builder.Finish(&grade_array).ok());

  auto table = arrow::Table::Make(schema, {id_array, name_array, score_array, grade_array});
  
  std::string filepath = std::string(PARQUET_TEST_DIR) + "/test_combined.parquet";
  std::shared_ptr<arrow::io::FileOutputStream> outfile;
  PARQUET_ASSIGN_OR_THROW(outfile, arrow::io::FileOutputStream::Open(filepath));
  PARQUET_THROW_NOT_OK(
      parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), outfile, 4));

  // Create Neug filter: score > 90.0
  auto filterExpr = std::make_shared<::common::Expression>();
  auto var_opr = filterExpr->add_operators();
  var_opr->mutable_var()->mutable_tag()->set_name("score");
  auto gt_opr = filterExpr->add_operators();
  gt_opr->set_logical(::common::Logical::GT);
  auto const_opr = filterExpr->add_operators();
  const_opr->mutable_const_()->set_f64(90.0);

  // Set up shared state with both filter and column pruning
  auto sharedState = std::make_shared<reader::ReadSharedState>();
  auto entrySchema = std::make_shared<reader::TableEntrySchema>();
  entrySchema->columnNames = {"id", "name", "score", "grade"};
  entrySchema->columnTypes = {createInt32Type(), createStringType(), 
                               createDoubleType(), createStringType()};

  reader::FileSchema fileSchema;
  fileSchema.paths = {filepath};
  fileSchema.format = "parquet";
  fileSchema.options = {{"batch_read", "false"}};

  reader::ExternalSchema externalSchema;
  externalSchema.entry = entrySchema;
  externalSchema.file = fileSchema;
  sharedState->schema = std::move(externalSchema);
  sharedState->skipColumns = {"name"};  // Prune "name" column
  sharedState->skipRows = filterExpr;   // Filter score > 90.0

  auto reader = createParquetReader(sharedState);
  auto localState = std::make_shared<reader::ReadLocalState>();
  execution::Context ctx;
  reader->read(localState, ctx);

  // Verify extension correctly combines filter and projection
  EXPECT_EQ(ctx.col_num(), 3)
      << "Extension should apply column pruning (3 of 4 columns)";
  EXPECT_EQ(ctx.row_num(), 2)
      << "Extension should apply filter (2 rows with score > 90.0)";
  EXPECT_EQ(sharedState->columnNum(), 3)
      << "Extension should update columnNum after pruning";
}

// =============================================================================
// Test Suite 4: Multi-file Handling
// Verify extension correctly handles multiple Parquet files
// =============================================================================

TEST_F(ParquetTest, TestMultiFile_ExplicitPaths) {
  // Create multiple Parquet files
  for (int fileIdx = 0; fileIdx < 3; ++fileIdx) {
    auto schema = arrow::schema({arrow::field("id", arrow::int32())});
    arrow::Int32Builder builder;
    
    for (int i = 0; i < 10; ++i) {
      ASSERT_TRUE(builder.Append(fileIdx * 10 + i).ok());
    }
    
    std::shared_ptr<arrow::Array> array;
    ASSERT_TRUE(builder.Finish(&array).ok());
    auto table = arrow::Table::Make(schema, {array});
    
    std::string filepath = std::string(PARQUET_TEST_DIR) + "/test_multi_" + 
                          std::to_string(fileIdx) + ".parquet";
    std::shared_ptr<arrow::io::FileOutputStream> outfile;
    PARQUET_ASSIGN_OR_THROW(outfile, arrow::io::FileOutputStream::Open(filepath));
    PARQUET_THROW_NOT_OK(
        parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), outfile, 10));
  }

  // Extension should handle multiple explicit file paths
  auto sharedState = std::make_shared<reader::ReadSharedState>();
  auto entrySchema = std::make_shared<reader::TableEntrySchema>();
  entrySchema->columnNames = {"id"};
  entrySchema->columnTypes = {createInt32Type()};

  reader::FileSchema fileSchema;
  fileSchema.paths = {
      std::string(PARQUET_TEST_DIR) + "/test_multi_0.parquet",
      std::string(PARQUET_TEST_DIR) + "/test_multi_1.parquet",
      std::string(PARQUET_TEST_DIR) + "/test_multi_2.parquet"
  };
  fileSchema.format = "parquet";
  fileSchema.options = {{"batch_read", "false"}};

  reader::ExternalSchema externalSchema;
  externalSchema.entry = entrySchema;
  externalSchema.file = fileSchema;
  sharedState->schema = std::move(externalSchema);

  auto reader = createParquetReader(sharedState);
  auto localState = std::make_shared<reader::ReadLocalState>();
  execution::Context ctx;
  reader->read(localState, ctx);

  EXPECT_EQ(ctx.col_num(), 1);
  EXPECT_EQ(ctx.row_num(), 30)
      << "Extension should correctly read and concatenate multiple Parquet files";
}

// End of Test Suites

}  // namespace test
}  // namespace neug
