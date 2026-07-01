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

#include <filesystem>
#include <fstream>
#include <memory>
#include <string>
#include <vector>

#include "neug/compiler/common/case_insensitive_map.h"
#include "neug/execution/common/columns/value_columns.h"
#include "neug/execution/common/context.h"
#include "neug/generated/proto/plan/basic_type.pb.h"
#include "neug/utils/exception/exception.h"
#include "neug/utils/io/read/common/options.h"
#include "neug/utils/io/read/common/reader_utils.h"
#include "neug/utils/io/read/common/schema.h"
#include "neug/utils/io/read/json/json_reader.h"

namespace neug {
namespace test {

static constexpr const char* JSON_IO_TEST_DIR = "/tmp/json_io_test";

class JsonIOTest : public ::testing::Test {
 public:
  void SetUp() override {
    if (std::filesystem::exists(JSON_IO_TEST_DIR)) {
      std::filesystem::remove_all(JSON_IO_TEST_DIR);
    }
    std::filesystem::create_directories(JSON_IO_TEST_DIR);
  }

  void TearDown() override {
    if (std::filesystem::exists(JSON_IO_TEST_DIR)) {
      std::filesystem::remove_all(JSON_IO_TEST_DIR);
    }
  }

  void createFile(const std::string& filename, const std::string& content) {
    std::ofstream file(std::string(JSON_IO_TEST_DIR) + "/" + filename);
    file << content;
    file.close();
  }

  std::string filePath(const std::string& filename) {
    return std::string(JSON_IO_TEST_DIR) + "/" + filename;
  }

  std::shared_ptr<::common::DataType> createInt32Type() {
    auto type = std::make_shared<::common::DataType>();
    type->set_primitive_type(::common::PrimitiveType::DT_SIGNED_INT32);
    return type;
  }

  std::shared_ptr<::common::DataType> createInt64Type() {
    auto type = std::make_shared<::common::DataType>();
    type->set_primitive_type(::common::PrimitiveType::DT_SIGNED_INT64);
    return type;
  }

  std::shared_ptr<::common::DataType> createUInt32Type() {
    auto type = std::make_shared<::common::DataType>();
    type->set_primitive_type(::common::PrimitiveType::DT_UNSIGNED_INT32);
    return type;
  }

  std::shared_ptr<::common::DataType> createDoubleType() {
    auto type = std::make_shared<::common::DataType>();
    type->set_primitive_type(::common::PrimitiveType::DT_DOUBLE);
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

  std::shared_ptr<::common::DataType> createBoolType() {
    auto type = std::make_shared<::common::DataType>();
    type->set_primitive_type(::common::PrimitiveType::DT_BOOL);
    return type;
  }

  std::shared_ptr<reader::ReadSharedState> createSharedState(
      const std::string& jsonFile, const std::vector<std::string>& columnNames,
      const std::vector<std::shared_ptr<::common::DataType>>& columnTypes,
      const common::case_insensitive_map_t<std::string>& options = {},
      const std::vector<std::string>& projectColumns = {}) {
    auto sharedState = std::make_shared<reader::ReadSharedState>();
    auto entrySchema = std::make_shared<reader::TableEntrySchema>();
    entrySchema->columnNames = columnNames;
    entrySchema->columnTypes = columnTypes;

    reader::FileSchema fileSchema;
    fileSchema.paths = {filePath(jsonFile)};
    fileSchema.format = "json";
    fileSchema.options = options;

    reader::ExternalSchema externalSchema;
    externalSchema.entry = entrySchema;
    externalSchema.file = fileSchema;

    sharedState->schema = std::move(externalSchema);
    sharedState->projectColumns = projectColumns;
    return sharedState;
  }

  std::shared_ptr<reader::JsonReader> createJsonReader(
      const std::shared_ptr<reader::ReadSharedState>& sharedState,
      bool json_array_input = false) {
    auto optionsBuilder = std::make_unique<reader::JsonOptionsBuilder>(
        sharedState, json_array_input);
    return std::make_shared<reader::JsonReader>(sharedState,
                                                std::move(optionsBuilder));
  }

  execution::Context readToContext(
      const std::shared_ptr<reader::JsonReader>& reader,
      const std::shared_ptr<reader::ReadSharedState>& sharedState) {
    return reader::toContext(reader->read(), *sharedState);
  }
};

// =============================================================================
// JSON Lines Format - Basic Reading
// =============================================================================

TEST_F(JsonIOTest, JsonLines_BasicRead) {
  createFile("basic.jsonl",
             "{\"id\":1,\"name\":\"Alice\",\"score\":95.5}\n"
             "{\"id\":2,\"name\":\"Bob\",\"score\":87.0}\n"
             "{\"id\":3,\"name\":\"Charlie\",\"score\":92.5}\n");
  auto state = createSharedState(
      "basic.jsonl", {"id", "name", "score"},
      {createInt64Type(), createStringType(), createDoubleType()},
      {{"batch_read", "false"}});
  auto reader = createJsonReader(state);
  auto ctx = readToContext(reader, state);
  EXPECT_EQ(ctx.col_num(), 3);
  EXPECT_EQ(ctx.row_num(), 3);
  EXPECT_EQ(ctx.chunk(0).columns()[0]->get_elem(0).GetValue<int64_t>(), 1);
  EXPECT_EQ(ctx.chunk(0).columns()[1]->get_elem(0).GetValue<std::string>(),
            "Alice");
  EXPECT_DOUBLE_EQ(ctx.chunk(0).columns()[2]->get_elem(0).GetValue<double>(),
                   95.5);
}

TEST_F(JsonIOTest, JsonLines_SingleRow) {
  createFile("single.jsonl", "{\"x\":42,\"y\":\"hello\"}\n");
  auto state = createSharedState("single.jsonl", {"x", "y"},
                                 {createInt32Type(), createStringType()},
                                 {{"batch_read", "false"}});
  auto reader = createJsonReader(state);
  auto ctx = readToContext(reader, state);
  EXPECT_EQ(ctx.row_num(), 1);
  EXPECT_EQ(ctx.chunk(0).columns()[0]->get_elem(0).GetValue<int32_t>(), 42);
}

TEST_F(JsonIOTest, JsonLines_EmptyLines) {
  // Empty lines between valid JSON lines should be skipped
  createFile("emptylines.jsonl",
             "{\"id\":1}\n"
             "\n"
             "{\"id\":2}\n"
             "\n");
  auto state =
      createSharedState("emptylines.jsonl", {"id"}, {createInt64Type()},
                        {{"batch_read", "false"}});
  auto reader = createJsonReader(state);
  auto ctx = readToContext(reader, state);
  EXPECT_EQ(ctx.row_num(), 2);
}

// =============================================================================
// JSON Array Format
// =============================================================================

TEST_F(JsonIOTest, JsonArray_BasicRead) {
  createFile("array.json",
             "[{\"id\":1,\"name\":\"Alice\"},{\"id\":2,\"name\":\"Bob\"}]");
  auto state = createSharedState("array.json", {"id", "name"},
                                 {createUInt32Type(), createStringType()},
                                 {{"batch_read", "false"}});
  auto reader = createJsonReader(state, true);
  auto ctx = readToContext(reader, state);
  EXPECT_EQ(ctx.col_num(), 2);
  EXPECT_EQ(ctx.row_num(), 2);
  EXPECT_EQ(ctx.chunk(0).columns()[0]->get_elem(0).GetValue<uint32_t>(), 1u);
  EXPECT_EQ(ctx.chunk(0).columns()[1]->get_elem(1).GetValue<std::string>(),
            "Bob");
}

TEST_F(JsonIOTest, JsonArray_LargeArray) {
  std::string content = "[";
  for (int i = 0; i < 500; ++i) {
    if (i > 0)
      content += ",";
    content += "{\"id\":" + std::to_string(i) +
               ",\"val\":" + std::to_string(i * 1.5) + "}";
  }
  content += "]";
  createFile("large_array.json", content);
  auto state = createSharedState("large_array.json", {"id", "val"},
                                 {createInt64Type(), createDoubleType()},
                                 {{"batch_read", "false"}});
  auto reader = createJsonReader(state, true);
  auto ctx = readToContext(reader, state);
  EXPECT_EQ(ctx.row_num(), 500);
}

// =============================================================================
// JSON Type Handling
// =============================================================================

TEST_F(JsonIOTest, Types_IntegerValues) {
  createFile("int.jsonl", "{\"i32\":42,\"i64\":9876543210}\n");
  auto state = createSharedState("int.jsonl", {"i32", "i64"},
                                 {createInt32Type(), createInt64Type()},
                                 {{"batch_read", "false"}});
  auto reader = createJsonReader(state);
  auto ctx = readToContext(reader, state);
  EXPECT_EQ(ctx.chunk(0).columns()[0]->get_elem(0).GetValue<int32_t>(), 42);
  EXPECT_EQ(ctx.chunk(0).columns()[1]->get_elem(0).GetValue<int64_t>(),
            9876543210LL);
}

TEST_F(JsonIOTest, Types_DoubleValues) {
  createFile("dbl.jsonl", "{\"pi\":3.14159265,\"e\":2.71828183}\n");
  auto state = createSharedState("dbl.jsonl", {"pi", "e"},
                                 {createDoubleType(), createDoubleType()},
                                 {{"batch_read", "false"}});
  auto reader = createJsonReader(state);
  auto ctx = readToContext(reader, state);
  EXPECT_NEAR(ctx.chunk(0).columns()[0]->get_elem(0).GetValue<double>(),
              3.14159265, 1e-8);
  EXPECT_NEAR(ctx.chunk(0).columns()[1]->get_elem(0).GetValue<double>(),
              2.71828183, 1e-8);
}

TEST_F(JsonIOTest, Types_BoolValues) {
  createFile("bool.jsonl",
             "{\"a\":true,\"b\":false}\n"
             "{\"a\":false,\"b\":true}\n");
  auto state = createSharedState("bool.jsonl", {"a", "b"},
                                 {createBoolType(), createBoolType()},
                                 {{"batch_read", "false"}});
  auto reader = createJsonReader(state);
  auto ctx = readToContext(reader, state);
  EXPECT_EQ(ctx.row_num(), 2);
  EXPECT_EQ(ctx.chunk(0).columns()[0]->get_elem(0).GetValue<bool>(), true);
  EXPECT_EQ(ctx.chunk(0).columns()[0]->get_elem(1).GetValue<bool>(), false);
}

TEST_F(JsonIOTest, Types_StringValues) {
  createFile("str.jsonl",
             "{\"name\":\"Alice\"}\n"
             "{\"name\":\"Bob with \\\"quotes\\\"\"}\n");
  auto state = createSharedState("str.jsonl", {"name"}, {createStringType()},
                                 {{"batch_read", "false"}});
  auto reader = createJsonReader(state);
  auto ctx = readToContext(reader, state);
  EXPECT_EQ(ctx.row_num(), 2);
  EXPECT_EQ(ctx.chunk(0).columns()[0]->get_elem(0).GetValue<std::string>(),
            "Alice");
  EXPECT_EQ(ctx.chunk(0).columns()[0]->get_elem(1).GetValue<std::string>(),
            "Bob with \"quotes\"");
}

TEST_F(JsonIOTest, Types_UnicodeStrings) {
  createFile("unicode.jsonl",
             "{\"msg\":\"你好世界\"}\n"
             "{\"msg\":\"café\"}\n");
  auto state = createSharedState("unicode.jsonl", {"msg"}, {createStringType()},
                                 {{"batch_read", "false"}});
  auto reader = createJsonReader(state);
  auto ctx = readToContext(reader, state);
  EXPECT_EQ(ctx.row_num(), 2);
  EXPECT_EQ(ctx.chunk(0).columns()[0]->get_elem(0).GetValue<std::string>(),
            "你好世界");
  EXPECT_EQ(ctx.chunk(0).columns()[0]->get_elem(1).GetValue<std::string>(),
            "café");
}

TEST_F(JsonIOTest, Types_NestedObjectAsString) {
  // Nested objects should be stringified
  createFile("nested.jsonl", "{\"id\":1,\"data\":{\"x\":10,\"y\":20}}\n");
  auto state = createSharedState("nested.jsonl", {"id", "data"},
                                 {createInt64Type(), createStringType()},
                                 {{"batch_read", "false"}});
  auto reader = createJsonReader(state);
  auto ctx = readToContext(reader, state);
  EXPECT_EQ(ctx.row_num(), 1);
  // Nested object should be stored as JSON string
  auto data_str =
      ctx.chunk(0).columns()[1]->get_elem(0).GetValue<std::string>();
  EXPECT_NE(data_str.find("\"x\""), std::string::npos);
  EXPECT_NE(data_str.find("10"), std::string::npos);
}

TEST_F(JsonIOTest, Types_NullHandling) {
  createFile("null.jsonl",
             "{\"id\":1,\"name\":null}\n"
             "{\"id\":2,\"name\":\"Bob\"}\n");
  auto state = createSharedState("null.jsonl", {"id", "name"},
                                 {createInt64Type(), createStringType()},
                                 {{"batch_read", "false"}});
  auto reader = createJsonReader(state);
  auto ctx = readToContext(reader, state);
  EXPECT_EQ(ctx.row_num(), 2);
  // First row: name is null
  auto name_col = ctx.chunk(0).columns()[1];
  EXPECT_TRUE(name_col->get_elem(0).IsNull());
  EXPECT_EQ(name_col->get_elem(1).GetValue<std::string>(), "Bob");
}

// =============================================================================
// JSON Error Handling
// =============================================================================

TEST_F(JsonIOTest, Error_InvalidJson) {
  createFile("invalid.jsonl", "not valid json\n");
  auto state = createSharedState("invalid.jsonl", {"id"}, {createInt64Type()},
                                 {{"batch_read", "false"}});
  auto reader = createJsonReader(state);
  EXPECT_THROW(readToContext(reader, state), exception::IOException);
}

TEST_F(JsonIOTest, Error_MissingColumn) {
  createFile("missing.jsonl", "{\"id\":1,\"name\":\"Alice\"}\n");
  auto state = createSharedState(
      "missing.jsonl", {"id", "name", "nonexistent"},
      {createInt64Type(), createStringType(), createStringType()},
      {{"batch_read", "false"}});
  auto reader = createJsonReader(state);
  EXPECT_THROW(readToContext(reader, state),
               exception::SchemaMismatchException);
}

TEST_F(JsonIOTest, Error_InvalidJsonArray) {
  createFile("bad_array.json", "[1, 2, 3]");  // Array of non-objects
  auto state = createSharedState("bad_array.json", {"val"}, {createInt64Type()},
                                 {{"batch_read", "false"}});
  auto reader = createJsonReader(state, true);
  EXPECT_THROW(readToContext(reader, state), exception::IOException);
}

// =============================================================================
// JSON Batch Mode
// =============================================================================

TEST_F(JsonIOTest, BatchMode_MultiChunk) {
  std::string content;
  for (int i = 0; i < 200; ++i) {
    content += "{\"id\":" + std::to_string(i) +
               ",\"val\":" + std::to_string(i * 2.0) + "}\n";
  }
  createFile("batch.jsonl", content);
  auto state = createSharedState("batch.jsonl", {"id", "val"},
                                 {createInt64Type(), createDoubleType()},
                                 {{"batch_read", "true"}});
  auto reader = createJsonReader(state);
  auto supplier = reader->read();
  ASSERT_NE(supplier, nullptr);
  int total_rows = 0;
  int chunk_count = 0;
  while (auto chunk = supplier->GetNextChunk()) {
    total_rows += static_cast<int>(chunk->row_num());
    chunk_count++;
  }
  EXPECT_EQ(total_rows, 200);
  // With default chunk_size=4096, all 200 rows fit in one chunk
  EXPECT_GE(chunk_count, 1);
}

// =============================================================================
// JSON Schema Inference
// =============================================================================

TEST_F(JsonIOTest, InferSchema_IntegerColumn) {
  createFile("infer_int.jsonl",
             "{\"id\":1,\"name\":\"a\"}\n"
             "{\"id\":2,\"name\":\"b\"}\n");
  auto state = createSharedState("infer_int.jsonl", {"id", "name"},
                                 {createStringType(), createStringType()},
                                 {{"batch_read", "false"}});
  auto reader = createJsonReader(state);
  auto result = reader->inferSchema();
  ASSERT_TRUE(result.has_value());
  auto schema = result.value();
  ASSERT_EQ(schema->columnNames.size(), 2u);
  EXPECT_EQ(schema->columnNames[0], "id");
  EXPECT_EQ(schema->columnNames[1], "name");
  // id should be inferred as integer
  EXPECT_EQ(schema->columnTypes[0]->primitive_type(),
            ::common::PrimitiveType::DT_SIGNED_INT64);
}

TEST_F(JsonIOTest, InferSchema_MixedTypes) {
  createFile("infer_mixed.jsonl",
             "{\"a\":1,\"b\":1.5,\"c\":\"hello\"}\n"
             "{\"a\":2,\"b\":2.5,\"c\":\"world\"}\n");
  auto state = createSharedState(
      "infer_mixed.jsonl", {"a", "b", "c"},
      {createStringType(), createStringType(), createStringType()},
      {{"batch_read", "false"}});
  auto reader = createJsonReader(state);
  auto result = reader->inferSchema();
  ASSERT_TRUE(result.has_value());
  auto schema = result.value();
  // a: integer, b: double, c: string
  EXPECT_EQ(schema->columnTypes[0]->primitive_type(),
            ::common::PrimitiveType::DT_SIGNED_INT64);
  EXPECT_EQ(schema->columnTypes[1]->primitive_type(),
            ::common::PrimitiveType::DT_DOUBLE);
  EXPECT_TRUE(schema->columnTypes[2]->has_string());
}

TEST_F(JsonIOTest, InferSchema_AutoDetectColumnNames) {
  createFile("infer_auto.jsonl", "{\"x\":1,\"y\":2,\"z\":3}\n");
  // Create state without explicit column_names to trigger auto-detection
  auto sharedState = std::make_shared<reader::ReadSharedState>();
  auto entrySchema = std::make_shared<reader::TableEntrySchema>();
  // Empty column names - should be auto-detected
  reader::FileSchema fileSchema;
  fileSchema.paths = {filePath("infer_auto.jsonl")};
  fileSchema.format = "json";
  fileSchema.options = {{"batch_read", "false"}};
  reader::ExternalSchema externalSchema;
  externalSchema.entry = entrySchema;
  externalSchema.file = fileSchema;
  sharedState->schema = std::move(externalSchema);

  auto optionsBuilder =
      std::make_unique<reader::JsonOptionsBuilder>(sharedState, false);
  auto reader = std::make_shared<reader::JsonReader>(sharedState,
                                                     std::move(optionsBuilder));
  auto result = reader->inferSchema();
  ASSERT_TRUE(result.has_value());
  auto schema = result.value();
  EXPECT_EQ(schema->columnNames.size(), 3u);
  // Check column names were auto-detected (order from rapidjson iteration)
  EXPECT_NE(
      std::find(schema->columnNames.begin(), schema->columnNames.end(), "x"),
      schema->columnNames.end());
}

TEST_F(JsonIOTest, InferSchema_NoDataRowsDefaultsToVarchar) {
  createFile("infer_empty.jsonl", "");
  auto state = createSharedState(
      "infer_empty.jsonl", {"a", "b", "c"},
      {createStringType(), createStringType(), createStringType()},
      {{"batch_read", "false"}});
  auto reader = createJsonReader(state);
  auto result = reader->inferSchema();
  ASSERT_TRUE(result.has_value());
  auto schema = result.value();
  EXPECT_EQ(schema->columnNames.size(), 3u);
  ASSERT_EQ(schema->columnTypes.size(), 3u);
  for (const auto& type : schema->columnTypes) {
    EXPECT_TRUE(type->has_string());
  }
}

TEST_F(JsonIOTest, ReadBoolFromString) {
  createFile("bool_str.jsonl", "{\"flag\":\"true\"}\n{\"flag\":\"false\"}\n");
  auto state = createSharedState("bool_str.jsonl", {"flag"}, {createBoolType()},
                                 {{"batch_read", "false"}});
  auto reader = createJsonReader(state);
  auto ctx = readToContext(reader, state);
  EXPECT_EQ(ctx.row_num(), 2);
  auto col = ctx.chunk(0).columns()[0];
  EXPECT_EQ(col->get_elem(0).GetValue<bool>(), true);
  EXPECT_EQ(col->get_elem(1).GetValue<bool>(), false);
}

// =============================================================================
// JSON Column Projection
// =============================================================================

TEST_F(JsonIOTest, ColumnProjection_SubsetColumns) {
  createFile("project.jsonl",
             "{\"a\":1,\"b\":\"x\",\"c\":3.14}\n"
             "{\"a\":2,\"b\":\"y\",\"c\":2.71}\n");
  auto state = createSharedState(
      "project.jsonl", {"a", "b", "c"},
      {createInt64Type(), createStringType(), createDoubleType()},
      {{"batch_read", "false"}}, {"a", "c"});
  auto reader = createJsonReader(state);
  auto ctx = readToContext(reader, state);
  EXPECT_EQ(ctx.col_num(), 2);
  EXPECT_EQ(ctx.row_num(), 2);
}

// =============================================================================
// JSON Multi-file
// =============================================================================

TEST_F(JsonIOTest, MultiFile_Read) {
  createFile("part1.jsonl", "{\"id\":1}\n{\"id\":2}\n");
  createFile("part2.jsonl", "{\"id\":3}\n{\"id\":4}\n");
  auto state = createSharedState("part1.jsonl", {"id"}, {createInt64Type()},
                                 {{"batch_read", "false"}});
  state->schema.file.paths.push_back(filePath("part2.jsonl"));
  auto reader = createJsonReader(state);
  auto ctx = readToContext(reader, state);
  EXPECT_EQ(ctx.row_num(), 4);
}

// =============================================================================
// JSON Large Data / Stress
// =============================================================================

TEST_F(JsonIOTest, LargeData_10000Rows) {
  std::string content;
  for (int i = 0; i < 10000; ++i) {
    content += "{\"id\":" + std::to_string(i) + ",\"name\":\"user_" +
               std::to_string(i) + "\"}\n";
  }
  createFile("large.jsonl", content);
  auto state = createSharedState("large.jsonl", {"id", "name"},
                                 {createInt64Type(), createStringType()},
                                 {{"batch_read", "false"}});
  auto reader = createJsonReader(state);
  auto ctx = readToContext(reader, state);
  EXPECT_EQ(ctx.row_num(), 10000);
}

}  // namespace test
}  // namespace neug
