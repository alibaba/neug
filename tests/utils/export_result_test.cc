/**
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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
#include <iterator>
#include <memory>
#include <string>
#include <vector>

#include "neug/common/columns/struct_columns.h"
#include "neug/common/columns/value_columns.h"
#include "neug/common/export/export_result.h"
#include "neug/common/types/data_chunk.h"
#include "neug/common/types/value.h"
#include "neug/compiler/function/export/json_export_function.h"
#include "neug/execution/common/context.h"
#include "neug/storages/graph/graph_interface.h"
#include "neug/storages/graph/graph_view.h"
#include "neug/storages/graph/property_graph.h"
#include "neug/utils/io/write/writer.h"
#include "unittest/utils.h"

namespace neug {
namespace test {
namespace {

constexpr const char* EXPORT_RESULT_TEST_DIR = "/tmp/neug_export_result_test";

std::shared_ptr<IContextColumn> intColumn(std::vector<int32_t> values) {
  ValueColumnBuilder<int32_t> builder;
  builder.reserve(values.size());
  for (auto value : values) {
    builder.push_back_opt(value);
  }
  return builder.finish();
}

std::shared_ptr<IContextColumn> stringColumn(
    const std::vector<std::string>& values) {
  ValueColumnBuilder<std::string> builder;
  builder.reserve(values.size());
  for (const auto& value : values) {
    builder.push_back_opt(value);
  }
  return builder.finish();
}

std::shared_ptr<IContextColumn> boolColumn(std::vector<bool> values) {
  ValueColumnBuilder<bool> builder;
  builder.reserve(values.size());
  for (auto value : values) {
    builder.push_back_opt(value);
  }
  return builder.finish();
}

std::shared_ptr<IContextColumn> nullIntColumn() {
  ValueColumnBuilder<int32_t> builder;
  builder.push_back_null();
  return builder.finish();
}

std::vector<std::string> readLines(const std::string& path) {
  std::ifstream file(path);
  std::vector<std::string> lines;
  std::string line;
  while (std::getline(file, line)) {
    lines.push_back(line);
  }
  return lines;
}

class ExportResultTest : public ::testing::Test {
 public:
  void SetUp() override {
    std::filesystem::remove_all(EXPORT_RESULT_TEST_DIR);
    std::filesystem::create_directories(EXPORT_RESULT_TEST_DIR);
  }

  void TearDown() override {
    std::filesystem::remove_all(EXPORT_RESULT_TEST_DIR);
  }
};

TEST_F(ExportResultTest, MaterializerMergesChunksBeforeCsvWrite) {
  execution::Context ctx;
  ctx.tag_ids = {0, 1, 2};

  DataChunk first;
  first.set(0, intColumn({1, 2}));
  first.set(1, stringColumn({"alice", "bob"}));
  first.set(2, boolColumn({true, false}));
  ctx.append_chunk(std::move(first));

  DataChunk second;
  second.set(0, intColumn({3}));
  second.set(1, stringColumn({"carol"}));
  second.set(2, boolColumn({true}));
  ctx.append_chunk(std::move(second));

  PropertyGraph graph;
  GraphView view(graph);
  StorageReadInterface reader(view, 1);

  auto export_result = materialize_result_for_export(ctx, reader);
  ASSERT_EQ(export_result.chunk.col_num(), 3);
  ASSERT_EQ(export_result.chunk.row_num(), 3);
  ASSERT_EQ(export_result.source_types.size(), 3);
  EXPECT_EQ(export_result.source_types[0].id(), DataTypeId::kInt32);
  EXPECT_EQ(export_result.source_types[1].id(), DataTypeId::kVarchar);
  EXPECT_EQ(export_result.source_types[2].id(), DataTypeId::kBoolean);

  reader::FileSchema schema;
  schema.paths = {std::string(EXPORT_RESULT_TEST_DIR) + "/merged.csv"};
  schema.format = "csv";
  auto entry_schema = std::make_shared<reader::TableEntrySchema>();
  entry_schema->columnNames = {"id", "name", "active"};

  writer::CsvQueryExportWriter writer(schema, entry_schema);
  auto status = writer.write(export_result.chunk, export_result.source_types);
  ASSERT_TRUE(status.ok()) << status.ToString();

  auto lines = readLines(schema.paths[0]);
  ASSERT_EQ(lines.size(), 4);
  EXPECT_EQ(lines[0], "id|name|active");
  EXPECT_EQ(lines[1], "1|\"alice\"|true");
  EXPECT_EQ(lines[2], "2|\"bob\"|false");
  EXPECT_EQ(lines[3], "3|\"carol\"|true");
}

TEST_F(ExportResultTest, JsonArrayWriterEmitsNestedValues) {
  auto list_type = DataType::List(DataType(DataTypeId::kInt32));
  std::vector<std::string> field_names = {"a", "items"};
  std::vector<DataType> field_types = {DataType(DataTypeId::kInt32), list_type};
  auto struct_type =
      DataType::Struct(std::move(field_names), std::move(field_types));

  StructColumnBuilder builder(struct_type);
  std::vector<Value> list_values;
  list_values.push_back(Value::INT32(1));
  list_values.push_back(Value::INT32(2));

  std::vector<Value> children;
  children.push_back(Value::INT32(7));
  children.push_back(
      Value::LIST(DataType(DataTypeId::kInt32), std::move(list_values)));
  builder.push_back_elem(Value::STRUCT(struct_type, std::move(children)));

  DataChunk chunk;
  chunk.set(0, builder.finish());

  reader::FileSchema schema;
  schema.paths = {std::string(EXPORT_RESULT_TEST_DIR) + "/nested.json"};
  schema.format = "json";
  auto entry_schema = std::make_shared<reader::TableEntrySchema>();
  entry_schema->columnNames = {"payload"};

  writer::JsonArrayExportWriter writer(schema, entry_schema);
  auto status = writer.write(chunk, {struct_type});
  ASSERT_TRUE(status.ok()) << status.ToString();

  std::ifstream file(schema.paths[0]);
  std::string json((std::istreambuf_iterator<char>(file)),
                   std::istreambuf_iterator<char>());
  EXPECT_EQ(json, R"([{"payload":{"a":7,"items":[1,2]}}])");
}

TEST_F(ExportResultTest, JsonWritersHonorIgnoreErrors) {
  reader::FileSchema schema;
  schema.format = "json";
  auto entry_schema = std::make_shared<reader::TableEntrySchema>();
  entry_schema->columnNames = {"value"};

  DataChunk null_chunk;
  null_chunk.set(0, nullIntColumn());
  const std::vector<DataType> int_source_types = {DataType(DataTypeId::kInt32)};

  schema.paths = {std::string(EXPORT_RESULT_TEST_DIR) + "/strict_null.json"};
  schema.options["IGNORE_ERRORS"] = "false";
  writer::JsonArrayExportWriter strict_null_writer(schema, entry_schema);
  auto status = strict_null_writer.write(null_chunk, int_source_types);
  EXPECT_FALSE(status.ok());

  schema.paths = {std::string(EXPORT_RESULT_TEST_DIR) + "/ignored_null.json"};
  schema.options["IGNORE_ERRORS"] = "true";
  writer::JsonArrayExportWriter ignored_null_writer(schema, entry_schema);
  status = ignored_null_writer.write(null_chunk, int_source_types);
  ASSERT_TRUE(status.ok()) << status.ToString();
  std::ifstream null_file(schema.paths[0]);
  std::string null_json((std::istreambuf_iterator<char>(null_file)),
                        std::istreambuf_iterator<char>());
  EXPECT_EQ(null_json, R"([{"value":null}])");

  DataChunk invalid_graph_chunk;
  invalid_graph_chunk.set(0, stringColumn({"not-json"}));
  const std::vector<DataType> graph_source_types = {
      DataType(DataTypeId::kVertex)};

  schema.paths = {std::string(EXPORT_RESULT_TEST_DIR) +
                  "/strict_invalid_graph.json"};
  schema.options["IGNORE_ERRORS"] = "false";
  writer::JsonArrayExportWriter strict_graph_writer(schema, entry_schema);
  status = strict_graph_writer.write(invalid_graph_chunk, graph_source_types);
  EXPECT_FALSE(status.ok());

  schema.paths = {std::string(EXPORT_RESULT_TEST_DIR) +
                  "/ignored_invalid_graph.json"};
  schema.options["IGNORE_ERRORS"] = "true";
  writer::JsonArrayExportWriter ignored_graph_writer(schema, entry_schema);
  status = ignored_graph_writer.write(invalid_graph_chunk, graph_source_types);
  ASSERT_TRUE(status.ok()) << status.ToString();
  std::ifstream graph_file(schema.paths[0]);
  std::string graph_json((std::istreambuf_iterator<char>(graph_file)),
                         std::istreambuf_iterator<char>());
  EXPECT_EQ(graph_json, R"([{"value":null}])");
}

TEST_F(ExportResultTest, JsonLWriterHonorsBatchSize) {
  DataChunk chunk;
  chunk.set(0, intColumn({1, 2, 3}));
  const std::vector<DataType> source_types = {DataType(DataTypeId::kInt32)};

  reader::FileSchema schema;
  schema.format = "jsonl";
  auto entry_schema = std::make_shared<reader::TableEntrySchema>();
  entry_schema->columnNames = {"id"};

  schema.paths = {std::string(EXPORT_RESULT_TEST_DIR) + "/batch_zero.jsonl"};
  schema.options["BATCH_SIZE"] = "0";
  writer::JsonLExportWriter invalid_writer(schema, entry_schema);
  auto status = invalid_writer.write(chunk, source_types);
  EXPECT_FALSE(status.ok());
  EXPECT_FALSE(std::filesystem::exists(schema.paths[0]));

  schema.paths = {std::string(EXPORT_RESULT_TEST_DIR) + "/batch_two.jsonl"};
  schema.options["BATCH_SIZE"] = "2";
  writer::JsonLExportWriter writer(schema, entry_schema);
  status = writer.write(chunk, source_types);
  ASSERT_TRUE(status.ok()) << status.ToString();
  EXPECT_EQ(
      readLines(schema.paths[0]),
      std::vector<std::string>({R"({"id":1})", R"({"id":2})", R"({"id":3})"}));
}

TEST_F(ExportResultTest, MaterializerPreservesContainersAroundGraphValues) {
  const DataType vertex_type(DataTypeId::kVertex);
  const auto vertex_list_type = DataType::List(vertex_type);
  const auto vertex_array_type = DataType::Array(vertex_type, 2);
  const auto source_type = DataType::Struct(
      {"nodes", "primary"}, {vertex_list_type, vertex_array_type});

  const auto graph_path =
      std::string(EXPORT_RESULT_TEST_DIR) + "/nested_graph_data";
  CheckpointManager checkpoint_mgr;
  checkpoint_mgr.Open(graph_path);
  PropertyGraph graph;
  graph.Open(make_checkpoint(checkpoint_mgr), MemoryLevel::kInMemory);
  CreateVertexTypeParamBuilder person_builder;
  ASSERT_TRUE(graph
                  .CreateVertexType(person_builder.VertexLabel("person")
                                        .AddProperty("id", Value::INT64(0))
                                        .AddProperty("name", Value::STRING(""))
                                        .AddPrimaryKeyName("id")
                                        .Build())
                  .ok());
  const auto person_label = graph.schema().get_vertex_label_id("person");
  vid_t person_vid;
  ASSERT_TRUE(graph
                  .AddVertex(person_label, Value::INT64(1),
                             {Value::STRING("Alice")}, person_vid, 0)
                  .ok());
  const VertexRecord person{person_label, person_vid};

  std::vector<Value> vertices;
  vertices.push_back(Value::VERTEX(person));
  vertices.emplace_back(vertex_type);
  std::vector<Value> primary;
  primary.push_back(Value::VERTEX(person));
  primary.emplace_back(vertex_type);
  std::vector<Value> payload;
  payload.push_back(Value::LIST(vertex_type, std::move(vertices)));
  payload.push_back(Value::ARRAY(vertex_array_type, std::move(primary)));

  StructColumnBuilder builder(source_type);
  builder.push_back_elem(Value::STRUCT(source_type, std::move(payload)));

  execution::Context ctx;
  ctx.tag_ids = {0};
  DataChunk input;
  input.set(0, builder.finish());
  ctx.append_chunk(std::move(input));

  GraphView view(graph);
  StorageReadInterface reader(view, 1);
  auto export_result = materialize_result_for_export(ctx, reader);

  ASSERT_EQ(export_result.source_types.size(), 1);
  EXPECT_EQ(export_result.source_types[0], source_type);
  const auto materialized_type =
      DataType::Struct({"nodes", "primary"},
                       {DataType::List(DataType(DataTypeId::kVarchar)),
                        DataType::Array(DataType(DataTypeId::kVarchar), 2)});
  ASSERT_EQ(export_result.chunk.col_num(), 1);
  ASSERT_NE(export_result.chunk.columns[0], nullptr);
  EXPECT_EQ(export_result.chunk.columns[0]->elem_type(), materialized_type);

  reader::FileSchema schema;
  schema.paths = {std::string(EXPORT_RESULT_TEST_DIR) + "/nested_graph.json"};
  schema.format = "json";
  auto entry_schema = std::make_shared<reader::TableEntrySchema>();
  entry_schema->columnNames = {"payload"};

  writer::JsonArrayExportWriter writer(schema, entry_schema);
  auto status = writer.write(export_result.chunk, export_result.source_types);
  ASSERT_TRUE(status.ok()) << status.ToString();

  std::ifstream file(schema.paths[0]);
  std::string json((std::istreambuf_iterator<char>(file)),
                   std::istreambuf_iterator<char>());
  EXPECT_EQ(
      json,
      R"([{"payload":{"nodes":[{"_ID":0,"_LABEL":"person","id":1,"name":"Alice"},null],"primary":[{"_ID":0,"_LABEL":"person","id":1,"name":"Alice"},null]}}])");
}

}  // namespace
}  // namespace test
}  // namespace neug
