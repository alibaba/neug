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

}  // namespace
}  // namespace test
}  // namespace neug
