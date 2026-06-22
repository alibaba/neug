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
#include <vector>

#include "neug/compiler/common/case_insensitive_map.h"
#include "neug/execution/common/columns/value_columns.h"
#include "neug/execution/common/context.h"
#include "neug/generated/proto/plan/basic_type.pb.h"
#include "neug/utils/reader/options.h"
#include "neug/utils/reader/reader.h"
#include "neug/utils/reader/schema.h"

namespace neug {
namespace test {

static constexpr const char* ARROW_READER_TEST_DIR = "/tmp/arrow_reader_test";

class JsonTest : public ::testing::Test {
 public:
  void SetUp() override {
    if (std::filesystem::exists(ARROW_READER_TEST_DIR)) {
      std::filesystem::remove_all(ARROW_READER_TEST_DIR);
    }
    std::filesystem::create_directories(ARROW_READER_TEST_DIR);
  }

  void TearDown() override {
    if (std::filesystem::exists(ARROW_READER_TEST_DIR)) {
      std::filesystem::remove_all(ARROW_READER_TEST_DIR);
    }
  }

  void createJsonFile(const std::string& filename, const std::string& content) {
    std::ofstream file(std::string(ARROW_READER_TEST_DIR) + "/" + filename);
    file << content;
    file.close();
  }

  std::shared_ptr<::common::DataType> createUInt32Type() {
    auto type = std::make_shared<::common::DataType>();
    type->set_primitive_type(::common::PrimitiveType::DT_UNSIGNED_INT32);
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

  std::shared_ptr<reader::ReadSharedState> createSharedState(
      const std::string& jsonFile, const std::vector<std::string>& columnNames,
      const std::vector<std::shared_ptr<::common::DataType>>& columnTypes,
      const common::case_insensitive_map_t<std::string>& options = {},
      const std::vector<std::string>& projectColumns = {},
      std::shared_ptr<::common::Expression> skipRows = nullptr) {
    auto sharedState = std::make_shared<reader::ReadSharedState>();

    auto entrySchema = std::make_shared<reader::TableEntrySchema>();
    entrySchema->columnNames = columnNames;
    entrySchema->columnTypes = columnTypes;

    reader::FileSchema fileSchema;
    fileSchema.paths = {std::string(ARROW_READER_TEST_DIR) + "/" + jsonFile};
    fileSchema.format = "json";
    fileSchema.options = options;

    reader::ExternalSchema externalSchema;
    externalSchema.entry = entrySchema;
    externalSchema.file = fileSchema;

    sharedState->schema = std::move(externalSchema);
    sharedState->projectColumns = projectColumns;
    sharedState->skipRows = skipRows;

    return sharedState;
  }

  std::shared_ptr<reader::JsonReader> createJsonReader(
      const std::shared_ptr<reader::ReadSharedState>& sharedState) {
    auto optionsBuilder =
        std::make_unique<reader::JsonOptionsBuilder>(sharedState, true);
    return std::make_shared<reader::JsonReader>(sharedState,
                                               std::move(optionsBuilder));
  }
};

TEST_F(JsonTest, TestJsonArray) {
  createJsonFile("test_json_array.json",
                 "[{\"id\": 1, \"name\": \"Alice\", \"age\": 25}, {\"id\": 2, "
                 "\"name\": \"Bob\", \"age\": 30}]");
  auto sharedState = createSharedState(
      "test_json_array.json", {"id", "name", "age"},
      {createUInt32Type(), createStringType(), createDoubleType()},
      {{"batch_read", "false"}});
  auto reader = createJsonReader(sharedState);
  auto localState = std::make_shared<reader::ReadLocalState>();
  execution::Context ctx;

  reader->read(localState, ctx);

  EXPECT_EQ(ctx.col_num(), 3);
  EXPECT_EQ(ctx.row_num(), 2);

  auto col0 = ctx.chunk(0).columns()[0];
  ASSERT_EQ(col0->column_type(), execution::ContextColumnType::kValue);
  EXPECT_EQ(col0->get_elem(0).GetValue<uint32_t>(), 1u);
  EXPECT_EQ(col0->get_elem(1).GetValue<uint32_t>(), 2u);

  auto col2 = ctx.chunk(0).columns()[2];
  ASSERT_EQ(col2->column_type(), execution::ContextColumnType::kValue);
  EXPECT_DOUBLE_EQ(col2->get_elem(0).GetValue<double>(), 25.0);
  EXPECT_DOUBLE_EQ(col2->get_elem(1).GetValue<double>(), 30.0);
}

}  // namespace test
}  // namespace neug
