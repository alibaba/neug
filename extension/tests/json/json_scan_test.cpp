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

#include <arrow/array.h>
#include <arrow/compute/api.h>
#include <arrow/type.h>
#include <gtest/gtest.h>
#include <filesystem>
#include <fstream>
#include <iostream>

#include "json/json_scan_function.h"
#include "neug/compiler/common/file_system/virtual_file_system.h"
#include "neug/compiler/gopt/g_vfs_holder.h"
#include "neug/execution/common/columns/arrow_context_column.h"
#include "neug/execution/common/context.h"
#include "neug/storages/graph/schema.h"
#include "neug/utils/property/types.h"

class JsonLoadFunctionTest : public ::testing::Test {
 protected:
  std::unique_ptr<gs::common::VirtualFileSystem> vfs_;

  void SetUp() override {
    // Initialize VirtualFileSystem for testing
    vfs_ = std::make_unique<gs::common::VirtualFileSystem>();
    gs::common::VFSHolder::setVFS(vfs_.get());

    std::filesystem::create_directories("/tmp/extension_test_data/json");
    createTestJsonArrayFile();
  }

  void TearDown() override {
    std::filesystem::remove_all("/tmp/extension_test_data/json");
    // Note: VFSHolder is static, we don't reset it here to avoid issues
    // with other tests that might run after this
  }

  void createTestJsonArrayFile() {
    std::ofstream file("/tmp/extension_test_data/json/test_array.json");
    file << R"([
            {"id": 1, "name": "Mika", "age": 17, "height": 157.5, "birthday": "2004-05-08"},
            {"id": 2, "name": "Yuuka", "age": 16, "height": 156.1, "birthday": "2005-03-14"},
            {"id": 3, "name": "Hina", "age": 17, "height": 142.8, "birthday": "2004-02-19"}
        ])";
    file.close();
  }
};

TEST_F(JsonLoadFunctionTest, TestExecFuncJsonArray) {
  std::vector<gs::DataTypeId> columnTypes = {
      gs::DataTypeId::kInt32,  // id
      gs::DataTypeId::kVarchar,  // name
      gs::DataTypeId::kInt32,  // age
      gs::DataTypeId::kDouble,   // height
      gs::DataTypeId::kDate      // birthday
  };

  // Create input directly for exec function testing
  gs::extension::JsonScanFuncInput input(
      "/tmp/extension_test_data/json/test_array.json", columnTypes,
      gs::extension::JsonFormat::ARRAY);

  gs::runtime::Context result =
      gs::extension::JsonScanFunction::execFunc(input);

  // Verify context has correct number of columns
  EXPECT_EQ(result.col_num(), 5);

  // Verify data (basic checks)
  auto col0 = result.get(0);  // id column
  auto col1 = result.get(1);  // name column
  auto col2 = result.get(2);  // age column
  auto col3 = result.get(3);  // height column
  auto col4 = result.get(4);  // birthday column

  ASSERT_NE(col0, nullptr);
  ASSERT_NE(col1, nullptr);
  ASSERT_NE(col2, nullptr);
  ASSERT_NE(col3, nullptr);
  ASSERT_NE(col4, nullptr);

  // Check we have 3 rows
  EXPECT_EQ(col0->size(), 3);
  EXPECT_EQ(col1->size(), 3);
  EXPECT_EQ(col2->size(), 3);
  EXPECT_EQ(col3->size(), 3);
  EXPECT_EQ(col4->size(), 3);

  // Cast to ArrowArrayContextColumn to access underlying Arrow data
  auto arrow_col0 =
      std::dynamic_pointer_cast<gs::runtime::ArrowArrayContextColumn>(col0);
  auto arrow_col1 =
      std::dynamic_pointer_cast<gs::runtime::ArrowArrayContextColumn>(col1);
  auto arrow_col2 =
      std::dynamic_pointer_cast<gs::runtime::ArrowArrayContextColumn>(col2);
  auto arrow_col3 =
      std::dynamic_pointer_cast<gs::runtime::ArrowArrayContextColumn>(col3);
  auto arrow_col4 =
      std::dynamic_pointer_cast<gs::runtime::ArrowArrayContextColumn>(col4);

  ASSERT_NE(arrow_col0, nullptr);
  ASSERT_NE(arrow_col1, nullptr);
  ASSERT_NE(arrow_col2, nullptr);
  ASSERT_NE(arrow_col3, nullptr);
  ASSERT_NE(arrow_col4, nullptr);

  // Test Int32 column (id)
  {
    const auto& columns = arrow_col0->GetColumns();
    ASSERT_FALSE(columns.empty());
    auto arrow_array = columns[0];
    ASSERT_NE(arrow_array, nullptr);
    EXPECT_EQ(arrow_array->type()->id(), arrow::Type::INT32);
    EXPECT_EQ(arrow_array->length(), 3);

    auto int32_array = std::static_pointer_cast<arrow::Int32Array>(arrow_array);
    std::vector<int32_t> expected_ids = {1, 2, 3};
    for (int i = 0; i < 3; ++i) {
      EXPECT_EQ(int32_array->Value(i), expected_ids[i]);
    }
  }

  // Test String column (name)
  {
    const auto& columns = arrow_col1->GetColumns();
    ASSERT_FALSE(columns.empty());
    auto arrow_array = columns[0];
    ASSERT_NE(arrow_array, nullptr);
    EXPECT_EQ(arrow_array->type()->id(), arrow::Type::STRING);
    EXPECT_EQ(arrow_array->length(), 3);

    auto string_array =
        std::static_pointer_cast<arrow::StringArray>(arrow_array);
    std::vector<std::string> expected_names = {"Mika", "Yuuka", "Hina"};
    for (int i = 0; i < 3; ++i) {
      EXPECT_EQ(string_array->GetString(i), expected_names[i]);
    }
  }

  // Test Int32 column (age)
  {
    const auto& columns = arrow_col2->GetColumns();
    ASSERT_FALSE(columns.empty());
    auto arrow_array = columns[0];
    ASSERT_NE(arrow_array, nullptr);
    EXPECT_EQ(arrow_array->type()->id(), arrow::Type::INT32);
    EXPECT_EQ(arrow_array->length(), 3);

    auto int32_array = std::static_pointer_cast<arrow::Int32Array>(arrow_array);
    std::vector<int32_t> expected_ages = {17, 16, 17};
    for (int i = 0; i < 3; ++i) {
      EXPECT_EQ(int32_array->Value(i), expected_ages[i]);
    }
  }

  // Test Double column (height)
  {
    const auto& columns = arrow_col3->GetColumns();
    ASSERT_FALSE(columns.empty());
    auto arrow_array = columns[0];
    ASSERT_NE(arrow_array, nullptr);
    EXPECT_EQ(arrow_array->type()->id(), arrow::Type::DOUBLE);
    EXPECT_EQ(arrow_array->length(), 3);

    auto double_array =
        std::static_pointer_cast<arrow::DoubleArray>(arrow_array);
    std::vector<double> expected_heights = {157.5, 156.1, 142.8};
    for (int i = 0; i < 3; ++i) {
      EXPECT_DOUBLE_EQ(double_array->Value(i), expected_heights[i]);
    }
  }

  // Test Date column
  {
    const auto& columns = arrow_col4->GetColumns();
    ASSERT_FALSE(columns.empty());
    auto arrow_array = columns[0];
    ASSERT_NE(arrow_array, nullptr);

    EXPECT_EQ(arrow_array->type()->id(), arrow::Type::DATE32);
    EXPECT_EQ(arrow_array->length(), 3);
    EXPECT_EQ(arrow_array->null_count(), 0);

    auto date32_array =
        std::static_pointer_cast<arrow::Date32Array>(arrow_array);

    std::vector<std::string> date_strings = {"2004-05-08", "2005-03-14",
                                             "2004-02-19"};
    std::vector<int32_t> expected_days;

    for (const auto& date_str : date_strings) {
      gs::Date date(date_str);
      expected_days.push_back(date.to_num_days());
    }

    for (int i = 0; i < 3; ++i) {
      int32_t actual_days = date32_array->Value(i);
      EXPECT_EQ(actual_days, expected_days[i])
          << "Date mismatch at row " << i << ": expected " << expected_days[i]
          << " days, got " << actual_days << " days";
    }

    LOG(INFO) << "Date column validation completed successfully";
  }
}

TEST_F(JsonLoadFunctionTest, TestGetFunctionSet) {
  // Test that getFunctionSet works correctly
  auto functionSet = gs::extension::JsonScanFunction::getFunctionSet();

  EXPECT_FALSE(functionSet.empty());
  EXPECT_EQ(functionSet.size(), 1);

  auto& function = functionSet[0];
  ASSERT_NE(function, nullptr);
}