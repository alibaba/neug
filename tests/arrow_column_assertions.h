/***
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef NEUG_TEST_ARROW_COLUMN_ASSERTIONS_H_
#define NEUG_TEST_ARROW_COLUMN_ASSERTIONS_H_

#include <gtest/gtest.h>

#include <arrow/array.h>
#include <arrow/array/concatenate.h>
#include <arrow/table.h>
#include <arrow/type.h>

#include <memory>
#include <string>
#include <vector>

namespace neug {
namespace test {

inline std::shared_ptr<arrow::Array> FlattenColumn(
    const std::shared_ptr<arrow::ChunkedArray>& column) {
  EXPECT_TRUE(column);
  if (column->num_chunks() == 0) {
    auto empty = arrow::MakeArrayOfNull(column->type(), 0);
    EXPECT_TRUE(empty.ok());
    return empty.ValueOrDie();
  }
  if (column->num_chunks() == 1) {
    return column->chunk(0);
  }
  auto concat =
      arrow::Concatenate(column->chunks(), arrow::default_memory_pool());
  EXPECT_TRUE(concat.ok());
  return concat.ValueOrDie();
}

inline std::shared_ptr<arrow::Array> ColumnToArray(
    const std::shared_ptr<arrow::Table>& table, int column_index) {
  EXPECT_TRUE(table);
  EXPECT_GE(column_index, 0);
  EXPECT_LT(column_index, table->num_columns());
  return FlattenColumn(table->column(column_index));
}

inline void AssertInt64Column(const std::shared_ptr<arrow::Table>& table,
                              int column_index,
                              const std::vector<int64_t>& expected) {
  auto array = ColumnToArray(table, column_index);
  EXPECT_NE(array, nullptr);
  auto tight = std::static_pointer_cast<arrow::Int64Array>(array);
  EXPECT_EQ(tight->length(), static_cast<int64_t>(expected.size()));
  for (int64_t i = 0; i < tight->length(); ++i) {
    EXPECT_EQ(tight->Value(i), expected[i]);
  }
}

inline void AssertInt32Column(const std::shared_ptr<arrow::Table>& table,
                              int column_index,
                              const std::vector<int32_t>& expected) {
  auto array = ColumnToArray(table, column_index);
  EXPECT_NE(array, nullptr);
  auto tight = std::static_pointer_cast<arrow::Int32Array>(array);
  EXPECT_EQ(tight->length(), static_cast<int64_t>(expected.size()));
  for (int64_t i = 0; i < tight->length(); ++i) {
    EXPECT_EQ(tight->Value(i), expected[i]);
  }
}

inline void AssertDate32Column(const std::shared_ptr<arrow::Table>& table,
                               int column_index,
                               const std::vector<int32_t>& expected) {
  auto array = ColumnToArray(table, column_index);
  EXPECT_NE(array, nullptr);
  auto tight = std::static_pointer_cast<arrow::Date32Array>(array);
  EXPECT_EQ(tight->length(), static_cast<int64_t>(expected.size()));
  for (int64_t i = 0; i < tight->length(); ++i) {
    EXPECT_EQ(tight->Value(i), expected[i]);
  }
}

inline void AssertDoubleColumn(const std::shared_ptr<arrow::Table>& table,
                               int column_index,
                               const std::vector<double>& expected) {
  auto array = ColumnToArray(table, column_index);
  EXPECT_NE(array, nullptr);
  auto tight = std::static_pointer_cast<arrow::DoubleArray>(array);
  EXPECT_EQ(tight->length(), static_cast<int64_t>(expected.size()));
  for (int64_t i = 0; i < tight->length(); ++i) {
    EXPECT_DOUBLE_EQ(tight->Value(i), expected[i]);
  }
}

inline void AssertStringColumn(const std::shared_ptr<arrow::Table>& table,
                               int column_index,
                               const std::vector<std::string>& expected) {
  auto array = ColumnToArray(table, column_index);
  EXPECT_NE(array, nullptr);
  if (array->type_id() == arrow::Type::STRING) {
    auto tight = std::static_pointer_cast<arrow::StringArray>(array);
    EXPECT_EQ(tight->length(), static_cast<int64_t>(expected.size()));
    for (int64_t i = 0; i < tight->length(); ++i) {
      EXPECT_EQ(tight->GetString(i), expected[i]);
    }
    return;
  }
  auto tight = std::static_pointer_cast<arrow::LargeStringArray>(array);
  EXPECT_EQ(tight->length(), static_cast<int64_t>(expected.size()));
  for (int64_t i = 0; i < tight->length(); ++i) {
    EXPECT_EQ(tight->GetString(i), expected[i]);
  }
}

}  // namespace test
}  // namespace neug

#endif  // NEUG_TEST_ARROW_COLUMN_ASSERTIONS_H_
