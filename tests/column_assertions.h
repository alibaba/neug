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

#pragma once

#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <vector>
#include "neug/generated/proto/response/response.pb.h"

namespace neug {
namespace test {

inline void AssertInt64Column(const neug::QueryResponse& table,
                              int column_index,
                              const std::vector<int64_t>& expected) {
  const auto& column = table.arrays(column_index);
  EXPECT_TRUE(column.has_int64_array());
  const auto& col = column.int64_array();
  EXPECT_EQ(col.values_size(), expected.size());
  for (int64_t i = 0; i < col.values_size(); ++i) {
    EXPECT_EQ(col.values(i), expected[i]);
  }
}

inline void AssertInt32Column(const neug::QueryResponse& table,
                              int column_index,
                              const std::vector<int32_t>& expected) {
  const auto& column = table.arrays(column_index);
  EXPECT_TRUE(column.has_int32_array());
  const auto& col = column.int32_array();
  EXPECT_EQ(col.values_size(), expected.size());
  for (int64_t i = 0; i < col.values_size(); ++i) {
    EXPECT_EQ(col.values(i), expected[i]);
  }
}

inline void AssertDate32Column(const neug::QueryResponse& table,
                               int column_index,
                               const std::vector<int64_t>& expected) {
  const auto& array = table.arrays(column_index);
  EXPECT_TRUE(array.has_date_array());
  const auto& col = array.date_array();
  EXPECT_EQ(col.values_size(), expected.size());
  for (int64_t i = 0; i < col.values_size(); ++i) {
    EXPECT_EQ(col.values(i), expected[i]);
  }
}

inline void AssertDoubleColumn(const neug::QueryResponse& table,
                               int column_index,
                               const std::vector<double>& expected) {
  const auto& column = table.arrays(column_index);
  EXPECT_TRUE(column.has_double_array());
  const auto& col = column.double_array();
  EXPECT_EQ(col.values_size(), expected.size());
  for (int64_t i = 0; i < col.values_size(); ++i) {
    EXPECT_DOUBLE_EQ(col.values(i), expected[i]);
  }
}

inline void AssertStringColumn(const neug::QueryResponse& table,
                               int column_index,
                               const std::vector<std::string>& expected) {
  const auto& column = table.arrays(column_index);
  EXPECT_TRUE(column.has_string_array());
  const auto& col = column.string_array();
  EXPECT_EQ(col.values_size(), expected.size());
  for (int64_t i = 0; i < col.values_size(); ++i) {
    EXPECT_EQ(col.values(i), expected[i]);
  }
}

}  // namespace test
}  // namespace neug
