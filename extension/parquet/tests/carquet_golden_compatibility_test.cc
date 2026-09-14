/**
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <gtest/gtest.h>

#include <cmath>
#include <cstdint>
#include <filesystem>
#include <limits>
#include <memory>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "carquet/scan.h"
#include "carquet/sniffer.h"
#include "neug/common/types/value.h"
#include "neug/execution/common/context.h"
#include "neug/utils/io/read/common/schema.h"
#include "neug/utils/io/stream/input_stream.h"

namespace neug::parquet {
namespace {

struct GoldenRead {
  std::shared_ptr<reader::EntrySchema> schema;
  execution::Context context;
};

std::filesystem::path fixturePath(const std::string& name) {
  return std::filesystem::path(NEUG_PARQUET_GOLDEN_DIR) / name;
}

GoldenRead readFixture(const std::string& name, bool batchRead) {
  const auto path = fixturePath(name);
  auto schema = sniffCarquet(
      [path]() { return io::openLocalInputStream(path.string()); });
  if (!schema) {
    throw std::runtime_error(schema.error().ToString());
  }

  auto state = std::make_shared<reader::ReadSharedState>();
  state->schema.entry = *schema;
  state->schema.file.paths = {path.string()};
  state->schema.file.format = "parquet";
  state->schema.file.options = {
      {"batch_read", batchRead ? "true" : "false"},
      {"parallel", "false"},
      {"parquet_batch_rows", "1"},
  };

  execution::Context context;
  scanCarquet(state, context);
  return {.schema = std::move(*schema), .context = std::move(context)};
}

std::vector<std::vector<Value>> materialize(const execution::Context& context) {
  std::vector<std::vector<Value>> rows;
  rows.reserve(context.row_num());
  for (const auto& contextChunk : context.chunks()) {
    const auto& chunk = contextChunk.chunk();
    for (size_t row = 0; row < chunk.row_num(); ++row) {
      std::vector<Value> values;
      values.reserve(chunk.col_num());
      for (const auto& column : chunk.columns) {
        values.emplace_back(column->get_elem(row));
      }
      rows.emplace_back(std::move(values));
    }
  }
  return rows;
}

template <typename T>
void expectValue(const std::vector<std::vector<Value>>& rows, size_t row,
                 size_t column, const T& expected) {
  SCOPED_TRACE("row " + std::to_string(row) + ", column " +
               std::to_string(column));
  ASSERT_LT(row, rows.size());
  ASSERT_LT(column, rows[row].size());
  ASSERT_FALSE(rows[row][column].IsNull());
  EXPECT_EQ(rows[row][column].GetValue<T>(), expected);
}

void expectTypeIds(const std::vector<Value>& row,
                   const std::vector<DataTypeId>& expected) {
  ASSERT_EQ(row.size(), expected.size());
  for (size_t column = 0; column < row.size(); ++column) {
    SCOPED_TRACE("column " + std::to_string(column));
    EXPECT_EQ(row[column].type().id(), expected[column]);
  }
}

TEST(CarquetGoldenCompatibilityTest, MatchesRecordedArrowTypeGroundTruth) {
  const std::vector<std::string> names = {
      "row_id",       "enabled",      "i8",           "i16",
      "i32",          "i64",          "u8",           "u16",
      "u32",          "u64",          "f32",          "f64",
      "text",         "large_text",   "event_date",   "timestamp_s",
      "timestamp_ms", "timestamp_us", "timestamp_ns", "timestamp_tz",
      "items",        "large_items",  "fixed3",
  };
  const std::vector<DataTypeId> types = {
      DataTypeId::kInt64,       DataTypeId::kBoolean,
      DataTypeId::kInt32,       DataTypeId::kInt32,
      DataTypeId::kInt32,       DataTypeId::kInt64,
      DataTypeId::kUInt32,      DataTypeId::kUInt32,
      DataTypeId::kUInt32,      DataTypeId::kUInt64,
      DataTypeId::kFloat,       DataTypeId::kDouble,
      DataTypeId::kVarchar,     DataTypeId::kVarchar,
      DataTypeId::kDate,        DataTypeId::kTimestampMs,
      DataTypeId::kTimestampMs, DataTypeId::kTimestampMs,
      DataTypeId::kTimestampMs, DataTypeId::kTimestampMs,
      DataTypeId::kList,        DataTypeId::kList,
      DataTypeId::kArray,
  };

  for (const bool batchRead : {false, true}) {
    SCOPED_TRACE(batchRead ? "batch" : "full");
    const auto result = readFixture("arrow_types.parquet", batchRead);
    EXPECT_EQ(result.schema->columnNames, names);
    EXPECT_EQ(result.context.chunk_num(), batchRead ? 4u : 1u);
    const auto rows = materialize(result.context);
    ASSERT_EQ(rows.size(), 4u);
    expectTypeIds(rows.front(), types);

    expectValue(rows, 0, 0, int64_t{1});
    expectValue(rows, 0, 1, true);
    expectValue(rows, 0, 2, int32_t{-128});
    expectValue(rows, 0, 3, int32_t{-32768});
    expectValue(rows, 0, 4, std::numeric_limits<int32_t>::min());
    expectValue(rows, 0, 5, std::numeric_limits<int64_t>::min());
    expectValue(rows, 1, 6, uint32_t{255});
    expectValue(rows, 1, 7, uint32_t{65535});
    expectValue(rows, 1, 8, std::numeric_limits<uint32_t>::max());
    expectValue(rows, 1, 9, std::numeric_limits<uint64_t>::max());
    expectValue(rows, 3, 9, uint64_t{1} << 63);

    EXPECT_TRUE(std::isnan(rows[0][10].GetValue<float>()));
    EXPECT_EQ(rows[1][10].GetValue<float>(),
              std::numeric_limits<float>::infinity());
    EXPECT_EQ(rows[2][10].GetValue<float>(),
              -std::numeric_limits<float>::infinity());
    EXPECT_TRUE(rows[3][10].IsNull());
    EXPECT_TRUE(std::signbit(rows[0][11].GetValue<double>()));
    EXPECT_FALSE(std::signbit(rows[1][11].GetValue<double>()));
    expectValue(rows, 2, 11, 1.25);
    EXPECT_TRUE(rows[3][11].IsNull());

    expectValue(rows, 0, 12, std::string{});
    expectValue(rows, 1, 12, std::string("a\0b", 3));
    expectValue(rows, 2, 12, std::string("中文🙂"));
    EXPECT_TRUE(rows[3][12].IsNull());
    expectValue(rows, 0, 13, std::string("large"));
    EXPECT_TRUE(rows[2][13].IsNull());
    expectValue(rows, 3, 13, std::string("终"));

    EXPECT_EQ(rows[0][14].GetValue<Date>().to_num_days(), -1);
    EXPECT_EQ(rows[3][14].GetValue<Date>().to_string(), "2024-02-29");
    for (size_t column = 15; column <= 19; ++column) {
      EXPECT_EQ(rows[0][column].GetValue<DateTime>().milli_second, -1000);
      EXPECT_EQ(rows[1][column].GetValue<DateTime>().milli_second, 0);
      EXPECT_TRUE(rows[2][column].IsNull());
    }
    EXPECT_EQ(rows[3][15].GetValue<DateTime>().milli_second, 1700000000000);
    for (size_t column = 16; column <= 19; ++column) {
      EXPECT_EQ(rows[3][column].GetValue<DateTime>().milli_second,
                1700000000123);
    }

    const auto& items = ListValue::GetChildren(rows[0][20]);
    ASSERT_EQ(items.size(), 3u);
    EXPECT_EQ(items[0].GetValue<int64_t>(), 1);
    EXPECT_TRUE(items[1].IsNull());
    EXPECT_EQ(items[2].GetValue<int64_t>(), 3);
    EXPECT_TRUE(ListValue::GetChildren(rows[1][20]).empty());
    EXPECT_TRUE(rows[2][20].IsNull());

    const auto& largeItems = ListValue::GetChildren(rows[0][21]);
    ASSERT_EQ(largeItems.size(), 2u);
    EXPECT_EQ(largeItems[0].GetValue<int64_t>(), 1);
    EXPECT_TRUE(largeItems[1].IsNull());
    EXPECT_TRUE(rows[2][21].IsNull());

    const auto& fixed = ArrayValue::GetChildren(rows[0][22]);
    ASSERT_EQ(fixed.size(), 3u);
    EXPECT_DOUBLE_EQ(fixed[0].GetValue<double>(), 1.0);
    EXPECT_TRUE(fixed[1].IsNull());
    EXPECT_DOUBLE_EQ(fixed[2].GetValue<double>(), 3.0);
  }
}

TEST(CarquetGoldenCompatibilityTest,
     ReadsRecordedDeltaEncodingsAndColumnCodecs) {
  for (const bool batchRead : {false, true}) {
    SCOPED_TRACE(batchRead ? "batch" : "full");
    const auto result = readFixture("arrow_delta_encodings.parquet", batchRead);
    EXPECT_EQ(result.schema->columnNames,
              (std::vector<std::string>{"id", "signed_value", "text_length",
                                        "text_prefix"}));
    const auto rows = materialize(result.context);
    ASSERT_EQ(rows.size(), 67u);
    for (int64_t row = 0; row < 67; ++row) {
      SCOPED_TRACE("row " + std::to_string(row));
      expectValue(rows, row, 0, row - 17);
      expectValue(rows, row, 1, static_cast<int32_t>(row * 37 - 911));
      expectValue(rows, row, 2,
                  "length-" + std::to_string(row) + "-" +
                      std::string(static_cast<size_t>(row % 7), 'x'));
      expectValue(rows, row, 3,
                  "shared-prefix-" + std::to_string(row / 4) + "-value-" +
                      std::to_string(row));
    }
  }
}

TEST(CarquetGoldenCompatibilityTest, ReadsRecordedByteStreamSplitAndLz4) {
  constexpr float f32[] = {
      std::numeric_limits<float>::quiet_NaN(),
      std::numeric_limits<float>::infinity(),
      -std::numeric_limits<float>::infinity(),
      -0.0F,
      0.0F,
      0.125F,
      1.25F,
      -2.5F,
      42.0F,
      -7.5F,
      100.5F,
      -200.25F,
  };
  constexpr double f64[] = {
      -0.0,
      0.0,
      1.25,
      -2.5,
      3.75,
      100.5,
      -200.25,
      0.125,
      42.0,
      -7.5,
      std::numeric_limits<double>::infinity(),
      -std::numeric_limits<double>::infinity(),
  };

  const auto result = readFixture("arrow_byte_stream_split.parquet", true);
  const auto rows = materialize(result.context);
  ASSERT_EQ(rows.size(), 12u);
  for (size_t row = 0; row < rows.size(); ++row) {
    SCOPED_TRACE("row " + std::to_string(row));
    expectValue(rows, row, 0, static_cast<int64_t>(row + 1));
    const float actualF32 = rows[row][1].GetValue<float>();
    if (std::isnan(f32[row])) {
      EXPECT_TRUE(std::isnan(actualF32));
    } else {
      EXPECT_EQ(actualF32, f32[row]);
      if (f32[row] == 0.0F) {
        EXPECT_EQ(std::signbit(actualF32), std::signbit(f32[row]));
      }
    }
    const double actualF64 = rows[row][2].GetValue<double>();
    EXPECT_EQ(actualF64, f64[row]);
    if (f64[row] == 0.0) {
      EXPECT_EQ(std::signbit(actualF64), std::signbit(f64[row]));
    }
  }
}

TEST(CarquetGoldenCompatibilityTest, ReadsEmptyAndAllNullRowGroups) {
  for (const bool batchRead : {false, true}) {
    SCOPED_TRACE(batchRead ? "batch" : "full");
    EXPECT_EQ(readFixture("arrow_empty.parquet", batchRead).context.row_num(),
              0u);

    const auto result = readFixture("arrow_all_null_groups.parquet", batchRead);
    const auto rows = materialize(result.context);
    ASSERT_EQ(rows.size(), 6u);
    for (size_t row = 0; row < rows.size(); ++row) {
      expectValue(rows, row, 0, static_cast<int64_t>(row + 1));
      EXPECT_TRUE(rows[row][1].IsNull());
      EXPECT_TRUE(rows[row][2].IsNull());
    }
  }
}

}  // namespace
}  // namespace neug::parquet
