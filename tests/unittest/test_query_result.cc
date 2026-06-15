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

#include <gtest/gtest.h>

#include <cstdint>
#include <string>
#include <vector>

#include "neug/main/query_result.h"
#include "neug/generated/proto/response/response.pb.h"
#include "neug/utils/exception/exception.h"

namespace neug {

// ---------------------------------------------------------------------------
// Helper: build a QueryResponse protobuf with typed columns and serialize it.
// ---------------------------------------------------------------------------

static QueryResult BuildTestResult() {
  neug::QueryResponse response;
  response.set_row_count(3);

  auto* schema = response.mutable_schema();
  schema->add_name("id");
  schema->add_name("name");
  schema->add_name("score");
  schema->add_name("active");

  // Column 0: Int32Array {10, 20, 30}
  auto* col0 = response.add_arrays();
  auto* int32_arr = col0->mutable_int32_array();
  int32_arr->add_values(10);
  int32_arr->add_values(20);
  int32_arr->add_values(30);

  // Column 1: StringArray {"alice", "bob", "charlie"}
  auto* col1 = response.add_arrays();
  auto* str_arr = col1->mutable_string_array();
  str_arr->add_values("alice");
  str_arr->add_values("bob");
  str_arr->add_values("charlie");

  // Column 2: DoubleArray {1.1, 2.2, 3.3}
  auto* col2 = response.add_arrays();
  auto* dbl_arr = col2->mutable_double_array();
  dbl_arr->add_values(1.1);
  dbl_arr->add_values(2.2);
  dbl_arr->add_values(3.3);

  // Column 3: BoolArray {true, false, true}
  auto* col3 = response.add_arrays();
  auto* bool_arr = col3->mutable_bool_array();
  bool_arr->add_values(true);
  bool_arr->add_values(false);
  bool_arr->add_values(true);

  std::string serialized;
  response.SerializeToString(&serialized);
  return QueryResult::From(std::move(serialized));
}

// Build a result with a NULL in row 1 of the int32 column.
static QueryResult BuildResultWithNull() {
  neug::QueryResponse response;
  response.set_row_count(2);

  auto* schema = response.mutable_schema();
  schema->add_name("val");

  auto* col = response.add_arrays();
  auto* int32_arr = col->mutable_int32_array();
  int32_arr->add_values(42);
  int32_arr->add_values(0);  // placeholder

  // validity bitmap: row 0 valid, row 1 null → bit0=1, bit1=0 → byte = 0x01
  std::string validity(1, static_cast<char>(0x01));
  int32_arr->set_validity(validity);

  std::string serialized;
  response.SerializeToString(&serialized);
  return QueryResult::From(std::move(serialized));
}

// Build a result with date / timestamp / interval columns.
static QueryResult BuildTemporalResult() {
  neug::QueryResponse response;
  response.set_row_count(2);

  auto* schema = response.mutable_schema();
  schema->add_name("d");
  schema->add_name("ts");
  schema->add_name("iv");

  // Column 0: DateArray
  auto* col0 = response.add_arrays();
  auto* date_arr = col0->mutable_date_array();
  date_arr->add_values(0);              // 1970-01-01
  date_arr->add_values(86400LL * 1000); // 1970-01-02 (ms since epoch)

  // Column 1: TimestampArray
  auto* col1 = response.add_arrays();
  auto* ts_arr = col1->mutable_timestamp_array();
  ts_arr->add_values(0);
  ts_arr->add_values(1000);

  // Column 2: IntervalArray (string-backed; neug stores Interval::to_string()
  // output, e.g. "1 day", not ISO-8601 durations)
  auto* col2 = response.add_arrays();
  auto* iv_arr = col2->mutable_interval_array();
  iv_arr->add_values("1 day");
  iv_arr->add_values("1 hour");

  std::string serialized;
  response.SerializeToString(&serialized);
  return QueryResult::From(std::move(serialized));
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

TEST(QueryResultTest, LengthAndColumnInfo) {
  auto result = BuildTestResult();
  EXPECT_EQ(result.length(), 3u);
  EXPECT_EQ(result.ColumnCount(), 4u);

  auto names = result.ColumnNames();
  ASSERT_EQ(names.size(), 4u);
  EXPECT_EQ(names[0], "id");
  EXPECT_EQ(names[1], "name");
  EXPECT_EQ(names[2], "score");
  EXPECT_EQ(names[3], "active");
}

TEST(QueryResultTest, CursorTraversal) {
  auto result = BuildTestResult();

  EXPECT_TRUE(result.hasNext());
  EXPECT_EQ(result.CurrentRowIndex(), 0u);

  result.next();
  EXPECT_EQ(result.CurrentRowIndex(), 1u);

  result.next();
  result.next();
  EXPECT_FALSE(result.hasNext());

  // Next() past end should throw
  EXPECT_THROW(result.next(), neug::exception::Exception);

  // Reset brings cursor back
  result.Reset();
  EXPECT_TRUE(result.hasNext());
  EXPECT_EQ(result.CurrentRowIndex(), 0u);
}

TEST(QueryResultTest, TypedGetters) {
  auto result = BuildTestResult();

  // Row 0
  EXPECT_EQ(result.GetInt32(0), 10);
  EXPECT_EQ(result.GetString(1), "alice");
  EXPECT_DOUBLE_EQ(result.GetDouble(2), 1.1);
  EXPECT_TRUE(result.GetBool(3));

  result.next();
  // Row 1
  EXPECT_EQ(result.GetInt32(0), 20);
  EXPECT_EQ(result.GetString(1), "bob");
  EXPECT_DOUBLE_EQ(result.GetDouble(2), 2.2);
  EXPECT_FALSE(result.GetBool(3));

  result.next();
  // Row 2
  EXPECT_EQ(result.GetInt32(0), 30);
  EXPECT_EQ(result.GetString(1), "charlie");
  EXPECT_DOUBLE_EQ(result.GetDouble(2), 3.3);
  EXPECT_TRUE(result.GetBool(3));
}

TEST(QueryResultTest, ImplicitWideningInt32ToInt64) {
  auto result = BuildTestResult();

  // Int32Array column read as Int64
  int64_t val = result.GetInt64(0);
  EXPECT_EQ(val, 10);
}

TEST(QueryResultTest, ImplicitWideningInt32ToDouble) {
  auto result = BuildTestResult();

  // Int32Array column read as Double
  double val = result.GetDouble(0);
  EXPECT_DOUBLE_EQ(val, 10.0);
}

TEST(QueryResultTest, ImplicitWideningInt32ToFloat) {
  auto result = BuildTestResult();

  // Int32Array column read as Float
  float val = result.GetFloat(0);
  EXPECT_FLOAT_EQ(val, 10.0f);
}

TEST(QueryResultTest, ImplicitWideningBoolToInt32) {
  auto result = BuildTestResult();

  // BoolArray column (col 3) read as Int32
  EXPECT_EQ(result.GetInt32(3), 1);  // true → 1

  result.next();
  EXPECT_EQ(result.GetInt32(3), 0);  // false → 0
}

TEST(QueryResultTest, GetStringFallback) {
  auto result = BuildTestResult();

  // Non-string column returns string representation
  std::string int_as_str = result.GetString(0);
  EXPECT_EQ(int_as_str, "10");

  std::string bool_as_str = result.GetString(3);
  EXPECT_EQ(bool_as_str, "true");
}

TEST(QueryResultTest, NarrowingConversionThrows) {
  auto result = BuildTestResult();

  // DoubleArray cannot be read as Int32
  EXPECT_THROW(result.GetInt32(2), neug::exception::Exception);

  // StringArray cannot be read as Int64
  EXPECT_THROW(result.GetInt64(1), neug::exception::Exception);
}

TEST(QueryResultTest, IsNull) {
  auto result = BuildResultWithNull();

  // Row 0: not null
  EXPECT_FALSE(result.IsNull(0));
  EXPECT_EQ(result.GetInt32(0), 42);

  result.next();
  // Row 1: null
  EXPECT_TRUE(result.IsNull(0));
}

TEST(QueryResultTest, GetStringAnyType) {
  auto result = BuildTestResult();

  // GetString works on non-string columns via fallback
  EXPECT_EQ(result.GetString(0), "10");
  EXPECT_EQ(result.GetString(1), "alice");
  EXPECT_EQ(result.GetString(3), "true");
}

TEST(QueryResultTest, SerializeAndDeserialize) {
  auto original = BuildTestResult();
  std::string serialized = original.Serialize();
  auto restored = QueryResult::From(std::move(serialized));

  EXPECT_EQ(restored.length(), 3u);
  EXPECT_EQ(restored.ColumnCount(), 4u);
  EXPECT_EQ(restored.GetInt32(0), 10);
  EXPECT_EQ(restored.GetString(1), "alice");
}

TEST(QueryResultTest, EmptyResult) {
  neug::QueryResponse response;
  response.set_row_count(0);
  std::string serialized;
  response.SerializeToString(&serialized);
  auto result = QueryResult::From(std::move(serialized));

  EXPECT_EQ(result.length(), 0u);
  EXPECT_FALSE(result.hasNext());
  EXPECT_THROW(result.next(), neug::exception::Exception);
}

TEST(QueryResultTest, GetByColumnName) {
  auto result = BuildTestResult();

  EXPECT_EQ(result.GetInt32("id"), 10);
  EXPECT_EQ(result.GetString("name"), "alice");
  EXPECT_DOUBLE_EQ(result.GetDouble("score"), 1.1);
  EXPECT_TRUE(result.GetBool("active"));

  result.next();
  EXPECT_EQ(result.GetInt32("id"), 20);
  EXPECT_EQ(result.GetString("name"), "bob");
  EXPECT_FALSE(result.GetBool("active"));
}

TEST(QueryResultTest, GetByColumnNameWidening) {
  auto result = BuildTestResult();

  // Int32 column read as Int64 by name
  EXPECT_EQ(result.GetInt64("id"), 10);
  // Int32 column read as Double by name
  EXPECT_DOUBLE_EQ(result.GetDouble("id"), 10.0);
}

TEST(QueryResultTest, IsNullByColumnName) {
  auto result = BuildResultWithNull();

  EXPECT_FALSE(result.IsNull("val"));
  result.next();
  EXPECT_TRUE(result.IsNull("val"));
}

TEST(QueryResultTest, InvalidColumnNameThrows) {
  auto result = BuildTestResult();
  EXPECT_THROW(result.GetInt32("nonexistent"), neug::exception::Exception);
  EXPECT_THROW(result.IsNull("nonexistent"), neug::exception::Exception);
}

TEST(QueryResultTest, ColumnIndexOutOfRange) {
  auto result = BuildTestResult();
  EXPECT_THROW(result.GetInt32(99), neug::exception::Exception);
  EXPECT_THROW(result.IsNull(99), neug::exception::Exception);
}

TEST(QueryResultTest, TemporalAsRawInt64) {
  auto result = BuildTemporalResult();

  // Date / Timestamp columns expose their raw int64 epoch value via GetInt64().
  EXPECT_EQ(result.GetInt64(0), 0);
  EXPECT_EQ(result.GetInt64("d"), 0);
  EXPECT_EQ(result.GetInt64(1), 0);
  EXPECT_EQ(result.GetInt64("ts"), 0);

  result.next();
  EXPECT_EQ(result.GetInt64(0), 86400LL * 1000);
  EXPECT_EQ(result.GetInt64(1), 1000);

  // Interval is string-backed and has no int64 representation.
  EXPECT_THROW(result.GetInt64(2), neug::exception::Exception);
}

TEST(QueryResultTest, TemporalGetStringFallback) {
  auto result = BuildTemporalResult();

  // Date / Timestamp / Interval are all reachable via GetString().
  EXPECT_FALSE(result.GetString(0).empty());  // canonical date string
  EXPECT_FALSE(result.GetString(1).empty());  // canonical datetime string
  EXPECT_EQ(result.GetString(2), "1 day");
}

TEST(QueryResultTest, GetCurrentRowAsString) {
  auto result = BuildTestResult();

  EXPECT_EQ(result.GetCurrentRowAsString(), "10, alice, 1.1, true");

  result.next();
  EXPECT_EQ(result.GetCurrentRowAsString(), "20, bob, 2.2, false");

  // Past-end cursor should throw.
  result.next();
  result.next();
  EXPECT_FALSE(result.hasNext());
  EXPECT_THROW(result.GetCurrentRowAsString(), neug::exception::Exception);
}

TEST(QueryResultTest, GetCurrentRowAsStringWithNull) {
  auto result = BuildResultWithNull();
  EXPECT_EQ(result.GetCurrentRowAsString(), "42");
  result.next();
  EXPECT_EQ(result.GetCurrentRowAsString(), "null");
}

}  // namespace neug
