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

#include "neug/execution/common/types/value.h"
#include "neug/utils/property/property.h"

namespace neug {
namespace execution {
namespace test {
Date CreateTestDate() { return Date("2023-01-15"); }

DateTime CreateTestTimestamp() { return DateTime("2023-06-15"); }

Interval CreateTestInterval() { return Interval(std::string("1000seconds")); }

VertexRecord CreateTestVertex() {
  VertexRecord vertex;
  vertex.label_ = 1;
  vertex.vid_ = 12345;
  return vertex;
}

EdgeRecord CreateTestEdge() {
  EdgeRecord edge;
  edge.label = LabelTriplet(0, 0, 1);
  edge.src = 0;
  edge.dst = 1;
  edge.prop = nullptr;
  edge.dir = Direction::kOut;
  return edge;
}

Path CreateTestPath() {
  std::vector<std::pair<Direction, const void*>> edge_data = {
      std::make_pair(Direction::kOut, nullptr),
      std::make_pair(Direction::kOut, nullptr)};
  Path path(0, 1, {0, 1, 2}, edge_data);
  return path;
}

class ValueTest : public ::testing::Test {
 protected:
  void SetUp() override {}
  void TearDown() override {}
};

TEST_F(ValueTest, BooleanConstruction) {
  Value val = Value::BOOLEAN(true);
  EXPECT_EQ(val.type().id(), DataTypeId::kBoolean);
  EXPECT_FALSE(val.IsNull());
  EXPECT_TRUE(val.GetValue<bool>());
  EXPECT_TRUE(val.IsTrue());

  Value false_val = Value::BOOLEAN(false);
  EXPECT_FALSE(false_val.IsTrue());
}

TEST_F(ValueTest, IntegerConstruction) {
  Value i32_val = Value::INT32(42);
  EXPECT_EQ(i32_val.type().id(), DataTypeId::kInt32);
  EXPECT_EQ(i32_val.GetValue<int32_t>(), 42);

  Value i64_val = Value::INT64(123456789012345LL);
  EXPECT_EQ(i64_val.type().id(), DataTypeId::kInt64);
  EXPECT_EQ(i64_val.GetValue<int64_t>(), 123456789012345LL);

  Value u32_val = Value::UINT32(4294967295U);
  EXPECT_EQ(u32_val.type().id(), DataTypeId::kUInt32);
  EXPECT_EQ(u32_val.GetValue<uint32_t>(), 4294967295U);

  Value u64_val = Value::UINT64(18446744073709551615ULL);
  EXPECT_EQ(u64_val.type().id(), DataTypeId::kUInt64);
  EXPECT_EQ(u64_val.GetValue<uint64_t>(), 18446744073709551615ULL);
}
TEST_F(ValueTest, FloatingPointConstruction) {
  Value float_val = Value::FLOAT(3.14f);
  EXPECT_EQ(float_val.type().id(), DataTypeId::kFloat);
  EXPECT_FLOAT_EQ(float_val.GetValue<float>(), 3.14f);

  Value double_val = Value::DOUBLE(2.718281828459045);
  EXPECT_EQ(double_val.type().id(), DataTypeId::kDouble);
  EXPECT_DOUBLE_EQ(double_val.GetValue<double>(), 2.718281828459045);
}

TEST_F(ValueTest, StringConstruction) {
  std::string test_str = "hello world";
  Value str_val = Value::STRING(test_str);
  EXPECT_EQ(str_val.type().id(), DataTypeId::kVarchar);
  EXPECT_EQ(StringValue::Get(str_val), test_str);
  EXPECT_EQ(str_val.GetValue<std::string>(), test_str);
}

TEST_F(ValueTest, DateAndTimestampConstruction) {
  Date date = CreateTestDate();
  Value date_val = Value::DATE(date);
  EXPECT_EQ(date_val.type().id(), DataTypeId::kDate);
  EXPECT_EQ(date_val.GetValue<date_t>(), date);

  DateTime timestamp = CreateTestTimestamp();
  Value ts_val = Value::TIMESTAMPMS(timestamp);
  EXPECT_EQ(ts_val.type().id(), DataTypeId::kTimestampMs);
  EXPECT_EQ(ts_val.GetValue<timestamp_ms_t>(), timestamp);
}

TEST_F(ValueTest, IntervalConstruction) {
  Interval interval = CreateTestInterval();
  Value interval_val = Value::INTERVAL(interval);
  EXPECT_EQ(interval_val.type().id(), DataTypeId::kInterval);
  EXPECT_EQ(interval_val.GetValue<interval_t>(), interval);
}

TEST_F(ValueTest, VertexAndEdgeConstruction) {
  VertexRecord vertex = CreateTestVertex();
  Value vertex_val = Value::VERTEX(vertex);
  EXPECT_EQ(vertex_val.type().id(), DataTypeId::kVertex);
  EXPECT_EQ(vertex_val.GetValue<vertex_t>(), vertex);

  EdgeRecord edge = CreateTestEdge();
  Value edge_val = Value::EDGE(edge);
  EXPECT_EQ(edge_val.type().id(), DataTypeId::kEdge);
  EXPECT_EQ(edge_val.GetValue<edge_t>(), edge);
}

TEST_F(ValueTest, PathConstruction) {
  Path path = CreateTestPath();
  Value path_val = Value::PATH(path);
  EXPECT_EQ(path_val.type().id(), DataTypeId::kPath);
  EXPECT_EQ(PathValue::Get(path_val), path);
}

// List and Struct tests
TEST_F(ValueTest, ListConstruction) {
  std::vector<Value> values = {Value::INT32(1), Value::INT32(2),
                               Value::INT32(3)};

  Value list_val = Value::LIST(std::move(values));
  EXPECT_EQ(list_val.type().id(), DataTypeId::kList);
  const auto& children = ListValue::GetChildren(list_val);
  EXPECT_EQ(children.size(), 3);
  EXPECT_EQ(children[0].GetValue<int32_t>(), 1);
  EXPECT_EQ(children[1].GetValue<int32_t>(), 2);
  EXPECT_EQ(children[2].GetValue<int32_t>(), 3);
}

TEST_F(ValueTest, StructConstruction) {
  std::vector<Value> values = {Value::INT32(42), Value::STRING("test"),
                               Value::DOUBLE(3.14)};

  Value struct_val = Value::STRUCT(std::move(values));
  EXPECT_EQ(struct_val.type().id(), DataTypeId::kStruct);
  const auto& children = StructValue::GetChildren(struct_val);
  EXPECT_EQ(children.size(), 3);
  EXPECT_EQ(children[0].GetValue<int32_t>(), 42);
  EXPECT_EQ(StringValue::Get(children[1]), "test");
  EXPECT_DOUBLE_EQ(children[2].GetValue<double>(), 3.14);
}

TEST_F(ValueTest, NullValues) {
  Value null_val(DataType::INT32);
  EXPECT_TRUE(null_val.IsNull());
  EXPECT_EQ(null_val.type().id(), DataTypeId::kInt32);

  // Operations on null should return null
  Value other = Value::INT32(5);
  Value result = null_val + other;
  EXPECT_TRUE(result.IsNull());
}

TEST_F(ValueTest, EqualityComparison) {
  Value val1 = Value::INT32(42);
  Value val2 = Value::INT32(42);
  Value val3 = Value::INT32(43);

  EXPECT_TRUE(val1 == val2);
  EXPECT_FALSE(val1 == val3);

  Value str_val = Value::STRING("42");
  EXPECT_FALSE(val1 == str_val);

  Value null1(DataType::INT32);
  Value null2(DataType::INT32);
  EXPECT_FALSE(null1 == null2);
}

TEST_F(ValueTest, LessThanComparison) {
  Value val1 = Value::INT32(10);
  Value val2 = Value::INT32(20);

  EXPECT_TRUE(val1 < val2);
  EXPECT_FALSE(val2 < val1);

  // Same values
  Value val3 = Value::INT32(10);
  EXPECT_FALSE(val1 < val3);
}

TEST_F(ValueTest, ArithmeticOperations) {
  Value val1 = Value::INT32(10);
  Value val2 = Value::INT32(5);

  // Addition
  Value sum = val1 + val2;
  EXPECT_EQ(sum.GetValue<int32_t>(), 15);

  // Subtraction
  Value diff = val1 - val2;
  EXPECT_EQ(diff.GetValue<int32_t>(), 5);

  // Multiplication
  Value prod = val1 * val2;
  EXPECT_EQ(prod.GetValue<int32_t>(), 50);

  // Division
  Value quot = val1 / val2;
  EXPECT_EQ(quot.GetValue<int32_t>(), 2);

  // Modulo
  Value mod = val1 % val2;
  EXPECT_EQ(mod.GetValue<int32_t>(), 0);

  Value date_val = Value::DATE(std::string("2020-01-01"));
  Value datetime_val = Value::TIMESTAMPMS(DateTime(std::string("2020-01-01")));
  Value interval_val = Value::INTERVAL(Interval(std::string("3days")));

  EXPECT_EQ(date_val + interval_val,
            Value::DATE(Date(std::string("2020-01-04"))));
  EXPECT_EQ(interval_val + date_val,
            Value::DATE(Date(std::string("2020-01-04"))));

  EXPECT_EQ(datetime_val + interval_val,
            Value::TIMESTAMPMS(DateTime(std::string("2020-01-04"))));
  EXPECT_EQ(interval_val + datetime_val,
            Value::TIMESTAMPMS(DateTime(std::string("2020-01-04"))));

  EXPECT_EQ(date_val - interval_val,
            Value::DATE(Date(std::string("2019-12-29"))));
  EXPECT_EQ(datetime_val - interval_val,
            Value::TIMESTAMPMS(DateTime(std::string("2019-12-29"))));
}

TEST_F(ValueTest, FloatingPointArithmetic) {
  Value val1 = Value::DOUBLE(10.5);
  Value val2 = Value::DOUBLE(3.2);

  Value sum = val1 + val2;
  EXPECT_DOUBLE_EQ(sum.GetValue<double>(), 13.7);

  Value prod = val1 * val2;
  EXPECT_DOUBLE_EQ(prod.GetValue<double>(), 33.6);
}

TEST_F(ValueTest, DateArithmetic) {
  Date date = CreateTestDate();
  Interval interval = CreateTestInterval();

  Value date_val = Value::DATE(date);
  Value interval_val = Value::INTERVAL(interval);

  // Date + Interval
  Value result = date_val + interval_val;
  EXPECT_EQ(result.type().id(), DataTypeId::kDate);

  // Date - Interval
  Value result2 = date_val - interval_val;
  EXPECT_EQ(result2.type().id(), DataTypeId::kDate);

  // Date - Date = Interval
  Value date2_val = Value::DATE(Date("2023-01-10"));
  Value diff = date_val - date2_val;
  EXPECT_EQ(diff.type().id(), DataTypeId::kInterval);
}

TEST_F(ValueTest, TimestampArithmetic) {
  DateTime ts = CreateTestTimestamp();
  Interval interval = CreateTestInterval();

  Value ts_val = Value::TIMESTAMPMS(ts);
  Value interval_val = Value::INTERVAL(interval);

  // Timestamp + Interval
  Value result = ts_val + interval_val;
  EXPECT_EQ(result.type().id(), DataTypeId::kTimestampMs);

  // Timestamp - Timestamp = Interval
  Value ts2_val = Value::TIMESTAMPMS(DateTime("2023-06-10"));
  Value diff = ts_val - ts2_val;
  EXPECT_EQ(diff.type().id(), DataTypeId::kInterval);
}

TEST_F(ValueTest, ToStringConversion) {
  Value none_val(DataType::SQLNULL);
  EXPECT_EQ(none_val.to_string(), "NULL");

  Value int_val = Value::INT32(42);
  EXPECT_EQ(int_val.to_string(), "42");

  Value str_val = Value::STRING("hello");
  EXPECT_EQ(str_val.to_string(), "hello");

  std::vector<Value> int32_list = {Value::INT32(0), Value::INT32(1),
                                   Value::INT32(2)};
  Value list_val = Value::LIST(std::move(int32_list));
  EXPECT_EQ(list_val.to_string(), "[0, 1, 2]");

  std::vector<Value> int64_list = {Value::INT64(0), Value::INT64(1),
                                   Value::INT64(2)};
  Value struct_val = Value::STRUCT(std::move(int64_list));
  EXPECT_EQ(struct_val.to_string(), "(0, 1, 2)");

  VertexRecord vertex = CreateTestVertex();
  Value vertex_val = Value::CreateValue(vertex);
  EXPECT_EQ(vertex_val.to_string(), "(1, 12345)");

  EdgeRecord edge = CreateTestEdge();
  Value edge_val = Value::CreateValue(edge);
  EXPECT_EQ(edge_val.to_string(), "(0, 0) -[1]-> (0, 1)");
}

TEST_F(ValueTest, JsonSerialization) {
  rapidjson::Document::AllocatorType allocator;

  // Test basic types
  Value int_val = Value::INT32(42);
  auto json_val = Value::ToJson(int_val, allocator);
  EXPECT_TRUE(json_val.IsInt());
  EXPECT_EQ(json_val.GetInt(), 42);

  Value str_val = Value::STRING("hello");
  auto json_str = Value::ToJson(str_val, allocator);
  EXPECT_TRUE(json_str.IsString());
  EXPECT_STREQ(json_str.GetString(), "hello");
}

TEST_F(ValueTest, JsonDeserialization) {
  // Test from string
  Value int_from_json = Value::FromJson("42", DataType::INT32);
  EXPECT_EQ(int_from_json.GetValue<int32_t>(), 42);

  Value str_from_json = Value::FromJson("\"hello\"", DataType::VARCHAR);
  EXPECT_EQ(StringValue::Get(str_from_json), "hello");

  Value bool_from_json = Value::FromJson("true", DataType::BOOLEAN);
  EXPECT_TRUE(bool_from_json.GetValue<bool>());

  // Test null
  Value null_from_json = Value::FromJson("null", DataType::INT32);
  EXPECT_TRUE(null_from_json.IsNull());
}

TEST_F(ValueTest, CopyAndMoveSemantics) {
  Value original = Value::INT32(42);

  // Copy constructor
  Value copy(original);
  EXPECT_EQ(copy.GetValue<int32_t>(), 42);
  EXPECT_EQ(original.GetValue<int32_t>(), 42);

  // Move constructor
  Value moved(std::move(original));
  EXPECT_EQ(moved.GetValue<int32_t>(), 42);
  // original is now in valid but unspecified state

  // Copy assignment
  Value assigned = Value::STRING("test");
  assigned = copy;
  EXPECT_EQ(assigned.GetValue<int32_t>(), 42);

  // Move assignment
  Value moved_assigned = Value::STRING("original");
  moved_assigned = std::move(moved);
  EXPECT_EQ(moved_assigned.GetValue<int32_t>(), 42);
}

TEST_F(ValueTest, CreateValueTemplate) {
  Value bool_val = Value::CreateValue(true);
  EXPECT_EQ(bool_val.GetValue<bool>(), true);

  Value int_val = Value::CreateValue<int32_t>(42);
  EXPECT_EQ(int_val.GetValue<int32_t>(), 42);

  Value str_val = Value::CreateValue(std::string("test"));
  EXPECT_EQ(StringValue::Get(str_val), "test");

  VertexRecord vertex = CreateTestVertex();
  Value vertex_val = Value::CreateValue(vertex);
  EXPECT_EQ(vertex_val.GetValue<VertexRecord>(), vertex);

  EdgeRecord edge = CreateTestEdge();
  Value edge_val = Value::CreateValue(edge);
  EXPECT_EQ(edge_val.GetValue<EdgeRecord>(), edge);

  Value int64_val = Value::CreateValue<int64_t>(42);
  EXPECT_EQ(Value::CreateValue(int64_val), int64_val.GetValue<Value>());
}

TEST_F(ValueTest, GetValueTemplate) {
  Value val = Value::INT64(123456789012345LL);
  int64_t result = val.GetValue<int64_t>();
  EXPECT_EQ(result, 123456789012345LL);

  Value str_val = Value::STRING("hello");
  std::string str_result = str_val.GetValue<std::string>();
  EXPECT_EQ(str_result, "hello");
}

TEST_F(ValueTest, PropertyConversion) {
  Value original_bool = Value::BOOLEAN(true);
  Property prop_bool = value_to_property(original_bool);
  Value converted_bool = property_to_value(prop_bool);
  EXPECT_TRUE(converted_bool == original_bool);

  Value original_int32 = Value::INT32(-42);
  Property prop_int32 = value_to_property(original_int32);
  Value converted_int32 = property_to_value(prop_int32);
  EXPECT_TRUE(converted_int32 == original_int32);

  Value original_int64 = Value::INT64(123456789012345LL);
  Property prop_int64 = value_to_property(original_int64);
  Value converted_int64 = property_to_value(prop_int64);
  EXPECT_TRUE(converted_int64 == original_int64);

  Value original_uint32 = Value::UINT32(428);
  Property prop_uint32 = value_to_property(original_uint32);
  Value converted_uint32 = property_to_value(prop_uint32);
  EXPECT_TRUE(converted_uint32 == original_uint32);

  Value original_uint64 = Value::UINT64(123456789012345ULL);
  Property prop_uint64 = value_to_property(original_uint64);
  Value converted_uint64 = property_to_value(prop_uint64);
  EXPECT_TRUE(converted_uint64 == original_uint64);

  Value original_str = Value::STRING("round trip test");
  Property prop_str = value_to_property(original_str);
  Value converted_str = property_to_value(prop_str);
  EXPECT_TRUE(converted_str == original_str);

  Value original_float = Value::FLOAT(3.1415);
  Property prop_float = value_to_property(original_float);
  Value converted_float = property_to_value(prop_float);
  EXPECT_TRUE(converted_float == original_float);

  Value original_double = Value::DOUBLE(3.141592653589793);
  Property prop_double = value_to_property(original_double);
  Value converted_double = property_to_value(prop_double);
  EXPECT_TRUE(converted_double == original_double);

  Value original_date = Value::DATE(Date(std::string("2020-01-01")));
  Property prop_date = value_to_property(original_date);
  Value converted_date = property_to_value(prop_date);
  EXPECT_TRUE(converted_date == original_date);

  Value original_datetime = Value::TIMESTAMPMS(DateTime(293092399));
  Property prop_datetime = value_to_property(original_datetime);
  Value converted_datetime = property_to_value(prop_datetime);
  EXPECT_TRUE(converted_datetime == original_datetime);

  Value original_interval = Value::INTERVAL(Interval(std::string("3years")));
  Property prop_interval = value_to_property(original_interval);
  Value converted_interval = property_to_value(prop_interval);
  EXPECT_TRUE(converted_interval == original_interval);
}

TEST_F(ValueTest, EdgeCases) {
  EXPECT_THROW({ Value::LIST(std::vector<Value>()); }, std::runtime_error);

  // LOG(FATAL) calls abort(); EXPECT_DEATH is unreliable under sanitizers.
#if !defined(__SANITIZE_ADDRESS__) && !defined(__SANITIZE_THREAD__) && \
    !defined(UNDEFINED_SANITIZER)
  EXPECT_DEATH({ ValueConverter<bool>::typed_from_string("invalid"); },
               "Invalid boolean string");
#endif
}

TEST_F(ValueTest, TypeConverter) {
  EXPECT_EQ(ValueConverter<int32_t>::type().id(), DataTypeId::kInt32);
  EXPECT_EQ(ValueConverter<int64_t>::type().id(), DataTypeId::kInt64);
  EXPECT_EQ(ValueConverter<uint32_t>::type().id(), DataTypeId::kUInt32);
  EXPECT_EQ(ValueConverter<uint64_t>::type().id(), DataTypeId::kUInt64);
  EXPECT_EQ(ValueConverter<float>::type().id(), DataTypeId::kFloat);
  EXPECT_EQ(ValueConverter<double>::type().id(), DataTypeId::kDouble);
  EXPECT_EQ(ValueConverter<bool>::type().id(), DataTypeId::kBoolean);
  EXPECT_EQ(ValueConverter<std::string>::type().id(), DataTypeId::kVarchar);
  EXPECT_EQ(ValueConverter<date_t>::type().id(), DataTypeId::kDate);
  EXPECT_EQ(ValueConverter<timestamp_ms_t>::type().id(),
            DataTypeId::kTimestampMs);
  EXPECT_EQ(ValueConverter<interval_t>::type().id(), DataTypeId::kInterval);
  EXPECT_EQ(ValueConverter<vertex_t>::type().id(), DataTypeId::kVertex);
  EXPECT_EQ(ValueConverter<edge_t>::type().id(), DataTypeId::kEdge);

  EXPECT_EQ(ValueConverter<int32_t>::name(), "int32");
  EXPECT_EQ(ValueConverter<int64_t>::name(), "int64");
  EXPECT_EQ(ValueConverter<uint32_t>::name(), "uint32");
  EXPECT_EQ(ValueConverter<uint64_t>::name(), "uint64");
  EXPECT_EQ(ValueConverter<float>::name(), "float");
  EXPECT_EQ(ValueConverter<double>::name(), "double");
  EXPECT_EQ(ValueConverter<bool>::name(), "bool");
  EXPECT_EQ(ValueConverter<std::string>::name(), "string");
  EXPECT_EQ(ValueConverter<date_t>::name(), "date");
  EXPECT_EQ(ValueConverter<timestamp_ms_t>::name(), "timestamp_ms");
  EXPECT_EQ(ValueConverter<interval_t>::name(), "interval");

  EXPECT_EQ(ValueConverter<int32_t>::typed_from_string("123"), 123);
  EXPECT_EQ(ValueConverter<int32_t>::typed_from_string("-456"), -456);

  EXPECT_EQ(ValueConverter<int64_t>::typed_from_string("123456789012345"),
            123456789012345LL);
  EXPECT_EQ(ValueConverter<int64_t>::typed_from_string("-987654321098765"),
            -987654321098765LL);

  EXPECT_EQ(ValueConverter<uint32_t>::typed_from_string("4294967295"),
            4294967295U);

  EXPECT_EQ(ValueConverter<uint64_t>::typed_from_string("18446744073709551615"),
            18446744073709551615ULL);

  EXPECT_FLOAT_EQ(ValueConverter<float>::typed_from_string("3.14159"),
                  3.14159f);
  EXPECT_FLOAT_EQ(ValueConverter<float>::typed_from_string("-2.718"), -2.718f);

  EXPECT_DOUBLE_EQ(
      ValueConverter<double>::typed_from_string("2.718281828459045"),
      2.718281828459045);
  EXPECT_DOUBLE_EQ(
      ValueConverter<double>::typed_from_string("-3.141592653589793"),
      -3.141592653589793);

  EXPECT_TRUE(ValueConverter<bool>::typed_from_string("true"));
  EXPECT_TRUE(ValueConverter<bool>::typed_from_string("1"));
  EXPECT_FALSE(ValueConverter<bool>::typed_from_string("false"));
  EXPECT_FALSE(ValueConverter<bool>::typed_from_string("0"));

  Date expected_date("2023-06-15");
  Date actual_date = ValueConverter<date_t>::typed_from_string("2023-06-15");
  EXPECT_EQ(actual_date, expected_date);

  DateTime expected_datetime("2023-12-25 10:30:45");
  DateTime actual_datetime =
      ValueConverter<timestamp_ms_t>::typed_from_string("2023-12-25 10:30:45");
  EXPECT_EQ(actual_datetime, expected_datetime);

  Interval expected_interval(
      Interval(std::string("1day2hours3minutes4seconds")));
  Interval actual_interval = ValueConverter<interval_t>::typed_from_string(
      "1day2hours3minutes4seconds");
  EXPECT_EQ(actual_interval, expected_interval);

  int32_t result_int32;
  EXPECT_TRUE(ValueConverter<int32_t>::cast(std::string("123"), result_int32));
  EXPECT_EQ(result_int32, 123);

  int64_t result_int64;
  EXPECT_TRUE(
      ValueConverter<int64_t>::cast(std::string("-456789"), result_int64));
  EXPECT_EQ(result_int64, -456789LL);

  uint32_t result_uint32;
  EXPECT_TRUE(
      ValueConverter<uint32_t>::cast(std::string("4294967295"), result_uint32));
  EXPECT_EQ(result_uint32, 4294967295U);

  uint64_t result_uint64;
  EXPECT_TRUE(ValueConverter<uint64_t>::cast(
      std::string("18446744073709551615"), result_uint64));
  EXPECT_EQ(result_uint64, 18446744073709551615ULL);

  float result_float;
  EXPECT_TRUE(
      ValueConverter<float>::cast(std::string("3.14159"), result_float));
  EXPECT_FLOAT_EQ(result_float, 3.14159f);

  double result_double;
  EXPECT_TRUE(ValueConverter<double>::cast(std::string("2.718281828459045"),
                                           result_double));
  EXPECT_DOUBLE_EQ(result_double, 2.718281828459045);

  Date date_result;
  DateTime ts = CreateTestTimestamp();
  EXPECT_TRUE(ValueConverter<date_t>::cast(ts, date_result));
  EXPECT_EQ(date_result.to_timestamp(), ts.milli_second);

  DateTime ts_result;
  Date date = CreateTestDate();
  EXPECT_TRUE(ValueConverter<timestamp_ms_t>::cast(date, ts_result));
  EXPECT_EQ(ts_result.milli_second, date.to_timestamp());
}

TEST_F(ValueTest, PerformCast) {
  // int32 to int64
  Value int32_val = Value::INT32(42);
  Value int64_result = performCast<int64_t>(int32_val);
  EXPECT_EQ(int64_result.GetValue<int64_t>(), 42);

  // int64 to int32 (within range)
  Value int64_val = Value::INT64(100);
  Value int32_result = performCast<int32_t>(int64_val);
  EXPECT_EQ(int32_result.GetValue<int32_t>(), 100);

  // float to double
  Value float_val = Value::FLOAT(2.718f);
  Value double_result = performCast<double>(float_val);
  EXPECT_FLOAT_EQ(static_cast<float>(double_result.GetValue<double>()), 2.718f);

  Value str_date = Value::STRING("2023-06-15");
  Value date_result = performCast<Date>(str_date);
  EXPECT_EQ(date_result.type().id(), DataTypeId::kDate);
  Value str_datetime_result = performCast<DateTime>(str_date);
  EXPECT_EQ(str_datetime_result.type().id(), DataTypeId::kTimestampMs);
}
}  // namespace test
}  // namespace execution
}  // namespace neug
