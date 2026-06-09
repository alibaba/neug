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
#include <filesystem>

#include "neug/execution/common/types/value.h"
#include "neug/utils/property/column.h"
#include "neug/utils/property/table.h"
#include "neug/utils/serialization/in_archive.h"
#include "neug/utils/serialization/out_archive.h"

namespace neug {
namespace test {
TEST(TestType, TestInterval) {
  Interval interval = Interval(
      std::string("4years3months2days20hours3minutes12seconds200milliseconds"));
  EXPECT_EQ(interval.year(), 4);
  EXPECT_EQ(interval.month(), 3);
  EXPECT_EQ(interval.day(), 2);
  EXPECT_EQ(interval.hour(), 20);
  EXPECT_EQ(interval.minute(), 3);
  EXPECT_EQ(interval.second(), 12);
  EXPECT_EQ(interval.millisecond(), 200);
  EXPECT_EQ(interval.to_mill_seconds(), 244992200);
  EXPECT_EQ(
      interval.to_string(),
      "4 years 3 months 2 days 20 hours 3 minutes 12 seconds 200 milliseconds");

  Interval left_interval, right_interval;
  left_interval.from_mill_seconds(244992200);
  right_interval =
      Interval(std::string("2days 20hours 3minutes 12seconds 200milliseconds"));
  EXPECT_EQ(left_interval, right_interval);

  Interval small_interval = Interval(std::string("59days 20hours"));
  Interval large_interval = Interval(std::string("2months 2hours"));
  EXPECT_GT(large_interval, small_interval);
  EXPECT_LT(small_interval, large_interval);
}

TEST(TestType, TestDate) {
  Date date = Date(std::string("2025-11-25"));
  EXPECT_EQ(date.to_num_days(), 20417);
  EXPECT_EQ(date.to_u32(), 33189664);
  EXPECT_EQ(date.year(), 2025);
  EXPECT_EQ(date.month(), 11);
  EXPECT_EQ(date.day(), 25);
  EXPECT_EQ(date.to_string(), "2025-11-25");
  EXPECT_EQ(date.to_timestamp(), 1764028800000);

  EXPECT_EQ(date, Date(int32_t{20417}));
  EXPECT_EQ(date, Date(int64_t{1764028800000}));
  Date u32_date;
  u32_date.from_u32(33189664);
  EXPECT_EQ(date, u32_date);

  Date previous_date = Date(std::string("2025-10-31"));
  Date next_date = Date(std::string("2025-12-09"));
  EXPECT_GT(date, previous_date);
  EXPECT_LT(date, next_date);

  EXPECT_EQ(Date(std::string("2025-01-31")) + Interval(std::string("1 month")),
            Date(std::string("2025-02-28")));
  EXPECT_EQ(Date(std::string("2023-12-31")) + Interval(std::string("2 months")),
            Date(std::string("2024-02-29")));
  EXPECT_EQ(
      Date(std::string("2025-01-31")) + Interval(std::string("1 month 3days")),
      Date(std::string("2025-03-03")));
  EXPECT_EQ(Date(std::string("2025-01-31")) +
                Interval(std::string("1 month 3days 76hours")),
            Date(std::string("2025-03-06")));
  EXPECT_EQ(Date(std::string("2025-01-31")) + Interval(std::string("96hours")),
            Date(std::string("2025-02-04")));
  EXPECT_EQ(Date(std::string("2025-01-31")) - Interval(std::string("96hours")),
            Date(std::string("2025-01-27")));
  Date start_date = Date(std::string("2025-01-31"));
  start_date += Interval(std::string("1 month"));
  EXPECT_EQ(start_date, Date(std::string("2025-02-28")));
  start_date -= Interval(std::string("1 month"));
  EXPECT_EQ(start_date, Date(std::string("2025-01-28")));

  EXPECT_EQ(Date(std::string("2025-02-04")) - Date(std::string("2025-01-31")),
            Interval(std::string("4days")));
}

TEST(TestType, TestDateTime) {
  DateTime datetime = DateTime(1763365457000);
  EXPECT_EQ(datetime.to_string(), "2025-11-17 07:44:17.000");

  DateTime str_datetime = DateTime(std::string("2025-11-17 07:44:17.000"));
  EXPECT_EQ(datetime, str_datetime);

  DateTime previous_datetime = DateTime(std::string("2025-10-31"));
  DateTime next_datetime = DateTime(std::string("2025-12-09 16:44:17.000"));
  EXPECT_GT(datetime, previous_datetime);
  EXPECT_LT(datetime, next_datetime);

  EXPECT_EQ(DateTime(std::string("2025-01-31 07:44:17.000")) +
                Interval(std::string("1 month")),
            DateTime(std::string("2025-02-28 07:44:17.000")));
  EXPECT_EQ(DateTime(std::string("2023-12-31 07:44:17.000")) +
                Interval(std::string("2 months")),
            DateTime(std::string("2024-02-29 07:44:17.000")));
  EXPECT_EQ(DateTime(std::string("2025-01-31 07:44:17.000")) +
                Interval(std::string("1 month 3days")),
            DateTime(std::string("2025-03-03 07:44:17.000")));
  EXPECT_EQ(DateTime(std::string("2025-01-31 07:44:17.000")) +
                Interval(std::string("1 month 3days 76hours")),
            DateTime(std::string("2025-03-06 11:44:17.000")));
  EXPECT_EQ(DateTime(std::string("2025-01-31 07:44:17.000")) +
                Interval(std::string("96hours 2minutes 30seconds")),
            DateTime(std::string("2025-02-04 07:46:47.000")));
  EXPECT_EQ(DateTime(std::string("2025-01-31 07:44:17.000")) -
                Interval(std::string("96hours 2minutes 30seconds")),
            DateTime(std::string("2025-01-27 07:41:47.000")));
  DateTime start_datetime = DateTime(std::string("2025-01-31 07:44:17.000"));
  start_datetime += Interval(std::string("1 month"));
  EXPECT_EQ(start_datetime, DateTime(std::string("2025-02-28 07:44:17.000")));
  start_datetime -= Interval(std::string("1 month"));
  EXPECT_EQ(start_datetime, DateTime(std::string("2025-01-28 07:44:17.000")));

  EXPECT_EQ(DateTime(std::string("2025-02-04 07:46:47.000")) -
                DateTime(std::string("2025-01-31 07:44:17.000")),
            Interval(std::string("4days 2minutes 30seconds")));
}

TEST(TestType, TestPropertyType) {
  EXPECT_EQ(std::to_string(DataTypeId::kEmpty), "Empty");
  EXPECT_EQ(std::to_string(DataTypeId::kBoolean), "Bool");
  EXPECT_EQ(std::to_string(DataTypeId::kInt32), "Int32");
  EXPECT_EQ(std::to_string(DataTypeId::kUInt32), "UInt32");
  EXPECT_EQ(std::to_string(DataTypeId::kInt64), "Int64");
  EXPECT_EQ(std::to_string(DataTypeId::kUInt64), "UInt64");
  EXPECT_EQ(std::to_string(DataTypeId::kFloat), "Float");
  EXPECT_EQ(std::to_string(DataTypeId::kDouble), "Double");
  EXPECT_EQ(std::to_string(DataTypeId::kVarchar), "StringView");
  EXPECT_EQ(std::to_string(DataTypeId::kDate), "Date");
  EXPECT_EQ(std::to_string(DataTypeId::kTimestampMs), "DateTime");
  EXPECT_EQ(std::to_string(DataTypeId::kInterval), "Interval");

  EXPECT_EQ(config_parsing::StringToPrimitivePropertyType(std::string("int32")),
            DataTypeId::kInt32);
  EXPECT_EQ(
      config_parsing::StringToPrimitivePropertyType(std::string("uint32")),
      DataTypeId::kUInt32);
  EXPECT_EQ(config_parsing::StringToPrimitivePropertyType(std::string("bool")),
            DataTypeId::kBoolean);
  EXPECT_EQ(config_parsing::StringToPrimitivePropertyType(std::string("Date")),
            DataTypeId::kDate);
  EXPECT_EQ(
      config_parsing::StringToPrimitivePropertyType(std::string("DateTime")),
      DataTypeId::kTimestampMs);
  EXPECT_EQ(
      config_parsing::StringToPrimitivePropertyType(std::string("Interval")),
      DataTypeId::kInterval);
  EXPECT_EQ(
      config_parsing::StringToPrimitivePropertyType(std::string("Timestamp")),
      DataTypeId::kTimestampMs);
  EXPECT_EQ(config_parsing::StringToPrimitivePropertyType(std::string("Empty")),
            DataTypeId::kEmpty);
  EXPECT_EQ(config_parsing::StringToPrimitivePropertyType(std::string("int64")),
            DataTypeId::kInt64);
  EXPECT_EQ(
      config_parsing::StringToPrimitivePropertyType(std::string("uint64")),
      DataTypeId::kUInt64);
  EXPECT_EQ(config_parsing::StringToPrimitivePropertyType(std::string("float")),
            DataTypeId::kFloat);
  EXPECT_EQ(
      config_parsing::StringToPrimitivePropertyType(std::string("double")),
      DataTypeId::kDouble);

  EXPECT_EQ(config_parsing::PrimitivePropertyTypeToString(DataTypeId::kEmpty),
            "Empty");
  EXPECT_EQ(config_parsing::PrimitivePropertyTypeToString(DataTypeId::kBoolean),
            DT_BOOL);
  EXPECT_EQ(config_parsing::PrimitivePropertyTypeToString(DataTypeId::kInt32),
            DT_SIGNED_INT32);
  EXPECT_EQ(config_parsing::PrimitivePropertyTypeToString(DataTypeId::kUInt32),
            DT_UNSIGNED_INT32);
  EXPECT_EQ(config_parsing::PrimitivePropertyTypeToString(DataTypeId::kInt64),
            DT_SIGNED_INT64);
  EXPECT_EQ(config_parsing::PrimitivePropertyTypeToString(DataTypeId::kUInt64),
            DT_UNSIGNED_INT64);
  EXPECT_EQ(config_parsing::PrimitivePropertyTypeToString(DataTypeId::kFloat),
            DT_FLOAT);
  EXPECT_EQ(config_parsing::PrimitivePropertyTypeToString(DataTypeId::kDouble),
            DT_DOUBLE);
  EXPECT_EQ(config_parsing::PrimitivePropertyTypeToString(DataTypeId::kVarchar),
            DT_STRING);
  EXPECT_EQ(config_parsing::PrimitivePropertyTypeToString(DataTypeId::kDate),
            DT_DATE);
  EXPECT_EQ(
      config_parsing::PrimitivePropertyTypeToString(DataTypeId::kTimestampMs),
      DT_DATETIME);
  EXPECT_EQ(
      config_parsing::PrimitivePropertyTypeToString(DataTypeId::kInterval),
      DT_INTERVAL);
}

TEST(TestType, TestGlobalId) {
  GlobalId global_id;
  global_id = GlobalId(2, 438);
  EXPECT_EQ(global_id.label_id(), 2);
  EXPECT_EQ(global_id.vid(), 438);
  EXPECT_EQ(GlobalId::get_label_id(global_id.global_id), 2);
  EXPECT_EQ(GlobalId::get_vid(global_id.global_id), 438);
}

TEST(TestType, TestArchive) {
  std::string string_value("test_value");
  DataTypeId property_type_value = DataTypeId::kEmpty;
  std::string_view string_view_value(string_value);
  GlobalId global_id_value = GlobalId(2, 438);
  Interval interval_value = Interval(std::string("2years"));

  InArchive in_archive;
  in_archive << property_type_value << string_view_value << global_id_value
             << interval_value;
  OutArchive out_archive;
  DataTypeId out_property_type;
  std::string_view out_string_view;
  GlobalId out_global_id;
  Interval out_interval;
  out_archive.SetSlice(in_archive.GetBuffer(), in_archive.GetSize());
  out_archive >> out_property_type >> out_string_view >> out_global_id >>
      out_interval;
  EXPECT_EQ(property_type_value, out_property_type);
  EXPECT_EQ(string_view_value, out_string_view);
  EXPECT_EQ(global_id_value, out_global_id);
  EXPECT_EQ(interval_value, out_interval);
}

class ValueTest : public ::testing::Test {
 protected:
  void SetUp() override {}
  void TearDown() override {}
};

TEST_F(ValueTest, DefaultConstructor) {
  execution::Value v;
  EXPECT_TRUE(v.IsNull());
}

TEST_F(ValueTest, BoolValue) {
  auto v1 = execution::Value::BOOLEAN(true);
  EXPECT_EQ(v1.type().id(), DataTypeId::kBoolean);
  EXPECT_TRUE(v1.GetValue<bool>());

  auto v2 = execution::Value::BOOLEAN(false);
  EXPECT_FALSE(v2.GetValue<bool>());
}

TEST_F(ValueTest, IntegerValues) {
  {
    auto v = execution::Value::INT32(42);
    EXPECT_EQ(v.type().id(), DataTypeId::kInt32);
    EXPECT_EQ(v.GetValue<int32_t>(), 42);
  }
  {
    auto v = execution::Value::UINT32(100U);
    EXPECT_EQ(v.type().id(), DataTypeId::kUInt32);
    EXPECT_EQ(v.GetValue<uint32_t>(), 100U);
  }
  {
    auto v = execution::Value::INT64(-1234567890123LL);
    EXPECT_EQ(v.type().id(), DataTypeId::kInt64);
    EXPECT_EQ(v.GetValue<int64_t>(), -1234567890123LL);
  }
  {
    auto v = execution::Value::UINT64(9876543210ULL);
    EXPECT_EQ(v.type().id(), DataTypeId::kUInt64);
    EXPECT_EQ(v.GetValue<uint64_t>(), 9876543210ULL);
  }
}

TEST_F(ValueTest, FloatValues) {
  {
    auto v = execution::Value::FLOAT(3.14f);
    EXPECT_EQ(v.type().id(), DataTypeId::kFloat);
    EXPECT_FLOAT_EQ(v.GetValue<float>(), 3.14f);
  }
  {
    auto v = execution::Value::DOUBLE(2.718281828);
    EXPECT_EQ(v.type().id(), DataTypeId::kDouble);
    EXPECT_DOUBLE_EQ(v.GetValue<double>(), 2.718281828);
  }
}

TEST_F(ValueTest, StringValue) {
  std::string str = "hello world";
  auto v = execution::Value::STRING(str);
  EXPECT_EQ(v.type().id(), DataTypeId::kVarchar);
  EXPECT_EQ(v.GetValue<std::string>(), str);
}

TEST_F(ValueTest, TemplateConstructor) {
  auto v1 = execution::Value::INT32(100);
  auto v2 = execution::Value::INT32(100);

  EXPECT_EQ(v1.type().id(), v2.type().id());
  EXPECT_EQ(v1.GetValue<int32_t>(), v2.GetValue<int32_t>());
}

TEST_F(ValueTest, GetStringValueUnified) {
  {
    auto v = execution::Value::STRING(std::string("hello"));
    EXPECT_EQ(v.GetValue<std::string>(), "hello");
  }
  {
    auto v = execution::Value::STRING(std::string("world"));
    EXPECT_EQ(v.GetValue<std::string>(), "world");
  }
}

TEST_F(ValueTest, LessThan) {
  auto v1 = execution::Value::INT32(10);
  auto v2 = execution::Value::INT32(20);
  EXPECT_TRUE(v1 < v2);
  EXPECT_FALSE(v2 < v1);
}

TEST_F(ValueTest, DateAndTimeValues) {
  Date d;
  d.from_u32(33189664);
  auto v_date = execution::Value::DATE(d);
  EXPECT_EQ(v_date.type().id(), DataTypeId::kDate);
  EXPECT_EQ(v_date.GetValue<Date>().to_u32(), 33189664U);

  auto v_dt = execution::Value::TIMESTAMPMS(DateTime(int64_t{1763365457000}));
  EXPECT_EQ(v_dt.type().id(), DataTypeId::kTimestampMs);
  EXPECT_EQ(v_dt.GetValue<DateTime>().to_string(), "2025-11-17 07:44:17.000");

  Interval iv(
      std::string("4years3months2days20hours3minutes12seconds200milliseconds"));
  auto v_iv = execution::Value::INTERVAL(iv);
  EXPECT_EQ(v_iv.type().id(), DataTypeId::kInterval);
  EXPECT_EQ(
      v_iv.GetValue<Interval>().to_string(),
      "4 years 3 months 2 days 20 hours 3 minutes 12 seconds 200 milliseconds");
}

TEST_F(ValueTest, AssignmentOperator) {
  auto v1 = execution::Value::INT32(42);
  auto v2 = v1;

  EXPECT_TRUE(v1 == v2);
}

TEST_F(ValueTest, EqualityOperator) {
  auto v1 = execution::Value::INT32(42);
  auto v2 = execution::Value::INT32(42);
  auto v3 = execution::Value::INT32(43);
  auto v4 = execution::Value::STRING(std::string("same"));
  auto v5 = execution::Value::STRING(std::string("same"));
  auto v6 = execution::Value::STRING(std::string("diff"));

  EXPECT_TRUE(v1 == v2);
  EXPECT_FALSE(v1 == v3);
  EXPECT_TRUE(v4 == v5);
  EXPECT_FALSE(v4 == v6);
  EXPECT_FALSE(v1 == v4);
}

}  // namespace test
}  // namespace neug