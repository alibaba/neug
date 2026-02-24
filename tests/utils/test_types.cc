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

class PropertyTest : public ::testing::Test {
 protected:
  void SetUp() override {}
  void TearDown() override {}
};

TEST_F(PropertyTest, DefaultConstructor) {
  Property p;
  EXPECT_EQ(p.type(), DataTypeId::kEmpty);
}

TEST_F(PropertyTest, BoolProperty) {
  auto p1 = Property::from_bool(true);
  EXPECT_EQ(p1.type(), DataTypeId::kBoolean);
  EXPECT_TRUE(p1.as_bool());

  Property p2;
  p2.set_bool(false);
  EXPECT_FALSE(p2.as_bool());
}

TEST_F(PropertyTest, IntegerProperties) {
  {
    auto p = Property::from_int32(42);
    EXPECT_EQ(p.type(), DataTypeId::kInt32);
    EXPECT_EQ(p.as_int32(), 42);
  }
  {
    auto p = Property::from_uint32(100U);
    EXPECT_EQ(p.type(), DataTypeId::kUInt32);
    EXPECT_EQ(p.as_uint32(), 100U);
  }
  {
    auto p = Property::from_int64(-1234567890123LL);
    EXPECT_EQ(p.type(), DataTypeId::kInt64);
    EXPECT_EQ(p.as_int64(), -1234567890123LL);
  }
  {
    auto p = Property::from_uint64(9876543210ULL);
    EXPECT_EQ(p.type(), DataTypeId::kUInt64);
    EXPECT_EQ(p.as_uint64(), 9876543210ULL);
  }
}

TEST_F(PropertyTest, FloatProperties) {
  {
    auto p = Property::from_float(3.14f);
    EXPECT_EQ(p.type(), DataTypeId::kFloat);
    EXPECT_FLOAT_EQ(p.as_float(), 3.14f);
  }
  {
    auto p = Property::from_double(2.718281828);
    EXPECT_EQ(p.type(), DataTypeId::kDouble);
    EXPECT_DOUBLE_EQ(p.as_double(), 2.718281828);
  }
}

TEST_F(PropertyTest, StringViewProperty) {
  std::string str = "hello world";
  std::string_view sv = str;

  auto p = Property::from_string_view(sv);
  EXPECT_EQ(p.type(), DataTypeId::kVarchar);
  EXPECT_EQ(p.as_string_view(), sv);
  EXPECT_EQ(p.to_string(), "hello world");
}

TEST_F(PropertyTest, TemplateConstructor) {
  Property p1 = Property::from_int32(100);
  Property p2 = Property::from_int32(100);

  EXPECT_EQ(p1.type(), p2.type());
  EXPECT_EQ(p1.as_int32(), p2.as_int32());
}

TEST_F(PropertyTest, ToString) {
  EXPECT_EQ(Property::from_bool(true).to_string(), "true");
  EXPECT_EQ(Property::from_bool(false).to_string(), "false");
  EXPECT_EQ(Property::from_int32(-42).to_string(), "-42");
  EXPECT_EQ(Property::from_uint64(123ULL).to_string(), "123");
  EXPECT_EQ(Property::from_double(1.5).to_string(), "1.500000");
  EXPECT_EQ(Property::from_string_view("abc").to_string(), "abc");
  EXPECT_EQ(Property::from_string_view(std::string_view("xyz")).to_string(),
            "xyz");
  EXPECT_EQ(Property::empty().to_string(), "EMPTY");
}

TEST_F(PropertyTest, AsStringViewUnified) {
  {
    auto p = Property::from_string_view("hello");
    EXPECT_EQ(p.as_string_view(), "hello");
  }
  {
    auto p = Property::from_string_view(std::string_view("world"));
    EXPECT_EQ(p.as_string_view(), "world");
  }
}

TEST_F(PropertyTest, LessThan) {
  auto p1 = Property::from_int32(10);
  auto p2 = Property::from_int32(20);
  EXPECT_TRUE(p1 < p2);
  EXPECT_FALSE(p2 < p1);
}

TEST_F(PropertyTest, DateProperty) {
  Property p;
  p.set_date(uint32_t{33189664});
  auto d = p.as_date();
  EXPECT_EQ(d.to_u32(), 33189664U);
  EXPECT_EQ(p.to_string(), "2025-11-25");

  p.set_datetime(int64_t{1763365457000});
  EXPECT_EQ(p.as_datetime().to_string(), "2025-11-17 07:44:17.000");

  p.set_interval(Interval(std::string(
      "4years3months2days20hours3minutes12seconds200milliseconds")));
  EXPECT_EQ(
      p.as_interval().to_string(),
      "4 years 3 months 2 days 20 hours 3 minutes 12 seconds 200 milliseconds");
}

TEST_F(PropertyTest, AssignmentOperator) {
  auto p1 = Property::from_int32(42);
  auto p2 = p1;

  EXPECT_TRUE(p1 == p2);
}

TEST_F(PropertyTest, EqualityOperator) {
  auto p1 = Property::from_int32(42);
  auto p2 = Property::from_int32(42);
  auto p3 = Property::from_int32(43);
  auto p4 = Property::from_string_view("same");
  auto p5 = Property::from_string_view("same");
  auto p6 = Property::from_string_view("diff");

  EXPECT_TRUE(p1 == p2);
  EXPECT_FALSE(p1 == p3);
  EXPECT_TRUE(p4 == p5);
  EXPECT_FALSE(p4 == p6);
  EXPECT_FALSE(p1 == p4);
}

TEST_F(PropertyTest, TestArchive) {
  Property p = Property::from_bool(true);
  InArchive in;
  in << p;
  Property p2;
  OutArchive out;
  out.SetSlice(in.GetBuffer(), in.GetSize());
  out >> p2;
  EXPECT_EQ(p, p2);
}

TEST_F(PropertyTest, ParsePropertyFromString) {
  // bool
  EXPECT_EQ(parse_property_from_string(DataTypeId::kBoolean, "true").as_bool(),
            true);
  EXPECT_EQ(parse_property_from_string(DataTypeId::kBoolean, "1").as_bool(),
            true);
  EXPECT_EQ(parse_property_from_string(DataTypeId::kBoolean, "TRUE").as_bool(),
            true);
  EXPECT_EQ(parse_property_from_string(DataTypeId::kBoolean, "false").as_bool(),
            false);
  EXPECT_EQ(parse_property_from_string(DataTypeId::kBoolean, "0").as_bool(),
            false);

  // int32
  auto p_int32 = parse_property_from_string(DataTypeId::kInt32, "-42");
  EXPECT_EQ(p_int32.type(), DataTypeId::kInt32);
  EXPECT_EQ(p_int32.as_int32(), -42);

  // uint64
  auto p_uint64 =
      parse_property_from_string(DataTypeId::kUInt64, "18446744073709551615");
  EXPECT_EQ(p_uint64.type(), DataTypeId::kUInt64);
  EXPECT_EQ(p_uint64.as_uint64(), UINT64_MAX);

  // string_view
  auto p_sv = parse_property_from_string(DataTypeId{DataTypeId::kVarchar},
                                         "hello world");
  EXPECT_EQ(p_sv.type(), DataTypeId::kVarchar);
  EXPECT_EQ(p_sv.as_string_view(), "hello world");

  // float/double
  auto p_f = parse_property_from_string(DataTypeId::kFloat, "3.14");
  EXPECT_FLOAT_EQ(p_f.as_float(), 3.14f);

  auto p_d = parse_property_from_string(DataTypeId::kDouble, "2.71828");
  EXPECT_DOUBLE_EQ(p_d.as_double(), 2.71828);

  // empty
  auto p_empty = parse_property_from_string(DataTypeId::kEmpty, "");
  EXPECT_EQ(p_empty.type(), DataTypeId::kEmpty);
}

Property round_trip_property(const Property& input, InArchive& arc) {
  arc.Clear();
  serialize_property(arc, input);

  Property output;
  OutArchive oarc;
  oarc.SetSlice(arc.GetBuffer(), arc.GetSize());
  deserialize_property(oarc, input.type(), output);
  return output;
}

TEST_F(PropertyTest, SerializeDeserializePropertyRoundTrip) {
  InArchive arc;

  // bool
  {
    auto p = Property::from_bool(true);
    auto restored = round_trip_property(p, arc);
    EXPECT_EQ(p, restored);
  }

  // int64
  {
    auto p = Property::from_int64(-123456789012345LL);
    auto restored = round_trip_property(p, arc);
    EXPECT_EQ(p, restored);
  }

  // uint32
  {
    auto p = Property::from_uint32(4294967295U);
    auto restored = round_trip_property(p, arc);
    EXPECT_EQ(p, restored);
  }

  // string_view (VARCHAR)
  {
    auto p = Property::from_string_view("view only");
    auto restored = round_trip_property(p, arc);
    EXPECT_EQ(restored.type(), DataTypeId::kVarchar);
    EXPECT_EQ(restored.as_string_view(), p.as_string_view());
  }

  // Date
  {
    Property p;
    p.set_date(uint32_t{20230101});
    auto restored = round_trip_property(p, arc);
    EXPECT_EQ(p.as_date().to_u32(), restored.as_date().to_u32());
  }

  // DateTime
  {
    auto p =
        Property::from_datetime(DateTime(1672531200000LL));  // 2023-01-01 UTC
    auto restored = round_trip_property(p, arc);
    EXPECT_EQ(p.as_datetime().milli_second,
              restored.as_datetime().milli_second);
  }

  // Interval
  {
    Interval iv;
    iv.from_mill_seconds(3600000);  // 1 hour
    auto p = Property::from_interval(iv);
    auto restored = round_trip_property(p, arc);
    EXPECT_EQ(p.as_interval().to_mill_seconds(),
              restored.as_interval().to_mill_seconds());
  }

  // empty
  {
    auto p = Property::empty();
    auto restored = round_trip_property(p, arc);
    EXPECT_EQ(p.type(), DataTypeId::kEmpty);
    EXPECT_EQ(restored.type(), DataTypeId::kEmpty);
  }
}

}  // namespace test
}  // namespace neug