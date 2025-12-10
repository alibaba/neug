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

namespace gs {
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
  EXPECT_EQ(PropertyType::Empty().type_enum, impl::PropertyTypeImpl::kEmpty);
  EXPECT_EQ(PropertyType::Empty().ToString(), "Empty");
  EXPECT_EQ(PropertyType::Bool().type_enum, impl::PropertyTypeImpl::kBool);
  EXPECT_EQ(PropertyType::Bool().ToString(), "Bool");
  EXPECT_EQ(PropertyType::Int32().type_enum, impl::PropertyTypeImpl::kInt32);
  EXPECT_EQ(PropertyType::Int32().ToString(), "Int32");
  EXPECT_EQ(PropertyType::UInt32().type_enum, impl::PropertyTypeImpl::kUInt32);
  EXPECT_EQ(PropertyType::UInt32().ToString(), "UInt32");
  EXPECT_EQ(PropertyType::Int64().type_enum, impl::PropertyTypeImpl::kInt64);
  EXPECT_EQ(PropertyType::Int64().ToString(), "Int64");
  EXPECT_EQ(PropertyType::UInt64().type_enum, impl::PropertyTypeImpl::kUInt64);
  EXPECT_EQ(PropertyType::UInt64().ToString(), "UInt64");
  EXPECT_EQ(PropertyType::Float().type_enum, impl::PropertyTypeImpl::kFloat);
  EXPECT_EQ(PropertyType::Float().ToString(), "Float");
  EXPECT_EQ(PropertyType::Double().type_enum, impl::PropertyTypeImpl::kDouble);
  EXPECT_EQ(PropertyType::Double().ToString(), "Double");
  EXPECT_EQ(PropertyType::String().type_enum, impl::PropertyTypeImpl::kString);
  EXPECT_EQ(PropertyType::String().ToString(), "String");
  EXPECT_EQ(PropertyType::StringView().type_enum,
            impl::PropertyTypeImpl::kStringView);
  EXPECT_EQ(PropertyType::StringView().ToString(), "StringView");
  EXPECT_EQ(PropertyType::Varchar(16).type_enum,
            impl::PropertyTypeImpl::kVarChar);
  EXPECT_EQ(PropertyType::Varchar(16).ToString(), "VarChar");
  EXPECT_EQ(PropertyType::Date().type_enum, impl::PropertyTypeImpl::kDate);
  EXPECT_EQ(PropertyType::Date().ToString(), "Date");
  EXPECT_EQ(PropertyType::DateTime().type_enum,
            impl::PropertyTypeImpl::kDateTime);
  EXPECT_EQ(PropertyType::DateTime().ToString(), "DateTime");
  EXPECT_EQ(PropertyType::Interval().type_enum,
            impl::PropertyTypeImpl::kInterval);
  EXPECT_EQ(PropertyType::Interval().ToString(), "Interval");

  EXPECT_EQ(config_parsing::StringToPrimitivePropertyType(std::string("int32")),
            PropertyType::kInt32);
  EXPECT_EQ(
      config_parsing::StringToPrimitivePropertyType(std::string("uint32")),
      PropertyType::kUInt32);
  EXPECT_EQ(config_parsing::StringToPrimitivePropertyType(std::string("bool")),
            PropertyType::kBool);
  EXPECT_EQ(config_parsing::StringToPrimitivePropertyType(std::string("Date")),
            PropertyType::kDate);
  EXPECT_EQ(
      config_parsing::StringToPrimitivePropertyType(std::string("DateTime")),
      PropertyType::kDateTime);
  EXPECT_EQ(
      config_parsing::StringToPrimitivePropertyType(std::string("Interval")),
      PropertyType::kInterval);
  EXPECT_EQ(
      config_parsing::StringToPrimitivePropertyType(std::string("Timestamp")),
      PropertyType::kDateTime);
  EXPECT_EQ(
      config_parsing::StringToPrimitivePropertyType(std::string("String")),
      PropertyType::Varchar(PropertyType::GetStringDefaultMaxLength()));
  EXPECT_EQ(config_parsing::StringToPrimitivePropertyType(std::string("Empty")),
            PropertyType::kEmpty);
  EXPECT_EQ(config_parsing::StringToPrimitivePropertyType(std::string("int64")),
            PropertyType::kInt64);
  EXPECT_EQ(
      config_parsing::StringToPrimitivePropertyType(std::string("uint64")),
      PropertyType::kUInt64);
  EXPECT_EQ(config_parsing::StringToPrimitivePropertyType(std::string("float")),
            PropertyType::kFloat);
  EXPECT_EQ(
      config_parsing::StringToPrimitivePropertyType(std::string("double")),
      PropertyType::kDouble);

  EXPECT_EQ(config_parsing::PrimitivePropertyTypeToString(PropertyType::kEmpty),
            "Empty");
  EXPECT_EQ(config_parsing::PrimitivePropertyTypeToString(PropertyType::kBool),
            DT_BOOL);
  EXPECT_EQ(config_parsing::PrimitivePropertyTypeToString(PropertyType::kInt32),
            DT_SIGNED_INT32);
  EXPECT_EQ(
      config_parsing::PrimitivePropertyTypeToString(PropertyType::kUInt32),
      DT_UNSIGNED_INT32);
  EXPECT_EQ(config_parsing::PrimitivePropertyTypeToString(PropertyType::kInt64),
            DT_SIGNED_INT64);
  EXPECT_EQ(
      config_parsing::PrimitivePropertyTypeToString(PropertyType::kUInt64),
      DT_UNSIGNED_INT64);
  EXPECT_EQ(config_parsing::PrimitivePropertyTypeToString(PropertyType::kFloat),
            DT_FLOAT);
  EXPECT_EQ(
      config_parsing::PrimitivePropertyTypeToString(PropertyType::kDouble),
      DT_DOUBLE);
  EXPECT_EQ(
      config_parsing::PrimitivePropertyTypeToString(PropertyType::kStringView),
      DT_STRING);
  EXPECT_EQ(config_parsing::PrimitivePropertyTypeToString(PropertyType::kDate),
            DT_DATE);
  EXPECT_EQ(
      config_parsing::PrimitivePropertyTypeToString(PropertyType::kDateTime),
      DT_DATETIME);
  EXPECT_EQ(
      config_parsing::PrimitivePropertyTypeToString(PropertyType::kInterval),
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
  PropertyType property_type_value = PropertyType::kEmpty;
  std::string_view string_view_value(string_value);
  GlobalId global_id_value = GlobalId(2, 438);
  LabelKey label_key_value = LabelKey(5);
  Interval interval_value = Interval(std::string("2years"));

  grape::InArchive in_archive;
  in_archive << property_type_value << string_view_value << global_id_value
             << label_key_value << interval_value;
  grape::OutArchive out_archive;
  PropertyType out_property_type;
  std::string_view out_string_view;
  GlobalId out_global_id;
  LabelKey out_label_key;
  Interval out_interval;
  out_archive.SetSlice(in_archive.GetBuffer(), in_archive.GetSize());
  out_archive >> out_property_type >> out_string_view >> out_global_id >>
      out_label_key >> out_interval;
  EXPECT_EQ(property_type_value, out_property_type);
  EXPECT_EQ(string_view_value, out_string_view);
  EXPECT_EQ(global_id_value, out_global_id);
  EXPECT_EQ(label_key_value.label_id, out_label_key.label_id);
  EXPECT_EQ(interval_value, out_interval);
}

class PropertyTest : public ::testing::Test {
 protected:
  void SetUp() override {}
  void TearDown() override {}
};

TEST_F(PropertyTest, DefaultConstructor) {
  Property p;
  EXPECT_EQ(p.type(), PropertyType::kEmpty);
}

TEST_F(PropertyTest, BoolProperty) {
  auto p1 = Property::from_bool(true);
  EXPECT_EQ(p1.type(), PropertyType::kBool);
  EXPECT_TRUE(p1.as_bool());

  Property p2;
  p2.set_bool(false);
  EXPECT_FALSE(p2.as_bool());
}

TEST_F(PropertyTest, IntegerProperties) {
  {
    auto p = Property::from_int32(42);
    EXPECT_EQ(p.type(), PropertyType::kInt32);
    EXPECT_EQ(p.as_int32(), 42);
  }
  {
    auto p = Property::from_uint32(100U);
    EXPECT_EQ(p.type(), PropertyType::kUInt32);
    EXPECT_EQ(p.as_uint32(), 100U);
  }
  {
    auto p = Property::from_int64(-1234567890123LL);
    EXPECT_EQ(p.type(), PropertyType::kInt64);
    EXPECT_EQ(p.as_int64(), -1234567890123LL);
  }
  {
    auto p = Property::from_uint64(9876543210ULL);
    EXPECT_EQ(p.type(), PropertyType::kUInt64);
    EXPECT_EQ(p.as_uint64(), 9876543210ULL);
  }
}

TEST_F(PropertyTest, FloatProperties) {
  {
    auto p = Property::from_float(3.14f);
    EXPECT_EQ(p.type(), PropertyType::kFloat);
    EXPECT_FLOAT_EQ(p.as_float(), 3.14f);
  }
  {
    auto p = Property::from_double(2.718281828);
    EXPECT_EQ(p.type(), PropertyType::kDouble);
    EXPECT_DOUBLE_EQ(p.as_double(), 2.718281828);
  }
}

TEST_F(PropertyTest, StringViewProperty) {
  std::string str = "hello world";
  std::string_view sv = str;

  auto p = Property::from_string_view(sv);
  EXPECT_EQ(p.type().type_enum, impl::PropertyTypeImpl::kStringView);
  EXPECT_EQ(p.as_string_view(), sv);
  EXPECT_EQ(p.to_string(), "hello world");
}

TEST_F(PropertyTest, StringProperty) {
  std::string input = "owned string";
  auto p = Property::from_string(input);

  EXPECT_EQ(p.type().type_enum, impl::PropertyTypeImpl::kString);
  EXPECT_EQ(p.as_string(), input);
  EXPECT_EQ(p.to_string(), input);

  Property p2 = p;
  EXPECT_EQ(p2.as_string(), input);
  EXPECT_NE(&p.as_string(), &p2.as_string());
}

TEST_F(PropertyTest, TemplateConstructor) {
  Property p1 = Property::from_int32(100);
  Property p2(static_cast<int32_t>(100));

  EXPECT_EQ(p1.type(), p2.type());
  EXPECT_EQ(p1.as_int32(), p2.as_int32());
}

TEST_F(PropertyTest, CopyConstructor) {
  std::string s = "copy me";
  Property p1;
  p1.set_string(s);

  Property p2(p1);
  EXPECT_EQ(p2.type(), p1.type());
  EXPECT_EQ(p2.as_string(), s);
}

TEST_F(PropertyTest, MoveConstructor) {
  std::string s = "move me";
  Property p1;
  p1.set_string(s);

  Property p2(std::move(p1));
  EXPECT_EQ(p2.type().type_enum, impl::PropertyTypeImpl::kString);
  EXPECT_EQ(p2.as_string(), s);
}

TEST_F(PropertyTest, ToString) {
  EXPECT_EQ(Property::from_bool(true).to_string(), "true");
  EXPECT_EQ(Property::from_bool(false).to_string(), "false");
  EXPECT_EQ(Property::from_int32(-42).to_string(), "-42");
  EXPECT_EQ(Property::from_uint64(123ULL).to_string(), "123");
  EXPECT_EQ(Property::from_double(1.5).to_string(), "1.500000");
  EXPECT_EQ(Property::from_string("abc").to_string(), "abc");
  EXPECT_EQ(Property::from_string_view(std::string_view("xyz")).to_string(),
            "xyz");
  EXPECT_EQ(Property::empty().to_string(), "EMPTY");
}

TEST_F(PropertyTest, AsStringViewUnified) {
  {
    auto p = Property::from_string("hello");
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
  auto p4 = Property::from_string("same");
  auto p5 = Property::from_string("same");
  auto p6 = Property::from_string("diff");

  EXPECT_TRUE(p1 == p2);
  EXPECT_FALSE(p1 == p3);
  EXPECT_TRUE(p4 == p5);
  EXPECT_FALSE(p4 == p6);
  EXPECT_FALSE(p1 == p4);
}

TEST_F(PropertyTest, TestArchive) {
  Property p = Property::from_bool(true);
  grape::InArchive in;
  in << p;
  Property p2;
  grape::OutArchive out;
  out.SetSlice(in.GetBuffer(), in.GetSize());
  out >> p2;
  EXPECT_EQ(p, p2);
}

TEST_F(PropertyTest, ParsePropertyFromString) {
  // bool
  EXPECT_EQ(parse_property_from_string(PropertyType::kBool, "true").as_bool(),
            true);
  EXPECT_EQ(parse_property_from_string(PropertyType::kBool, "1").as_bool(),
            true);
  EXPECT_EQ(parse_property_from_string(PropertyType::kBool, "TRUE").as_bool(),
            true);
  EXPECT_EQ(parse_property_from_string(PropertyType::kBool, "false").as_bool(),
            false);
  EXPECT_EQ(parse_property_from_string(PropertyType::kBool, "0").as_bool(),
            false);

  // int32
  auto p_int32 = parse_property_from_string(PropertyType::kInt32, "-42");
  EXPECT_EQ(p_int32.type(), PropertyType::kInt32);
  EXPECT_EQ(p_int32.as_int32(), -42);

  // uint64
  auto p_uint64 =
      parse_property_from_string(PropertyType::kUInt64, "18446744073709551615");
  EXPECT_EQ(p_uint64.type(), PropertyType::kUInt64);
  EXPECT_EQ(p_uint64.as_uint64(), UINT64_MAX);

  // string_view
  auto p_sv = parse_property_from_string(
      PropertyType{impl::PropertyTypeImpl::kStringView}, "hello world");
  EXPECT_EQ(p_sv.type().type_enum, impl::PropertyTypeImpl::kStringView);
  EXPECT_EQ(p_sv.as_string_view(), "hello world");

  // float/double
  auto p_f = parse_property_from_string(PropertyType::kFloat, "3.14");
  EXPECT_FLOAT_EQ(p_f.as_float(), 3.14f);

  auto p_d = parse_property_from_string(PropertyType::kDouble, "2.71828");
  EXPECT_DOUBLE_EQ(p_d.as_double(), 2.71828);

  // empty
  auto p_empty = parse_property_from_string(PropertyType::kEmpty, "");
  EXPECT_EQ(p_empty.type(), PropertyType::kEmpty);
}

Property round_trip_property(const Property& input, grape::InArchive& arc) {
  arc.Clear();
  serialize_property(arc, input);

  Property output;
  grape::OutArchive oarc;
  oarc.SetSlice(arc.GetBuffer(), arc.GetSize());
  deserialize_property(oarc, input.type(), output);
  return output;
}

TEST_F(PropertyTest, SerializeDeserializePropertyRoundTrip) {
  grape::InArchive arc;

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

  // string (kString)
  {
    auto p = Property::from_string("owned string");
    auto restored = round_trip_property(p, arc);
    EXPECT_EQ(restored.type().type_enum, impl::PropertyTypeImpl::kString);
    EXPECT_EQ(restored.as_string(), "owned string");
  }

  // string_view (kStringView)
  {
    auto p = Property::from_string_view("view only");
    auto restored = round_trip_property(p, arc);
    EXPECT_EQ(restored.type().type_enum, impl::PropertyTypeImpl::kStringView);
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
    EXPECT_EQ(p.type(), PropertyType::kEmpty);
    EXPECT_EQ(restored.type(), PropertyType::kEmpty);
  }
}

TEST_F(PropertyTest, DeserializeStringSupported) {
  Property p_orig;
  p_orig.set_string("hello string");

  grape::InArchive arc;
  serialize_property(arc, p_orig);

  Property p_restored;
  grape::OutArchive oarc;
  oarc.SetSlice(arc.GetBuffer(), arc.GetSize());
  deserialize_property(oarc, PropertyType{impl::PropertyTypeImpl::kString},
                       p_restored);

  EXPECT_EQ(p_restored.type().type_enum, impl::PropertyTypeImpl::kString);
  EXPECT_EQ(p_restored.as_string(), "hello string");
}

}  // namespace test
}  // namespace gs