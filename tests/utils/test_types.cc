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
  EXPECT_EQ(PropertyType::Bool().type_enum, impl::PropertyTypeImpl::kBool);
  EXPECT_EQ(PropertyType::Int32().type_enum, impl::PropertyTypeImpl::kInt32);
  EXPECT_EQ(PropertyType::UInt32().type_enum, impl::PropertyTypeImpl::kUInt32);
  EXPECT_EQ(PropertyType::Int64().type_enum, impl::PropertyTypeImpl::kInt64);
  EXPECT_EQ(PropertyType::UInt64().type_enum, impl::PropertyTypeImpl::kUInt64);
  EXPECT_EQ(PropertyType::Float().type_enum, impl::PropertyTypeImpl::kFloat);
  EXPECT_EQ(PropertyType::Double().type_enum, impl::PropertyTypeImpl::kDouble);
  EXPECT_EQ(PropertyType::String().type_enum, impl::PropertyTypeImpl::kString);
  EXPECT_EQ(PropertyType::StringView().type_enum,
            impl::PropertyTypeImpl::kStringView);
  EXPECT_EQ(PropertyType::Varchar(16).type_enum,
            impl::PropertyTypeImpl::kVarChar);
  EXPECT_EQ(PropertyType::Date().type_enum, impl::PropertyTypeImpl::kDate);
  EXPECT_EQ(PropertyType::DateTime().type_enum,
            impl::PropertyTypeImpl::kDateTime);
  EXPECT_EQ(PropertyType::Interval().type_enum,
            impl::PropertyTypeImpl::kInterval);
}

}  // namespace test
}  // namespace gs