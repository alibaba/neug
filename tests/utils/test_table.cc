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

static constexpr const char* TEST_DIR = "/tmp/table_test";

static const std::vector<bool> bool_data = {1, 0, 0, 1, 1, 0, 1, 0, 1, 1};
static const std::vector<int32_t> int32_data = {1, 4, -1, 2, 9, 2, 4, 3, 1, -2};
static const std::vector<uint32_t> uint32_data = {0, 5, 2, 9, 3, 4, 3, 5, 7, 0};
static const std::vector<int64_t> int64_data = {1, 4, -1, 2, 9, 2, 4, 3, 1, -2};
static const std::vector<uint64_t> uint64_data = {0, 5, 2, 9, 3, 4, 3, 5, 7, 0};
static const std::vector<float> float_data = {1.0, 4.5,  -1.3, 2.2, 9.7,
                                              2.4, 4.12, 3.6,  1.8, -2.49};
static const std::vector<double> double_data = {1.0, 4.5,  -1.3, 2.2, 9.7,
                                                2.4, 4.12, 3.6,  1.8, -2.49};
static const std::vector<neug::Date> date_data = {
    neug::Date(0), neug::Date(5), neug::Date(2), neug::Date(9), neug::Date(3),
    neug::Date(4), neug::Date(3), neug::Date(5), neug::Date(7), neug::Date(0)};
static const std::vector<neug::DateTime> datetime_data = {
    neug::DateTime(0), neug::DateTime(5), neug::DateTime(2), neug::DateTime(9),
    neug::DateTime(3), neug::DateTime(4), neug::DateTime(3), neug::DateTime(5),
    neug::DateTime(7), neug::DateTime(0)};
static const std::vector<neug::Interval> interval_data = {
    neug::Interval(std::string("0hour")),
    neug::Interval(std::string("5hours")),
    neug::Interval(std::string("2minutes")),
    neug::Interval(std::string("9hours")),
    neug::Interval(std::string("3seconds")),
    neug::Interval(std::string("4days")),
    neug::Interval(std::string("3years")),
    neug::Interval(std::string("5milliseconds")),
    neug::Interval(std::string("7minutes")),
    neug::Interval(std::string("0day"))};

static const std::vector<std::string> string_data = {
    std::string("0hour"),    std::string("5hours"),
    std::string("2minutes"), std::string("9hours"),
    std::string("3seconds"), std::string("4days"),
    std::string("3years"),   std::string("5milliseconds"),
    std::string("7minutes"), std::string("0day")};

namespace neug {
namespace test {
TEST(TableTest, TestTableBasic) {
  if (std::filesystem::exists(TEST_DIR)) {
    std::filesystem::remove_all(TEST_DIR);
  }
  std::filesystem::create_directories(TEST_DIR);
  std::filesystem::create_directories(std::string(TEST_DIR) + "/checkpoint");
  std::filesystem::create_directories(std::string(TEST_DIR) + "/runtime/tmp");

  Table disk_table, mem_table, none_table;

  std::vector<std::string> col_name = {
      "bool_column",     "int32_column",    "uint32_column", "int64_column",
      "uint64_column",   "float_column",    "double_column", "date_column",
      "datetime_column", "interval_column", "string_column"};

  std::vector<DataType> property_types = {
      {DataTypeId::kBoolean},     {DataTypeId::kInt32},
      {DataTypeId::kUInt32},      {DataTypeId::kInt64},
      {DataTypeId::kUInt64},      {DataTypeId::kFloat},
      {DataTypeId::kDouble},      {DataTypeId::kDate},
      {DataTypeId::kTimestampMs}, {DataTypeId::kInterval},
      {DataTypeId::kVarchar}};

  disk_table.open("test_dist", TEST_DIR, col_name, property_types);
  mem_table.open("test_dist", TEST_DIR, col_name, property_types);
  none_table.open("test_dist", TEST_DIR, col_name, property_types);

  disk_table.resize(10);
  mem_table.resize(10);
  none_table.resize(10);
  size_t index = 0;
  for (size_t i = 0; i < 10; i++) {
    disk_table.get_column("bool_column")
        ->set_prop(index, Property::from_bool(bool_data[i]));
    mem_table.get_column("bool_column")
        ->set_prop(index, Property::from_bool(bool_data[i]));

    disk_table.get_column("int32_column")
        ->set_prop(index, Property::from_int32(int32_data[i]));
    mem_table.get_column("int32_column")
        ->set_prop(index, Property::from_int32(int32_data[i]));

    disk_table.get_column("uint32_column")
        ->set_prop(index, Property::from_uint32(uint32_data[i]));
    mem_table.get_column("uint32_column")
        ->set_prop(index, Property::from_uint32(uint32_data[i]));

    disk_table.get_column("int64_column")
        ->set_prop(index, Property::from_int64(int64_data[i]));
    mem_table.get_column("int64_column")
        ->set_prop(index, Property::from_int64(int64_data[i]));

    disk_table.get_column("uint64_column")
        ->set_prop(index, Property::from_uint64(uint64_data[i]));
    mem_table.get_column("uint64_column")
        ->set_prop(index, Property::from_uint64(uint64_data[i]));

    disk_table.get_column("float_column")
        ->set_prop(index, Property::from_float(float_data[i]));
    mem_table.get_column("float_column")
        ->set_prop(index, Property::from_float(float_data[i]));

    disk_table.get_column("double_column")
        ->set_prop(index, Property::from_double(double_data[i]));
    mem_table.get_column("double_column")
        ->set_prop(index, Property::from_double(double_data[i]));

    disk_table.get_column("date_column")
        ->set_prop(index, Property::from_date(date_data[i]));
    mem_table.get_column("date_column")
        ->set_prop(index, Property::from_date(date_data[i]));

    disk_table.get_column("datetime_column")
        ->set_prop(index, Property::from_datetime(datetime_data[i]));
    mem_table.get_column("datetime_column")
        ->set_prop(index, Property::from_datetime(datetime_data[i]));

    disk_table.get_column("interval_column")
        ->set_prop(index, Property::from_interval(interval_data[i]));
    mem_table.get_column("interval_column")
        ->set_prop(index, Property::from_interval(interval_data[i]));

    disk_table.get_column("string_column")
        ->set_prop(index, Property::from_string_view(string_data[i]));
    mem_table.get_column("string_column")
        ->set_prop(index, Property::from_string_view(string_data[i]));
    index++;
  }

  EXPECT_EQ(disk_table.get_column_by_id(0)->size(), 10);
  EXPECT_EQ(mem_table.get_column_by_id(0)->size(), 10);
  EXPECT_EQ(disk_table.col_num(), 11);
  EXPECT_EQ(mem_table.col_num(), 11);

  {
    EXPECT_EQ(
        disk_table
            .get_column_by_id(disk_table.get_column_id_by_name("bool_column"))
            ->type(),
        DataTypeId::kBoolean);
    EXPECT_EQ(
        mem_table
            .get_column_by_id(disk_table.get_column_id_by_name("bool_column"))
            ->type(),
        DataTypeId::kBoolean);

    EXPECT_EQ(
        disk_table
            .get_column_by_id(disk_table.get_column_id_by_name("int32_column"))
            ->type(),
        DataTypeId::kInt32);
    EXPECT_EQ(
        mem_table
            .get_column_by_id(disk_table.get_column_id_by_name("int32_column"))
            ->type(),
        DataTypeId::kInt32);

    EXPECT_EQ(
        disk_table
            .get_column_by_id(disk_table.get_column_id_by_name("uint32_column"))
            ->type(),
        DataTypeId::kUInt32);
    EXPECT_EQ(
        mem_table
            .get_column_by_id(disk_table.get_column_id_by_name("uint32_column"))
            ->type(),
        DataTypeId::kUInt32);

    EXPECT_EQ(
        disk_table
            .get_column_by_id(disk_table.get_column_id_by_name("int64_column"))
            ->type(),
        DataTypeId::kInt64);
    EXPECT_EQ(
        mem_table
            .get_column_by_id(disk_table.get_column_id_by_name("int64_column"))
            ->type(),
        DataTypeId::kInt64);

    EXPECT_EQ(
        disk_table
            .get_column_by_id(disk_table.get_column_id_by_name("uint64_column"))
            ->type(),
        DataTypeId::kUInt64);
    EXPECT_EQ(
        mem_table
            .get_column_by_id(disk_table.get_column_id_by_name("uint64_column"))
            ->type(),
        DataTypeId::kUInt64);

    EXPECT_EQ(
        disk_table
            .get_column_by_id(disk_table.get_column_id_by_name("float_column"))
            ->type(),
        DataTypeId::kFloat);
    EXPECT_EQ(
        mem_table
            .get_column_by_id(disk_table.get_column_id_by_name("float_column"))
            ->type(),
        DataTypeId::kFloat);

    EXPECT_EQ(
        disk_table
            .get_column_by_id(disk_table.get_column_id_by_name("double_column"))
            ->type(),
        DataTypeId::kDouble);
    EXPECT_EQ(
        mem_table
            .get_column_by_id(disk_table.get_column_id_by_name("double_column"))
            ->type(),
        DataTypeId::kDouble);

    EXPECT_EQ(
        disk_table
            .get_column_by_id(disk_table.get_column_id_by_name("date_column"))
            ->type(),
        DataTypeId::kDate);
    EXPECT_EQ(
        mem_table
            .get_column_by_id(disk_table.get_column_id_by_name("date_column"))
            ->type(),
        DataTypeId::kDate);

    EXPECT_EQ(disk_table
                  .get_column_by_id(
                      disk_table.get_column_id_by_name("datetime_column"))
                  ->type(),
              DataTypeId::kTimestampMs);
    EXPECT_EQ(mem_table
                  .get_column_by_id(
                      disk_table.get_column_id_by_name("datetime_column"))
                  ->type(),
              DataTypeId::kTimestampMs);

    EXPECT_EQ(disk_table
                  .get_column_by_id(
                      disk_table.get_column_id_by_name("interval_column"))
                  ->type(),
              DataTypeId::kInterval);
    EXPECT_EQ(mem_table
                  .get_column_by_id(
                      disk_table.get_column_id_by_name("interval_column"))
                  ->type(),
              DataTypeId::kInterval);

    EXPECT_EQ(
        disk_table
            .get_column_by_id(disk_table.get_column_id_by_name("string_column"))
            ->type(),
        DataTypeId::kVarchar);
    EXPECT_EQ(
        mem_table
            .get_column_by_id(disk_table.get_column_id_by_name("string_column"))
            ->type(),
        DataTypeId::kVarchar);
  }

  {
    EXPECT_EQ(disk_table.columns().size(), 11);
    EXPECT_EQ(disk_table.column_names().size(), 11);
    EXPECT_EQ(disk_table.column_types().size(), 11);
    EXPECT_EQ(disk_table.column_ptrs().size(), 11);
    EXPECT_EQ(disk_table.column_name(0), "bool_column");
    EXPECT_EQ(disk_table.get_row(0).size(), 11);
    EXPECT_EQ(disk_table.get_column("bool_column")->type(),
              DataTypeId::kBoolean);
    disk_table.set_name("disk_table");
    std::vector<Property> properties = disk_table.get_row(9);
    disk_table.insert(9, properties, false);
  }

  disk_table.dump("disk_table", std::string(TEST_DIR) + "/checkpoint");
  disk_table.drop();
  mem_table.drop();

  disk_table.open("disk_table", std::string(TEST_DIR), col_name,
                  property_types);
  EXPECT_EQ(disk_table.col_num(), 11);
  EXPECT_EQ(disk_table.get_column_by_id(0)->size(), 10);
  disk_table.reset_header(col_name);
  disk_table.rename_column("bool_column", "renamed_bool_column");
  EXPECT_EQ(disk_table.get_column_id_by_name("renamed_bool_column"), 0);
  disk_table.delete_column("renamed_bool_column");
  EXPECT_EQ(disk_table.col_num(), 10);
  disk_table.set_work_dir(std::string(TEST_DIR));
  disk_table.drop();

  mem_table.open_in_memory("disk_table", std::string(TEST_DIR), col_name,
                           property_types);
  EXPECT_EQ(mem_table.col_num(), 11);
  EXPECT_EQ(mem_table.get_column_by_id(0)->size(), 10);
  const Table& mem_table_ref = mem_table;
  EXPECT_EQ(mem_table_ref.get_column("bool_column")->type(),
            DataTypeId::kBoolean);
  EXPECT_EQ(mem_table_ref.get_column_by_id(0)->type(), DataTypeId::kBoolean);
}

TEST(TableTest, StringColumnDistinguishesUnsetFromEmptyString) {
  if (std::filesystem::exists(TEST_DIR)) {
    std::filesystem::remove_all(TEST_DIR);
  }
  std::filesystem::create_directories(TEST_DIR);
  std::filesystem::create_directories(std::string(TEST_DIR) + "/checkpoint");
  std::filesystem::create_directories(std::string(TEST_DIR) + "/runtime/tmp");

  Table table;
  std::vector<std::string> col_name = {"string_column"};
  std::vector<DataType> property_types = {{DataTypeId::kVarchar}};

  table.open("test_string_validity", TEST_DIR, col_name, property_types);
  table.resize(2, {Property::from_string_view("default_value")});

  auto string_column = std::dynamic_pointer_cast<StringColumn>(
      table.get_column("string_column"));
  ASSERT_NE(string_column, nullptr);

  EXPECT_EQ(string_column->get_prop(0).as_string_view(), "default_value");

  string_column->set_prop(1, Property::from_string_view(""));
  EXPECT_TRUE(string_column->get_prop(1).as_string_view().empty());
  EXPECT_EQ(string_column->get_prop(1).type(), DataTypeId::kVarchar);
  string_column->set_prop(
      1, Property::from_string_view("new value new value new value"));
  EXPECT_EQ(string_column->get_prop(1).as_string_view(),
            "new value new value new value");
  std::string path = std::string(TEST_DIR) + "/string_column";
  string_column->dump(path);

  StringColumn new_string_column;
  new_string_column.open_in_memory(path);
  EXPECT_EQ(new_string_column.get_prop(0).as_string_view(), "default_value");
  EXPECT_EQ(new_string_column.get_prop(1).as_string_view(),
            "new value new value new value");
}

}  // namespace test
}  // namespace neug
