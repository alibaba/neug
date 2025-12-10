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
static const std::vector<gs::Date> date_data = {
    gs::Date(0), gs::Date(5), gs::Date(2), gs::Date(9), gs::Date(3),
    gs::Date(4), gs::Date(3), gs::Date(5), gs::Date(7), gs::Date(0)};
static const std::vector<gs::DateTime> datetime_data = {
    gs::DateTime(0), gs::DateTime(5), gs::DateTime(2), gs::DateTime(9),
    gs::DateTime(3), gs::DateTime(4), gs::DateTime(3), gs::DateTime(5),
    gs::DateTime(7), gs::DateTime(0)};
static const std::vector<gs::Interval> interval_data = {
    gs::Interval(std::string("0hour")),
    gs::Interval(std::string("5hours")),
    gs::Interval(std::string("2minutes")),
    gs::Interval(std::string("9hours")),
    gs::Interval(std::string("3seconds")),
    gs::Interval(std::string("4days")),
    gs::Interval(std::string("3years")),
    gs::Interval(std::string("5milliseconds")),
    gs::Interval(std::string("7minutes")),
    gs::Interval(std::string("0day"))};

static const std::vector<std::string> string_data = {
    std::string("0hour"),    std::string("5hours"),
    std::string("2minutes"), std::string("9hours"),
    std::string("3seconds"), std::string("4days"),
    std::string("3years"),   std::string("5milliseconds"),
    std::string("7minutes"), std::string("0day")};

namespace gs {
namespace test {
TEST(TableTest, TestTableBasic) {
  if (std::filesystem::exists(TEST_DIR)) {
    std::filesystem::remove_all(TEST_DIR);
  }
  std::filesystem::create_directories(TEST_DIR);
  std::filesystem::create_directories(std::string(TEST_DIR) + "/checkpoint");
  std::filesystem::create_directories(std::string(TEST_DIR) + "/runtime/tmp");
  Table disk_table, mem_table, none_table;
  std::vector<std::string> col_name;
  std::vector<PropertyType> property_types;
  std::vector<StorageStrategy> disk_strategies, mem_strategies, none_strategies;

  col_name.emplace_back("bool_column");
  property_types.emplace_back(PropertyType::kBool);
  col_name.emplace_back("int32_column");
  property_types.emplace_back(PropertyType::kInt32);
  col_name.emplace_back("uint32_column");
  property_types.emplace_back(PropertyType::kUInt32);
  col_name.emplace_back("int64_column");
  property_types.emplace_back(PropertyType::kInt64);
  col_name.emplace_back("uint64_column");
  property_types.emplace_back(PropertyType::kUInt64);
  col_name.emplace_back("float_column");
  property_types.emplace_back(PropertyType::kFloat);
  col_name.emplace_back("double_column");
  property_types.emplace_back(PropertyType::kDouble);
  col_name.emplace_back("date_column");
  property_types.emplace_back(PropertyType::kDate);
  col_name.emplace_back("datetime_column");
  property_types.emplace_back(PropertyType::kDateTime);
  col_name.emplace_back("interval_column");
  property_types.emplace_back(PropertyType::kInterval);
  col_name.emplace_back("string_column");
  property_types.emplace_back(PropertyType::kStringView);

  disk_strategies.resize(col_name.size(), StorageStrategy::kDisk);
  mem_strategies.resize(col_name.size(), StorageStrategy::kMem);
  none_strategies.resize(col_name.size(), StorageStrategy::kNone);

  disk_table.init("test_dist", TEST_DIR, col_name, property_types,
                  disk_strategies);
  mem_table.init("test_dist", TEST_DIR, col_name, property_types,
                 mem_strategies);
  none_table.init("test_dist", TEST_DIR, col_name, property_types,
                  none_strategies);

  disk_table.resize(10);
  mem_table.resize(10);
  none_table.resize(10);
  size_t index = 0;
  for (size_t i = 0; i < 10; i++) {
    disk_table.get_column("bool_column")
        ->set_prop(index, Property(bool_data[i]));
    mem_table.get_column("bool_column")
        ->set_prop(index, Property(bool_data[i]));

    disk_table.get_column("int32_column")
        ->set_prop(index, Property(int32_data[i]));
    mem_table.get_column("int32_column")
        ->set_prop(index, Property(int32_data[i]));

    disk_table.get_column("uint32_column")
        ->set_prop(index, Property(uint32_data[i]));
    mem_table.get_column("uint32_column")
        ->set_prop(index, Property(uint32_data[i]));

    disk_table.get_column("int64_column")
        ->set_prop(index, Property(int64_data[i]));
    mem_table.get_column("int64_column")
        ->set_prop(index, Property(int64_data[i]));

    disk_table.get_column("uint64_column")
        ->set_prop(index, Property(uint64_data[i]));
    mem_table.get_column("uint64_column")
        ->set_prop(index, Property(uint64_data[i]));

    disk_table.get_column("float_column")
        ->set_prop(index, Property(float_data[i]));
    mem_table.get_column("float_column")
        ->set_prop(index, Property(float_data[i]));

    disk_table.get_column("double_column")
        ->set_prop(index, Property(double_data[i]));
    mem_table.get_column("double_column")
        ->set_prop(index, Property(double_data[i]));

    disk_table.get_column("date_column")
        ->set_prop(index, Property(date_data[i]));
    mem_table.get_column("date_column")
        ->set_prop(index, Property(date_data[i]));

    disk_table.get_column("datetime_column")
        ->set_prop(index, Property(datetime_data[i]));
    mem_table.get_column("datetime_column")
        ->set_prop(index, Property(datetime_data[i]));

    disk_table.get_column("interval_column")
        ->set_prop(index, Property(interval_data[i]));
    mem_table.get_column("interval_column")
        ->set_prop(index, Property(interval_data[i]));

    disk_table.get_column("string_column")
        ->set_prop(index, Property(string_data[i]));
    mem_table.get_column("string_column")
        ->set_prop(index, Property(string_data[i]));
    index++;
  }

  EXPECT_EQ(disk_table.row_num(), 10);
  EXPECT_EQ(mem_table.row_num(), 10);
  EXPECT_EQ(disk_table.col_num(), 11);
  EXPECT_EQ(mem_table.col_num(), 11);

  {
    EXPECT_EQ(
        disk_table
            .get_column_by_id(disk_table.get_column_id_by_name("bool_column"))
            ->type(),
        PropertyType::kBool);
    EXPECT_EQ(
        mem_table
            .get_column_by_id(disk_table.get_column_id_by_name("bool_column"))
            ->type(),
        PropertyType::kBool);

    EXPECT_EQ(
        disk_table
            .get_column_by_id(disk_table.get_column_id_by_name("int32_column"))
            ->type(),
        PropertyType::kInt32);
    EXPECT_EQ(
        mem_table
            .get_column_by_id(disk_table.get_column_id_by_name("int32_column"))
            ->type(),
        PropertyType::kInt32);

    EXPECT_EQ(
        disk_table
            .get_column_by_id(disk_table.get_column_id_by_name("uint32_column"))
            ->type(),
        PropertyType::kUInt32);
    EXPECT_EQ(
        mem_table
            .get_column_by_id(disk_table.get_column_id_by_name("uint32_column"))
            ->type(),
        PropertyType::kUInt32);

    EXPECT_EQ(
        disk_table
            .get_column_by_id(disk_table.get_column_id_by_name("int64_column"))
            ->type(),
        PropertyType::kInt64);
    EXPECT_EQ(
        mem_table
            .get_column_by_id(disk_table.get_column_id_by_name("int64_column"))
            ->type(),
        PropertyType::kInt64);

    EXPECT_EQ(
        disk_table
            .get_column_by_id(disk_table.get_column_id_by_name("uint64_column"))
            ->type(),
        PropertyType::kUInt64);
    EXPECT_EQ(
        mem_table
            .get_column_by_id(disk_table.get_column_id_by_name("uint64_column"))
            ->type(),
        PropertyType::kUInt64);

    EXPECT_EQ(
        disk_table
            .get_column_by_id(disk_table.get_column_id_by_name("float_column"))
            ->type(),
        PropertyType::kFloat);
    EXPECT_EQ(
        mem_table
            .get_column_by_id(disk_table.get_column_id_by_name("float_column"))
            ->type(),
        PropertyType::kFloat);

    EXPECT_EQ(
        disk_table
            .get_column_by_id(disk_table.get_column_id_by_name("double_column"))
            ->type(),
        PropertyType::kDouble);
    EXPECT_EQ(
        mem_table
            .get_column_by_id(disk_table.get_column_id_by_name("double_column"))
            ->type(),
        PropertyType::kDouble);

    EXPECT_EQ(
        disk_table
            .get_column_by_id(disk_table.get_column_id_by_name("date_column"))
            ->type(),
        PropertyType::kDate);
    EXPECT_EQ(
        mem_table
            .get_column_by_id(disk_table.get_column_id_by_name("date_column"))
            ->type(),
        PropertyType::kDate);

    EXPECT_EQ(disk_table
                  .get_column_by_id(
                      disk_table.get_column_id_by_name("datetime_column"))
                  ->type(),
              PropertyType::kDateTime);
    EXPECT_EQ(mem_table
                  .get_column_by_id(
                      disk_table.get_column_id_by_name("datetime_column"))
                  ->type(),
              PropertyType::kDateTime);

    EXPECT_EQ(disk_table
                  .get_column_by_id(
                      disk_table.get_column_id_by_name("interval_column"))
                  ->type(),
              PropertyType::kInterval);
    EXPECT_EQ(mem_table
                  .get_column_by_id(
                      disk_table.get_column_id_by_name("interval_column"))
                  ->type(),
              PropertyType::kInterval);

    EXPECT_EQ(
        disk_table
            .get_column_by_id(disk_table.get_column_id_by_name("string_column"))
            ->type(),
        PropertyType::kStringView);
    EXPECT_EQ(
        mem_table
            .get_column_by_id(disk_table.get_column_id_by_name("string_column"))
            ->type(),
        PropertyType::kStringView);
  }

  {
    EXPECT_EQ(disk_table.columns().size(), 11);
    EXPECT_EQ(disk_table.column_names().size(), 11);
    EXPECT_EQ(disk_table.column_types().size(), 11);
    EXPECT_EQ(disk_table.column_ptrs().size(), 11);
    EXPECT_EQ(disk_table.column_name(0), "bool_column");
    EXPECT_EQ(disk_table.get_row(0).size(), 11);
    EXPECT_EQ(disk_table.get_column("bool_column")->type(),
              PropertyType::kBool);
    disk_table.set_name("disk_table");
    std::vector<Property> properties = disk_table.get_row(9);
    disk_table.insert_with_resize(9, properties);
  }

  disk_table.dump("disk_table", std::string(TEST_DIR) + "/checkpoint");
  disk_table.drop();
  mem_table.drop();

  disk_table.open("disk_table", std::string(TEST_DIR), col_name, property_types,
                  disk_strategies);
  EXPECT_EQ(disk_table.col_num(), 11);
  EXPECT_EQ(disk_table.row_num(), 10);
  disk_table.reset_header(col_name);
  disk_table.rename_column("bool_column", "renamed_bool_column");
  EXPECT_EQ(disk_table.get_column_id_by_name("renamed_bool_column"), 0);
  disk_table.delete_column("renamed_bool_column");
  EXPECT_EQ(disk_table.col_num(), 10);
  disk_table.copy_to_tmp("disk_table", std::string(TEST_DIR) + "/checkpoint",
                         std::string(TEST_DIR));
  disk_table.set_work_dir(std::string(TEST_DIR));
  disk_table.drop();

  mem_table.open_in_memory("disk_table", std::string(TEST_DIR), col_name,
                           property_types, mem_strategies);
  EXPECT_EQ(mem_table.col_num(), 11);
  EXPECT_EQ(mem_table.row_num(), 10);
  const Table& mem_table_ref = mem_table;
  EXPECT_EQ(mem_table_ref.get_column("bool_column")->type(),
            PropertyType::kBool);
  EXPECT_EQ(mem_table_ref.get_column_by_id(0)->type(), PropertyType::kBool);
}

}  // namespace test
}  // namespace gs