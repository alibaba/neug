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
#include <unistd.h>
#include <filesystem>

#include "neug/execution/common/types/value.h"
#include "neug/storages/checkpoint_manager.h"
#include "neug/storages/checkpoint_manifest.h"
#include "neug/storages/module/module_broker.h"
#include "neug/storages/module_descriptor.h"
#include "neug/utils/property/column.h"
#include "neug/utils/property/table.h"
#include "unittest/utils.h"

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

// Test-side Open / Dump for Table: round-trips columns through ModuleBroker
// + CheckpointManifest the same way the production OpenVertexTable flow does.
// Pass an empty CheckpointManifest to initialize fresh columns, or one returned
// from a previous DumpTableLegacy to restore.
inline std::string TablePropKey(size_t i) {
  return "table/col_" + std::to_string(i);
}

static void OpenTableLegacy(Table& t, Checkpoint& ckp,
                            const CheckpointManifest& meta, MemoryLevel level,
                            const std::vector<DataType>& types) {
  if (!meta.has_module(TablePropKey(0))) {
    t.Init(ckp, level);
    return;
  }
  ModuleBroker store;
  store.Open(ckp, meta, level);
  for (size_t i = 0; i < types.size(); ++i) {
    t.SetColumn(static_cast<int>(i),
                store.TakeModule<ColumnBase>(TablePropKey(i)));
  }
}

static CheckpointManifest DumpTableLegacy(Table& t, Checkpoint& ckp) {
  // Table holds columns by unique_ptr; dump inline directly.
  CheckpointManifest meta;
  for (size_t i = 0; i < t.col_num(); ++i) {
    t.get_column_by_id(i)->Dump(ckp, meta, TablePropKey(i));
  }
  return meta;
}

class TableTest : public ::testing::Test {
 protected:
  void SetUp() override {
    temp_dir_ =
        std::filesystem::temp_directory_path() /
        ("table_test_" + std::to_string(::getpid()) + "_" + GetTestName());
    if (std::filesystem::exists(temp_dir_)) {
      std::filesystem::remove_all(temp_dir_);
    }
    std::filesystem::create_directories(temp_dir_);
    ws.Open(temp_dir_.string());
  }

  void TearDown() override {
    if (std::filesystem::exists(temp_dir_)) {
      std::filesystem::remove_all(temp_dir_);
    }
  }

  neug::CheckpointManager& Workspace() { return ws; }

  std::string TempDir() const { return temp_dir_.string(); }

 private:
  std::filesystem::path temp_dir_;
  neug::CheckpointManager ws;

  std::string GetTestName() const {
    const testing::TestInfo* const test_info =
        testing::UnitTest::GetInstance()->current_test_info();
    return std::string(test_info->name());
  }
};

TEST_F(TableTest, ModuleBrokerDoesNotSkipUnmarkedReferencedTopLevelModule) {
  auto ckp = Workspace().GetCheckpoint(Workspace().CreateCheckpoint());

  ModuleDescriptor top_level_desc;
  top_level_desc.module_type = TypedColumn<int32_t>::type_name();

  ModuleDescriptor owner_desc;
  owner_desc.set_ref("borrowed", "top_level");

  CheckpointManifest meta;
  meta.set_module("top_level", top_level_desc);
  meta.set_module("owner", owner_desc);

  ModuleBroker store;
  store.Open(*ckp, meta, MemoryLevel::kInMemory);

  EXPECT_TRUE(store.Contains("top_level"));
}

TEST_F(TableTest, ModuleBrokerSkipsMarkedReferencedModule) {
  auto ckp = Workspace().GetCheckpoint(Workspace().CreateCheckpoint());

  ModuleDescriptor child_desc;
  child_desc.module_type = TypedColumn<int32_t>::type_name();
  child_desc.mark_as_referenced_module();

  CheckpointManifest meta;
  meta.set_module("child", child_desc);

  ModuleBroker store;
  store.Open(*ckp, meta, MemoryLevel::kInMemory);

  EXPECT_FALSE(store.Contains("child"));
}

TEST_F(TableTest, TestTableBasic) {
  auto& ws = Workspace();
  auto ckp = make_checkpoint(ws);

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

  Table disk_table(col_name, property_types);
  Table mem_table(col_name, property_types);
  Table none_table(col_name, property_types);
  OpenTableLegacy(disk_table, *ckp, CheckpointManifest(),
                  MemoryLevel::kSyncToFile, property_types);
  OpenTableLegacy(mem_table, *ckp, CheckpointManifest(), MemoryLevel::kInMemory,
                  property_types);
  OpenTableLegacy(none_table, *ckp, CheckpointManifest(),
                  MemoryLevel::kInMemory, property_types);

  disk_table.resize(10);
  mem_table.resize(10);
  none_table.resize(10);
  size_t index = 0;
  for (size_t i = 0; i < 10; i++) {
    disk_table.get_column("bool_column")
        ->set_any(index, execution::Value::BOOLEAN(bool_data[i]), false);
    mem_table.get_column("bool_column")
        ->set_any(index, execution::Value::BOOLEAN(bool_data[i]), false);

    disk_table.get_column("int32_column")
        ->set_any(index, execution::Value::INT32(int32_data[i]), false);
    mem_table.get_column("int32_column")
        ->set_any(index, execution::Value::INT32(int32_data[i]), false);

    disk_table.get_column("uint32_column")
        ->set_any(index, execution::Value::UINT32(uint32_data[i]), false);
    mem_table.get_column("uint32_column")
        ->set_any(index, execution::Value::UINT32(uint32_data[i]), false);

    disk_table.get_column("int64_column")
        ->set_any(index, execution::Value::INT64(int64_data[i]), false);
    mem_table.get_column("int64_column")
        ->set_any(index, execution::Value::INT64(int64_data[i]), false);

    disk_table.get_column("uint64_column")
        ->set_any(index, execution::Value::UINT64(uint64_data[i]), false);
    mem_table.get_column("uint64_column")
        ->set_any(index, execution::Value::UINT64(uint64_data[i]), false);

    disk_table.get_column("float_column")
        ->set_any(index, execution::Value::FLOAT(float_data[i]), false);
    mem_table.get_column("float_column")
        ->set_any(index, execution::Value::FLOAT(float_data[i]), false);

    disk_table.get_column("double_column")
        ->set_any(index, execution::Value::DOUBLE(double_data[i]), false);
    mem_table.get_column("double_column")
        ->set_any(index, execution::Value::DOUBLE(double_data[i]), false);

    disk_table.get_column("date_column")
        ->set_any(index, execution::Value::DATE(date_data[i]), false);
    mem_table.get_column("date_column")
        ->set_any(index, execution::Value::DATE(date_data[i]), false);

    disk_table.get_column("datetime_column")
        ->set_any(index, execution::Value::TIMESTAMPMS(datetime_data[i]),
                  false);
    mem_table.get_column("datetime_column")
        ->set_any(index, execution::Value::TIMESTAMPMS(datetime_data[i]),
                  false);

    disk_table.get_column("interval_column")
        ->set_any(index, execution::Value::INTERVAL(interval_data[i]), false);
    mem_table.get_column("interval_column")
        ->set_any(index, execution::Value::INTERVAL(interval_data[i]), false);

    disk_table.get_column("string_column")
        ->set_any(index, execution::Value::STRING(string_data[i]), true);
    mem_table.get_column("string_column")
        ->set_any(index, execution::Value::STRING(string_data[i]), true);
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
    EXPECT_EQ(disk_table.col_num(), 11);
    EXPECT_EQ(disk_table.column_names().size(), 11);
    EXPECT_EQ(disk_table.column_types().size(), 11);
    EXPECT_EQ(disk_table.column_name(0), "bool_column");
    EXPECT_EQ(disk_table.get_row(0).size(), 11);
    EXPECT_EQ(disk_table.get_column("bool_column")->type(),
              DataTypeId::kBoolean);
    const auto& properties = disk_table.get_row(9);
    disk_table.insert(9, properties, false);
  }

  // dump disk_table and reopen it from the descriptor
  auto disk_desc = DumpTableLegacy(disk_table, *ckp);
  disk_table.close();
  mem_table.close();

  Table reopened_disk_table(col_name, property_types);
  OpenTableLegacy(reopened_disk_table, *ckp, disk_desc,
                  MemoryLevel::kSyncToFile, property_types);
  EXPECT_EQ(reopened_disk_table.col_num(), 11);
  EXPECT_EQ(reopened_disk_table.get_column_by_id(0)->size(), 10);
  reopened_disk_table.reset_header(col_name);
  reopened_disk_table.rename_column("bool_column", "renamed_bool_column");
  EXPECT_EQ(reopened_disk_table.get_column_id_by_name("renamed_bool_column"),
            0);
  reopened_disk_table.delete_column("renamed_bool_column");
  EXPECT_EQ(reopened_disk_table.col_num(), 10);
  reopened_disk_table.close();

  // reopen from the same descriptor in-memory
  Table reopened_mem_table(col_name, property_types);
  OpenTableLegacy(reopened_mem_table, *ckp, disk_desc, MemoryLevel::kInMemory,
                  property_types);
  EXPECT_EQ(reopened_mem_table.col_num(), 11);
  EXPECT_EQ(reopened_mem_table.get_column_by_id(0)->size(), 10);
  const Table& mem_table_ref = reopened_mem_table;
  EXPECT_EQ(mem_table_ref.get_column("bool_column")->type(),
            DataTypeId::kBoolean);
  EXPECT_EQ(mem_table_ref.get_column_by_id(0)->type(), DataTypeId::kBoolean);
}

TEST_F(TableTest, StringColumnDistinguishesUnsetFromEmptyString) {
  auto ckp = make_checkpoint(Workspace());

  std::vector<std::string> col_name = {"string_column"};
  std::vector<DataType> property_types = {{DataTypeId::kVarchar}};

  Table table(col_name, property_types);
  OpenTableLegacy(table, *ckp, CheckpointManifest(), MemoryLevel::kInMemory,
                  property_types);
  table.resize(2, std::vector<execution::Value>{
                      execution::Value::STRING(std::string("default_value"))});

  auto string_column =
      dynamic_cast<StringColumn*>(table.get_column("string_column"));
  ASSERT_NE(string_column, nullptr);

  EXPECT_EQ(string_column->get_any(0).GetValue<std::string>(), "default_value");

  string_column->set_any(1, execution::Value::STRING(std::string("")), true);
  EXPECT_TRUE(string_column->get_any(1).GetValue<std::string>().empty());
  EXPECT_EQ(string_column->get_any(1).type().id(), DataTypeId::kVarchar);
  string_column->set_any(
      1, execution::Value::STRING(std::string("new value new value new value")),
      true);
  EXPECT_EQ(string_column->get_any(1).GetValue<std::string>(),
            "new value new value new value");
  CheckpointManifest meta;
  string_column->Dump(*ckp, meta, TablePropKey(0));
  auto desc = meta.module(TablePropKey(0));
  ASSERT_TRUE(desc.has_value());
  StringColumn new_string_column;
  new_string_column.Open(*ckp, desc.value(), MemoryLevel::kInMemory);
  EXPECT_EQ(new_string_column.get_any(0).GetValue<std::string>(),
            "default_value");
  EXPECT_EQ(new_string_column.get_any(1).GetValue<std::string>(),
            "new value new value new value");
}

}  // namespace test
}  // namespace neug
