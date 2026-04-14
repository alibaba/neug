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

#include "neug/utils/property/table.h"

#include "neug/storages/module/module_factory.h"

#include <assert.h>
#include <glog/logging.h>

#include <ostream>
#include <utility>

#include "neug/storages/file_names.h"
#include "neug/utils/exception/exception.h"
#include "neug/utils/property/column.h"
#include "neug/utils/serialization/out_archive.h"

namespace neug {

Table::Table() {}
Table::~Table() { close(); }

void Table::initColumns(const std::vector<std::string>& col_name,
                        const std::vector<DataType>& property_types) {
  size_t col_num = col_name.size();
  columns_.clear();
  col_names_.clear();
  col_id_map_.clear();
  columns_.resize(col_num, nullptr);

  for (size_t i = 0; i < col_num; ++i) {
    int col_id = col_names_.size();
    col_id_map_.insert({col_name[i], col_id});
    col_names_.emplace_back(col_name[i]);
    assert(i < property_types.size());
    columns_[col_id] = CreateColumn(property_types[i]);
  }
  columns_.resize(col_id_map_.size());
}

void Table::Open(Checkpoint& ckp, const ModuleDescriptor& descriptor,
                 MemoryLevel memory_level,
                 const std::vector<std::string>& col_name,
                 const std::vector<DataType>& property_types) {
  LOG(INFO) << "Opening table with columns: " << col_name.size();
  initColumns(col_name, property_types);
  for (size_t i = 0; i < columns_.size(); ++i) {
    auto sub_module_desc =
        descriptor.get_sub_module("col_" + std::to_string(i));
    columns_[i]->Open(ckp, sub_module_desc, memory_level);
  }
  buildColumnPtrs();
}

ModuleDescriptor Table::Dump(Checkpoint& ckp) {
  int i = 0;
  ModuleDescriptor desc;
  for (auto col : columns_) {
    desc.set_sub_module("col_" + std::to_string(i++), col->Dump(ckp));
  }
  return desc;
}

void Table::reset_header(const std::vector<std::string>& col_name) {
  std::unordered_map<std::string, int> new_col_id_map;
  size_t col_num = col_name.size();
  for (size_t i = 0; i < col_num; ++i) {
    new_col_id_map.insert({col_name[i], i});
    col_names_[i] = col_name[i];
  }
  CHECK_EQ(col_num, new_col_id_map.size());
  col_id_map_.swap(new_col_id_map);
}

void Table::add_columns(Checkpoint& ckp,
                        const std::vector<std::string>& col_names,
                        const std::vector<DataType>& col_types,
                        const std::vector<Property>& default_property_values,
                        size_t capacity, MemoryLevel memory_level) {
  if (default_property_values.size() != col_names.size()) {
    THROW_RUNTIME_ERROR("default_property_values size mismatch: expected " +
                        std::to_string(col_names.size()) + " but got " +
                        std::to_string(default_property_values.size()));
  }
  // When add_columns are called, the table is already initialized and col_files
  // are opened.
  std::stringstream ss;
  for (const auto& col_name : col_names) {
    ss << col_name << " ";
  }
  size_t old_size = columns_.size();
  columns_.resize(old_size + col_names.size());

  for (size_t i = 0; i < col_names.size(); ++i) {
    int col_id = col_names_.size();
    col_id_map_.insert({col_names[i], col_id});
    col_names_.emplace_back(col_names[i]);
    columns_[col_id] = CreateColumn(col_types[i]);
  }
  for (size_t i = old_size; i < columns_.size(); ++i) {
    columns_[i]->Open(ckp, ModuleDescriptor(), MemoryLevel::kInMemory);
    columns_[i]->resize(capacity, default_property_values[i - old_size]);
  }
  buildColumnPtrs();
}

void Table::rename_column(const std::string& old_name,
                          const std::string& new_name) {
  auto it = col_id_map_.find(old_name);
  if (it != col_id_map_.end()) {
    int col_id = it->second;
    col_id_map_.erase(it);
    col_id_map_.insert({new_name, col_id});
    col_names_[col_id] = new_name;
  } else {
    LOG(ERROR) << "Column " << old_name << " does not exist.";
  }
}

void Table::delete_column(const std::string& col_name) {
  auto it = col_id_map_.find(col_name);
  if (it != col_id_map_.end()) {
    int col_id = it->second;
    col_id_map_.erase(it);
    columns_[col_id]->Close();
    columns_[col_id].reset();
    columns_.erase(columns_.begin() + col_id);
    col_names_.erase(col_names_.begin() + col_id);
    for (size_t i = col_id; i < column_ptrs_.size() - 1; i++) {
      column_ptrs_[i] = column_ptrs_[i + 1];
    }
    for (auto& pair : col_id_map_) {
      if (pair.second > col_id) {
        pair.second -= 1;
      }
    }
    column_ptrs_.resize(column_ptrs_.size() - 1);
  } else {
    LOG(ERROR) << "Column " << col_name << " does not exist.";
  }
}

const std::vector<std::string>& Table::column_names() const {
  return col_names_;
}

std::string Table::column_name(size_t index) const {
  CHECK(index < col_names_.size());
  return col_names_[index];
}

int Table::get_column_id_by_name(const std::string& name) const {
  auto it = col_id_map_.find(name);
  if (it != col_id_map_.end()) {
    return it->second;
  }
  return -1;
}

std::vector<DataTypeId> Table::column_types() const {
  size_t col_num = col_id_map_.size();
  std::vector<DataTypeId> types(col_num);
  for (size_t col_i = 0; col_i < col_num; ++col_i) {
    types[col_i] = columns_[col_i]->type();
  }
  return types;
}

std::shared_ptr<ColumnBase> Table::get_column(const std::string& name) {
  auto it = col_id_map_.find(name);
  if (it != col_id_map_.end()) {
    int col_id = it->second;
    if (static_cast<size_t>(col_id) < columns_.size()) {
      return columns_[col_id];
    }
  }

  return nullptr;
}

const std::shared_ptr<ColumnBase> Table::get_column(
    const std::string& name) const {
  auto it = col_id_map_.find(name);
  if (it != col_id_map_.end()) {
    int col_id = it->second;
    if (static_cast<size_t>(col_id) < columns_.size()) {
      return columns_[col_id];
    }
  }

  return nullptr;
}

std::vector<Property> Table::get_row(size_t row_id) const {
  std::vector<Property> ret;
  for (auto ptr : columns_) {
    ret.push_back(ptr->get_prop(row_id));
  }
  return ret;
}

std::shared_ptr<ColumnBase> Table::get_column_by_id(size_t index) {
  if (index >= columns_.size()) {
    return nullptr;
  } else {
    return columns_[index];
  }
}

const std::shared_ptr<ColumnBase> Table::get_column_by_id(size_t index) const {
  if (index >= columns_.size()) {
    return nullptr;
  } else {
    return columns_[index];
  }
}

size_t Table::col_num() const { return columns_.size(); }
std::vector<std::shared_ptr<ColumnBase>>& Table::columns() { return columns_; }
// get column pointers
std::vector<ColumnBase*>& Table::column_ptrs() { return column_ptrs_; }

void Table::insert(size_t index, const std::vector<Property>& values,
                   bool insert_safe) {
  assert(values.size() == columns_.size());
  CHECK_EQ(values.size(), columns_.size());
  size_t col_num = columns_.size();
  for (size_t i = 0; i < col_num; ++i) {
    columns_[i]->set_any(index, values[i], insert_safe);
  }
}

void Table::resize(size_t row_num) {
  for (auto col : columns_) {
    col->resize(row_num);
  }
}

void Table::resize(size_t row_num,
                   const std::vector<Property>& default_values) {
  if (default_values.size() != columns_.size()) {
    THROW_RUNTIME_ERROR("default_values size mismatch: expected " +
                        std::to_string(columns_.size()) + " but got " +
                        std::to_string(default_values.size()));
  }
  for (size_t i = 0; i < columns_.size(); ++i) {
    columns_[i]->resize(row_num, default_values[i]);
  }
}

void Table::ingest(uint32_t index, OutArchive& arc) {
  if (column_ptrs_.size() == 0) {
    return;
  }

  CHECK_GT(column_ptrs_[0]->size(), index);
  uint32_t num_updates;
  arc >> num_updates;
  for (uint32_t i = 0; i < num_updates; ++i) {
    uint32_t col_id;
    arc >> col_id;
    if (col_id >= column_ptrs_.size()) {
      THROW_INTERNAL_EXCEPTION(
          "Column id out of range: " + std::to_string(col_id) +
          " >= " + std::to_string(column_ptrs_.size()) + "Table::ingest");
      continue;
    }
    column_ptrs_[col_id]->ingest(index, arc);
  }
}

void Table::buildColumnPtrs() {
  size_t col_num = columns_.size();
  column_ptrs_.clear();
  column_ptrs_.resize(col_num);
  for (size_t col_i = 0; col_i < col_num; ++col_i) {
    column_ptrs_[col_i] = columns_[col_i].get();
  }
}

void Table::close() {
  columns_.clear();
  column_ptrs_.clear();
}

void Table::drop() {
  close();
  // TODO(zhanglei): delete files in work_dir
}

std::unique_ptr<Table> Table::Fork(Checkpoint& ckp, MemoryLevel level) const {
  auto new_table = std::make_unique<Table>();
  new_table->col_id_map_ = col_id_map_;
  new_table->col_names_ = col_names_;

  for (size_t i = 0; i < columns_.size(); ++i) {
    new_table->columns_.emplace_back(std::shared_ptr<ColumnBase>(
        static_cast<ColumnBase*>(columns_[i]->Fork(ckp, level).release())));
  }
  new_table->buildColumnPtrs();
  return new_table;
}

// ---------------------------------------------------------------------------
// ModuleFactory registrations for TypedColumn
// ---------------------------------------------------------------------------
using TypedColumn_empty = TypedColumn<EmptyType>;
using TypedColumn_bool = TypedColumn<bool>;
using TypedColumn_int32 = TypedColumn<int32_t>;
using TypedColumn_uint32 = TypedColumn<uint32_t>;
using TypedColumn_int64 = TypedColumn<int64_t>;
using TypedColumn_uint64 = TypedColumn<uint64_t>;
using TypedColumn_float = TypedColumn<float>;
using TypedColumn_double = TypedColumn<double>;
using TypedColumn_date = TypedColumn<Date>;
using TypedColumn_datetime = TypedColumn<DateTime>;
using TypedColumn_interval = TypedColumn<Interval>;
using TypedColumn_string = TypedColumn<std::string_view>;

NEUG_REGISTER_MODULE("column_empty", TypedColumn_empty);
NEUG_REGISTER_MODULE("column_bool", TypedColumn_bool);
NEUG_REGISTER_MODULE("column_int32", TypedColumn_int32);
NEUG_REGISTER_MODULE("column_uint32", TypedColumn_uint32);
NEUG_REGISTER_MODULE("column_int64", TypedColumn_int64);
NEUG_REGISTER_MODULE("column_uint64", TypedColumn_uint64);
NEUG_REGISTER_MODULE("column_float", TypedColumn_float);
NEUG_REGISTER_MODULE("column_double", TypedColumn_double);
NEUG_REGISTER_MODULE("column_date", TypedColumn_date);
NEUG_REGISTER_MODULE("column_datetime", TypedColumn_datetime);
NEUG_REGISTER_MODULE("column_interval", TypedColumn_interval);
NEUG_REGISTER_MODULE("column_string", TypedColumn_string);

}  // namespace neug
