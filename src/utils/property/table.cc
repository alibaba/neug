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

#include <assert.h>
#include <glog/logging.h>

#include <ostream>
#include <utility>

#include "neug/storages/file_names.h"
#include "neug/utils/exception/exception.h"
#include "neug/utils/property/column.h"
#include "neug/utils/serialization/out_archive.h"

namespace neug {

Table::Table() : touched_(false) {}
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

void Table::open(const std::string& name, const std::string& work_dir,
                 const std::vector<std::string>& col_name,
                 const std::vector<DataType>& property_types) {
  name_ = name;
  work_dir_ = work_dir;
  snapshot_dir_ = checkpoint_dir(work_dir_);
  initColumns(col_name, property_types);
  for (size_t i = 0; i < columns_.size(); ++i) {
    columns_[i]->open(name + ".col_" + std::to_string(i), snapshot_dir_,
                      tmp_dir(work_dir));
  }
  touched_ = false;
  buildColumnPtrs();
}

void Table::open_in_memory(const std::string& name, const std::string& work_dir,
                           const std::vector<std::string>& col_name,
                           const std::vector<DataType>& property_types) {
  name_ = name;
  work_dir_ = work_dir;
  snapshot_dir_ = checkpoint_dir(work_dir_);
  initColumns(col_name, property_types);
  for (size_t i = 0; i < columns_.size(); ++i) {
    columns_[i]->open_in_memory(snapshot_dir_ + "/" + name + ".col_" +
                                std::to_string(i));
  }
  touched_ = true;
  buildColumnPtrs();
}

void Table::open_with_hugepages(const std::string& name,
                                const std::string& work_dir,
                                const std::vector<std::string>& col_name,
                                const std::vector<DataType>& property_types) {
  name_ = name;
  work_dir_ = work_dir;
  snapshot_dir_ = checkpoint_dir(work_dir);
  initColumns(col_name, property_types);
  for (size_t i = 0; i < columns_.size(); ++i) {
    columns_[i]->open_with_hugepages(snapshot_dir_ + "/" + name + ".col_" +
                                     std::to_string(i));
  }
  touched_ = true;
  buildColumnPtrs();
}

void Table::dump(const std::string& name, const std::string& snapshot_dir) {
  int i = 0;
  for (auto col : columns_) {
    col->dump(snapshot_dir + "/" + name + ".col_" + std::to_string(i++));
  }
  columns_.clear();
  column_ptrs_.clear();
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

void Table::add_columns(const std::vector<std::string>& col_names,
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
    if (memory_level == MemoryLevel::kSyncToFile) {
      columns_[i]->open(name_ + ".col_" + std::to_string(i), "",
                        tmp_dir(work_dir_));
    } else if (memory_level == MemoryLevel::kInMemory) {
      columns_[i]->open_in_memory(tmp_dir(work_dir_) + "/" + name_ + ".col_" +
                                  std::to_string(i));
    } else if (memory_level == MemoryLevel::kHugePagePrefered) {
      columns_[i]->open_with_hugepages(tmp_dir(work_dir_) + "/" + name_ +
                                       ".col_" + std::to_string(i));
    } else {
      THROW_NOT_IMPLEMENTED_EXCEPTION(
          "Unsupported memory level: " +
          std::to_string(static_cast<int>(memory_level)));
    }
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
    columns_[col_id]->close();
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
    col->ensure_writable(work_dir_);
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
    columns_[i]->ensure_writable(work_dir_);
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

void Table::set_name(const std::string& name) { name_ = name; }

void Table::set_work_dir(const std::string& work_dir) { work_dir_ = work_dir; }

void Table::ensure_writable(size_t col_id) {
  if (col_id >= columns_.size()) {
    THROW_INVALID_ARGUMENT_EXCEPTION("Column id out of range: " +
                                     std::to_string(col_id));
  }
  columns_[col_id]->ensure_writable(work_dir_);
}

}  // namespace neug
