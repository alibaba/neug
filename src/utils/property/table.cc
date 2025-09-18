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

#include <assert.h>        // for assert
#include <glog/logging.h>  // for Check_EQImpl, CHECK_EQ, CHECK, COMPACT...
                           // for __alloc_traits<>::value_type
#include <ostream>         // for operator<<, basic_ostream

#include "neug/utils/id_indexer.h"       // for IdIndexer
#include "neug/utils/property/column.h"  // for ColumnBase, CreateColumn

namespace grape {
class OutArchive;
}  // namespace grape

namespace gs {

Table::Table() : touched_(false) {}
Table::~Table() { close(); }

void Table::initColumns(const std::vector<std::string>& col_name,
                        const std::vector<PropertyType>& property_types,
                        const std::vector<StorageStrategy>& strategies_) {
  size_t col_num = col_name.size();
  columns_.clear();
  col_names_.clear();
  col_id_map_.clear();
  columns_.resize(col_num, nullptr);
  auto strategies = strategies_;
  strategies.resize(col_num, StorageStrategy::kMem);

  for (size_t i = 0; i < col_num; ++i) {
    int col_id = col_names_.size();
    col_id_map_.insert({col_name[i], col_id});
    col_names_.emplace_back(col_name[i]);
    columns_[col_id] = CreateColumn(property_types[i], strategies[i]);
  }
  columns_.resize(col_id_map_.size());
}

void Table::init(const std::string& name, const std::string& work_dir,
                 const std::vector<std::string>& col_name,
                 const std::vector<PropertyType>& property_types,
                 const std::vector<StorageStrategy>& strategies_) {
  name_ = name;
  work_dir_ = work_dir;
  initColumns(col_name, property_types, strategies_);
  for (size_t i = 0; i < columns_.size(); ++i) {
    columns_[i]->open(name + ".col_" + std::to_string(i), "", work_dir);
  }
  touched_ = true;
  buildColumnPtrs();
}

void Table::open(const std::string& name, const std::string& snapshot_dir,
                 const std::string& work_dir,
                 const std::vector<std::string>& col_name,
                 const std::vector<PropertyType>& property_types,
                 const std::vector<StorageStrategy>& strategies_) {
  name_ = name;
  work_dir_ = work_dir;
  snapshot_dir_ = snapshot_dir;
  initColumns(col_name, property_types, strategies_);
  for (size_t i = 0; i < columns_.size(); ++i) {
    columns_[i]->open(name + ".col_" + std::to_string(i), snapshot_dir,
                      tmp_dir(work_dir));
  }
  touched_ = false;
  buildColumnPtrs();
}

void Table::open_in_memory(const std::string& name, const std::string& work_dir,
                           const std::vector<std::string>& col_name,
                           const std::vector<PropertyType>& property_types,
                           const std::vector<StorageStrategy>& strategies_) {
  name_ = name;
  work_dir_ = work_dir;
  snapshot_dir_ = checkpoint_dir(work_dir_);
  initColumns(col_name, property_types, strategies_);
  for (size_t i = 0; i < columns_.size(); ++i) {
    columns_[i]->open_in_memory(snapshot_dir_ + "/" + name + ".col_" +
                                std::to_string(i));
  }
  touched_ = true;
  buildColumnPtrs();
}

void Table::open_with_hugepages(const std::string& name,
                                const std::string& snapshot_dir,
                                const std::vector<std::string>& col_name,
                                const std::vector<PropertyType>& property_types,
                                const std::vector<StorageStrategy>& strategies_,
                                bool force) {
  initColumns(col_name, property_types, strategies_);
  for (size_t i = 0; i < columns_.size(); ++i) {
    columns_[i]->open_with_hugepages(
        snapshot_dir + "/" + name + ".col_" + std::to_string(i), force);
  }
  touched_ = true;
  buildColumnPtrs();
}

void Table::copy_to_tmp(const std::string& name,
                        const std::string& snapshot_dir,
                        const std::string& work_dir) {
  int i = 0;
  for (auto& col : columns_) {
    col->copy_to_tmp(snapshot_dir + "/" + name + ".col_" + std::to_string(i),
                     work_dir + "/" + name + ".col_" + std::to_string(i));
    ++i;
  }
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

void Table::add_column(const std::string& col_name,
                       const PropertyType& col_types,
                       std::shared_ptr<ColumnBase> column) {
  int col_id = col_names_.size();
  columns_.emplace_back(column);
  col_id_map_.insert({col_name, col_id});
  col_names_.emplace_back(col_name);
  CHECK_EQ(col_id, columns_.size() - 1);

  buildColumnPtrs();
}

void Table::add_columns(const std::vector<std::string>& col_names,
                        const std::vector<PropertyType>& col_types,
                        int memory_level) {
  // When add_columns are called, the table is already initialized and col_files
  // are opened.
  size_t old_size = columns_.size();
  columns_.resize(old_size + col_names.size());

  for (size_t i = 0; i < col_names.size(); ++i) {
    int col_id = col_names_.size();
    col_id_map_.insert({col_names[i], col_id});
    col_names_.emplace_back(col_names[i]);
    columns_[col_id] = CreateColumn(col_types[i], StorageStrategy::kMem);
  }
  for (size_t i = old_size; i < columns_.size(); ++i) {
    if (memory_level == 0) {
      columns_[i]->open(name_ + ".col_" + std::to_string(i), "",
                        tmp_dir(work_dir_));
    } else if (memory_level == 1) {
      columns_[i]->open_in_memory(tmp_dir(work_dir_) + "/" + name_ + ".col_" +
                                  std::to_string(i));
    } else {
      THROW_NOT_IMPLEMENTED_EXCEPTION("Unsupported memory level");
    }
    columns_[i]->resize(row_num());
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

std::vector<PropertyType> Table::column_types() const {
  size_t col_num = col_id_map_.size();
  std::vector<PropertyType> types(col_num);
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

std::vector<Any> Table::get_row(size_t row_id) const {
  std::vector<Any> ret;
  for (auto ptr : columns_) {
    ret.push_back(ptr->get(row_id));
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
size_t Table::row_num() const {
  if (columns_.empty()) {
    return 0;
  }
  return columns_[0]->size();
}
std::vector<std::shared_ptr<ColumnBase>>& Table::columns() { return columns_; }
// get column pointers
std::vector<ColumnBase*>& Table::column_ptrs() { return column_ptrs_; }

void Table::insert(size_t index, const std::vector<Any>& values) {
  assert(values.size() == columns_.size());
  CHECK_EQ(values.size(), columns_.size());
  size_t col_num = columns_.size();
  for (size_t i = 0; i < col_num; ++i) {
    columns_[i]->set_any(index, values[i]);
  }
}

void Table::insert_with_resize(size_t index, const std::vector<Any>& values) {
  assert(values.size() == columns_.size());
  CHECK_EQ(values.size(), columns_.size());
  size_t col_num = columns_.size();
  for (size_t i = 0; i < col_num; ++i) {
    columns_[i]->set_any_with_resize(index, values[i]);
  }
}

// column_id_mapping is the mapping from the column id in the input table to
// the column id in the current table
void Table::insert(size_t index, const std::vector<Any>& values,
                   const std::vector<int32_t>& col_ind_mapping) {
  assert(values.size() == columns_.size() + 1);
  CHECK_EQ(values.size(), columns_.size() + 1);
  for (size_t i = 0; i < values.size(); ++i) {
    if (col_ind_mapping[i] != -1) {
      columns_[col_ind_mapping[i]]->set_any(index, values[i]);
    }
  }
}

void Table::resize(size_t row_num) {
  for (auto col : columns_) {
    col->ensure_writable(work_dir_);
    col->resize(row_num);
  }
}

void Table::ingest(uint32_t index, grape::OutArchive& arc) {
  if (column_ptrs_.size() == 0) {
    return;
  }

  CHECK_GT(row_num(), index);
  for (auto col : column_ptrs_) {
    col->ingest(index, arc);
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

void renameProperty(std::string& col_name, std::string& new_col_name) {}

void Table::close() {
  columns_.clear();
  column_ptrs_.clear();
}

void Table::drop() {
  close();
  // TODO: delete files in work_dir
}

void Table::set_name(const std::string& name) { name_ = name; }

void Table::set_work_dir(const std::string& work_dir) { work_dir_ = work_dir; }

}  // namespace gs
