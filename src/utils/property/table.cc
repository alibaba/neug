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

#include "src/utils/property/table.h"

#include <assert.h>        // for assert
#include <glog/logging.h>  // for Check_EQImpl, CHECK_EQ, CHECK, COMPACT...
                           // for __alloc_traits<>::value_type
#include <ostream>         // for operator<<, basic_ostream

#include "src/utils/id_indexer.h"       // for IdIndexer
#include "src/utils/property/column.h"  // for ColumnBase, CreateColumn

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
  columns_.resize(col_num, nullptr);
  auto strategies = strategies_;
  strategies.resize(col_num, StorageStrategy::kMem);

  for (size_t i = 0; i < col_num; ++i) {
    int col_id;
    col_id_indexer_.add(col_name[i], col_id);
    columns_[col_id] = CreateColumn(property_types[i], strategies[i]);
  }
  columns_.resize(col_id_indexer_.size());
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
                      work_dir);
  }
  touched_ = false;
  buildColumnPtrs();
}

void Table::open_in_memory(const std::string& name,
                           const std::string& snapshot_dir,
                           const std::vector<std::string>& col_name,
                           const std::vector<PropertyType>& property_types,
                           const std::vector<StorageStrategy>& strategies_) {
  name_ = name;
  snapshot_dir_ = snapshot_dir;
  initColumns(col_name, property_types, strategies_);
  for (size_t i = 0; i < columns_.size(); ++i) {
    columns_[i]->open_in_memory(snapshot_dir + "/" + name + ".col_" +
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

void Table::touch(const std::string& name, const std::string& work_dir) {
  if (touched_) {
    LOG(ERROR) << "Table " << name << " has been touched before";
    return;
  }
  int i = 0;
  for (auto& col : columns_) {
    col->touch(work_dir + "/" + name + ".col_" + std::to_string(i++));
  }
  touched_ = true;
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

void Table::dump_without_close(const std::string& name,
                               const std::string& snapshot_dir) {
  int i = 0;
  for (auto col : columns_) {
    col->dump_without_close(snapshot_dir + "/" + name + ".col_" +
                            std::to_string(i++));
  }
}

void Table::reset_header(const std::vector<std::string>& col_name) {
  IdIndexer<std::string, int> new_col_id_indexer;
  size_t col_num = col_name.size();
  for (size_t i = 0; i < col_num; ++i) {
    int tmp;
    new_col_id_indexer.add(col_name[i], tmp);
  }
  CHECK_EQ(col_num, new_col_id_indexer.size());
  col_id_indexer_.swap(new_col_id_indexer);
}

void Table::add_column(const std::string& col_name,
                       const PropertyType& col_types,
                       std::shared_ptr<ColumnBase> column) {
  int col_id;
  columns_.emplace_back(column);
  col_id_indexer_.add(col_name, col_id);
  CHECK_EQ(col_id, columns_.size() - 1);

  buildColumnPtrs();
}

void Table::add_columns(const std::vector<std::string>& col_names,
                        const std::vector<PropertyType>& col_types) {
  // When add_columns are called, the table is already initialized and col_files
  // are opened.
  size_t old_size = columns_.size();
  columns_.resize(old_size + col_names.size());

  for (size_t i = 0; i < col_names.size(); ++i) {
    int col_id;
    col_id_indexer_.add(col_names[i], col_id);
    columns_[col_id] = CreateColumn(col_types[i], StorageStrategy::kMem);
  }
  for (size_t i = old_size; i < columns_.size(); ++i) {
    columns_[i]->open_in_memory(snapshot_dir_ + "/" + name_ + ".col_" +
                                std::to_string(i));
    columns_[i]->resize(row_num());
  }
  buildColumnPtrs();
}

void Table::rename_column(const std::string& old_name,
                          const std::string& new_name) {
  int col_id;
  if (col_id_indexer_.get_index(old_name, col_id)) {
    col_id_indexer_.remove(old_name);
    col_id_indexer_.add(new_name, col_id);
  } else {
    LOG(ERROR) << "Column " << old_name << " does not exist.";
  }
}

void Table::delete_column(const std::string& col_name) {
  int col_id;
  if (col_id_indexer_.get_index(col_name, col_id)) {
    col_id_indexer_.remove(col_name);
    columns_[col_id]->close();
    columns_[col_id].reset();
    columns_.erase(columns_.begin() + col_id);
    column_ptrs_[col_id] = nullptr;
  } else {
    LOG(ERROR) << "Column " << col_name << " does not exist.";
  }
}

std::vector<std::string> Table::column_names() const {
  size_t col_num = col_id_indexer_.size();
  std::vector<std::string> names(col_num);
  for (size_t col_i = 0; col_i < col_num; ++col_i) {
    CHECK(col_id_indexer_.get_key(col_i, names[col_i]));
  }
  return names;
}

std::string Table::column_name(size_t index) const {
  size_t col_num = col_id_indexer_.size();
  CHECK(index < col_num);
  std::string name{};
  CHECK(col_id_indexer_.get_key(index, name));
  return name;
}

int Table::get_column_id_by_name(const std::string& name) const {
  int col_id;
  if (col_id_indexer_.get_index(name, col_id)) {
    return col_id;
  }
  return -1;
}

std::vector<PropertyType> Table::column_types() const {
  size_t col_num = col_id_indexer_.size();
  std::vector<PropertyType> types(col_num);
  for (size_t col_i = 0; col_i < col_num; ++col_i) {
    types[col_i] = columns_[col_i]->type();
  }
  return types;
}

std::shared_ptr<ColumnBase> Table::get_column(const std::string& name) {
  int col_id;
  if (col_id_indexer_.get_index(name, col_id)) {
    if (static_cast<size_t>(col_id) < columns_.size()) {
      return columns_[col_id];
    }
  }

  return nullptr;
}

const std::shared_ptr<ColumnBase> Table::get_column(
    const std::string& name) const {
  int col_id;
  if (col_id_indexer_.get_index(name, col_id)) {
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

// column_id_mapping is the mapping from the column id in the input table to the
// column id in the current table
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

}  // namespace gs
