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

#include "neug/utils/exception/exception.h"
#include "neug/utils/property/column.h"
#include "neug/utils/serialization/out_archive.h"

namespace neug {

Table::Table() {}
Table::~Table() { close(); }

Table::Table(const std::vector<std::string>& col_names,
             const std::vector<DataType>& property_types) {
  size_t col_num = col_names.size();
  columns_.clear();
  col_names_.clear();
  col_id_map_.clear();
  columns_.resize(col_num);

  for (size_t i = 0; i < col_num; ++i) {
    int col_id = col_names_.size();
    col_id_map_.insert({col_names[i], col_id});
    col_names_.emplace_back(col_names[i]);
    assert(i < property_types.size());
    columns_[col_id] = CreateColumn(property_types[i]);
  }
  columns_.resize(col_id_map_.size());
}
std::unique_ptr<Table> Table::Clone() const {
  auto cow_clone = std::make_unique<Table>();
  cow_clone->col_names_ = col_names_;
  cow_clone->col_id_map_ = col_id_map_;
  cow_clone->columns_.reserve(columns_.size());
  for (const auto& col : columns_) {
    cow_clone->columns_.push_back(std::unique_ptr<ColumnBase>(
        static_cast<ColumnBase*>(col->Clone().release())));
  }
  return cow_clone;
}

void Table::DetachColumn(size_t col_id, Checkpoint& ckp, MemoryLevel level) {
  if (col_id >= columns_.size())
    return;
  columns_[col_id]->Detach(ckp, level);
}

void Table::DetachAllColumns(Checkpoint& ckp, MemoryLevel level) {
  for (size_t i = 0; i < columns_.size(); ++i) {
    DetachColumn(i, ckp, level);
  }
}

void Table::Init(Checkpoint& ckp, MemoryLevel level) {
  const ModuleDescriptor empty{};
  for (size_t i = 0; i < columns_.size(); ++i) {
    columns_[i]->Open(ckp, empty, level);
  }
}

void Table::SetColumn(int idx, std::unique_ptr<ColumnBase> col) {
  if (idx < 0 || static_cast<size_t>(idx) >= columns_.size()) {
    THROW_INVALID_ARGUMENT_EXCEPTION(
        "Table::SetColumn: index " + std::to_string(idx) +
        " out of range (col_num=" + std::to_string(columns_.size()) + ")");
  }
  columns_[idx] = std::move(col);
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

void Table::add_columns(
    Checkpoint& ckp, const std::vector<std::string>& col_names,
    const std::vector<DataType>& col_types,
    const std::vector<execution::Value>& default_property_values,
    size_t capacity, MemoryLevel memory_level) {
  if (default_property_values.size() != col_names.size()) {
    THROW_RUNTIME_ERROR("default_property_values size mismatch: expected " +
                        std::to_string(col_names.size()) + " but got " +
                        std::to_string(default_property_values.size()));
  }
  // When add_columns are called, the table is already initialized and
  // col_files are opened.
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
    columns_[col_id] = std::unique_ptr<ColumnBase>(CreateColumn(col_types[i]));
  }
  for (size_t i = old_size; i < columns_.size(); ++i) {
    columns_[i]->Open(ckp, ModuleDescriptor(), memory_level);
    columns_[i]->resize(capacity, default_property_values[i - old_size]);
  }
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
    columns_[col_id].reset();
    columns_.erase(columns_.begin() + col_id);
    col_names_.erase(col_names_.begin() + col_id);
    for (auto& pair : col_id_map_) {
      if (pair.second > col_id) {
        pair.second -= 1;
      }
    }
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

ColumnBase* Table::get_column(const std::string& name) {
  auto it = col_id_map_.find(name);
  if (it != col_id_map_.end()) {
    int col_id = it->second;
    if (static_cast<size_t>(col_id) < columns_.size()) {
      return columns_[col_id].get();
    }
  }

  return nullptr;
}

const ColumnBase* Table::get_column(const std::string& name) const {
  auto it = col_id_map_.find(name);
  if (it != col_id_map_.end()) {
    int col_id = it->second;
    if (static_cast<size_t>(col_id) < columns_.size()) {
      return columns_[col_id].get();
    }
  }

  return nullptr;
}

std::vector<execution::Value> Table::get_row(size_t row_id) const {
  std::vector<execution::Value> ret;
  for (auto& ptr : columns_) {
    ret.push_back(ptr->get_any(row_id));
  }
  return ret;
}

ColumnBase* Table::get_column_by_id(size_t index) {
  if (index >= columns_.size()) {
    return nullptr;
  } else {
    return columns_[index].get();
  }
}

const ColumnBase* Table::get_column_by_id(size_t index) const {
  if (index >= columns_.size()) {
    return nullptr;
  } else {
    return columns_[index].get();
  }
}

size_t Table::col_num() const { return columns_.size(); }

void Table::insert(size_t index, const std::vector<execution::Value>& values,
                   bool insert_safe) {
  assert(values.size() == columns_.size());
  CHECK_EQ(values.size(), columns_.size());
  size_t col_num = columns_.size();
  for (size_t i = 0; i < col_num; ++i) {
    columns_[i]->set_any(index, values[i], insert_safe);
  }
}

void Table::resize(size_t row_num) {
  for (const auto& col : columns_) {
    col->resize(row_num);
  }
}

void Table::resize(size_t row_num,
                   const std::vector<execution::Value>& default_values) {
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
  if (columns_.size() == 0) {
    return;
  }

  CHECK_GT(columns_[0]->size(), index);
  uint32_t num_updates;
  arc >> num_updates;
  for (uint32_t i = 0; i < num_updates; ++i) {
    uint32_t col_id;
    arc >> col_id;
    if (col_id >= columns_.size()) {
      THROW_INTERNAL_EXCEPTION(
          "Column id out of range: " + std::to_string(col_id) +
          " >= " + std::to_string(columns_.size()) + "Table::ingest");
      continue;
    }
    columns_[col_id]->ingest(index, arc);
  }
}

void Table::close() { columns_.clear(); }

}  // namespace neug
