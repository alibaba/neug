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
#pragma once

#include <stddef.h>
#include <stdint.h>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "neug/common/types/value.h"
#include "neug/config.h"
#include "neug/storages/checkpoint.h"
#include "neug/storages/checkpoint_manager.h"
#include "neug/storages/module/module.h"
#include "neug/utils/property/column.h"
#include "neug/utils/property/types.h"

namespace neug {

class TableView;

class Table {
 public:
  Table();

  Table(const std::vector<std::string>& col_names,
        const std::vector<DataType>& property_types);

  ~Table();

  void Init(Checkpoint& ckp, MemoryLevel level);

  void SetColumn(int idx, std::unique_ptr<ColumnBase> col);

  void Open(Checkpoint& ckp, const ModuleDescriptor& descriptor,
            MemoryLevel memory_level, const std::vector<std::string>& col_name,
            const std::vector<DataType>& property_types);

  ModuleDescriptor Dump(Checkpoint& ckp);

  std::unique_ptr<Table> Clone() const;

  void DetachColumn(size_t col_id, Checkpoint& ckp, MemoryLevel level);

  void DetachAllColumns(Checkpoint& ckp, MemoryLevel level);

  void reset_header(const std::vector<std::string>& col_name);

  void add_columns(Checkpoint& ckp, const std::vector<std::string>& col_names,
                   const std::vector<DataType>& col_types,
                   const std::vector<Value>& default_property_values,
                   size_t capacity,
                   MemoryLevel memory_level = MemoryLevel::kInMemory);

  const std::vector<std::string>& column_names() const;

  std::string column_name(size_t index) const;

  int get_column_id_by_name(const std::string& name) const;

  std::vector<DataTypeId> column_types() const;

  ColumnBase* get_column(const std::string& name);

  const ColumnBase* get_column(const std::string& name) const;

  std::vector<Value> get_row(size_t row_id) const;

  ColumnBase* get_column_by_id(size_t index);

  const ColumnBase* get_column_by_id(size_t index) const;

  void rename_column(const std::string& old_name, const std::string& new_name);

  void delete_column(const std::string& col_name);

  size_t col_num() const;
  inline size_t size() const {
    if (columns_.empty()) {
      return 0;
    } else {
      return columns_[0]->size();
    }
  }

  void insert(size_t index, const std::vector<Value>& values, bool insert_safe);

  void resize(size_t row_num);
  /**
   * @brief Resize the table to row_num, and fill the new rows with default
   * values. Assume it is safe to insert the default value even if it is
   * reserving, since user could always override.
   */
  void resize(size_t row_num, const std::vector<Value>& default_values);

  void ingest(uint32_t index, OutArchive& arc);

  void close();

 private:
  std::unordered_map<std::string, int> col_id_map_;
  std::vector<std::string> col_names_;

  std::vector<std::unique_ptr<ColumnBase>> columns_;

  friend class TableView;
};

}  // namespace neug
