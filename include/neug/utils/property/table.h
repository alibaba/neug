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

#include "neug/config.h"
#include "neug/storages/module/module.h"
#include "neug/storages/workspace.h"
#include "neug/utils/property/column.h"
#include "neug/utils/property/property.h"
#include "neug/utils/property/types.h"

namespace neug {

class Table {
 public:
  Table();
  ~Table();

  void Open(Checkpoint& ckp, const ModuleDescriptor& descriptor,
            MemoryLevel memory_level, const std::vector<std::string>& col_name,
            const std::vector<DataType>& property_types);

  ModuleDescriptor Dump(Checkpoint& ckp);

  void reset_header(const std::vector<std::string>& col_name);

  void add_columns(Checkpoint& ckp, const std::vector<std::string>& col_names,
                   const std::vector<DataType>& col_types,
                   const std::vector<Property>& default_property_values,
                   size_t capacity,
                   MemoryLevel memory_level = MemoryLevel::kInMemory);

  const std::vector<std::string>& column_names() const;

  std::string column_name(size_t index) const;

  int get_column_id_by_name(const std::string& name) const;

  std::vector<DataTypeId> column_types() const;

  std::shared_ptr<ColumnBase> get_column(const std::string& name);

  const std::shared_ptr<ColumnBase> get_column(const std::string& name) const;

  std::vector<Property> get_row(size_t row_id) const;

  std::shared_ptr<ColumnBase> get_column_by_id(size_t index);

  const std::shared_ptr<ColumnBase> get_column_by_id(size_t index) const;

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
  std::vector<std::shared_ptr<ColumnBase>>& columns();
  std::vector<ColumnBase*>& column_ptrs();

  void insert(size_t index, const std::vector<Property>& values,
              bool insert_safe);

  void resize(size_t row_num);
  /**
   * @brief Resize the table to row_num, and fill the new rows with default
   * values. Assume it is safe to insert the default value even if it is
   * reserving, since user could always override.
   */
  void resize(size_t row_num, const std::vector<Property>& default_values);

  inline Property at(size_t row_id, size_t col_id) const {
    return column_ptrs_[col_id]->get_prop(row_id);
  }

  void ingest(uint32_t index, OutArchive& arc);

  void close();

  void drop();

 private:
  void buildColumnPtrs();
  void initColumns(const std::vector<std::string>& col_name,
                   const std::vector<DataType>& types);

  std::unordered_map<std::string, int> col_id_map_;
  std::vector<std::string> col_names_;

  std::vector<std::shared_ptr<ColumnBase>> columns_;
  std::vector<ColumnBase*> column_ptrs_;
};

}  // namespace neug
