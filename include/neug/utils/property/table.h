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

#include "neug/utils/property/column.h"
#include "neug/utils/property/property.h"
#include "neug/utils/property/types.h"

namespace gs {

class Table {
 public:
  Table();
  ~Table();

  void init(const std::string& name, const std::string& work_dir,
            const std::vector<std::string>& col_name,
            const std::vector<DataTypeId>& types,
            const std::vector<Property>& default_property_values,
            const std::vector<StorageStrategy>& strategies_);

  void open(
      const std::string& name, const std::string& work_dir,
      const std::vector<std::string>& col_name,
      const std::vector<DataTypeId>& property_types,
      const std::vector<Property>& default_property_values,
      const std::vector<StorageStrategy>& strategies_,
      const std::vector<std::shared_ptr<ExtraTypeInfo>>& extra_type_infos = {});

  void open_in_memory(
      const std::string& name, const std::string& work_dir,
      const std::vector<std::string>& col_name,
      const std::vector<DataTypeId>& property_types,
      const std::vector<Property>& default_property_values,
      const std::vector<StorageStrategy>& strategies_,
      const std::vector<std::shared_ptr<ExtraTypeInfo>>& extra_type_infos = {});

  void open_with_hugepages(
      const std::string& name, const std::string& work_dir,
      const std::vector<std::string>& col_name,
      const std::vector<DataTypeId>& property_types,
      const std::vector<Property>& default_property_values,
      const std::vector<StorageStrategy>& strategies_,
      const std::vector<std::shared_ptr<ExtraTypeInfo>>& extra_type_infos = {},
      bool force = false);

  void copy_to_tmp(const std::string& name, const std::string& snapshot_dir,
                   const std::string& work_dir);

  void dump(const std::string& name, const std::string& snapshot_dir);

  void reset_header(const std::vector<std::string>& col_name);

  void add_column(const std::string& col_name, const DataTypeId& col_types,
                  const Property& default_value,
                  std::shared_ptr<ColumnBase> column);

  void add_columns(
      const std::vector<std::string>& col_names,
      const std::vector<DataTypeId>& col_types,
      const std::vector<Property>& default_property_values,
      const std::vector<StorageStrategy>& strategies_ = {},
      const std::vector<std::shared_ptr<ExtraTypeInfo>>& extra_type_infos = {},
      int memory_level = 0);

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
  size_t row_num() const;
  std::vector<std::shared_ptr<ColumnBase>>& columns();
  std::vector<ColumnBase*>& column_ptrs();

  void insert(size_t index, const std::vector<Property>& values);

  /**
   * @brief Different from insert, this function will resize the columns
   * if the index is larger than the current row number.
   * @param index The index to insert the row.
   * @param values The values to insert.
   */
  void insert_with_resize(size_t index, const std::vector<Property>& values);

  // insert properties except for the primary key
  // col_ind_mapping: the mapping from the column index in
  // the raw file row to the column index in the schema
  void insert(size_t index, const std::vector<Property>& values,
              const std::vector<int32_t>& col_ind_mapping);

  void resize(size_t row_num);

  inline Property at(size_t row_id, size_t col_id) const {
    return column_ptrs_[col_id]->get_prop(row_id);
  }

  void ingest(uint32_t index, OutArchive& arc);

  void close();

  void drop();

  void ensure_writable(size_t col_id);

  void set_name(const std::string& name);

  void set_work_dir(const std::string& work_dir);

 private:
  void mark_column_deleted(const std::string& col_name);
  void buildColumnPtrs();
  void initColumns(
      const std::vector<std::string>& col_name,
      const std::vector<DataTypeId>& types,
      const std::vector<Property>& default_property_values,
      const std::vector<StorageStrategy>& strategies_,
      const std::vector<std::shared_ptr<ExtraTypeInfo>>& extra_type_infos = {});

  std::unordered_map<std::string, int> col_id_map_;
  std::vector<std::string> col_names_;
  std::vector<Property> col_default_values_;

  std::vector<std::shared_ptr<ColumnBase>> columns_;
  std::vector<ColumnBase*> column_ptrs_;
  std::vector<bool> col_deleted_;

  bool touched_;
  std::string name_;
  std::string work_dir_, snapshot_dir_;
};

}  // namespace gs
