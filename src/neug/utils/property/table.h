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

#ifndef UTILS_PROPERTY_TABLE_H_
#define UTILS_PROPERTY_TABLE_H_

#include <stddef.h>  // for size_t
#include <stdint.h>  // for int32_t, uint32_t
#include <memory>
#include <string>  // for string
#include <vector>  // for vector

#include "neug/utils/id_indexer.h"
#include "neug/utils/property/column.h"
#include "neug/utils/property/types.h"  // for StorageStrategy, Any, Propert...

namespace grape {
class OutArchive;
}  // namespace grape

namespace gs {

class Table {
 public:
  Table();
  ~Table();

  void init(const std::string& name, const std::string& work_dir,
            const std::vector<std::string>& col_name,
            const std::vector<PropertyType>& types,
            const std::vector<StorageStrategy>& strategies_);

  void open(const std::string& name, const std::string& snapshot_dir,
            const std::string& work_dir,
            const std::vector<std::string>& col_name,
            const std::vector<PropertyType>& property_types,
            const std::vector<StorageStrategy>& strategies_);

  void open_in_memory(const std::string& name, const std::string& snapshot_dir,
                      const std::vector<std::string>& col_name,
                      const std::vector<PropertyType>& property_types,
                      const std::vector<StorageStrategy>& strategies_);

  void open_with_hugepages(const std::string& name,
                           const std::string& snapshot_dir,
                           const std::vector<std::string>& col_name,
                           const std::vector<PropertyType>& property_types,
                           const std::vector<StorageStrategy>& strategies_,
                           bool force);

  void touch(const std::string& name, const std::string& work_dir);

  void copy_to_tmp(const std::string& name, const std::string& snapshot_dir,
                   const std::string& work_dir);

  void dump(const std::string& name, const std::string& snapshot_dir);

  void dump_without_close(const std::string& name,
                          const std::string& snapshot_dir);

  void reset_header(const std::vector<std::string>& col_name);

  void add_column(const std::string& col_name, const PropertyType& col_types,
                  std::shared_ptr<ColumnBase> column);

  void add_columns(const std::vector<std::string>& col_names,
                   const std::vector<PropertyType>& col_types);

  std::vector<std::string> column_names() const;

  std::string column_name(size_t index) const;

  int get_column_id_by_name(const std::string& name) const;

  std::vector<PropertyType> column_types() const;

  std::shared_ptr<ColumnBase> get_column(const std::string& name);

  const std::shared_ptr<ColumnBase> get_column(const std::string& name) const;

  std::vector<Any> get_row(size_t row_id) const;

  std::shared_ptr<ColumnBase> get_column_by_id(size_t index);

  const std::shared_ptr<ColumnBase> get_column_by_id(size_t index) const;

  void rename_column(const std::string& old_name, const std::string& new_name);

  void delete_column(const std::string& col_name);

  size_t col_num() const;
  size_t row_num() const;
  std::vector<std::shared_ptr<ColumnBase>>& columns();
  std::vector<ColumnBase*>& column_ptrs();

  void insert(size_t index, const std::vector<Any>& values);

  /**
   * @brief Different from insert, this function will resize the columns
   * if the index is larger than the current row number.
   * @param index The index to insert the row.
   * @param values The values to insert.
   */
  void insert_with_resize(size_t index, const std::vector<Any>& values);

  // insert properties except for the primary key
  // col_ind_mapping: the mapping from the column index in
  // the raw file row to the column index in the schema
  void insert(size_t index, const std::vector<Any>& values,
              const std::vector<int32_t>& col_ind_mapping);

  void resize(size_t row_num);

  inline Any at(size_t row_id, size_t col_id) {
    return column_ptrs_[col_id]->get(row_id);
  }

  inline Any at(size_t row_id, size_t col_id) const {
    return column_ptrs_[col_id]->get(row_id);
  }

  void ingest(uint32_t index, grape::OutArchive& arc);

  void renameProperty(std::string& col_name, std::string& new_col_name);

  void close();

 private:
  void buildColumnPtrs();
  void initColumns(const std::vector<std::string>& col_name,
                   const std::vector<PropertyType>& types,
                   const std::vector<StorageStrategy>& strategies_);

  IdIndexer<std::string, int> col_id_indexer_;

  std::vector<std::shared_ptr<ColumnBase>> columns_;
  std::vector<ColumnBase*> column_ptrs_;

  bool touched_;
  std::string name_;
  std::string work_dir_, snapshot_dir_;
};

}  // namespace gs

#endif  // UTILS_PROPERTY_TABLE_H_
