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

#ifndef STORAGES_RT_MUTABLE_GRAPH_VERTEX_TABLE_H_
#define STORAGES_RT_MUTABLE_GRAPH_VERTEX_TABLE_H_

#include "neug/utils/indexers.h"
#include "neug/utils/mmap_array.h"
#include "neug/utils/property/table.h"

namespace gs {
class VertexTable {
 public:
  VertexTable(std::string v_label_name, PropertyType pk_type,
              const std::vector<std::string>& property_names,
              const std::vector<PropertyType>& property_types,
              const std::vector<StorageStrategy>& storage_strategies)
      : table_(std::make_unique<Table>()),
        v_label_name_(v_label_name),
        pk_type_(pk_type),
        property_names(property_names),
        property_types(property_types),
        storage_strategies(storage_strategies),
        memory_level_(1),
        work_dir_(""),
        is_vertex_table_modified_(false) {
    indexer_.init(pk_type);
  }

  VertexTable(VertexTable&& other)
      : indexer_(std::move(other.indexer_)),
        table_(std::move(other.table_)),
        v_label_name_(std::move(other.v_label_name_)),
        pk_type_(other.pk_type_),
        property_names(std::move(other.property_names)),
        property_types(std::move(other.property_types)),
        storage_strategies(std::move(other.storage_strategies)),
        vertex_ts_(std::move(other.vertex_ts_)),
        memory_level_(other.memory_level_),
        work_dir_(other.work_dir_),
        is_vertex_table_modified_(false) {}

  VertexTable(const VertexTable&) = delete;

  void Open(const std::string& snapshot_dir, const std::string& tmp_dir_path,
            int memory_level, bool build_empty_graph = false);

  void Dump(const std::string& snapshot_dir_path);

  void Close();

  void Reserve(size_t cap);

  bool is_dropped() const { return table_ == nullptr; }

  bool get_index(const Any& oid, vid_t& lid,
                 timestamp_t ts = MAX_TIMESTAMP) const;

  Any get_oid(vid_t lid, timestamp_t ts = MAX_TIMESTAMP) const;

  vid_t add_vertex(const Any& id, timestamp_t ts = MAX_TIMESTAMP);

  vid_t add_vertex_safe(const Any& id, timestamp_t ts = MAX_TIMESTAMP);

  size_t vertex_num(timestamp_t ts = MAX_TIMESTAMP) const;

  size_t lid_num() const;  // We don't need a timestamp here since lid_num is
                           // the size of the indexer

  bool is_valid_lid(vid_t lid, timestamp_t ts = MAX_TIMESTAMP) const;

  inline Table& get_properties_table() { return *table_; }

  inline const Table& get_properties_table() const { return *table_; }

  IndexerType& get_indexer() { return indexer_; }
  const IndexerType& get_indexer() const { return indexer_; }

  inline std::shared_ptr<RefColumnBase> get_vertex_id_column() const {
    if (indexer_.get_type() == PropertyType::kInt64) {
      return std::make_shared<TypedRefColumn<int64_t>>(
          dynamic_cast<const TypedColumn<int64_t>&>(indexer_.get_keys()));
    } else if (indexer_.get_type() == PropertyType::kInt32) {
      return std::make_shared<TypedRefColumn<int32_t>>(
          dynamic_cast<const TypedColumn<int32_t>&>(indexer_.get_keys()));
    } else if (indexer_.get_type() == PropertyType::kUInt64) {
      return std::make_shared<TypedRefColumn<uint64_t>>(
          dynamic_cast<const TypedColumn<uint64_t>&>(indexer_.get_keys()));
    } else if (indexer_.get_type() == PropertyType::kUInt32) {
      return std::make_shared<TypedRefColumn<uint32_t>>(
          dynamic_cast<const TypedColumn<uint32_t>&>(indexer_.get_keys()));
    } else if (indexer_.get_type() == PropertyType::kStringView) {
      return std::make_shared<TypedRefColumn<std::string_view>>(
          dynamic_cast<const TypedColumn<std::string_view>&>(
              indexer_.get_keys()));
    } else {
      THROW_NOT_SUPPORTED_EXCEPTION(
          "Only (u)int64/32 and string_view types for pk are supported, but "
          "got: " +
          indexer_.get_type().ToString());
      return nullptr;
    }
  }

  inline std::shared_ptr<ColumnBase> get_property_column(
      const std::string& prop) const {
    return table_->get_column(prop);
  }

  inline const mmap_array<timestamp_t>& get_vertex_timestamps() const {
    return vertex_ts_;
  }

  /**
   * @brief Check if there are any update operations (add/delete vertex) made
   * on this vertex table since it was opened.
   * @return true if there are update operations made, false otherwise.
   */
  inline bool vertex_table_modified() const {
    return is_vertex_table_modified_;
  }

  void BatchAddVertices(std::vector<Any>&& ids, std::unique_ptr<Table> table,
                        timestamp_t ts);

  void BatchDeleteVertices(const std::vector<vid_t>& vids);

  void AddProperties(
      const std::vector<std::string>& property_names,
      const std::vector<PropertyType>& property_types,
      const std::vector<StorageStrategy>& storage_strategies = {});

  void DeleteProperties(const std::vector<std::string>& properties);

  void Drop();

  void RenameProperties(const std::vector<std::string>& old_names,
                        const std::vector<std::string>& new_names);

  std::string work_dir() const { return work_dir_; }

  void Compact(bool reset_timestamp, timestamp_t ts);

 private:
  IndexerType indexer_;
  std::unique_ptr<Table> table_;
  std::string v_label_name_;
  PropertyType pk_type_;
  std::vector<std::string> property_names;
  std::vector<PropertyType> property_types;
  std::vector<StorageStrategy> storage_strategies;
  mmap_array<timestamp_t> vertex_ts_;  // maintains the timestamp of each vertex

  int memory_level_;
  std::string work_dir_;
  bool is_vertex_table_modified_;  // No lock or atomic need,
                                   // synchronized with version manager
                                   // by UpdateTransaction.
};
}  // namespace gs

#endif  // STORAGES_RT_MUTABLE_GRAPH_VERTEX_TABLE_H_
