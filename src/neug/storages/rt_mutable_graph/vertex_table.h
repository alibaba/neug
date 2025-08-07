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
#include "neug/utils/property/table.h"
namespace gs {
class VertexTable {
 public:
  VertexTable(std::string v_label_name, PropertyType pk_type,
              const std::vector<std::string>& property_names,
              const std::vector<PropertyType>& property_types,
              const std::vector<StorageStrategy>& storage_strategies)
      : vertex_capacity_(0),
        dumped_num_(0),
        table_(std::make_unique<Table>()),
        v_label_name_(v_label_name),
        pk_type_(pk_type),
        property_names(property_names),
        property_types(property_types),
        storage_strategies(storage_strategies) {
    indexer_.init(pk_type);
  }

  VertexTable(VertexTable&& other)
      : vertex_capacity_(other.vertex_capacity_),
        dumped_num_(other.dumped_num_),
        indexer_(std::move(other.indexer_)),
        table_(std::move(other.table_)),
        v_label_name_(std::move(other.v_label_name_)),
        pk_type_(other.pk_type_),
        property_names(std::move(other.property_names)),
        property_types(std::move(other.property_types)),
        storage_strategies(std::move(other.storage_strategies)),
        vertex_tomb_(std::move(other.vertex_tomb_)) {}

  VertexTable(const VertexTable&) = delete;

  void Open(const std::string& snapshot_dir, const std::string& tmp_dir_path,
            int memory_level, bool build_empty_graph = false);

  void Dump(const std::string& snapshot_dir_path) {
    dumped_num_ = indexer_.size();
    indexer_.dump(
        IndexerType::prefix() + "_" + vertex_map_prefix(v_label_name_),
        snapshot_dir_path);
    table_->resize(dumped_num_);
    table_->dump(vertex_table_prefix(v_label_name_), snapshot_dir_path);
    vertex_tomb_->clear();
  }

  void Close() {
    indexer_.close();
    table_->close();
    vertex_tomb_->clear();
  }

  bool get_index(const Any& oid, vid_t& lid) const;

  Any get_oid(vid_t lid) const;

  vid_t add_vertex(const Any& id);

  vid_t add_vertex_safe(const Any& id);

  size_t vertex_num() const;

  size_t lid_num() const;

  bool is_valid_lid(vid_t lid) const;

  inline Table& get_properties_table() { return *table_; }

  inline const Table& get_properties_table() const { return *table_; }

  IndexerType& get_indexer() { return indexer_; }
  const IndexerType& get_indexer() const { return indexer_; }

  size_t get_vertex_capacity() const { return vertex_capacity_; }

  // only called after Dump
  size_t get_dumped_num() const { return dumped_num_; }

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
      LOG(ERROR) << "Unsupported vertex id type: " << indexer_.get_type();
      return nullptr;
    }
  }

  inline std::shared_ptr<ColumnBase> get_property_column(
      const std::string& prop) const {
    return table_->get_column(prop);
  }

  inline std::shared_ptr<Bitset> get_vertex_tomb() const {
    return vertex_tomb_;
  }

  inline bool is_deleted() const { return vertex_tomb_->count() != 0; }

  void Reserve(size_t cap) {
    if (cap > indexer_.capacity()) {
      indexer_.reserve(cap);
    }
    table_->resize(cap);
    vertex_tomb_->resize(cap);
    vertex_capacity_ = cap;
  }

  void BatchDeleteVertices(const std::vector<vid_t>& vids);

  void AddProperties(
      const std::vector<std::string>& property_names,
      const std::vector<PropertyType>& property_types,
      const std::vector<StorageStrategy>& storage_strategies = {});

  void DeleteProperties(const std::vector<std::string>& properties);

  void Drop();

  void RenameProperties(const std::vector<std::string>& old_names,
                        const std::vector<std::string>& new_names);

 private:
  size_t vertex_capacity_ = 0;
  size_t dumped_num_;
  IndexerType indexer_;
  std::unique_ptr<Table> table_;
  std::string v_label_name_;
  PropertyType pk_type_;
  std::vector<std::string> property_names;
  std::vector<PropertyType> property_types;
  std::vector<StorageStrategy> storage_strategies;
  std::shared_ptr<Bitset> vertex_tomb_;

  int memory_level_;
  std::string work_dir_;
};
}  // namespace gs

#endif  // STORAGES_RT_MUTABLE_GRAPH_VERTEX_TABLE_H_
