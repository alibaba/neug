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

#include "neug/storages/graph/schema.h"
#include "neug/storages/graph/vertex_timestamp.h"
#include "neug/storages/loader/loader_utils.h"
#include "neug/utils/arrow_utils.h"
#include "neug/utils/indexers.h"
#include "neug/utils/property/table.h"

namespace neug {

class VertexSet {
 public:
  VertexSet(vid_t size, const VertexTimestamp& v_ts_, timestamp_t ts)
      : size_(size), v_ts_(v_ts_), ts_(ts) {}
  ~VertexSet() {}

  class iterator {
   public:
    iterator(vid_t v, vid_t limit, const VertexTimestamp& v_tracker,
             timestamp_t ts)
        : v_(v), limit_(limit), v_ts_(v_tracker), ts_(ts) {
      assert(v_ <= limit_);
      assert(limit_ <= v_ts_.Capacity());
      while (v_ < limit_ && !v_ts_.IsVertexValid(v_, ts_)) {
        ++v_;
      }
    }
    ~iterator() {}

    inline vid_t operator*() const { return v_; }

    inline iterator& operator++() {
      do {
        ++v_;
      } while (v_ < limit_ && !v_ts_.IsVertexValid(v_, ts_));
      return *this;
    }

    inline bool operator==(const iterator& rhs) const { return v_ == rhs.v_; }

    inline bool operator!=(const iterator& rhs) const { return v_ != rhs.v_; }

   private:
    vid_t v_, limit_;
    const VertexTimestamp& v_ts_;
    timestamp_t ts_;
  };

  template <typename FUNC_T>
  void foreach_vertex(const FUNC_T& func) const {
    v_ts_.foreach_vertex(func, size_, ts_);
  }

  inline iterator begin() const { return iterator(0, size_, v_ts_, ts_); }
  inline iterator end() const { return iterator(size_, size_, v_ts_, ts_); }
  inline size_t size() const { return size_; }

 private:
  vid_t size_;
  const VertexTimestamp& v_ts_;
  timestamp_t ts_;
};

class PropertyGraph;
class VertexTable {
 public:
  VertexTable(std::shared_ptr<const VertexSchema> vertex_schema)
      : table_(std::make_unique<Table>()),
        vertex_schema_(vertex_schema),
        v_ts_(),
        memory_level_(1),
        work_dir_("") {
    assert(vertex_schema->primary_keys.size() == 1);
    pk_type_ = std::get<0>(vertex_schema->primary_keys[0]);
    indexer_.init(pk_type_.id());
  }

  VertexTable(VertexTable&& other)
      : indexer_(std::move(other.indexer_)),
        table_(std::move(other.table_)),
        pk_type_(other.pk_type_),
        vertex_schema_(other.vertex_schema_),
        v_ts_(std::move(other.v_ts_)),
        memory_level_(other.memory_level_),
        work_dir_(other.work_dir_) {}

  VertexTable(const VertexTable&) = delete;

  void Swap(VertexTable& other) {
    indexer_.swap(other.indexer_);
    table_.swap(other.table_);
    std::swap(pk_type_, other.pk_type_);
    std::swap(vertex_schema_, other.vertex_schema_);
    v_ts_.Swap(other.v_ts_);
    std::swap(memory_level_, other.memory_level_);
    std::swap(work_dir_, other.work_dir_);
  }

  void Open(const std::string& work_dir, int memory_level);

  void Dump(const std::string& target_dir);

  void Close();

  void SetVertexSchema(std::shared_ptr<const VertexSchema> vertex_schema);

  size_t EnsureCapacity(size_t capacity);

  bool is_dropped() const { return table_ == nullptr; }

  bool get_index(const Property& oid, vid_t& lid,
                 timestamp_t ts = MAX_TIMESTAMP) const;

  Property GetOid(vid_t lid, timestamp_t ts = MAX_TIMESTAMP) const;

  // Return false if the reserved space is not enough.
  bool AddVertex(const Property& id, const std::vector<Property>& props,
                 vid_t& vid, timestamp_t ts = 0, bool insert_safe = false);

  bool UpdateProperty(vid_t vid, int32_t prop_id, const Property& value,
                      timestamp_t ts);

  size_t VertexNum(timestamp_t ts = MAX_TIMESTAMP) const;

  size_t LidNum() const;  // We don't need a timestamp here since LidNum is the
                          // size of the indexer

  // Capacity of the vertex table
  inline size_t Capacity() const { return indexer_.capacity(); }

  inline size_t Size() const { return indexer_.size(); }

  bool IsValidLid(vid_t lid, timestamp_t ts = MAX_TIMESTAMP) const;

  IndexerType& get_indexer() { return indexer_; }
  const IndexerType& get_indexer() const { return indexer_; }

  inline std::shared_ptr<RefColumnBase> GetPropertyColumn(
      const std::string& prop) const {
    auto pk = vertex_schema_->primary_keys[0];
    if (prop == std::get<1>(pk)) {
      return CreateRefColumn(indexer_.get_keys());
    }
    auto ptr = table_->get_column(prop);
    if (ptr == nullptr) {
      return nullptr;
    }
    return CreateRefColumn(*ptr);
  }

  inline std::shared_ptr<ColumnBase> get_property_column(int32_t col_id) const {
    assert(col_id >= 0 && col_id < static_cast<int32_t>(table_->col_num()));
    return table_->get_column_by_id(col_id);
  }

  inline VertexSet GetVertexSet(timestamp_t ts) const {
    return VertexSet(LidNum(), v_ts_, ts);
  }

  void BatchDeleteVertices(const std::vector<vid_t>& vids);

  void DeleteVertex(const Property& id, timestamp_t ts);

  void DeleteVertex(vid_t lid, timestamp_t ts);

  void RevertDeleteVertex(vid_t lid, timestamp_t ts);

  void AddProperties(
      const std::vector<std::string>& property_names,
      const std::vector<DataType>& property_types,
      const std::vector<Property>& default_property_values,
      const std::vector<StorageStrategy>& storage_strategies = {});

  void DeleteProperties(const std::vector<std::string>& properties);

  void Drop();

  void RenameProperties(const std::vector<std::string>& old_names,
                        const std::vector<std::string>& new_names);

  std::string work_dir() const { return work_dir_; }

  void Compact(timestamp_t ts = MAX_TIMESTAMP);

  inline std::string& work_dir() { return work_dir_; }

  void insert_vertices(std::shared_ptr<IRecordBatchSupplier> suppliers);

  const VertexTimestamp& get_vertex_timestamp() const { return v_ts_; }

 private:
  vid_t insert_vertex_pk(const Property& id, timestamp_t ts);
  template <typename PK_T>
  std::vector<vid_t> insert_primary_keys(
      std::shared_ptr<arrow::Array> primary_key_column) {
    size_t row_num = primary_key_column->length();
    std::vector<vid_t> vids;
    vids.resize(row_num);
    if constexpr (!std::is_same<std::string_view, PK_T>::value &&
                  !std::is_same<std::string, PK_T>::value) {
      auto expected_type = neug::TypeConverter<PK_T>::ArrowTypeValue();
      using arrow_array_t = typename neug::TypeConverter<PK_T>::ArrowArrayType;
      if (!primary_key_column->type()->Equals(expected_type)) {
        LOG(FATAL) << "Inconsistent data type, expect "
                   << expected_type->ToString() << ", but got "
                   << primary_key_column->type()->ToString();
      }
      auto casted_array =
          std::static_pointer_cast<arrow_array_t>(primary_key_column);

      for (size_t j = 0; j < row_num; ++j) {
        auto oid = PropUtils<PK_T>::to_prop(casted_array->Value(j));
        if (NEUG_UNLIKELY(indexer_.get_index(oid, vids[j]))) {
          if (NEUG_UNLIKELY(v_ts_.IsVertexValid(vids[j], MAX_TIMESTAMP))) {
            vids[j] = std::numeric_limits<vid_t>::max();
          } else {
            v_ts_.InsertVertex(vids[j], 0);
          }
          continue;  // already exists
        }
        vids[j] = insert_vertex_pk(oid, 0);
      }
    } else {
      if (primary_key_column->type()->Equals(arrow::utf8())) {
        auto casted_array =
            std::static_pointer_cast<arrow::StringArray>(primary_key_column);
        for (size_t j = 0; j < row_num; ++j) {
          auto oid = Property::from_string_view(casted_array->GetView(j));
          if (indexer_.get_index(oid, vids[j])) {
            if (NEUG_UNLIKELY(v_ts_.IsVertexValid(vids[j], MAX_TIMESTAMP))) {
              vids[j] = std::numeric_limits<vid_t>::max();
            } else {
              v_ts_.InsertVertex(vids[j], 0);
            }
            continue;  // already exists
          }
          vids[j] = insert_vertex_pk(oid, 0);
        }
      } else if (primary_key_column->type()->Equals(arrow::large_utf8())) {
        auto casted_array = std::static_pointer_cast<arrow::LargeStringArray>(
            primary_key_column);
        for (size_t j = 0; j < row_num; ++j) {
          auto oid = Property::from_string_view(casted_array->GetView(j));
          if (indexer_.get_index(oid, vids[j])) {
            if (NEUG_UNLIKELY(v_ts_.IsVertexValid(vids[j], MAX_TIMESTAMP))) {
              vids[j] = std::numeric_limits<vid_t>::max();
            } else {
              v_ts_.InsertVertex(vids[j], 0);
            }
            continue;  // already exists
          }
          vids[j] = insert_vertex_pk(oid, 0);
        }
      } else {
        LOG(FATAL) << "Not support type: "
                   << primary_key_column->type()->ToString();
      }
    }
    return vids;
  }

  template <typename PK_T>
  void insert_vertices_impl(std::shared_ptr<IRecordBatchSupplier> supplier) {
    while (true) {
      auto batch = supplier->GetNextBatch();
      if (batch == nullptr) {
        break;
      }
      auto columns = batch->columns();
      const auto& property_names = vertex_schema_->property_names;
      CHECK(columns.size() == property_names.size() + 1)
          << "Number of columns in the batch (" << columns.size()
          << ") does not match the number of properties ("
          << property_names.size() + 1 << ").";
      auto ind = std::get<2>(vertex_schema_->primary_keys[0]);
      auto pk_array = columns[ind];
      columns.erase(columns.begin() + ind);
      size_t new_size = indexer_.size() + pk_array->length();
      if (new_size >= indexer_.capacity()) {
        size_t new_cap = new_size;
        while (new_size >= new_cap) {
          new_cap = new_cap < 4096 ? 4096 : new_cap + new_cap / 4;
        }
        EnsureCapacity(new_cap);
      }

      auto vids = insert_primary_keys<PK_T>(pk_array);

      for (size_t i = 0; i < columns.size(); ++i) {
        auto col = table_->get_column_by_id(i);
        auto chunked_array = std::make_shared<arrow::ChunkedArray>(columns[i]);
        set_properties_column(col, chunked_array, vids);
      }
      VLOG(10) << "Inserted " << pk_array->length()
               << " vertices, current vertex num: " << VertexNum();
    }
  }

  IndexerType indexer_;
  std::unique_ptr<Table> table_;
  DataType pk_type_;
  std::shared_ptr<const VertexSchema> vertex_schema_;
  VertexTimestamp v_ts_;
  int memory_level_;

  std::string work_dir_;

  friend class PropertyGraph;
};
}  // namespace neug
