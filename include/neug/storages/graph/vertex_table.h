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

#include "neug/execution/common/types/value.h"
#include "neug/storages/graph/schema.h"
#include "neug/storages/graph/vertex_timestamp.h"
#include "neug/storages/loader/loader_utils.h"
#include "neug/storages/module/module.h"
#include "neug/utils/indexers.h"
#include "neug/utils/property/table.h"

namespace neug {

class ModuleBroker;
class CheckpointManifest;
class Checkpoint;
class VertexTableView;

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

  inline bool valid(vid_t v) const {
    return v < size_ && v_ts_.IsVertexValid(v, ts_);
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
  VertexTable()
      : ckp_(nullptr),
        indexer_(std::make_unique<IndexerType>()),
        table_(nullptr),
        pk_type_(DataTypeId::kUnknown),
        vertex_schema_(nullptr),
        v_ts_(std::make_unique<VertexTimestamp>()),
        memory_level_(MemoryLevel::kInMemory) {}

  VertexTable(std::shared_ptr<const VertexSchema> vertex_schema)
      : ckp_(nullptr),
        indexer_(std::make_unique<IndexerType>()),
        table_(std::make_unique<Table>()),
        pk_type_(std::get<0>(vertex_schema->primary_keys[0])),
        vertex_schema_(vertex_schema),
        v_ts_(std::make_unique<VertexTimestamp>()),
        memory_level_(MemoryLevel::kInMemory) {
    assert(vertex_schema->primary_keys.size() == 1);
  }

  VertexTable(VertexTable&& other) noexcept
      : ckp_(std::move(other.ckp_)),
        indexer_(std::move(other.indexer_)),
        table_(std::move(other.table_)),
        pk_type_(other.pk_type_),
        vertex_schema_(other.vertex_schema_),
        v_ts_(std::move(other.v_ts_)),
        memory_level_(other.memory_level_) {}

  VertexTable(const VertexTable&) = delete;

  void Swap(VertexTable& other) {
    std::swap(ckp_, other.ckp_);
    indexer_.swap(other.indexer_);
    table_.swap(other.table_);
    std::swap(pk_type_, other.pk_type_);
    std::swap(vertex_schema_, other.vertex_schema_);
    v_ts_.swap(other.v_ts_);
    std::swap(memory_level_, other.memory_level_);
  }

  void Init(std::shared_ptr<Checkpoint> ckp, MemoryLevel memory_level);

  // --- Snapshot key builders (flat manifest convention) ---
  static std::string KeyKeys(const std::string& label);
  static std::string KeyIndices(const std::string& label);
  static std::string KeyIndexer(const std::string& label);
  static std::string KeyVertexTimestamp(const std::string& label);
  static std::string KeyProperty(const std::string& label, size_t index);

  // --- Snapshot orchestration ---
  /// Restore a VertexTable from a ModuleBroker + CheckpointManifest snapshot.
  /// Falls back to Init() when no checkpoint state exists for this label.
  static VertexTable OpenFrom(std::shared_ptr<Checkpoint> ckp,
                              std::shared_ptr<const VertexSchema> schema,
                              ModuleBroker& store,
                              const CheckpointManifest& meta,
                              MemoryLevel level);

  /// Transfer every leaf module out of this VertexTable into @p store / @p
  /// meta so that a subsequent store.Dump() persists them.  After this call
  /// the table is empty.
  void DisassembleTo(ModuleBroker& store, CheckpointManifest& meta,
                     Checkpoint& ckp);

  void SetIndexer(std::unique_ptr<IndexerType> indexer) {
    indexer_ = std::move(indexer);
  }
  void SetTable(std::unique_ptr<Table> table) { table_ = std::move(table); }
  void SetVertexTimestamp(std::unique_ptr<VertexTimestamp> v_ts) {
    v_ts_ = std::move(v_ts);
  }
  void SetMemoryLevel(MemoryLevel level) { memory_level_ = level; }

  std::unique_ptr<Table> TakeTable() { return std::move(table_); }
  std::unique_ptr<VertexTimestamp> TakeVertexTimestamp() {
    return std::move(v_ts_);
  }
  VertexTable Clone() const;

  void DetachIndexer();
  void DetachVertexTimestamp();

  void Close();

  void SetVertexSchema(std::shared_ptr<const VertexSchema> vertex_schema);

  std::shared_ptr<const VertexSchema> get_vertex_schema_ptr() const {
    return vertex_schema_;
  }

  size_t EnsureCapacity(size_t capacity);

  bool get_index(const execution::Value& oid, vid_t& lid,
                 timestamp_t ts = MAX_TIMESTAMP) const;

  execution::Value GetOid(vid_t lid, timestamp_t ts = MAX_TIMESTAMP) const;

  // Return false if the reserved space is not enough.
  bool AddVertex(const execution::Value& id,
                 const std::vector<execution::Value>& props, vid_t& vid,
                 timestamp_t ts, bool insert_safe);

  bool UpdateProperty(vid_t vid, int32_t prop_id, const execution::Value& value,
                      timestamp_t ts);

  size_t VertexNum(timestamp_t ts = MAX_TIMESTAMP) const;

  size_t LidNum() const;  // We don't need a timestamp here since LidNum is
                          // the size of the indexer

  // Capacity of the vertex table
  inline size_t Capacity() const { return indexer_->capacity(); }

  inline size_t Size() const { return indexer_->size(); }

  bool IsValidLid(vid_t lid, timestamp_t ts = MAX_TIMESTAMP) const;

  IndexerType& get_indexer() { return *indexer_; }
  const IndexerType& get_indexer() const { return *indexer_; }

  inline std::shared_ptr<RefColumnBase> GetPropertyColumn(
      const std::string& prop) const {
    auto pk = vertex_schema_->primary_keys[0];
    if (prop == std::get<1>(pk)) {
      return CreateRefColumn(indexer_->get_keys());
    }
    auto ptr = table_->get_column(prop);
    if (ptr == nullptr) {
      return nullptr;
    }
    return CreateRefColumn(*ptr);
  }

  inline std::shared_ptr<RefColumnBase> GetPropertyColumn(
      int32_t col_id) const {
    auto ptr = table_->get_column_by_id(col_id);
    if (ptr == nullptr) {
      return nullptr;
    }
    return CreateRefColumn(*ptr);
  }

  inline VertexSet GetVertexSet(timestamp_t ts) const {
    return VertexSet(LidNum(), *v_ts_, ts);
  }

  void BatchDeleteVertices(const std::vector<vid_t>& vids);

  void DeleteVertex(const execution::Value& id, timestamp_t ts);

  void DeleteVertex(vid_t lid, timestamp_t ts);

  void RevertDeleteVertex(vid_t lid, timestamp_t ts);

  void AddProperties(
      Checkpoint& ckp, const std::vector<std::string>& property_names,
      const std::vector<DataType>& property_types,
      const std::vector<execution::Value>& default_property_values);

  void DeleteProperties(const std::vector<std::string>& properties);

  void RenameProperties(const std::vector<std::string>& old_names,
                        const std::vector<std::string>& new_names);

  void Compact(timestamp_t ts = MAX_TIMESTAMP);

  void insert_vertices(std::shared_ptr<IDataChunkSupplier> suppliers);

  const VertexTimestamp& get_vertex_timestamp() const { return *v_ts_; }

  const Table& get_table() const { return *table_; }
  Table& get_table() { return *table_; }

 private:
  vid_t insert_vertex_pk(const execution::Value& id, timestamp_t ts,
                         bool insert_safe);
  template <typename PK_T>
  std::vector<vid_t> insert_primary_keys(
      const std::shared_ptr<execution::IContextColumn>& pk_col) {
    size_t row_num = pk_col->size();
    std::vector<vid_t> vids;
    vids.resize(row_num);
    for (size_t j = 0; j < row_num; ++j) {
      auto oid = pk_col->get_elem(j);
      if (NEUG_UNLIKELY(indexer_->get_index(oid, vids[j]))) {
        if (NEUG_UNLIKELY(v_ts_->IsVertexValid(vids[j], MAX_TIMESTAMP))) {
          vids[j] = std::numeric_limits<vid_t>::max();
        } else {
          v_ts_->InsertVertex(vids[j], 0);
        }
        continue;
      }
      bool is_string = std::is_same_v<PK_T, std::string_view> ||
                       std::is_same_v<PK_T, std::string>;
      vids[j] = insert_vertex_pk(oid, 0, is_string);
    }
    return vids;
  }

  template <typename PK_T>
  void insert_vertices_impl(std::shared_ptr<IDataChunkSupplier> supplier) {
    auto row_nums = supplier->RowNum();
    if (row_nums <= 0) {
      LOG(WARNING) << "Row number from supplier is negative, treat it as 0.";
      row_nums = 0;
    }
    size_t new_size = indexer_->size() + row_nums;
    if (new_size > indexer_->capacity()) {
      size_t cap = indexer_->capacity();
      while (new_size >= cap) {
        cap = cap < 4096 ? 4096 : cap + cap / 4;
      }
      EnsureCapacity(cap);
    }
    std::shared_mutex rw_mutex;
    while (true) {
      auto chunk = supplier->GetNextChunk();
      if (chunk == nullptr) {
        break;
      }
      auto& columns = chunk->columns;
      const auto& property_names = vertex_schema_->property_names;
      CHECK(columns.size() == property_names.size() + 1)
          << "Number of columns in the chunk (" << columns.size()
          << ") does not match the number of properties ("
          << property_names.size() + 1 << ").";
      auto ind = std::get<2>(vertex_schema_->primary_keys[0]);
      auto pk_col = columns[ind];

      // Build a list of property columns excluding the PK column.
      std::vector<std::shared_ptr<execution::IContextColumn>> prop_cols;
      prop_cols.reserve(columns.size() - 1);
      for (size_t i = 0; i < columns.size(); ++i) {
        if (static_cast<int>(i) != ind) {
          prop_cols.push_back(columns[i]);
        }
      }

      // Capacity check for actual batch size.
      size_t chunk_rows = chunk->row_num();
      size_t new_size = indexer_->size() + chunk_rows;
      if (new_size > indexer_->capacity()) {
        size_t cap = indexer_->capacity();
        while (new_size >= cap) {
          cap = cap < 4096 ? 4096 : cap + cap / 4;
        }
        EnsureCapacity(cap);
      }

      auto vids = insert_primary_keys<PK_T>(pk_col);

      for (size_t i = 0; i < prop_cols.size(); ++i) {
        auto col = table_->get_column_by_id(i);
        set_properties_from_context_column(col, prop_cols[i], vids, rw_mutex);
      }
      VLOG(10) << "Inserted " << chunk_rows
               << " vertices, current vertex num: " << VertexNum();
    }
  }

  std::shared_ptr<Checkpoint> ckp_;
  std::unique_ptr<IndexerType> indexer_;
  std::unique_ptr<Table> table_;
  DataType pk_type_;
  std::shared_ptr<const VertexSchema> vertex_schema_;
  std::unique_ptr<VertexTimestamp> v_ts_;
  MemoryLevel memory_level_;

  friend class PropertyGraph;
  friend class VertexTableView;
};

namespace internal {
vid_t insert_vertex_pk_internal(IndexerType& indexer, VertexTimestamp& v_ts,
                                const execution::Value& id, timestamp_t ts,
                                bool insert_safe);
}  // namespace internal

}  // namespace neug
