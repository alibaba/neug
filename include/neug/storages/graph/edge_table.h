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
#include <atomic>
#include <memory>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "neug/execution/common/types/value.h"
#include "neug/storages/allocators.h"
#include "neug/storages/checkpoint_manager.h"
#include "neug/storages/csr/csr_base.h"
#include "neug/storages/csr/csr_view.h"
#include "neug/storages/graph/schema.h"
#include "neug/storages/module/module.h"
#include "neug/utils/indexers.h"
#include "neug/utils/property/table.h"
#include "neug/utils/property/types.h"

namespace neug {

class ModuleBroker;
class CheckpointManifest;
class PropertyGraph;

class IDataChunkSupplier;

class EdgeTable {
 public:
  EdgeTable(std::shared_ptr<const EdgeSchema> meta) : meta_(meta) {}
  EdgeTable(EdgeTable&& edge_table);

  EdgeTable(const EdgeTable&) = delete;
  ~EdgeTable() = default;

  void Swap(EdgeTable& other);

  EdgeTable Clone() const;

  void DetachOutCsr();
  void DetachInCsr();

  void SetEdgeSchema(std::shared_ptr<const EdgeSchema> meta);

  void Init(std::shared_ptr<Checkpoint> ckp, MemoryLevel memory_level);

  // --- Snapshot key builders (flat manifest convention) ---
  static std::string KeyOutCsr(const std::string& src, const std::string& edge,
                               const std::string& dst);
  static std::string KeyInCsr(const std::string& src, const std::string& edge,
                              const std::string& dst);
  static std::string KeyProperty(const std::string& src,
                                 const std::string& edge,
                                 const std::string& dst, size_t index);
  static std::string ScalarKey(const std::string& src, const std::string& edge,
                               const std::string& dst,
                               const std::string& field);

  // --- Snapshot orchestration ---
  static EdgeTable OpenFrom(std::shared_ptr<Checkpoint> ckp,
                            std::shared_ptr<const EdgeSchema> schema,
                            ModuleBroker& store, const CheckpointManifest& meta,
                            MemoryLevel level);

  void DisassembleTo(ModuleBroker& store, CheckpointManifest& meta,
                     Checkpoint& ckp);

  void SetInCsr(std::unique_ptr<CsrBase> csr);
  void SetOutCsr(std::unique_ptr<CsrBase> csr);
  void SetTable(std::unique_ptr<Table> table) { table_ = std::move(table); }
  void SetTableIdx(uint64_t n) { table_idx_.store(n); }
  void SetCapacity(uint64_t n) { capacity_.store(n); }
  void SetMemoryLevel(MemoryLevel level) { memory_level_ = level; }

  std::unique_ptr<CsrBase> TakeInCsr() { return std::move(in_csr_); }
  std::unique_ptr<CsrBase> TakeOutCsr() { return std::move(out_csr_); }
  std::unique_ptr<Table> TakeTable() { return std::move(table_); }
  uint64_t GetTableIdx() const { return table_idx_.load(); }
  uint64_t GetCapacity() const { return capacity_.load(); }

  std::shared_ptr<const EdgeSchema> get_edge_schema_ptr() const {
    return meta_;
  }

  Table* table() { return table_.get(); }
  const Table* table() const { return table_.get(); }

  void Close();

  void SortByEdgeData(timestamp_t ts);

  void BatchDeleteVertices(const std::set<vid_t>& src_set,
                           const std::set<vid_t>& dst_set);

  void BatchDeleteEdges(const std::vector<vid_t>& src_list,
                        const std::vector<vid_t>& dst_list);

  void BatchDeleteEdges(const std::vector<std::pair<vid_t, int32_t>>& oe_edges,
                        const std::vector<std::pair<vid_t, int32_t>>& ie_edges);

  void EnsureCapacity(size_t capacity);

  void EnsureCapacity(vid_t src_vertex_num, vid_t dst_vertex_num,
                      size_t capacity = 0);

  size_t EdgeNum() const;

  size_t PropertyNum() const;

  CsrView get_outgoing_view(timestamp_t ts) const;
  CsrView get_incoming_view(timestamp_t ts) const;

  EdgeDataAccessor get_edge_data_accessor(int col_id) const;

  EdgeDataAccessor get_edge_data_accessor(const std::string& col_name) const;

  void BatchAddEdges(const IndexerType& src_indexer,
                     const IndexerType& dst_indexer,
                     std::shared_ptr<IDataChunkSupplier> supplier);

  // Add edges in batch to the edge table.
  void BatchAddEdges(
      const std::vector<vid_t>& src_lid_list,
      const std::vector<vid_t>& dst_lid_list,
      const std::vector<std::vector<execution::Value>>& edge_data_list);

  // Add a single edge to the edge table. Note this method requires an Allocator
  // to allocate memory for the edge data. Should be called in tp mode.
  std::pair<int32_t, const void*> AddEdge(
      vid_t src_lid, vid_t dst_lid,
      const std::vector<execution::Value>& properties, timestamp_t ts,
      Allocator& alloc, bool insert_safe);

  void RenameProperties(const std::vector<std::string>& old_names,
                        const std::vector<std::string>& new_names);

  void AddProperties(Checkpoint& ckp, const std::vector<std::string>& names,
                     const std::vector<DataType>& types,
                     const std::vector<execution::Value>& default_values = {});

  void DeleteProperties(Checkpoint& ckp,
                        const std::vector<std::string>& col_names);

  void DeleteEdge(vid_t src_lid, vid_t dst_lid, int32_t oe_offset,
                  int32_t ie_offset, timestamp_t ts);

  /**
   * @brief Delete edges associated with a vertex.
   * @param is_src Whether the vertex is the source vertex.
   * @param lid The local id of the vertex.
   * @param ts The timestamp.
   */
  void DeleteVertex(bool is_src, vid_t lid, timestamp_t ts);

  void UpdateEdgeProperty(vid_t src_lid, vid_t dst_lid, int32_t oe_offset,
                          int32_t ie_offset, int32_t col_id,
                          const execution::Value& new_prop, timestamp_t ts);

  void Compact(bool compact_csr,
               const std::optional<std::string>& sort_key_for_nbr,
               timestamp_t ts);

  size_t PropTableSize() const;

  size_t Capacity() const;

  std::shared_ptr<const EdgeSchema> meta() const { return meta_; }

  /// Detach the outgoing adjlist of vertex vid.
  /// Called by UpdateTransaction via PropertyGraphCowState when the adjlist
  /// has not yet been detached in the current transaction.
  void DetachOutAdjlist(vid_t vid, Allocator& alloc);

  /// Detach the incoming adjlist of vertex vid.
  void DetachInAdjlist(vid_t vid, Allocator& alloc);

 private:
  void dropAndCreateNewBundledCSR(Checkpoint& ckp, ColumnBase* prev_data_col);
  void dropAndCreateNewUnbundledCSR(Checkpoint& ckp, bool delete_property);

  std::shared_ptr<Checkpoint> ckp_;
  std::shared_ptr<const EdgeSchema> meta_;
  MemoryLevel memory_level_{MemoryLevel::kSyncToFile};
  std::unique_ptr<CsrBase> out_csr_;
  std::unique_ptr<CsrBase> in_csr_;
  std::unique_ptr<Table> table_;
  std::atomic<uint64_t> table_idx_{0};
  std::atomic<uint64_t> capacity_{0};

  friend class PropertyGraph;
  friend class EdgeTableView;
  friend class GraphView;
};

namespace internal {
template <typename TABLE>
std::pair<int32_t, const void*> insert_edge_into_csr_internal(
    CsrBase& out_csr, CsrBase& in_csr, TABLE& table,
    std::atomic<uint64_t>& table_idx, const EdgeSchema& meta, vid_t src_lid,
    vid_t dst_lid, const std::vector<execution::Value>& properties,
    timestamp_t ts, Allocator& alloc, bool insert_safe) {
  int32_t oe_offset;
  const void* data_ptr = nullptr;
  if (meta.is_bundled()) {
    assert(
        properties.size() == 1 ||
        (properties.size() == 0 && (meta.properties.empty() ||
                                    meta.properties[0] == DataTypeId::kEmpty)));
    execution::Value bundled_data =
        properties.empty() ? execution::Value(DataType::EMPTY) : properties[0];
    in_csr.put_generic_edge(dst_lid, src_lid, bundled_data, ts, alloc);
    auto out_ret =
        out_csr.put_generic_edge(src_lid, dst_lid, bundled_data, ts, alloc);
    oe_offset = out_ret.first;
    data_ptr = out_ret.second;
  } else {
    if (meta.properties.size() != properties.size()) {
      THROW_INVALID_ARGUMENT_EXCEPTION(
          "edge data size not match edge table property size");
    }
    size_t row_id = table_idx.fetch_add(1);
    execution::Value prop = execution::Value::UINT64(row_id);
    in_csr.put_generic_edge(dst_lid, src_lid, prop, ts, alloc);
    auto out_ret = out_csr.put_generic_edge(src_lid, dst_lid, prop, ts, alloc);
    oe_offset = out_ret.first;
    data_ptr = out_ret.second;
    table.insert(row_id, properties, insert_safe);
  }
  return {oe_offset, data_ptr};
}

}  // namespace internal

}  // namespace neug
