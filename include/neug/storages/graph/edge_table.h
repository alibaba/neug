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
#include <vector>

#include "neug/storages/csr/csr_base.h"
#include "neug/storages/csr/generic_view.h"
#include "neug/storages/graph/schema.h"
#include "neug/utils/allocators.h"
#include "neug/utils/indexers.h"
#include "neug/utils/property/property.h"
#include "neug/utils/property/table.h"
#include "neug/utils/property/types.h"

namespace neug {

class PropertyGraph;

class IRecordBatchSupplier;

class EdgeTable {
 public:
  EdgeTable(std::shared_ptr<const EdgeSchema> meta);
  EdgeTable(EdgeTable&& edge_table);

  EdgeTable(const EdgeTable&) = delete;
  ~EdgeTable() = default;

  void Swap(EdgeTable& other);

  void SetEdgeSchema(std::shared_ptr<const EdgeSchema> meta);

  void Open(const std::string& work_dir);

  void OpenInMemory(const std::string& work_dir);

  void OpenWithHugepages(const std::string& work_dir);

  void Dump(const std::string& checkpoint_dir_path);

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

  GenericView get_outgoing_view(timestamp_t ts) const;
  GenericView get_incoming_view(timestamp_t ts) const;

  EdgeDataAccessor get_edge_data_accessor(int col_id) const;

  EdgeDataAccessor get_edge_data_accessor(const std::string& col_name) const;

  void BatchAddEdges(const IndexerType& src_indexer,
                     const IndexerType& dst_indexer,
                     std::shared_ptr<IRecordBatchSupplier> supplier);

  // Add edges in batch to the edge table.
  void BatchAddEdges(const std::vector<vid_t>& src_lid_list,
                     const std::vector<vid_t>& dst_lid_list,
                     const std::vector<std::vector<Property>>& edge_data_list);

  // Add a single edge to the edge table. Note this method requires an Allocator
  // to allocate memory for the edge data. Should be called in tp mode.
  int32_t AddEdge(vid_t src_lid, vid_t dst_lid,
                  const std::vector<Property>& properties, timestamp_t ts,
                  Allocator& alloc, bool insert_safe = false);

  void RenameProperties(const std::vector<std::string>& old_names,
                        const std::vector<std::string>& new_names);

  void AddProperties(const std::vector<std::string>& names,
                     const std::vector<DataType>& types,
                     const std::vector<Property>& default_values = {},
                     const std::vector<StorageStrategy>& strategies = {});

  void DeleteProperties(const std::vector<std::string>& col_names);

  void DeleteEdge(vid_t src_lid, vid_t dst_lid, int32_t oe_offset,
                  int32_t ie_offset, timestamp_t ts);

  /**
   * @brief Delete edges associated with a vertex.
   * @param is_src Whether the vertex is the source vertex.
   * @param lid The local id of the vertex.
   * @param ts The timestamp.
   */
  void DeleteVertex(bool is_src, vid_t lid, timestamp_t ts);

  void RevertDeleteEdge(vid_t src_lid, vid_t dst_lid, int32_t oe_offset,
                        int32_t ie_offset, timestamp_t ts);

  void UpdateEdgeProperty(vid_t src_lid, vid_t dst_lid, int32_t oe_offset,
                          int32_t ie_offset, int32_t col_id,
                          const Property& new_prop, timestamp_t ts);

  void Compact(bool compact_csr, bool sort_on_compaction, timestamp_t ts);

  size_t Size() const;

  size_t Capacity() const;

 private:
  void dropAndCreateNewBundledCSR();
  void dropAndCreateNewUnbundledCSR(bool delete_property);
  std::string get_next_csr_path_suffix();

  std::shared_ptr<const EdgeSchema> meta_;
  std::string work_dir_;
  int memory_level_{0};
  std::atomic<int32_t> csr_alter_version_{0};
  std::unique_ptr<CsrBase> out_csr_;
  std::unique_ptr<CsrBase> in_csr_;
  std::unique_ptr<Table> table_;
  std::atomic<uint64_t> table_idx_{0};
  std::atomic<uint64_t> capacity_{0};

  friend class PropertyGraph;
};
}  // namespace neug
