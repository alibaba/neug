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

#ifndef STORAGES_RT_MUTABLE_GRAPH_EDGE_TABLE_H_
#define STORAGES_RT_MUTABLE_GRAPH_EDGE_TABLE_H_

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

namespace grape {
class OutArchive;
}  // namespace grape

namespace gs {

class PropertyGraph;

class IRecordBatchSupplier;

class EdgeTable {
 public:
  EdgeTable(std::shared_ptr<EdgeSchema> meta);
  EdgeTable(EdgeTable&& edge_table);

  EdgeTable(const EdgeTable&) = delete;
  ~EdgeTable() = default;

  void Open(const std::string& work_dir);

  void OpenInMemory(const std::string& work_dir, size_t src_v_cap,
                    size_t dst_v_cap);

  void OpenWithHugepages(const std::string& work_dir, size_t src_v_cap,
                         size_t dst_v_cap);

  void Dump(const std::string& checkpoint_dir_path);

  void IngestEdge(vid_t src, vid_t dst, grape::OutArchive& oarc, timestamp_t ts,
                  Allocator& alloc);

  void SortByEdgeData(timestamp_t ts);

  void BatchDeleteVertices(const std::set<vid_t>& src_set,
                           const std::set<vid_t>& dst_set);

  void BatchDeleteEdges(const std::vector<vid_t>& src_list,
                        const std::vector<vid_t>& dst_list);

  void Resize(vid_t src_vertex_num, vid_t dst_vertex_num);

  size_t EdgeNum() const;

  GenericView get_outgoing_view(timestamp_t ts) const;
  GenericView get_incoming_view(timestamp_t ts) const;

  EdgeDataAccessor get_edge_data_accessor(int col_id) const;

  void BatchAddEdges(const IndexerType& src_indexer,
                     const IndexerType& dst_indexer,
                     std::shared_ptr<IRecordBatchSupplier> supplier);

  // Add edges in batch to the edge table.
  void BatchAddEdges(const std::vector<vid_t>& src_lid_list,
                     const std::vector<vid_t>& dst_lid_list,
                     const std::vector<std::vector<Property>>& edge_data_list);

  // Add a single edge to the edge table. Note this method requires an Allocator
  // to allocate memory for the edge data. Should be called in tp mode.
  void AddEdge(vid_t src_lid, vid_t dst_lid,
               const std::vector<Property>& properties, timestamp_t ts,
               Allocator& alloc);

  void RenameProperties(const std::vector<std::string>& old_names,
                        const std::vector<std::string>& new_names);

  void AddProperties(const std::vector<std::string>&,
                     const std::vector<PropertyType>&);

  void DeleteProperties(const std::vector<std::string>& col_names);

  void RemoveEdge(vid_t src_lid, vid_t dst_lid, timestamp_t ts);

  inline Table& get_properties_table() { return *table_; }
  inline const Table& get_properties_table() const { return *table_; }

  void Compact(bool reset_timestamp, bool compact_csr, bool sort_on_compaction,
               float reserve_ratio, timestamp_t ts);

 private:
  inline void mark_as_deleted() { deleted_ = true; }
  inline bool is_deleted() const { return deleted_; }
  inline void revert_deleted() { deleted_ = false; }
  void dropAndCreateNewBundledCSR();
  void dropAndCreateNewUnbundledCSR(bool delete_property);
  std::string get_next_csr_path_suffix();

  std::shared_ptr<EdgeSchema> meta_;
  std::string work_dir_;
  int memory_level_{0};
  std::atomic<int32_t> csr_alter_version_{0};
  std::unique_ptr<CsrBase> out_csr_;
  std::unique_ptr<CsrBase> in_csr_;
  std::unique_ptr<Table> table_;
  std::atomic<uint64_t> table_idx_{0};
  bool deleted_{false};  // indicates whether the edge table is deleted softly

  friend class PropertyGraph;
};
}  // namespace gs

#endif  // STORAGES_RT_MUTABLE_GRAPH_EDGE_TABLE_H_