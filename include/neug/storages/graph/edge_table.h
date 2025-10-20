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
#include "neug/utils/allocators.h"
#include "neug/utils/indexers.h"
#include "neug/utils/property/property.h"
#include "neug/utils/property/table.h"
#include "neug/utils/property/types.h"

namespace grape {
class OutArchive;
}  // namespace grape

namespace gs {

class IRecordBatchSupplier;

class EdgeTableMeta {
 public:
  EdgeTableMeta(const std::string& src_label_name_,
                const std::string& dst_label_name_,
                const std::string& edge_label_name_, bool ie_mutable_,
                bool oe_mutable_, EdgeStrategy ie_strategy_,
                EdgeStrategy oe_strategy_,
                const std::vector<PropType>& properties_,
                const std::vector<std::string>& property_names_,
                const std::vector<StorageStrategy>& strategies_);
  EdgeTableMeta(const std::string& src_label_name_,
                const std::string& dst_label_name_,
                const std::string& edge_label_name_, bool ie_mutable_,
                bool oe_mutable_, EdgeStrategy ie_strategy_,
                EdgeStrategy oe_strategy_,
                const std::vector<PropertyType>& properties_,
                const std::vector<std::string>& property_names_,
                const std::vector<StorageStrategy>& strategies_);
  ~EdgeTableMeta() = default;

  bool is_bundled() const;

  std::string src_label_name;
  std::string dst_label_name;
  std::string edge_label_name;
  bool ie_mutable;
  bool oe_mutable;
  EdgeStrategy oe_strategy;
  EdgeStrategy ie_strategy;
  std::vector<PropType> properties;
  std::vector<std::string> property_names;
  std::vector<StorageStrategy> strategies;
};
class EdgeTable {
 public:
  EdgeTable(const EdgeTableMeta& meta);
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

  void RenameProperties(const std::vector<std::string>& old_names,
                        const std::vector<std::string>& new_names);

  void AddProperties(const std::vector<std::string>&,
                     const std::vector<PropertyType>&);

  void DeleteProperties(const std::vector<std::string>& col_names);

  inline Table& get_properties_table() { return *table_; }
  inline const Table& get_properties_table() const { return *table_; }

  void Compact(bool reset_timestamp, bool compact_csr, bool sort_on_compaction,
               float reserve_ratio, timestamp_t ts);

 private:
  void dropAndCreateNewBundledCSR();
  void dropAndCreateNewUnbundledCSR(bool delete_property);
  std::string get_next_csr_path_suffix();

  EdgeTableMeta meta_;
  std::string work_dir_;
  int memory_level_{0};
  std::atomic<int32_t> csr_alter_version_{0};
  std::unique_ptr<CsrBase> out_csr_;
  std::unique_ptr<CsrBase> in_csr_;
  std::unique_ptr<Table> table_;
  std::atomic<uint64_t> table_idx_{0};
};
}  // namespace gs

#endif  // STORAGES_RT_MUTABLE_GRAPH_EDGE_TABLE_H_