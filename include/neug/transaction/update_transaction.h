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

#ifndef ENGINES_GRAPH_DB_DATABASE_UPDATE_TRANSACTION_H_
#define ENGINES_GRAPH_DB_DATABASE_UPDATE_TRANSACTION_H_

#include <stddef.h>
#include <stdint.h>
#include <limits>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "flat_hash_map/flat_hash_map.hpp"
#include "libgrape-lite/grape/serialization/in_archive.h"
#include "neug/storages/csr/mutable_csr.h"
#include "neug/storages/graph/property_graph.h"
#include "neug/transaction/transaction_utils.h"
#include "neug/utils/allocators.h"
#include "neug/utils/id_indexer.h"
#include "neug/utils/property/table.h"
#include "neug/utils/property/types.h"

namespace gs {

class PropertyGraph;
class IWalWriter;
class VersionManager;
class NeugDBSession;
class CsrConstEdgeIterBase;
class Schema;
class UpdateBatch;
template <typename INDEX_T>
class IdIndexerBase;

class UpdateTransaction {
 public:
  UpdateTransaction(const NeugDBSession& session, PropertyGraph& graph,
                    Allocator& alloc, const std::string& work_dir,
                    IWalWriter& logger, VersionManager& vm,
                    timestamp_t timestamp);

  ~UpdateTransaction();

  /**
   * @brief By default update transaction will not resize the vertex column
   * When inserting properties for a vertex. By setting this to true, it will
   * resize the vertex column.
   */
  void set_insert_vertex_with_resize(bool insert_vertex_with_resize);

  timestamp_t timestamp() const;

  const Schema& schema() const { return graph_.schema(); }

  bool Commit();

  void Abort();

  bool AddVertex(label_t label, const Any& oid, const std::vector<Any>& props);

  bool AddVertex(label_t label, const Any& oid, const std::vector<Any>& props,
                 vid_t& vid);

  bool AddEdge(label_t src_label, const Any& src, label_t dst_label,
               const Any& dst, label_t edge_label, const Any& value);

  bool AddEdge(label_t src_label, vid_t src, label_t dst_label, vid_t dst,
               label_t edge_label, const Any& value);

  class vertex_iterator {
   public:
    vertex_iterator(label_t label, vid_t cur, vid_t& num,
                    UpdateTransaction* txn);
    ~vertex_iterator();
    bool IsValid() const;
    void Next();
    void Goto(vid_t target);

    Any GetId() const;

    vid_t GetIndex() const;

    Any GetField(int col_id) const;

    bool SetField(int col_id, const Any& value);

   private:
    label_t label_;
    vid_t cur_;

    vid_t& num_;
    UpdateTransaction* txn_;
  };

  class edge_iterator {
   public:
    edge_iterator(bool dir, label_t label, vid_t v, label_t neighbor_label,
                  label_t edge_label, const vid_t* aeb, const vid_t* aee,
                  std::shared_ptr<CsrConstEdgeIterBase> init_iter,
                  UpdateTransaction* txn);
    ~edge_iterator();

    Any GetData() const;

    void SetData(const Any& value);

    bool IsValid() const;

    void Next();

    void Forward(size_t offset);

    vid_t GetNeighbor() const;

    label_t GetNeighborLabel() const;

    label_t GetEdgeLabel() const;

   private:
    bool dir_;

    label_t label_;
    vid_t v_;

    label_t neighbor_label_;
    label_t edge_label_;

    const vid_t* added_edges_cur_;
    const vid_t* added_edges_end_;

    std::shared_ptr<CsrConstEdgeIterBase> init_iter_;
    UpdateTransaction* txn_;
    size_t offset_;
  };

  vertex_iterator GetVertexIterator(label_t label);

  edge_iterator GetOutEdgeIterator(label_t label, vid_t u,
                                   label_t neighbor_label, label_t edge_label);

  edge_iterator GetInEdgeIterator(label_t label, vid_t u,
                                  label_t neighbor_label, label_t edge_label);

  Any GetVertexField(label_t label, vid_t lid, int col_id) const;

  bool SetVertexField(label_t label, vid_t lid, int col_id, const Any& value);

  // set col_id = -1 to set the whole edge data
  void SetEdgeData(bool dir, label_t label, vid_t v, label_t neighbor_label,
                   vid_t nbr, label_t edge_label, const Any& value,
                   int32_t col_id = 0);

  bool GetUpdatedEdgeData(bool dir, label_t label, vid_t v,
                          label_t neighbor_label, vid_t nbr, label_t edge_label,
                          Any& ret) const;

  static void IngestWal(PropertyGraph& graph, const std::string& work_dir,
                        uint32_t timestamp, char* data, size_t length,
                        Allocator& alloc);
  Any GetVertexId(label_t label, vid_t lid) const;

  const NeugDBSession& GetSession() const;

  PropertyGraph& GetGraph() const { return graph_; }

  /**
   * @brief Check if the vertex with the given label and oid exists.
   * @param label The label of the vertex.
   * @param oid The oid of the vertex.
   * @return true if the vertex exists, false otherwise.
   */
  bool HasVertex(label_t label, const Any& oid) const;

 private:
  friend class NeugDBSession;
  bool batch_commit(UpdateBatch& batch);

  void set_edge_data_with_offset(bool dir, label_t label, vid_t v,
                                 label_t neighbor_label, vid_t nbr,
                                 label_t edge_label, const Any& value,
                                 size_t offset, int32_t col_id = 0);

  size_t get_in_csr_index(label_t src_label, label_t dst_label,
                          label_t edge_label) const;

  size_t get_out_csr_index(label_t src_label, label_t dst_label,
                           label_t edge_label) const;

  bool oid_to_lid(label_t label, const Any& oid, vid_t& lid) const;

  Any lid_to_oid(label_t label, vid_t lid) const;

  void release();

  void applyVerticesUpdates();

  void applyEdgesUpdates();

  const NeugDBSession& session_;

  bool insert_vertex_with_resize_;

  PropertyGraph& graph_;
  Allocator& alloc_;
  IWalWriter& logger_;
  VersionManager& vm_;
  timestamp_t timestamp_;

  grape::InArchive arc_;
  int op_num_;

  size_t vertex_label_num_;
  size_t edge_label_num_;

  std::vector<std::shared_ptr<IdIndexerBase<vid_t>>> added_vertices_;
  std::vector<vid_t> added_vertices_base_;
  std::vector<vid_t> vertex_nums_;
  std::vector<ska::flat_hash_map<vid_t, vid_t>> vertex_offsets_;
  std::vector<Table> extra_vertex_properties_;

  std::vector<ska::flat_hash_map<vid_t, std::vector<vid_t>>> added_edges_;
  std::vector<ska::flat_hash_map<
      vid_t, ska::flat_hash_map<vid_t, std::tuple<Any, int32_t, size_t>>>>
      updated_edge_data_;

  std::vector<std::string> sv_vec_;
};

}  // namespace gs

#endif  // ENGINES_GRAPH_DB_DATABASE_UPDATE_TRANSACTION_H_
