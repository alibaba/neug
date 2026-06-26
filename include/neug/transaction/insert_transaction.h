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
#include <limits>
#include <map>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "neug/execution/common/types/value.h"
#include "neug/storages/allocators.h"
#include "neug/storages/graph/graph_interface.h"
#include "neug/storages/graph/graph_view.h"
#include "neug/storages/graph_snapshot_store.h"
#include "neug/utils/property/types.h"
#include "neug/utils/serialization/in_archive.h"

namespace neug {

class PropertyGraph;
class IWalWriter;
class IVersionManager;
class Schema;

/**
 * @brief Transaction for inserting new vertices and edges into the graph.
 *
 * InsertTransaction handles the insertion of new graph elements with WAL
 * durability and per-record-timestamp visibility.
 *
 * **Isolation model — NOT snapshot isolation.**
 * Unlike UpdateTransaction, InsertTransaction does NOT COW-clone the
 * PropertyGraph. It pins the current GraphSnapshotStore slot and, on Commit(),
 * applies the WAL ops directly to the live PropertyGraph behind that slot via
 * `IngestWal(slot_->mutable_view(), ...)`. Concurrent ReadTransactions on the
 * same slot see the in-progress live state.
 *
 * Reader isolation is therefore enforced **entirely** by per-record timestamp
 * filtering: every read path must compare a record's commit timestamp against
 * the reader's `read_ts` and skip records with `ts > read_ts`. If any read
 * path forgets to filter, concurrent inserts will leak partial writes to
 * readers. Adding a new read API on GraphView without propagating `read_ts`
 * is a correctness bug, not a performance optimization.
 *
 * **Concurrency contract** (VersionManager state machine):
 * - Insert requires update_state_==0 (normal); multiple concurrent inserts
 *   allowed (active_inserters_ counter). Update/compact transitions state
 *   away from 0, blocking new inserts.
 * - Insert does NOT block readers, and readers do NOT block insert.
 * - The pinned slot's PropertyGraph is shared with all readers on that slot.
 *
 * **Key Features:**
 * - Write-Ahead Logging for durability
 * - Vertex insertion with property validation
 * - Edge insertion with vertex existence checking (including just-added
 *   vertices via added_vertices_ side table)
 * - Validates property types against schema
 *
 * @since v0.1.0
 */
class InsertTransaction {
 public:
  /**
   * @brief Construct an InsertTransaction with a pinned SnapshotSlot.
   *
   * @param slot Reference to the pinned SnapshotSlot from PinCurrentSnapshot()
   * @param snapshot_store Reference to GraphSnapshotStore for releasing slot
   * @param alloc Reference to memory allocator
   * @param logger Reference to WAL writer
   * @param vm Reference to version manager
   * @param timestamp Transaction timestamp
   *
   * @since v0.1.0
   */
  InsertTransaction(SnapshotGuard guard, Allocator& alloc, IWalWriter& logger,
                    IVersionManager& vm, timestamp_t timestamp);

  /**
   * @brief Destructor that calls Abort().
   *
   * Implementation: Calls Abort() to ensure proper cleanup and release
   * resources.
   *
   * @since v0.1.0
   */
  ~InsertTransaction();

  /**
   * @brief Add a new vertex to the transaction.
   *
   * Validates properties against schema and serializes the vertex insertion to
   * WAL. Tracks the added vertex for later edge resolution.
   *
   * @param label Vertex label/type
   * @param id Vertex primary key value
   * @param props Vector of property values matching schema order
   * @param vid Output: assigned internal vertex ID on success
   * @return Status::OK() on success, or an error Status if validation fails
   *         (e.g. property count/type mismatch).
   *
   * Implementation: Validates property count against schema, serializes
   * operation to arc_ with op_type=0, adds vertex to added_vertices_ tracking
   * map.
   *
   * @since v0.1.0
   */
  Status AddVertex(label_t label, const execution::Value& id,
                   const std::vector<execution::Value>& props, vid_t& vid);

  /**
   * @brief Add a new edge to the transaction.
   *
   * Checks for existence of source and destination vertices (including newly
   * added ones), then serializes the edge insertion to WAL.
   *
   * @param src_label Source vertex label
   * @param src Source vertex ID
   * @param dst_label Destination vertex label
   * @param dst Destination vertex ID
   * @param edge_label Edge label/type
   * @param properties Edge property values matching schema order
   * @param prop Output: pointer to the inserted edge property storage. For an
   *             insert transaction this is always set to nullptr because the
   *             edge property is not actually inserted into the graph until
   *             commit.
   * @return Status::OK() on success, or an error Status if validation fails
   *         (e.g. missing source/destination vertex, property mismatch).
   *
   * Implementation: Uses graph.get_lid() and added_vertices_ to find vertices,
   * serializes operation to arc_ with op_type=1.
   *
   * @since v0.1.0
   */
  Status AddEdge(label_t src_label, vid_t src, label_t dst_label, vid_t dst,
                 label_t edge_label,
                 const std::vector<execution::Value>& properties,
                 const void*& prop);

  /**
   * @brief Commit the transaction.
   *
   * Writes the WAL data to persistent storage and releases the timestamp.
   * Returns early if no operations were performed.
   *
   * @return true if commit successful
   *
   * Implementation: Checks if any operations in arc_, writes WAL via logger_,
   * calls vm_.release_insert_timestamp(), then calls clear().
   *
   * @since v0.1.0
   */
  bool Commit();

  void Abort();

  timestamp_t timestamp() const;

  /**
   * @brief Apply an insert-WAL byte stream to a writable GraphView.
   *
   * Used both:
   *  - by InsertTransaction::Commit() — passing its own writable view_, with
   *    the transaction timestamp; and
   *  - by NeugDB recovery — the caller constructs a writable GraphView over
   *    the initial PropertyGraph (with a per-thread allocator and
   *    `read_ts = MAX_TIMESTAMP` so just-inserted vertices are visible while
   *    resolving edge endpoints), and replays each WAL unit at its own
   *    timestamp.
   *
   * Replays the WAL ops via the writable @p view. Capacity is assumed to be
   * sufficient (no auto-grow / EnsureCapacity at this level); the strict
   * insert path will throw if a buffer is exhausted.
   *
   * @param view Writable GraphView.
   * @param timestamp Insert timestamp for each AddVertex/AddEdge in the WAL.
   * @param data Serialized op buffer.
   * @param length Byte length of @p data.
   * @param alloc Per-thread allocator for adjacency-list growth in CSR.
   */
  static void IngestWal(GraphView& view, uint32_t timestamp, char* data,
                        size_t length, Allocator& alloc);

  const Schema& schema() const;

  bool GetVertexIndex(label_t label, const execution::Value& oid,
                      vid_t& lid) const;

 private:
  execution::Value get_vertex_id(label_t label, vid_t lid) const;

  void create_id_indexer_if_not_exists(label_t label);

  void clear();

  InArchive arc_;

  std::vector<std::unique_ptr<neug::IdIndexerBase<vid_t>>> added_vertices_;
  std::vector<vid_t> added_vertices_base_;
  std::vector<vid_t> vertex_nums_;

  SnapshotGuard guard_;
  GraphView* view_;

  Allocator& alloc_;
  IWalWriter& logger_;
  IVersionManager& vm_;
  timestamp_t timestamp_;
};

class StorageTPInsertInterface : public StorageInsertInterface {
 public:
  explicit StorageTPInsertInterface(InsertTransaction& txn) : txn_(txn) {}
  ~StorageTPInsertInterface() {}

  Status AddVertex(label_t label, const execution::Value& id,
                   const std::vector<execution::Value>& props,
                   vid_t& vid) override {
    return txn_.AddVertex(label, id, props, vid);
  }

  Status AddEdge(label_t src_label, vid_t src, label_t dst_label, vid_t dst,
                 label_t edge_label,
                 const std::vector<execution::Value>& properties,
                 const void*& prop) override {
    return txn_.AddEdge(src_label, src, dst_label, dst, edge_label, properties,
                        prop);
  }

  inline const Schema& schema() const override { return txn_.schema(); }

  bool GetVertexIndex(label_t label, const execution::Value& id,
                      vid_t& index) const override {
    return txn_.GetVertexIndex(label, id, index);
  }

  Status BatchAddVertices(
      label_t v_label_id,
      std::shared_ptr<IRecordBatchSupplier> supplier) override;

  Status BatchAddEdges(label_t src_label, label_t dst_label, label_t edge_label,
                       std::shared_ptr<IRecordBatchSupplier> supplier) override;

 private:
  InsertTransaction& txn_;
};

}  // namespace neug
