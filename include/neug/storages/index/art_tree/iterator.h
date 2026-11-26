/** Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * This file is based on the DuckDB project
 * (https://github.com/duckdb/duckdb) Licensed under the MIT License.
 */

#pragma once

#include "neug/storages/index/art_tree/art_key.h"
#include "neug/storages/index/art_tree/leaf.h"
#include "neug/storages/index/art_tree/node.h"

namespace neug::art {

//! Keeps track of the byte leading to the currently active child of the node.
struct IteratorEntry {
  IteratorEntry(NodePtr node, uint8_t byte) : node(node), byte(byte) {}

  NodePtr node;
  uint8_t byte = 0;
};

//! Keeps track of the current key in the iterator leading down to the top node
//! in the stack.
class IteratorKey {
 public:
  //! Pushes a byte into the current key.
  inline void Push(const uint8_t key_byte) { key_bytes.push_back(key_byte); }
  //! Pops n bytes from the current key.
  inline void Pop(const idx_t n) { key_bytes.resize(key_bytes.size() - n); }
  //! Returns the byte at idx.
  inline uint8_t& operator[](idx_t idx) {
    D_ASSERT(idx < key_bytes.size());
    return key_bytes[idx];
  }
  // Returns the number of key bytes.
  inline idx_t Size() const { return key_bytes.size(); }
  //! Returns a pointer to the key bytes.
  inline const_data_ptr_t Data() const {
    return const_data_ptr_cast(key_bytes.data());
  }

  //! Returns true, if key_bytes contains all bytes of key.
  bool Contains(const ARTKey& key) const;
  //! Returns true, if key_bytes is greater than [or equal to] the key.
  bool GreaterThan(const ARTKey& key, const bool equal,
                   const uint8_t nested_depth) const;

 private:
  unsafe_vector<uint8_t> key_bytes;
};

//===--------------------------------------------------------------------===//
// Scan Output Policies
//===--------------------------------------------------------------------===//

//! Output policy for scanning row IDs only into a set.
struct RowIdSetOutput {
  set<row_t>& row_ids;
  const idx_t capacity;

  RowIdSetOutput(set<row_t>& row_ids, const idx_t capacity)
      : row_ids(row_ids), capacity(capacity) {}

  bool IsFull() const {
    D_ASSERT(row_ids.size() <= capacity);
    return row_ids.size() >= capacity;
  }
  void SetKey(const IteratorKey&, const idx_t) {
    // No-op: we don't need keys for row ID output.
  }
  void Add(const row_t rid) { row_ids.insert(rid); }
};

// One bounded output policy preserving both keys and values (no set dedup).
struct KeyValueOutput {
  vector<ARTKeyValue>& rows;
  idx_t capacity;
  vector<uint8_t> key;
  bool IsFull() const { return rows.size() >= capacity; }
  void SetKey(const IteratorKey& current, idx_t length) {
    key.assign(current.Data(), current.Data() + length);
  }
  void Add(row_t value) {
    if (value < 0 || uint64_t(value) >= INVALID_INDEX_ID)
      throw InternalException("ART value outside NeuG index id range");
    rows.push_back({key, static_cast<index_id_t>(value)});
  }
};

//! Scanning state. The scanning output policies allow us to pass in a capacity
//! while scanning, so that when the scan fills up the capacity, we can pause
//! the scan state at that location, and resume scanning later.
enum class ARTScanResult : uint8_t { COMPLETED = 0, PAUSED = 1 };

class Iterator {
 public:
  static constexpr uint8_t ROW_ID_SIZE = sizeof(row_t);

 public:
  explicit Iterator(const ArtTree& art)
      : art(art), status(GateStatus::GATE_NOT_SET){};
  //! Holds the current key leading down to the top node on the stack.
  IteratorKey current_key;

 public:
  //! Templated scan implementation. Output policy defines how results are
  //! emitted. Returns COMPLETED if scan finished, PAUSED if stopped due to
  //! output capacity.
  template <typename Output>
  ARTScanResult Scan(const ARTKey& upper_bound, Output& output, bool equal);

  //! Finds the minimum (leaf) of the current subtree.
  void FindMinimum(NodePtr current);
  // Seek an arbitrary encoded prefix, including one ending inside a PREFIX.
  bool SeekPrefix(NodePtr current, const ARTKey& prefix);
  //! Finds the lower bound of the ArtTree and adds the nodes to the stack.
  //! Returns false, if the lower bound exceeds the maximum value of the
  //! ArtTree.
  bool LowerBound(NodePtr current, const ARTKey& key, const bool equal);

  //! Returns the nested depth.
  uint8_t GetNestedDepth() const { return nested_depth; }

 private:
  //! The ArtTree.
  const ArtTree& art;
  //! Stack of nodes from the root to the currently active node.
  stack<IteratorEntry> nodes;
  //! Last visited leaf node.
  NodePtr last_leaf = NodePtr();
  //! Holds the row ID of nested leaves.
  uint8_t row_id[ROW_ID_SIZE];
  //! True, if we passed a gate.
  GateStatus status;
  //! Depth in a nested leaf.
  uint8_t nested_depth = 0;
  //! True, if we entered a nested leaf to retrieve the next node.
  bool entered_nested_leaf = false;

  //! State for resuming a scan after early return due to the Output policy
  //! capacity (see note for the ARTScanResult enum).
  struct ResumeScanState {
    //! For nested leaves: current byte position and whether we've started.
    uint8_t nested_byte = 0;
    bool nested_started = false;
  };
  ResumeScanState resume_state;

 private:
  //! Goes to the next leaf in the ArtTree and sets it as last_leaf,
  //! returns false if there is no next leaf.
  bool Next();
  //! Pop the top node from the stack of iterator entries and adjust the current
  //! key.
  void PopNode();
};
}  // namespace neug::art
