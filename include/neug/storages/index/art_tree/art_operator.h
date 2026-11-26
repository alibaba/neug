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

#include "neug/storages/index/art_tree/art.h"
#include "neug/storages/index/art_tree/art_key.h"
#include "neug/storages/index/art_tree/base_node.h"
#include "neug/storages/index/art_tree/const_prefix_handle.h"
#include "neug/storages/index/art_tree/leaf.h"
#include "neug/storages/index/art_tree/prefix.h"

namespace neug::art {

//! ARTOperator provides functionality for different ArtTree operations.
class ARTOperator {
 public:
  //! Lookup returns the leaf matching the key, or an empty OptionalNodePtr if
  //! no such leaf exists.
  static OptionalNodePtr Lookup(const ArtTree& art, const NodePtr& node,
                                const ARTKey& key, idx_t depth) {
    NodePtr current(node);

    while (current.HasMetadata()) {
      if (current.IsAnyLeaf() ||
          current.GetGateStatus() == GateStatus::GATE_SET) {
        return current;
      }

      if (current.GetType() == NType::PREFIX) {
        ConstNodeHandle handle(art, current);
        auto data = handle.GetPtr();
        NodePtr child = ConstPrefixHandle::ChildRef(art, handle);
        for (idx_t i = 0; i < data[art.PrefixCount()]; i++) {
          if (data[i] != key[depth]) {
            return OptionalNodePtr();
          }
          depth++;
        }
        current = child;
        continue;
      }

      D_ASSERT(depth < key.len);
      auto child = current.GetChildNode(art, key[depth]);
      if (!child) {
        return OptionalNodePtr();
      }

      current = child.Get();
      D_ASSERT(current.HasMetadata());
      depth++;
    }

    return OptionalNodePtr();
  }

  //! LookupInLeaf returns true if the rowid is in the leaf:
  //! 1) If the leaf is an inlined leaf, check if the rowid matches.
  //! 2) If the leaf is a gate node, perform a search in the nested ArtTree for
  //! the rowid.
  static bool LookupInLeaf(const ArtTree& art, const NodePtr& node,
                           const ARTKey& rowid) {
    NodePtr current(node);
    idx_t depth = 0;

    while (current.HasMetadata()) {
      const auto type = current.GetType();
      switch (type) {
      case NType::LEAF_INLINED: {
        return current.GetRowId() == rowid.GetRowId();
      }
      case NType::NODE_7_LEAF:
      case NType::NODE_15_LEAF:
      case NType::NODE_256_LEAF: {
        D_ASSERT(depth + 1 == Prefix::ROW_ID_SIZE);
        const auto byte = rowid[Prefix::ROW_ID_COUNT];
        return current.HasByte(art, byte);
      }
      case NType::NODE_4:
      case NType::NODE_16:
      case NType::NODE_48:
      case NType::NODE_256: {
        D_ASSERT(depth < Prefix::ROW_ID_SIZE);
        auto child = current.GetChildNode(art, rowid[depth]);
        if (child) {
          // Continue in the child.
          current = child.Get();
          depth++;
          D_ASSERT(current.HasMetadata());
          continue;
        }
        return false;
      }
      case NType::PREFIX: {
        ConstNodeHandle handle(art, current);
        const auto* data = handle.GetPtr();
        for (idx_t i = 0; i < data[art.PrefixCount()]; i++) {
          if (data[i] != rowid[depth]) {
            // The key and the prefix don't match.
            return false;
          }
          depth++;
        }
        current = ConstPrefixHandle::ChildRef(art, handle);
      }
      }
    }
    return false;
  }

  //! Insert a key and its row ID into the node.
  //! Starts at depth (in the key).
  //! status indicates if the insert happens inside a gate or not.
  static void Insert(ARTArenaAllocator& arena, ArtTree& art, NodePtr& node,
                     const ARTKey& key, idx_t depth, const ARTKey& row_id,
                     GateStatus status) {
    reference<NodePtr> active_node_ref(node);
    reference<const ARTKey> active_key_ref(key);

    // Early-out, if the node is empty.
    if (!node.HasMetadata()) {
      D_ASSERT(depth == 0);
      if (status == GateStatus::GATE_SET) {
        Leaf::New(node, row_id.GetRowId());
        return;
      }

      Prefix::New(art, active_node_ref, active_key_ref.get(), depth,
                  active_key_ref.get().len);
      Leaf::New(active_node_ref, row_id.GetRowId());
      return;
    }

    while (active_node_ref.get().HasMetadata()) {
      auto& active_node = active_node_ref.get();
      auto& active_key = active_key_ref.get();

      // status is GATE_SET, if we've passed a gate in the previous iteration.
      // In that case, we have not adjusted the reference.
      if (status == GateStatus::GATE_NOT_SET &&
          active_node.GetGateStatus() == GateStatus::GATE_SET) {
        // Enter the nested ID ART for this key.
        active_key_ref = row_id;
        depth = 0;
        status = GateStatus::GATE_SET;
        continue;
      }

      const auto type = active_node.GetType();
      switch (type) {
      case NType::LEAF_INLINED: {
        return InsertIntoInlined(arena, art, active_node, row_id, depth,
                                 status);
      }
      case NType::NODE_7_LEAF:
      case NType::NODE_15_LEAF:
      case NType::NODE_256_LEAF: {
        // Row IDs are unique; there are never any duplicate byte conflicts.
        auto byte = active_key[Prefix::ROW_ID_COUNT];
        NodePtr::InsertChild(art, active_node, byte);
        return;
      }
      case NType::NODE_4:
      case NType::NODE_16:
      case NType::NODE_48:
      case NType::NODE_256: {
        D_ASSERT(depth < active_key.len);
        auto child = active_node.GetChildMutable(art, active_key[depth]);
        if (child) {
          // Continue in the child.
          active_node_ref = *child;
          depth++;
          D_ASSERT(active_node_ref.get().HasMetadata());
          continue;
        }
        InsertIntoNode(art, active_node, key, row_id, depth, status);
        return;
      }
      case NType::PREFIX: {
        Prefix prefix(art, active_node);
        for (idx_t i = 0; i < prefix.data[art.PrefixCount()]; i++) {
          if (prefix.data[i] != active_key[depth]) {
            // The active key and the prefix don't match.
            InsertIntoPrefix(art, active_node_ref, active_key, row_id, i, depth,
                             status);
            return;
          }
          depth++;
        }
        active_node_ref = *prefix.child_slot;
        D_ASSERT(active_node_ref.get().HasMetadata());
        continue;
      }
      default:
        throw InternalException("Invalid node type for ARTOperator::Insert.");
      }
    }
    throw InternalException("node without metadata in ARTOperator::Insert");
  }

 private:
  static void InsertIntoInlined(ARTArenaAllocator& arena, ArtTree& art,
                                NodePtr& node, const ARTKey& row_id,
                                idx_t depth, GateStatus status) {
    NodePtr row_id_node;
    Leaf::New(row_id_node, row_id.GetRowId());
    Leaf::MergeInlined(arena, art, node, row_id_node, status, depth);
  }

  static void InsertIntoNode(ArtTree& art, NodePtr& node, const ARTKey& key,
                             const ARTKey& row_id, const idx_t depth,
                             const GateStatus status) {
    if (status == GateStatus::GATE_SET) {
      // Inside gates, we compress prefixes that only have an inlined
      // row ID as their child.
      NodePtr row_id_node;
      Leaf::New(row_id_node, row_id.GetRowId());
      NodePtr::InsertChild(art, node, row_id[depth], row_id_node);
      return;
    }

    NodePtr leaf;
    reference<NodePtr> leaf_ref(leaf);
    if (depth + 1 < key.len) {
      // Outside of gates, we create a prefix for the inlined leaf.
      auto count = key.len - depth - 1;
      Prefix::New(art, leaf_ref, key, depth + 1, count);
    }

    // Create and insert the inlined leaf.
    Leaf::New(leaf_ref, row_id.GetRowId());
    NodePtr::InsertChild(art, node, key[depth], leaf);
  }

  static void InsertIntoPrefix(ArtTree& art, reference<NodePtr>& node_ref,
                               const ARTKey& key, const ARTKey& row_id,
                               const idx_t pos, const idx_t depth,
                               const GateStatus status) {
    const auto cast_pos = UnsafeNumericCast<uint8_t>(pos);
    const auto byte = Prefix::GetByte(art, node_ref, cast_pos);

    NodePtr child;
    const auto split_status = Prefix::Split(art, node_ref, child, cast_pos);

    Node4::New(art, node_ref);
    node_ref.get().SetGateStatus(split_status);

    Node4::InsertChild(art, node_ref, byte, child);
    InsertIntoNode(art, node_ref, key, row_id, depth, status);
  }
};

}  // namespace neug::art
