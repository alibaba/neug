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

#include "neug/storages/index/art_tree/leaf.h"

#include "neug/storages/index/art_tree/art.h"
#include "neug/storages/index/art_tree/art_key.h"
#include "neug/storages/index/art_tree/base_leaf.h"
#include "neug/storages/index/art_tree/base_node.h"
#include "neug/storages/index/art_tree/common.h"
#include "neug/storages/index/art_tree/node.h"
#include "neug/storages/index/art_tree/prefix.h"

namespace neug::art {

void Leaf::New(NodePtr& node, const row_t row_id) {
  D_ASSERT(row_id < MAX_ROW_ID_LOCAL);
  node.Clear();
  node.SetMetadata(static_cast<uint8_t>(INLINED));
  node.SetRowId(row_id);
}

void Leaf::MergeInlined(ARTArenaAllocator& arena, ArtTree& art, NodePtr& left,
                        NodePtr& right, GateStatus status, idx_t depth) {
  D_ASSERT(left.GetType() == NType::LEAF_INLINED);
  D_ASSERT(right.GetType() == NType::LEAF_INLINED);

  status = status == GateStatus::GATE_NOT_SET ? GateStatus::GATE_SET
                                              : GateStatus::GATE_NOT_SET;
  if (status == GateStatus::GATE_SET) {
    // Case 1: We are outside a nested leaf,
    // so we create a nested leaf.
    depth = 0;
  }
  // Otherwise, case 2: we are in a nested leaf with two 'compressed' prefixes.
  // A 'compressed prefix' is an inlined leaf that could've been expanded to
  // a prefix with an inlined leaf as its only child.

  // Get the corresponding row IDs and their ArtTree keys.
  auto left_row_id = left.GetRowId();
  auto right_row_id = right.GetRowId();
  auto left_key = ARTKey::CreateARTKey<row_t>(arena, left_row_id);
  auto right_key = ARTKey::CreateARTKey<row_t>(arena, right_row_id);

  auto pos = left_key.GetMismatchPos(right_key, depth);

  left.Clear();
  reference<NodePtr> left_ref(left);
  if (pos != depth) {
    // The row IDs share a prefix.
    Prefix::New(art, left_ref, left_key, depth, pos - depth);
  }

  auto left_byte = left_key.data[pos];
  auto right_byte = right_key.data[pos];

  if (pos == Prefix::ROW_ID_COUNT) {
    // The row IDs differ on the last byte.
    Node7Leaf::New(art, left_ref);
    Node7Leaf::InsertByte(art, left_ref, left_byte);
    Node7Leaf::InsertByte(art, left_ref, right_byte);
    left.SetGateStatus(status);
    return;
  }

  // Create and insert the (compressed) children.
  // We inline directly into the node, instead of creating prefixes
  // with a single inlined leaf as their child.
  Node4::New(art, left_ref);

  NodePtr left_child;
  Leaf::New(left_child, left_row_id);
  Node4::InsertChild(art, left_ref, left_byte, left_child);

  NodePtr right_child;
  Leaf::New(right_child, right_row_id);
  Node4::InsertChild(art, left_ref, right_byte, right_child);

  left.SetGateStatus(status);
}

}  // namespace neug::art
