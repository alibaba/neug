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

#include "neug/storages/index/art_tree/base_leaf.h"

#include "neug/storages/index/art_tree/node256_leaf.h"

namespace neug::art {

//===--------------------------------------------------------------------===//
// BaseLeaf
//===--------------------------------------------------------------------===//

template <uint8_t CAPACITY, NType NODE_TYPE>
void BaseLeaf<CAPACITY, NODE_TYPE>::InsertByteInternal(BaseLeaf& n,
                                                       const uint8_t byte) {
  // Still space. Insert the child.
  uint8_t child_pos = 0;
  while (child_pos < n.count && n.key[child_pos] < byte) {
    child_pos++;
  }

  // Move children backwards to make space.
  for (uint8_t i = n.count; i > child_pos; i--) {
    n.key[i] = n.key[i - 1];
  }

  n.key[child_pos] = byte;
  n.count++;
}

//===--------------------------------------------------------------------===//
// Node7Leaf
//===--------------------------------------------------------------------===//

void Node7Leaf::InsertByte(ArtTree& art, NodePtr& node, const uint8_t byte) {
  {
    NodeHandle handle(art, node);
    auto& n7 = handle.Get<Node7Leaf>();

    if (n7.count != CAPACITY) {
      InsertByteInternal(n7, byte);
      return;
    }
  }
  // The node is full. Grow to Node15.
  auto node7 = node;
  Node15Leaf::GrowNode7Leaf(art, node, node7);
  Node15Leaf::InsertByte(art, node, byte);
}

//===--------------------------------------------------------------------===//
// Node15Leaf
//===--------------------------------------------------------------------===//

void Node15Leaf::InsertByte(ArtTree& art, NodePtr& node, const uint8_t byte) {
  {
    NodeHandle n15_handle(art, node);
    auto& n15 = n15_handle.Get<Node15Leaf>();
    if (n15.count != CAPACITY) {
      InsertByteInternal(n15, byte);
      return;
    }
  }
  auto node15 = node;
  Node256Leaf::GrowNode15Leaf(art, node, node15);
  Node256Leaf::InsertByte(art, node, byte);
}

void Node15Leaf::GrowNode7Leaf(ArtTree& art, NodePtr& node15_leaf,
                               NodePtr& node7_leaf) {
  {
    NodeHandle n7_handle(art, node7_leaf);
    auto& n7 = n7_handle.Get<Node7Leaf>();

    auto n15_handle = New(art, node15_leaf);
    auto& n15 = n15_handle.Get<Node15Leaf>();
    node15_leaf.SetGateStatus(node7_leaf.GetGateStatus());

    n15.count = n7.count;
    for (uint8_t i = 0; i < n7.count; i++) {
      n15.key[i] = n7.key[i];
    }
  }
  NodePtr::FreeNode(art, node7_leaf);
}

}  // namespace neug::art
