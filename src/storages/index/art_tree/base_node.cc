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

#include "neug/storages/index/art_tree/base_node.h"

#include "neug/storages/index/art_tree/node48.h"

namespace neug::art {

//===--------------------------------------------------------------------===//
// BaseNode
//===--------------------------------------------------------------------===//

template <uint8_t CAPACITY, NType NODE_TYPE>
void BaseNode<CAPACITY, NODE_TYPE>::InsertChildInternal(BaseNode& n,
                                                        const uint8_t byte,
                                                        const NodePtr child) {
  // Still space. Insert the child.
  uint8_t child_pos = 0;
  while (child_pos < n.count && n.key[child_pos] < byte) {
    child_pos++;
  }

  // Move children backwards to make space.
  for (uint8_t i = n.count; i > child_pos; i--) {
    n.key[i] = n.key[i - 1];
    n.children[i] = n.children[i - 1];
  }

  n.key[child_pos] = byte;
  n.children[child_pos] = child;
  n.count++;
}

//===--------------------------------------------------------------------===//
// Node4
//===--------------------------------------------------------------------===//

void Node4::InsertChild(ArtTree& art, NodePtr& node, const uint8_t byte,
                        const NodePtr child) {
  {
    NodeHandle handle(art, node);
    auto& n = handle.Get<Node4>();

    if (n.count != CAPACITY) {
      InsertChildInternal(n, byte, child);
      return;
    }
  }
  // The node is full.
  // Grow to Node16.
  auto node4 = node;
  Node16::GrowNode4(art, node, node4);
  Node16::InsertChild(art, node, byte, child);
}

//===--------------------------------------------------------------------===//
// Node16
//===--------------------------------------------------------------------===//

void Node16::InsertChild(ArtTree& art, NodePtr& node, const uint8_t byte,
                         const NodePtr child) {
  {
    NodeHandle handle(art, node);
    auto& n = handle.Get<Node16>();
    if (n.count != CAPACITY) {
      InsertChildInternal(n, byte, child);
      return;
    }
  }
  // The node is full.
  // Grow to Node48.
  auto node16 = node;
  Node48::GrowNode16(art, node, node16);
  Node48::InsertChild(art, node, byte, child);
}

void Node16::GrowNode4(ArtTree& art, NodePtr& node16, NodePtr& node4) {
  {
    NodeHandle n4_handle(art, node4);
    auto& n4 = n4_handle.Get<Node4>();

    auto n16_handle = New(art, node16);
    auto& n16 = n16_handle.Get<Node16>();
    node16.SetGateStatus(node4.GetGateStatus());

    n16.count = n4.count;
    for (uint8_t i = 0; i < n4.count; i++) {
      n16.key[i] = n4.key[i];
      n16.children[i] = n4.children[i];
    }
  }
  NodePtr::FreeNode(art, node4);
}

}  // namespace neug::art
