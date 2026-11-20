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

#include "neug/storages/index/art_tree/node48.h"

#include "neug/storages/index/art_tree/base_node.h"
#include "neug/storages/index/art_tree/node256.h"

namespace neug::art {

void Node48::InsertChild(ArtTree& art, NodePtr& node, const uint8_t byte,
                         const NodePtr child) {
  {
    NodeHandle handle(art, node);
    auto& n = handle.Get<Node48>();

    if (n.count != CAPACITY) {
      // Still space. Insert the child.
      // Find an empty position in the node list.
      auto child_pos = n.count;
      if (n.children[child_pos].HasMetadata()) {
        child_pos = 0;
        while (n.children[child_pos].HasMetadata()) {
          child_pos++;
        }
      }

      n.children[child_pos] = child;
      n.child_index[byte] = child_pos;
      n.count++;
      return;
    }
  }

  // The node is full.
  // Grow to Node256.
  auto node48 = node;
  Node256::GrowNode48(art, node, node48);
  Node256::InsertChild(art, node, byte, child);
}

void Node48::GrowNode16(ArtTree& art, NodePtr& node48, NodePtr& node16) {
  {
    NodeHandle n16_handle(art, node16);
    auto& n16 = n16_handle.Get<Node16>();

    auto n48_handle = New(art, node48);
    auto& n48 = n48_handle.Get<Node48>();
    node48.SetGateStatus(node16.GetGateStatus());

    n48.count = n16.count;
    for (uint8_t i = 0; i < n16.count; i++) {
      n48.child_index[n16.key[i]] = i;
      n48.children[i] = n16.children[i];
    }
  }
  NodePtr::FreeNode(art, node16);
}

}  // namespace neug::art
