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

#include "neug/storages/index/art_tree/node256.h"

#include "neug/storages/index/art_tree/node48.h"

namespace neug::art {

void Node256::InsertChild(ArtTree& art, NodePtr& node, const uint8_t byte,
                          const NodePtr child) {
  NodeHandle handle(art, node);
  auto& n = handle.Get<Node256>();
  n.count++;
  n.children[byte] = child;
}

void Node256::GrowNode48(ArtTree& art, NodePtr& node256, NodePtr& node48) {
  {
    NodeHandle n48_handle(art, node48);
    auto& n48 = n48_handle.Get<Node48>();

    auto n256_handle = New(art, node256);
    auto& n256 = n256_handle.Get<Node256>();
    node256.SetGateStatus(node48.GetGateStatus());

    n256.count = n48.count;
    for (uint16_t i = 0; i < CAPACITY; i++) {
      if (n48.child_index[i] != Node48::EMPTY_MARKER) {
        n256.children[i] = n48.children[n48.child_index[i]];
      }
    }
  }
  NodePtr::FreeNode(art, node48);
}

}  // namespace neug::art
