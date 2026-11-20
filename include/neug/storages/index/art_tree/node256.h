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
#include "neug/storages/index/art_tree/fixed_size_allocator.h"
#include "neug/storages/index/art_tree/node.h"

namespace neug::art {

//! Node256 holds up to 256 children. They are indexed by their key byte.
class Node256 {
  friend class Node48;

 public:
  static constexpr NType NODE_256 = NType::NODE_256;
  static constexpr NType TYPE = NType::NODE_256;
  static constexpr uint16_t CAPACITY = 256;

 public:
  Node256() = delete;
  Node256(const Node256&) = delete;
  Node256& operator=(const Node256&) = delete;

 private:
  uint16_t count;
  NodePtr children[CAPACITY];

 public:
  //! Get a new Node256 handle and initialize the Node256.
  static NodeHandle New(ArtTree& art, NodePtr& node) {
    node = NodePtr::GetAllocator(art, NODE_256).New();
    node.SetMetadata(static_cast<uint8_t>(NODE_256));

    NodeHandle handle(art, node);
    auto& n = handle.Get<Node256>();

    // Reset the node (count and children).
    n.count = 0;
    for (uint16_t i = 0; i < CAPACITY; i++) {
      n.children[i].Clear();
    }

    return handle;
  }

  //! Insert a child at byte.
  static void InsertChild(ArtTree& art, NodePtr& node, const uint8_t byte,
                          const NodePtr child);

 public:
  template <class F, class NODE>
  static void Iterator(NODE& n, F&& lambda) {
    D_ASSERT(n.count);
    for (idx_t i = 0; i < CAPACITY; i++) {
      if (n.children[i].HasMetadata()) {
        lambda(n.children[i]);
      }
    }
  }

  //! Get the child node at byte, if it exists.
  static OptionalNodePtr GetChildNode(const Node256& n, const uint8_t byte) {
    if (n.children[byte].HasMetadata()) {
      return n.children[byte];
    }
    return OptionalNodePtr();
  }

  //! Get the first child node >= byte, if it exists, and update byte.
  static OptionalNodePtr GetNextChildNode(const Node256& n, uint8_t& byte) {
    for (idx_t i = byte; i < CAPACITY; i++) {
      if (n.children[i].HasMetadata()) {
        byte = UnsafeNumericCast<uint8_t>(i);
        return n.children[i];
      }
    }
    return OptionalNodePtr();
  }

  template <class NODE>
  static unsafe_optional_ptr<NodePtr> GetChild(NODE& n, const uint8_t byte,
                                               const bool unsafe = false) {
    if (unsafe) {
      return &n.children[byte];
    }
    if (n.children[byte].HasMetadata()) {
      return &n.children[byte];
    }
    return nullptr;
  }

  template <class NODE>
  static unsafe_optional_ptr<NodePtr> GetNextChild(NODE& n, uint8_t& byte) {
    for (idx_t i = byte; i < CAPACITY; i++) {
      if (n.children[i].HasMetadata()) {
        byte = UnsafeNumericCast<uint8_t>(i);
        return &n.children[i];
      }
    }
    return nullptr;
  }

 private:
  static void GrowNode48(ArtTree& art, NodePtr& node256, NodePtr& node48);
};
}  // namespace neug::art
