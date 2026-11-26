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
#include "neug/storages/index/art_tree/node256.h"

namespace neug::art {

//! Node48 holds up to 48 children. The child_index array is indexed by the key
//! byte. It contains the position of the child node in the children array.
class Node48 {
  friend class Node16;
  friend class Node256;

 public:
  static constexpr NType NODE_48 = NType::NODE_48;
  static constexpr NType TYPE = NType::NODE_48;
  static constexpr uint8_t CAPACITY = 48;
  static constexpr uint8_t EMPTY_MARKER = 48;

 public:
  Node48() = delete;
  Node48(const Node48&) = delete;
  Node48& operator=(const Node48&) = delete;

 private:
  uint8_t count;
  uint8_t child_index[Node256::CAPACITY];
  NodePtr children[CAPACITY];

 public:
  //! Get a new Node48 handle and initialize the Node48.
  static NodeHandle New(ArtTree& art, NodePtr& node) {
    node = NodePtr::GetAllocator(art, NODE_48).New();
    node.SetMetadata(static_cast<uint8_t>(NODE_48));

    NodeHandle handle(art, node);
    auto& n = handle.Get<Node48>();

    // Reset the node (count and child_index).
    n.count = 0;
    for (uint16_t i = 0; i < Node256::CAPACITY; i++) {
      n.child_index[i] = EMPTY_MARKER;
    }
    // Zero-initialize the node.
    for (uint8_t i = 0; i < CAPACITY; i++) {
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
    for (idx_t i = 0; i < Node256::CAPACITY; i++) {
      if (n.child_index[i] != EMPTY_MARKER) {
        lambda(n.children[n.child_index[i]]);
      }
    }
  }

  //! Get the child node at byte, if it exists.
  static OptionalNodePtr GetChildNode(const Node48& n, const uint8_t byte) {
    if (n.child_index[byte] != EMPTY_MARKER) {
      if (!n.children[n.child_index[byte]].HasMetadata()) {
        throw InternalException(
            "empty child for byte %d in Node48::GetChildNode", byte);
      }
      return n.children[n.child_index[byte]];
    }
    return OptionalNodePtr();
  }

  //! Get the first child node >= byte, if it exists, and update byte.
  static OptionalNodePtr GetNextChildNode(const Node48& n, uint8_t& byte) {
    for (idx_t i = byte; i < Node256::CAPACITY; i++) {
      if (n.child_index[i] != EMPTY_MARKER) {
        if (!n.children[n.child_index[i]].HasMetadata()) {
          throw InternalException(
              "empty child for byte %d in Node48::GetNextChildNode",
              UnsafeNumericCast<uint8_t>(i));
        }
        byte = UnsafeNumericCast<uint8_t>(i);
        return n.children[n.child_index[i]];
      }
    }
    return OptionalNodePtr();
  }

  template <class NODE>
  static unsafe_optional_ptr<NodePtr> GetChild(NODE& n, const uint8_t byte,
                                               const bool unsafe = false) {
    if (n.child_index[byte] != Node48::EMPTY_MARKER) {
      if (!unsafe && !n.children[n.child_index[byte]].HasMetadata()) {
        throw InternalException("empty child for byte %d in Node48::GetChild",
                                byte);
      }
      return &n.children[n.child_index[byte]];
    }
    return nullptr;
  }

  template <class NODE>
  static unsafe_optional_ptr<NodePtr> GetNextChild(NODE& n, uint8_t& byte) {
    for (idx_t i = byte; i < Node256::CAPACITY; i++) {
      if (n.child_index[i] != EMPTY_MARKER) {
        byte = UnsafeNumericCast<uint8_t>(i);
        return &n.children[n.child_index[i]];
      }
    }
    return nullptr;
  }

 private:
  static void GrowNode16(ArtTree& art, NodePtr& node48, NodePtr& node16);
};
}  // namespace neug::art
