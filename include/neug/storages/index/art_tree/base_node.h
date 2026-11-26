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

template <uint8_t CAPACITY, NType NODE_TYPE>
class BaseNode {
  friend class Node4;
  friend class Node16;
  friend class Node48;

 public:
  static constexpr NType TYPE = NODE_TYPE;

  BaseNode() = delete;
  BaseNode(const BaseNode&) = delete;
  BaseNode& operator=(const BaseNode&) = delete;

 private:
  uint8_t count;
  uint8_t key[CAPACITY];
  NodePtr children[CAPACITY];

 public:
  //! Get a new BaseNode handle and initialize the base node.
  static NodeHandle New(ArtTree& art, NodePtr& node) {
    node = NodePtr::GetAllocator(art, TYPE).New();
    node.SetMetadata(static_cast<uint8_t>(TYPE));

    NodeHandle handle(art, node);
    auto& n = handle.Get<BaseNode>();

    // Reset the node (count).
    n.count = 0;
    // Zero-initialize the node.
    for (uint8_t i = 0; i < CAPACITY; i++) {
      n.key[i] = 0;
      n.children[i].Clear();
    }

    return handle;
  }

  //! Get the child node at byte, if it exists.
  static OptionalNodePtr GetChildNode(const BaseNode& n, const uint8_t byte) {
    for (uint8_t i = 0; i < n.count; i++) {
      if (n.key[i] == byte) {
        if (!n.children[i].HasMetadata()) {
          throw InternalException(
              "empty child i = %d for byte %d in BaseNode::GetChildNode", i,
              byte);
        }
        return n.children[i];
      }
    }
    return OptionalNodePtr();
  }

  //! Get the first child node >= byte, if it exists, and update byte.
  static OptionalNodePtr GetNextChildNode(const BaseNode& n, uint8_t& byte) {
    for (uint8_t i = 0; i < n.count; i++) {
      if (n.key[i] >= byte) {
        if (!n.children[i].HasMetadata()) {
          throw InternalException(
              "empty child i = %d for byte %d in BaseNode::GetNextChildNode", i,
              n.key[i]);
        }
        byte = n.key[i];
        return n.children[i];
      }
    }
    return OptionalNodePtr();
  }

  //! Get the child at byte.
  static unsafe_optional_ptr<NodePtr> GetChild(BaseNode& n, const uint8_t byte,
                                               const bool unsafe = false) {
    for (uint8_t i = 0; i < n.count; i++) {
      if (n.key[i] == byte) {
        if (!unsafe && !n.children[i].HasMetadata()) {
          throw InternalException(
              "empty child i = %d for byte %d in BaseNode::GetChild", i, byte);
        }
        return &n.children[i];
      }
    }
    return nullptr;
  }

  //! Get the first child greater than or equal to the byte.
  static unsafe_optional_ptr<NodePtr> GetNextChild(BaseNode& n, uint8_t& byte) {
    for (uint8_t i = 0; i < n.count; i++) {
      if (n.key[i] >= byte) {
        byte = n.key[i];
        return &n.children[i];
      }
    }
    return nullptr;
  }

 public:
  template <class F>
  static void Iterator(BaseNode<CAPACITY, TYPE>& n, F&& lambda) {
    for (uint8_t i = 0; i < n.count; i++) {
      lambda(n.children[i]);
    }
  }

  template <class F>
  static void Iterator(const BaseNode<CAPACITY, TYPE>& n, F&& lambda) {
    for (uint8_t i = 0; i < n.count; i++) {
      lambda(n.children[i]);
    }
  }

 private:
  static void InsertChildInternal(BaseNode& n, const uint8_t byte,
                                  const NodePtr child);
};

//! Node4 holds up to four children sorted by their key byte.
class Node4 : public BaseNode<4, NType::NODE_4> {
  friend class Node16;

 public:
  static constexpr NType NODE_4 = NType::NODE_4;
  static constexpr uint8_t CAPACITY = 4;

 public:
  //! Insert a child at byte.
  static void InsertChild(ArtTree& art, NodePtr& node, const uint8_t byte,
                          const NodePtr child);
};

class Node16 : public BaseNode<16, NType::NODE_16> {
  friend class Node4;
  friend class Node48;

 public:
  static constexpr NType NODE_16 = NType::NODE_16;
  static constexpr uint8_t CAPACITY = 16;

 public:
  //! Insert a child at byte.
  static void InsertChild(ArtTree& art, NodePtr& node, const uint8_t byte,
                          const NodePtr child);

 private:
  static void GrowNode4(ArtTree& art, NodePtr& node16, NodePtr& node4);
};

}  // namespace neug::art
