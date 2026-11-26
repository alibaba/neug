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
class BaseLeaf {
  friend class Node7Leaf;
  friend class Node15Leaf;
  friend class Node256Leaf;

 public:
  static constexpr NType TYPE = NODE_TYPE;

  BaseLeaf() = delete;
  BaseLeaf(const BaseLeaf&) = delete;
  BaseLeaf& operator=(const BaseLeaf&) = delete;

 private:
  uint8_t count;
  uint8_t key[CAPACITY];

 public:
  //! Get a new BaseLeaf and initialize it.
  static NodeHandle New(ArtTree& art, NodePtr& node) {
    node = NodePtr::GetAllocator(art, TYPE).New();
    node.SetMetadata(static_cast<uint8_t>(TYPE));

    NodeHandle handle(art, node);
    auto& n = handle.Get<BaseLeaf>();

    n.count = 0;
    return handle;
  }

  //! Returns true, if the byte exists, else false.
  bool HasByte(const uint8_t byte) const {
    for (uint8_t i = 0; i < count; i++) {
      if (key[i] == byte) {
        return true;
      }
    }
    return false;
  }

  //! Get the first byte greater than or equal to the byte.
  //! Returns true, if such a byte exists, else false.
  bool GetNextByte(uint8_t& byte) const {
    for (uint8_t i = 0; i < count; i++) {
      if (key[i] >= byte) {
        byte = key[i];
        return true;
      }
    }
    return false;
  }

 private:
  static void InsertByteInternal(BaseLeaf& n, const uint8_t byte);
};

//! Node7Leaf holds up to seven sorted bytes.
class Node7Leaf : public BaseLeaf<7, NType::NODE_7_LEAF> {
  friend class Node15Leaf;

 public:
  static constexpr NType NODE_7_LEAF = NType::NODE_7_LEAF;
  static constexpr uint8_t CAPACITY = 7;
  static constexpr idx_t AND_LAST_BYTE = 0xFFFFFFFFFFFFFF00;

 public:
  //! Insert a byte.
  static void InsertByte(ArtTree& art, NodePtr& node, const uint8_t byte);
};

//! Node15Leaf holds up to 15 sorted bytes.
class Node15Leaf : public BaseLeaf<15, NType::NODE_15_LEAF> {
  friend class Node7Leaf;
  friend class Node256Leaf;

 public:
  static constexpr NType NODE_15_LEAF = NType::NODE_15_LEAF;
  static constexpr uint8_t CAPACITY = 15;

 public:
  //! Insert a byte.
  static void InsertByte(ArtTree& art, NodePtr& node, const uint8_t byte);

 private:
  static void GrowNode7Leaf(ArtTree& art, NodePtr& node15_leaf,
                            NodePtr& node7_leaf);
};

}  // namespace neug::art
