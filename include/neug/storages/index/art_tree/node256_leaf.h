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

//! Node256Leaf is a bitmask containing 256 bits.
class Node256Leaf {
  friend class Node15Leaf;

 public:
  static constexpr NType NODE_256_LEAF = NType::NODE_256_LEAF;
  static constexpr NType TYPE = NType::NODE_256_LEAF;
  static constexpr uint16_t CAPACITY = Node256::CAPACITY;

 public:
  Node256Leaf() = delete;
  Node256Leaf(const Node256Leaf&) = delete;
  Node256Leaf& operator=(const Node256Leaf&) = delete;

 private:
  uint16_t count;
  validity_t mask[CAPACITY / ValidityMask::BITS_PER_VALUE];

 public:
  //! Get a new Node256Leaf handle and initialize the leaf.
  static NodeHandle New(ArtTree& art, NodePtr& node);

  //! Insert a byte.
  static void InsertByte(ArtTree& art, NodePtr& node, const uint8_t byte);

  //! Returns true, if the byte exists, else false.
  bool HasByte(const uint8_t byte) const;

  //! Get the first byte greater or equal to the byte.
  //! Returns true, if such a byte exists, else false.
  bool GetNextByte(uint8_t& byte) const;

 private:
  static void GrowNode15Leaf(ArtTree& art, NodePtr& node256_leaf,
                             NodePtr& node15_leaf);
};

}  // namespace neug::art
