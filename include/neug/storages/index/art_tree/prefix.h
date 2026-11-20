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

class ARTKey;

//! Prefix is a wrapper class to access a prefix.
//! The prefix contains up to the ArtTree's prefix size bytes and an additional
//! byte for the count. It also contains a child NodePtr.
class Prefix {
 public:
  static constexpr NType PREFIX = NType::PREFIX;

  static constexpr uint8_t ROW_ID_SIZE = sizeof(row_t);
  static constexpr uint8_t ROW_ID_COUNT = ROW_ID_SIZE - 1;
  // The child pointer and prefix byte count.
  static constexpr uint8_t METADATA_SIZE = sizeof(NodePtr) + 1;

 public:
  Prefix() = delete;
  Prefix(const ArtTree& art, const NodePtr node);

  data_ptr_t data;
  NodePtr* child_slot;

 public:
  static uint8_t GetByte(const ArtTree& art, const NodePtr& node,
                         const uint8_t pos);

 public:
  //! Get a new list of prefix nodes. The node reference holds the child of the
  //! last prefix node.
  static void New(ArtTree& art, reference<NodePtr>& node_ref, const ARTKey& key,
                  const idx_t depth, idx_t count);

  //! Splits the prefix at pos.
  //! node_ref references the node that replaces the split byte.
  //! child references the remaining node after the split.
  //! Returns GATE_SET, if a gate node was freed, else GATE_NOT_SET.
  //! If it returns GATE_SET, then the caller must set the gate for the node
  //! replacing the split byte, after its creation.
  static GateStatus Split(ArtTree& art, reference<NodePtr>& node_ref,
                          NodePtr& child, const uint8_t pos);

 private:
  static Prefix NewInternal(ArtTree& art, NodePtr& node, const data_ptr_t data,
                            const uint8_t count, const idx_t offset);

  Prefix Append(ArtTree& art, const uint8_t byte);
  void Append(ArtTree& art, NodePtr other);
};
}  // namespace neug::art
