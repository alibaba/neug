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

//! Leaves inline a row ID or use a nested ID ART behind a gate. If an ArtTree
//! key contains multiple row IDs, then we use the row IDs as keys and create a
//! nested ArtTree behind the gate node. As row IDs are always unique, these
//! nested ARTs never contain duplicates themselves.
class Leaf {
 public:
  static constexpr NType INLINED = NType::LEAF_INLINED;
  //! Inline a row ID into a node pointer.
  static void New(NodePtr& node, const row_t row_id);

  //! Merge two inlined leaf nodes.
  static void MergeInlined(ARTArenaAllocator& arena, ArtTree& art,
                           NodePtr& left, NodePtr& right, GateStatus status,
                           idx_t depth);
};
}  // namespace neug::art
