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

namespace neug::art {

class ARTBuilder {
 public:
  ARTBuilder() = delete;
  ARTBuilder(ARTArenaAllocator& arena, ArtTree& art,
             std::span<const ARTKey> keys, std::span<const ARTKey> row_ids)
      : arena(arena), art(art), keys(keys), row_ids(row_ids) {}

 public:
  //! Initialize the ArtTree builder by passing a reference to the root node.
  void Init(NodePtr& node, const idx_t end) { s.emplace(node, 0, end, 0); }
  //! Build the ArtTree starting at the first entry in the stack.
  ARTConflictType Build();

 private:
  struct NodeEntry {
    NodeEntry() = delete;
    NodeEntry(NodePtr& node, const idx_t start, const idx_t end,
              const idx_t depth)
        : node(node), start(start), end(end), depth(depth){};

    NodePtr& node;
    idx_t start;
    idx_t end;
    idx_t depth;
  };

  //! The arena holds any temporary memory allocated during the Build phase.
  ARTArenaAllocator& arena;
  //! The ArtTree holding the node memory.
  ArtTree& art;
  //! The keys to build the ArtTree from.
  std::span<const ARTKey> keys;
  //! The row IDs matching the keys.
  std::span<const ARTKey> row_ids;
  //! The stack. While merging, NodeEntry elements are pushed onto of the stack.
  stack<NodeEntry> s;
};

}  // namespace neug::art
