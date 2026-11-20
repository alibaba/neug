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

#include "neug/storages/index/art_tree/node256_leaf.h"

#include "neug/storages/index/art_tree/base_leaf.h"
#include "neug/storages/index/art_tree/node48.h"

namespace neug::art {

NodeHandle Node256Leaf::New(ArtTree& art, NodePtr& node) {
  node = NodePtr::GetAllocator(art, NODE_256_LEAF).New();
  node.SetMetadata(static_cast<uint8_t>(NODE_256_LEAF));

  NodeHandle handle(art, node);
  auto& n = handle.Get<Node256Leaf>();

  n.count = 0;
  ValidityMask mask(&n.mask[0], Node256::CAPACITY);
  mask.SetAllInvalid(CAPACITY);

  return handle;
}

void Node256Leaf::InsertByte(ArtTree& art, NodePtr& node, const uint8_t byte) {
  NodeHandle handle(art, node);
  auto& n = handle.Get<Node256Leaf>();

  n.count++;
  ValidityMask mask(&n.mask[0], Node256::CAPACITY);
  mask.SetValid(byte);
}

bool Node256Leaf::HasByte(const uint8_t byte) const {
  idx_t entry_idx = 0;
  idx_t idx_in_entry = 0;
  ValidityMask::GetEntryIndex(byte, entry_idx, idx_in_entry);
  return ValidityMask::RowIsValid(mask[entry_idx], idx_in_entry);
}

bool Node256Leaf::GetNextByte(uint8_t& byte) const {
  for (uint16_t i = byte; i < CAPACITY; i++) {
    idx_t entry_idx = 0;
    idx_t idx_in_entry = 0;
    ValidityMask::GetEntryIndex(i, entry_idx, idx_in_entry);
    if (ValidityMask::RowIsValid(mask[entry_idx], idx_in_entry)) {
      byte = UnsafeNumericCast<uint8_t>(i);
      return true;
    }
  }

  return false;
}

void Node256Leaf::GrowNode15Leaf(ArtTree& art, NodePtr& node256_leaf,
                                 NodePtr& node15_leaf) {
  {
    NodeHandle n15_handle(art, node15_leaf);
    auto& n15 = n15_handle.Get<Node15Leaf>();

    auto n256_handle = New(art, node256_leaf);
    auto& n256 = n256_handle.Get<Node256Leaf>();
    node256_leaf.SetGateStatus(node15_leaf.GetGateStatus());

    n256.count = n15.count;
    ValidityMask mask(&n256.mask[0], Node256::CAPACITY);
    for (uint8_t i = 0; i < n15.count; i++) {
      mask.SetValid(n15.key[i]);
    }
  }
  NodePtr::FreeNode(art, node15_leaf);
}

}  // namespace neug::art
