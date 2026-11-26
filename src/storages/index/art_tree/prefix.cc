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

#include "neug/storages/index/art_tree/prefix.h"

#include "neug/storages/index/art_tree/art.h"
#include "neug/storages/index/art_tree/art_key.h"
#include "neug/storages/index/art_tree/common.h"
#include "neug/storages/index/art_tree/node.h"
#include "neug/storages/index/art_tree/prefix_handle.h"

namespace neug::art {

Prefix::Prefix(const ArtTree& art, const NodePtr node) {
  data = NodePtr::GetAllocator(art, PREFIX).Get(node);
  child_slot = &PrefixHandle::ChildRefWithCount(data, art.PrefixCount());
}

uint8_t Prefix::GetByte(const ArtTree& art, const NodePtr& node,
                        const uint8_t pos) {
  D_ASSERT(node.GetType() == PREFIX);
  Prefix prefix(art, node);
  return prefix.data[pos];
}

Prefix Prefix::NewInternal(ArtTree& art, NodePtr& node, const data_ptr_t data,
                           const uint8_t count, const idx_t offset) {
  node = NodePtr::GetAllocator(art, PREFIX).New();
  node.SetMetadata(static_cast<uint8_t>(PREFIX));

  Prefix prefix(art, node);
  prefix.data[art.PrefixCount()] = count;
  if (data) {
    D_ASSERT(count);
    memcpy(prefix.data, data + offset, count);
  }
  prefix.child_slot->Clear();
  return prefix;
}

void Prefix::New(ArtTree& art, reference<NodePtr>& node_ref, const ARTKey& key,
                 const idx_t depth, idx_t count) {
  idx_t offset = 0;

  while (count) {
    auto min = MinValue(UnsafeNumericCast<idx_t>(art.PrefixCount()), count);
    auto this_count = UnsafeNumericCast<uint8_t>(min);
    auto prefix =
        NewInternal(art, node_ref, key.data, this_count, offset + depth);

    node_ref = *prefix.child_slot;
    offset += this_count;
    count -= this_count;
  }
}

GateStatus Prefix::Split(ArtTree& art, reference<NodePtr>& node_ref,
                         NodePtr& child, const uint8_t pos) {
  D_ASSERT(node_ref.get().HasMetadata());

  Prefix prefix(art, node_ref);

  // The split is at the last prefix byte, and the prefix is full.
  // We decrease the count and return.
  // We get:
  // [this prefix minus its last byte] ->
  // [new node at split byte] ->
  // [child at split byte: prefix.child_slot].
  if (pos + 1 == art.PrefixCount()) {
    prefix.data[art.PrefixCount()]--;
    node_ref = *prefix.child_slot;
    child = *prefix.child_slot;
    return GateStatus::GATE_NOT_SET;
  }

  if (pos + 1 < prefix.data[art.PrefixCount()]) {
    // The split is not at the last prefix byte.
    // We get:
    // [this prefix minus split byte, minus remaining bytes] ->
    // [new node at split byte] ->
    // [child with remaining bytes, and possibly remaining prefix nodes].

    // Create a new prefix and
    // 1. copy the remaining bytes of this prefix.
    // 2. append remaining prefix nodes.
    auto new_prefix = NewInternal(art, child, nullptr, 0, 0);
    new_prefix.data[art.PrefixCount()] =
        prefix.data[art.PrefixCount()] - pos - 1;
    memcpy(new_prefix.data, prefix.data + pos + 1,
           new_prefix.data[art.PrefixCount()]);

    if (prefix.child_slot->GetType() == PREFIX &&
        prefix.child_slot->GetGateStatus() == GateStatus::GATE_NOT_SET) {
      new_prefix.Append(art, *prefix.child_slot);
    } else {
      *new_prefix.child_slot = *prefix.child_slot;
    }

  } else {
    D_ASSERT(pos + 1 == prefix.data[art.PrefixCount()]);
    // The split is at the last prefix byte, but the prefix is not full.
    // There are no other bytes or prefixes after the split.
    // We get:
    // [this prefix minus split byte (can be its only byte, then we free it)] ->
    // [new node at split byte] ->
    // [child at split byte: prefix.child_slot].
    child = *prefix.child_slot;
  }

  // Set the new count of this node (can be empty).
  prefix.data[art.PrefixCount()] = pos;

  // No bytes left before the split, free this node.
  if (pos == 0) {
    auto old_status = node_ref.get().GetGateStatus();
    NodePtr::FreeNode(art, node_ref);
    return old_status;
  }

  // There are bytes left before the split.
  // The subsequent node replaces the split byte.
  node_ref = *prefix.child_slot;
  return GateStatus::GATE_NOT_SET;
}

Prefix Prefix::Append(ArtTree& art, const uint8_t byte) {
  if (data[art.PrefixCount()] != art.PrefixCount()) {
    data[data[art.PrefixCount()]] = byte;
    data[art.PrefixCount()]++;
    return *this;
  }

  auto prefix = NewInternal(art, *child_slot, nullptr, 0, 0);
  return prefix.Append(art, byte);
}

void Prefix::Append(ArtTree& art, NodePtr other) {
  D_ASSERT(other.HasMetadata());

  Prefix prefix = *this;
  while (other.GetType() == PREFIX) {
    if (other.GetGateStatus() == GateStatus::GATE_SET) {
      *prefix.child_slot = other;
      return;
    }

    Prefix other_prefix(art, other);
    for (idx_t i = 0; i < other_prefix.data[art.PrefixCount()]; i++) {
      prefix = prefix.Append(art, other_prefix.data[i]);
    }

    *prefix.child_slot = *other_prefix.child_slot;
    NodePtr::FreeNode(art, other);
    other = *prefix.child_slot;
  }
}

}  // namespace neug::art
