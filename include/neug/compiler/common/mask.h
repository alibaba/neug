/**
 * Copyright 2020 Alibaba Group Holding Limited.
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
 * This file is originally from the Kùzu project
 * (https://github.com/kuzudb/kuzu) Licensed under the MIT License. Modified by
 * Zhou Xiaoli in 2025 to support Neug-specific features.
 */

#pragma once

#include "neug/compiler/common/types/types.h"

namespace neug {
namespace common {

// Note that this class is NOT thread-safe.
class SemiMask {
 public:
  explicit SemiMask(offset_t maxOffset) : maxOffset{maxOffset}, enabled{false} {}

  virtual ~SemiMask() = default;

  virtual void mask(offset_t nodeOffset) = 0;
  virtual void maskRange(offset_t startNodeOffset, offset_t endNodeOffset) = 0;

  virtual bool isMasked(offset_t startNodeOffset) = 0;

  // include&exclude
  virtual offset_vec_t range(uint32_t start, uint32_t end) = 0;

  virtual uint64_t getNumMaskedNodes() const = 0;

  virtual offset_vec_t collectMaskedNodes(uint64_t size) const = 0;

  offset_t getMaxOffset() const { return maxOffset; }

  bool isEnabled() const { return enabled; }
  void enable() { enabled = true; }

 private:
  offset_t maxOffset;
  bool enabled;
};

class NodeOffsetMaskMap {
 public:
  NodeOffsetMaskMap() = default;

  offset_t getNumMaskedNode() const;

  void addMask(table_id_t tableID, std::unique_ptr<SemiMask> mask) {
    NEUG_ASSERT(!maskMap.contains(tableID));
    maskMap.insert({tableID, std::move(mask)});
  }

  table_id_map_t<SemiMask*> getMasks() const {
    table_id_map_t<SemiMask*> result;
    for (auto& [tableID, mask] : maskMap) {
      result.emplace(tableID, mask.get());
    }
    return result;
  }

  bool containsTableID(table_id_t tableID) const { return maskMap.contains(tableID); }
  SemiMask* getOffsetMask(table_id_t tableID) const {
    NEUG_ASSERT(containsTableID(tableID));
    return maskMap.at(tableID).get();
  }

  void pin(table_id_t tableID) {
    if (maskMap.contains(tableID)) {
      pinnedMask = maskMap.at(tableID).get();
    } else {
      pinnedMask = nullptr;
    }
  }
  bool hasPinnedMask() const { return pinnedMask != nullptr; }
  SemiMask* getPinnedMask() const { return pinnedMask; }

  bool valid(offset_t offset) const {
    NEUG_ASSERT(pinnedMask != nullptr);
    return pinnedMask->isMasked(offset);
  }
  bool valid(nodeID_t nodeID) const {
    NEUG_ASSERT(maskMap.contains(nodeID.tableID));
    return maskMap.at(nodeID.tableID)->isMasked(nodeID.offset);
  }

 private:
  table_id_map_t<std::unique_ptr<SemiMask>> maskMap;
  SemiMask* pinnedMask = nullptr;
};

}  // namespace common
}  // namespace neug
