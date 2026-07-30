/** Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once

#include <array>
#include <atomic>
#include <cassert>
#include <cstdint>
#include <unordered_map>

#include "neug/utils/property/types.h"

namespace neug {

/**
 * @brief Table / schema dirty bits for one PropertyGraph
 *
 * A bit is true iff Mark*() was called since the last ClearAll().
 * Vertex slots are dense (label_t = uint8_t); edge slots are keyed by the
 * sparse triplet index and must mirror the graph's set of edge tables:
 * add / erase a slot whenever an edge table is created / removed.
 *
 * Marking and reading existing slots are thread-safe (relaxed atomics), but
 * only while no concurrent slot management is in progress: edge_ is a
 * std::unordered_map, so find() is unsafe against insert/erase/rehash.
 * Slot management, Set* and whole-object copy / assignment require exclusive
 * access (e.g. Clone / compact_schema).
 */
class DirtyTracker {
 public:
  // Dense vertex slots cover the full label_t domain; keep label_t 1-byte.
  static_assert(sizeof(label_t) == 1,
                "DirtyTracker dense vertex array requires label_t = uint8_t");
  static constexpr size_t kMaxVertexLabels = 1u << (sizeof(label_t) * 8);

  void MarkVertex(label_t label) { vertex_[label] = true; }
  bool IsVertexDirty(label_t label) const { return vertex_[label]; }
  void SetVertex(label_t label, bool dirty) { vertex_[label] = dirty; }

  void MarkEdge(uint32_t index) {
    auto it = edge_.find(index);
    assert(it != edge_.end());
    it->second = true;
  }
  /// Returns false for indices without a slot.
  bool IsEdgeDirty(uint32_t index) const {
    auto it = edge_.find(index);
    return it != edge_.end() && it->second;
  }
  void AddEdgeSlot(uint32_t index, bool dirty = false) {
    edge_.try_emplace(index, dirty);
  }
  void EraseEdgeSlot(uint32_t index) { edge_.erase(index); }

  void MarkSchema() { schema_ = true; }
  bool IsSchemaDirty() const { return schema_; }
  void SetSchema(bool dirty) { schema_ = dirty; }

  /// True if schema or any table-level bit is set since the last ClearAll().
  bool IsModified() const {
    if (schema_) {
      return true;
    }
    for (const auto& bit : vertex_) {
      if (bit) {
        return true;
      }
    }
    for (const auto& [_, bit] : edge_) {
      if (bit) {
        return true;
      }
    }
    return false;
  }

  /// Set every bit (table-level and schema) to false; edge slots are kept.
  void ClearAll() {
    for (auto& bit : vertex_) {
      bit = false;
    }
    for (auto& [_, bit] : edge_) {
      bit = false;
    }
    schema_ = false;
  }

  /// Drop all edge slots and clear every bit (the graph is being reset).
  void Reset() {
    edge_.clear();
    ClearAll();
  }

 private:
  // Copyable wrapper around a relaxed atomic bool, so the flags can live in
  // std::array / std::unordered_map directly. Copies are non-atomic.
  class Flag {
   public:
    Flag() = default;
    Flag(bool v) : v_(v) {}  // NOLINT(runtime/explicit): bool-like by design
    Flag(const Flag& o) : v_(o.load()) {}
    Flag& operator=(const Flag& o) { return *this = o.load(); }
    Flag& operator=(bool v) {
      // Check first so repeated marks don't keep dirtying the cache line.
      if (load() != v) {
        v_.store(v, std::memory_order_relaxed);
      }
      return *this;
    }
    operator bool() const { return load(); }

   private:
    bool load() const { return v_.load(std::memory_order_relaxed); }
    std::atomic<bool> v_{false};
  };

  std::array<Flag, kMaxVertexLabels> vertex_{};
  std::unordered_map<uint32_t, Flag> edge_;
  Flag schema_;
};

}  // namespace neug
