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
#pragma once

#include <atomic>
#include <cstdint>

namespace neug {

// Tracks whether an object changed since its persisted revision. Mutations may
// arrive concurrently; persisting and resetting are caller-serialized.
class ModificationTracker {
 public:
  using Revision = uint64_t;

  ModificationTracker() = default;

  ModificationTracker(ModificationTracker&& other) noexcept {
    CopyFrom(other);
    other.Reset();
  }

  ModificationTracker& operator=(ModificationTracker&& other) noexcept {
    if (this != &other) {
      CopyFrom(other);
      other.Reset();
    }
    return *this;
  }

  ModificationTracker(const ModificationTracker&) = delete;
  ModificationTracker& operator=(const ModificationTracker&) = delete;

  void MarkModified() { revision_.fetch_add(1, std::memory_order_relaxed); }

  bool HasChanges() const {
    return revision_.load(std::memory_order_relaxed) !=
           persisted_revision_.load(std::memory_order_relaxed);
  }

  Revision CurrentRevision() const {
    return revision_.load(std::memory_order_relaxed);
  }

  void MarkPersisted(Revision revision) {
    persisted_revision_.store(revision, std::memory_order_relaxed);
  }

  void Reset() {
    revision_.store(0, std::memory_order_relaxed);
    persisted_revision_.store(0, std::memory_order_relaxed);
  }

  void CopyFrom(const ModificationTracker& other) {
    revision_.store(other.revision_.load(std::memory_order_relaxed),
                    std::memory_order_relaxed);
    persisted_revision_.store(
        other.persisted_revision_.load(std::memory_order_relaxed),
        std::memory_order_relaxed);
  }

  void Swap(ModificationTracker& other) {
    const auto revision = revision_.load(std::memory_order_relaxed);
    const auto persisted = persisted_revision_.load(std::memory_order_relaxed);
    revision_.store(other.revision_.load(std::memory_order_relaxed),
                    std::memory_order_relaxed);
    persisted_revision_.store(
        other.persisted_revision_.load(std::memory_order_relaxed),
        std::memory_order_relaxed);
    other.revision_.store(revision, std::memory_order_relaxed);
    other.persisted_revision_.store(persisted, std::memory_order_relaxed);
  }

 private:
  std::atomic<uint64_t> revision_{0};
  std::atomic<uint64_t> persisted_revision_{0};
};

}  // namespace neug
