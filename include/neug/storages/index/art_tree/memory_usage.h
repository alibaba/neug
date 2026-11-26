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
#include <atomic>
#include <cstddef>
#include <memory>

namespace neug::art {
struct MemoryUsage {
  size_t node_buffer_bytes = 0;
  // Informational subset of node_buffer_bytes, not an additional allocation.
  size_t occupied_node_bytes = 0;
  size_t occupancy_bytes = 0;
  size_t free_slots_bytes = 0;
  size_t directory_bytes = 0;
  // Total memory requested by the ART index, excluding additional overhead
  // introduced by the underlying allocator's size-rounding policy.
  size_t TotalBytes() const;
  MemoryUsage& operator+=(const MemoryUsage& other);
};

// Count directory node requests rather than estimating std::map/set layouts.
// All allocator rebinds of one container share a counter. malloc rounding,
// private metadata and shared_ptr control blocks are outside this accounting.
template <class T>
class CountingAllocator {
 public:
  using value_type = T;
  std::shared_ptr<std::atomic<size_t>> counter;
  CountingAllocator() : counter(std::make_shared<std::atomic<size_t>>(0)) {}
  template <class U>
  CountingAllocator(const CountingAllocator<U>& other) noexcept
      : counter(other.counter) {}
  T* allocate(size_t n) {
    auto* pointer = std::allocator<T>{}.allocate(n);
    counter->fetch_add(n * sizeof(T), std::memory_order_relaxed);
    return pointer;
  }
  void deallocate(T* pointer, size_t n) noexcept {
    counter->fetch_sub(n * sizeof(T), std::memory_order_relaxed);
    std::allocator<T>{}.deallocate(pointer, n);
  }
  CountingAllocator select_on_container_copy_construction() const {
    return CountingAllocator();
  }
  size_t Bytes() const { return counter->load(std::memory_order_relaxed); }
  template <class U>
  bool operator==(const CountingAllocator<U>& other) const noexcept {
    return counter == other.counter;
  }
  template <class U>
  bool operator!=(const CountingAllocator<U>& other) const noexcept {
    return !(*this == other);
  }
};
}  // namespace neug::art
