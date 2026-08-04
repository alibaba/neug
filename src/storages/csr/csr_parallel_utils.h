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

#include <algorithm>
#include <atomic>
#include <bit>
#include <cstddef>
#include <limits>
#include <system_error>
#include <thread>
#include <vector>

namespace neug::csr_parallel {

// Vertex count per scheduling unit. Vertex degrees follow a power-law
// distribution, so a small chunk size with dynamic scheduling keeps
// high-degree vertices from creating stragglers.
constexpr size_t kDefaultChunkSize = 256;

// Creating a std::thread costs noticeably more than scanning a small CSR.
// Keep small jobs serial and require enough estimated edge-level work for
// every worker before adding it.
constexpr size_t kMinWorkPerWorker = 32 * 1024;
constexpr size_t kMinParallelWork = 2 * kMinWorkPerWorker;

namespace detail {

inline int load_degree(const int& degree) { return degree; }

inline int load_degree(const std::atomic<int>& degree) {
  return degree.load(std::memory_order_relaxed);
}

inline size_t normalize_work(int degree) {
  return degree > 0 ? static_cast<size_t>(degree) : 0;
}

// std::sort is O(n log n). Use the number of expected comparisons as its
// edge-level work estimate.
inline size_t sort_work(int degree) {
  if (degree <= 1) {
    return 0;
  }
  const size_t edge_count = static_cast<size_t>(degree);
  const size_t levels = std::bit_width(edge_count - 1);
  const size_t max = std::numeric_limits<size_t>::max();
  return edge_count > max / levels ? max : edge_count * levels;
}

template <typename DEGREE_T, typename FUNC>
void parallel_for_degree_ranges(size_t total, const DEGREE_T* degrees,
                                size_t (*work_for_degree)(int), FUNC&& func,
                                size_t chunk_size) {
  if (total == 0) {
    return;
  }
  if (chunk_size == 0) {
    chunk_size = kDefaultChunkSize;
  }

  const size_t range_count = 1 + (total - 1) / chunk_size;
  const size_t hardware_threads =
      std::max<size_t>(1, std::thread::hardware_concurrency());
  const size_t max_workers = std::min(hardware_threads, range_count);

  const size_t max = std::numeric_limits<size_t>::max();
  const size_t work_limit = max_workers > max / kMinWorkPerWorker
                                ? max
                                : max_workers * kMinWorkPerWorker;
  size_t estimated_work = 0;
  for (size_t i = 0; i < total && estimated_work < work_limit; ++i) {
    const size_t item_work = work_for_degree(load_degree(degrees[i]));
    estimated_work += std::min(item_work, work_limit - estimated_work);
  }

  size_t workers = 1;
  if (estimated_work >= kMinParallelWork) {
    workers = std::min(max_workers, estimated_work / kMinWorkPerWorker);
  }
  if (workers <= 1) {
    func(0, total);
    return;
  }
  std::atomic<size_t> cursor{0};
  auto work = [&]() {
    while (true) {
      const size_t first =
          cursor.fetch_add(chunk_size, std::memory_order_relaxed);
      if (first >= total) {
        break;
      }
      const size_t last =
          first + chunk_size < total ? first + chunk_size : total;
      func(first, last);
    }
  };
  std::vector<std::thread> threads;
  threads.reserve(workers - 1);
  for (size_t i = 1; i < workers; ++i) {
    try {
      threads.emplace_back(work);
    } catch (const std::system_error&) {
      // Handle thread creation failure before joinable threads can unwind.
      // The workers already created plus the caller finish the remaining work.
      break;
    }
  }
  work();
  for (auto& t : threads) {
    t.join();
  }
}

}  // namespace detail

// Dynamic-chunk parallel normalization over vertex ranges.
template <typename DEGREE_T, typename FUNC>
void parallel_for_normalize_ranges(size_t total, const DEGREE_T* degrees,
                                   FUNC&& func,
                                   size_t chunk_size = kDefaultChunkSize) {
  detail::parallel_for_degree_ranges(total, degrees, detail::normalize_work,
                                     func, chunk_size);
}

// Dynamic-chunk parallel sorting over vertex ranges. Sorting work is estimated
// as degree * ceil(log2(degree)).
template <typename DEGREE_T, typename FUNC>
void parallel_for_sort_ranges(size_t total, const DEGREE_T* degrees,
                              FUNC&& func,
                              size_t chunk_size = kDefaultChunkSize) {
  detail::parallel_for_degree_ranges(total, degrees, detail::sort_work, func,
                                     chunk_size);
}

}  // namespace neug::csr_parallel
