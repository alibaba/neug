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

#pragma once

#include <algorithm>
#include <atomic>
#include <thread>
#include <vector>

#include "neug/storages/graph/vertex_table.h"
#include "neug/utils/property/types.h"

namespace neug {
namespace gds {
struct ParallelUtils {
  template <typename FUNC>
  static void parallel_for(const VertexSet& vertex_set, const FUNC& func,
                           int concurrency, int chunk_size = 4096) {
    if (concurrency <= 0) {
      concurrency = static_cast<int>(std::thread::hardware_concurrency());
    }
    if (concurrency <= 1) {
      for (vid_t v : vertex_set) {
        func(v, 0);
      }
      return;
    }

    struct WorkerArgs {
      const VertexSet* vertex_set;
      std::atomic<size_t>* current;
      int chunk_size;
      const FUNC* func;
      int thread_id;
    };

    std::atomic<size_t> current(0);
    size_t vertex_count = vertex_set.size();
    size_t thread_num = std::min(static_cast<size_t>(concurrency),
                                 (vertex_count + chunk_size - 1) / chunk_size);

    std::vector<std::thread> threads;
    std::vector<WorkerArgs> args(thread_num - 1);
    threads.reserve(thread_num - 1);

    for (size_t i = 0; i < thread_num - 1; ++i) {
      args[i] = WorkerArgs{&vertex_set, &current, chunk_size, &func,
                           static_cast<int>(i + 1)};
      threads.emplace_back(
          [](WorkerArgs* params) {
            size_t vertex_count = params->vertex_set->size();
            const auto& vertex_set = *params->vertex_set;
            while (true) {
              size_t local_start =
                  params->current->fetch_add(params->chunk_size);
              if (local_start >= vertex_count) {
                break;
              }
              size_t local_end = local_start + params->chunk_size;
              local_end = (local_end > vertex_count) ? vertex_count : local_end;
              for (size_t offset = local_start; offset < local_end; ++offset) {
                if (vertex_set.valid(offset)) {
                  (*params->func)(offset, params->thread_id);
                }
              }
            }
          },
          &args[i]);
    }
    // Main thread also works
    while (true) {
      size_t local_start = current.fetch_add(chunk_size);
      if (local_start >= vertex_count) {
        break;
      }
      size_t local_end = std::min(local_start + chunk_size, vertex_count);
      for (size_t i = local_start; i < local_end; ++i) {
        if (vertex_set.valid(i)) {
          func(i, 0);
        }
      }
    }
    for (auto& thread : threads) {
      if (thread.joinable()) {
        thread.join();
      }
    }
  }

  template <typename FUNC>
  static void parallel_for(const vid_t* items, size_t size, const FUNC& func,
                           int concurrency, int chunk_size = 4096) {
    if (concurrency <= 0) {
      concurrency = std::thread::hardware_concurrency();
    }
    if (concurrency <= 1) {
      for (size_t i = 0; i < size; ++i) {
        func(items[i], 0);
      }
      return;
    }
    struct WorkerArgs {
      const vid_t* items;
      std::atomic<size_t>* current;
      size_t size;
      int chunk_size;
      const FUNC* func;
      int thread_id;
    };
    std::atomic<size_t> current(0);
    size_t thread_num = std::min(static_cast<size_t>(concurrency),
                                 (size + chunk_size - 1) / chunk_size);

    std::vector<std::thread> threads;
    std::vector<WorkerArgs> args(thread_num - 1);
    threads.reserve(thread_num - 1);

    for (size_t i = 0; i < thread_num - 1; ++i) {
      args[i] = WorkerArgs{items,      &current, size,
                           chunk_size, &func,    static_cast<int>(i + 1)};
      threads.emplace_back(
          [](WorkerArgs* params) {
            while (true) {
              size_t local_start =
                  params->current->fetch_add(params->chunk_size);
              if (local_start >= params->size) {
                break;
              }
              size_t local_end = local_start + params->chunk_size;
              local_end = (local_end > params->size) ? params->size : local_end;
              for (size_t offset = local_start; offset < local_end; ++offset) {
                (*params->func)(params->items[offset], params->thread_id);
              }
            }
          },
          &args[i]);
    }
    // Main thread also works
    while (true) {
      size_t local_start = current.fetch_add(chunk_size);
      if (local_start >= size) {
        break;
      }
      size_t local_end = std::min(local_start + chunk_size, size);
      for (size_t i = local_start; i < local_end; ++i) {
        func(items[i], 0);
      }
    }
    for (auto& thread : threads) {
      if (thread.joinable()) {
        thread.join();
      }
    }
  }
};
}  // namespace gds
}  // namespace neug
