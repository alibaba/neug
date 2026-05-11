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

#include <bthread/bthread.h>
#include <algorithm>
#include <atomic>
#include <vector>

#include "neug/utils/property/types.h"

namespace neug {
namespace gds {
struct ParallelUtils {
  template <typename FUNC>
  static void parallel_for(const vid_t* items, size_t size, const FUNC& func,
                           int concurrency, int chunk_size = 1024) {
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
    std::vector<bthread_t> threads(concurrency - 1);
    std::vector<WorkerArgs> args(concurrency - 1);
    for (int i = 0; i < concurrency - 1; ++i) {
      args[i] = WorkerArgs{items, &current, size, chunk_size, &func, i + 1};
      bthread_start_urgent(
          &threads[i], nullptr,
          [](void* arg) -> void* {
            auto* params = static_cast<WorkerArgs*>(arg);
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
            return nullptr;
          },
          &args[i]);
    }
    // Main thread also participates in processing
    while (true) {
      size_t local_start = current.fetch_add(chunk_size);
      if (local_start >= size)
        break;
      size_t local_end = std::min(local_start + chunk_size, size);
      for (size_t i = local_start; i < local_end; ++i) {
        func(items[i], 0);
      }
    }
    for (auto& thread : threads) {
      bthread_join(thread, nullptr);
    }
  }
};
}  // namespace gds
}  // namespace neug