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
#include <stdexcept>
#include <thread>

#include "neug/storages/graph/vertex_table.h"
#include "neug/utils/parallel_utils.h"
#include "neug/utils/property/types.h"

namespace neug {
namespace gds {
struct ParallelUtils {
  template <typename FUNC>
  static void parallel_for(const VertexSet& vertex_set, const FUNC& func,
                           int concurrency, int chunk_size = 4096) {
    auto options = Options(concurrency, chunk_size);
    auto status = neug::parallel_for(
        0, vertex_set.size(), options, [&](size_t i, size_t worker_id) {
          if (vertex_set.valid(i))
            func(static_cast<vid_t>(i), static_cast<int>(worker_id));
        });
    if (!status.ok())
      throw std::runtime_error(status.ToString());
  }

  template <typename FUNC>
  static void parallel_for(const vid_t* items, size_t size, const FUNC& func,
                           int concurrency, int chunk_size = 4096) {
    auto status =
        neug::parallel_for(0, size, Options(concurrency, chunk_size),
                           [&](size_t i, size_t worker_id) {
                             func(items[i], static_cast<int>(worker_id));
                           });
    if (!status.ok())
      throw std::runtime_error(status.ToString());
  }

 private:
  static ParallelOptions Options(int concurrency, int chunk_size) {
    if (chunk_size <= 0)
      throw std::invalid_argument("parallel_for chunk_size must be positive");
    const size_t workers =
        concurrency > 0 ? static_cast<size_t>(concurrency)
                        : std::max(1u, std::thread::hardware_concurrency());
    return {workers, static_cast<size_t>(chunk_size)};
  }
};
}  // namespace gds
}  // namespace neug
