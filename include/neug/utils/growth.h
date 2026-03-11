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

#include <cstddef>
#include <cstdint>

namespace neug {
inline size_t calculate_new_capacity(size_t current_capacity,
                                     bool is_vertex_table) {
  if (current_capacity < 4096) {
    return 4096;  // Start with a reasonable default capacity.
  }
  static constexpr size_t MAX_CAPACITY = std::numeric_limits<size_t>::max();
  if (is_vertex_table) {
    // For vertex tables, we grow exponentially: double the current capacity,
    // with a 4K floor.
    return current_capacity <= MAX_CAPACITY / 2 ? current_capacity * 2
                                                : MAX_CAPACITY;
  } else {
    // For edge tables, we grow linearly: new capacity = current capacity +
    // (current capacity + 4) / 5.
    return current_capacity <= MAX_CAPACITY - (current_capacity + 4) / 5
               ? current_capacity + (current_capacity + 4) / 5
               : MAX_CAPACITY;
  }
}
}  // namespace neug
