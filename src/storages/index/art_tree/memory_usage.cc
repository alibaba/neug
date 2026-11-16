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

#include "neug/storages/index/art_tree/memory_usage.h"

namespace neug::art {
size_t MemoryUsage::TotalBytes() const {
  return node_buffer_bytes + occupancy_bytes + free_slots_bytes +
         directory_bytes;
}
MemoryUsage& MemoryUsage::operator+=(const MemoryUsage& other) {
  node_buffer_bytes += other.node_buffer_bytes;
  occupied_node_bytes += other.occupied_node_bytes;
  occupancy_bytes += other.occupancy_bytes;
  free_slots_bytes += other.free_slots_bytes;
  directory_bytes += other.directory_bytes;
  return *this;
}
}  // namespace neug::art
