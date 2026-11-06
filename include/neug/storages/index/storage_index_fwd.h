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

#include <cstdint>
#include <optional>
#include <string>
#include <variant>

namespace neug {

class StorageIndex;
struct PendingIndex;
struct ShowIndexStats;

struct OptimizeIndexStats {
  uint64_t indexed_count{0};
  uint64_t skipped_count{0};
  bool changed{false};
};

struct IndexOptions {
  std::optional<std::string> type;
  std::optional<std::string> index_name;
};

struct OptimizeIndexResult {
  std::string type;
  std::string index_name;
  OptimizeIndexStats stats;
  double duration_ms{0};
};

using CreatedIndex = std::variant<StorageIndex*, PendingIndex*>;

}  // namespace neug
