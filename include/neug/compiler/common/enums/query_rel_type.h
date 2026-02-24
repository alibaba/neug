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

/**
 * This file is originally from the Kùzu project
 * (https://github.com/kuzudb/kuzu) Licensed under the MIT License. Modified by
 * Zhou Xiaoli in 2025 to support Neug-specific features.
 */

#pragma once

#include <cstdint>
#include <memory>

#include "path_semantic.h"

namespace neug {
namespace function {
class RJAlgorithm;
}

namespace common {

enum class QueryRelType : uint8_t {
  NON_RECURSIVE = 0,
  VARIABLE_LENGTH_WALK = 1,
  VARIABLE_LENGTH_TRAIL = 2,
  VARIABLE_LENGTH_ACYCLIC = 3,
  SHORTEST = 4,
  ALL_SHORTEST = 5,
  WEIGHTED_SHORTEST = 6,
  ALL_WEIGHTED_SHORTEST = 7,
};

struct QueryRelTypeUtils {
  static bool isRecursive(QueryRelType type) {
    return type != QueryRelType::NON_RECURSIVE;
  }

  static bool isWeighted(QueryRelType type) {
    return type == QueryRelType::WEIGHTED_SHORTEST ||
           type == QueryRelType::ALL_WEIGHTED_SHORTEST;
  }

  static PathSemantic getPathSemantic(QueryRelType queryRelType);

  static std::unique_ptr<function::RJAlgorithm> getFunction(QueryRelType type);
};

}  // namespace common
}  // namespace neug
