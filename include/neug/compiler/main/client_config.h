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
#include <string>

#include "neug/compiler/common/enums/path_semantic.h"

namespace neug {
namespace main {

struct ClientConfigDefault {
  // 0 means timeout is disabled by default.
  static constexpr uint64_t TIMEOUT_IN_MS = 0;
  static constexpr uint32_t VAR_LENGTH_MAX_DEPTH = 30;
  static constexpr uint64_t SPARSE_FRONTIER_THRESHOLD = 1000;
  static constexpr bool ENABLE_SEMI_MASK = true;
  static constexpr bool ENABLE_ZONE_MAP = true;
  static constexpr bool ENABLE_PROGRESS_BAR = false;
  static constexpr uint64_t SHOW_PROGRESS_AFTER = 1000;
  static constexpr common::PathSemantic RECURSIVE_PATTERN_SEMANTIC =
      common::PathSemantic::WALK;
  static constexpr uint32_t RECURSIVE_PATTERN_FACTOR = 100;
  static constexpr bool DISABLE_MAP_KEY_CHECK = true;
  static constexpr uint64_t WARNING_LIMIT = 8 * 1024;
  static constexpr bool ENABLE_PLAN_OPTIMIZER = true;
  static constexpr bool ENABLE_INTERNAL_CATALOG = false;
};

struct ClientConfig {
  // System home directory.
  std::string homeDirectory;
  // File search path.
  std::string fileSearchPath;
  // If using semi mask in join.
  bool enableSemiMask = ClientConfigDefault::ENABLE_SEMI_MASK;
  // If using zone map in scan.
  bool enableZoneMap = ClientConfigDefault::ENABLE_ZONE_MAP;
  // Number of threads for execution.
  uint64_t numThreads = 1;
  // Timeout (milliseconds).
  uint64_t timeoutInMS = ClientConfigDefault::TIMEOUT_IN_MS;
  // Variable length maximum depth.
  uint32_t varLengthMaxDepth = ClientConfigDefault::VAR_LENGTH_MAX_DEPTH;
  // Threshold determines when to switch from sparse frontier to dense frontier
  uint64_t sparseFrontierThreshold =
      ClientConfigDefault::SPARSE_FRONTIER_THRESHOLD;
  // If using progress bar.
  bool enableProgressBar = ClientConfigDefault::ENABLE_PROGRESS_BAR;
  // time before displaying progress bar
  uint64_t showProgressAfter = ClientConfigDefault::SHOW_PROGRESS_AFTER;
  // Semantic for recursive pattern, can be either WALK, TRAIL, ACYCLIC
  common::PathSemantic recursivePatternSemantic =
      ClientConfigDefault::RECURSIVE_PATTERN_SEMANTIC;
  // Scale factor for recursive pattern cardinality estimation.
  uint32_t recursivePatternCardinalityScaleFactor =
      ClientConfigDefault::RECURSIVE_PATTERN_FACTOR;
  // Maximum number of cached warnings
  uint64_t warningLimit = ClientConfigDefault::WARNING_LIMIT;
  bool disableMapKeyCheck = ClientConfigDefault::DISABLE_MAP_KEY_CHECK;
  // If enable plan optimizer
  bool enablePlanOptimizer = ClientConfigDefault::ENABLE_PLAN_OPTIMIZER;
  // If use internal catalog during binding
  bool enableInternalCatalog = ClientConfigDefault::ENABLE_INTERNAL_CATALOG;
};

}  // namespace main
}  // namespace neug
