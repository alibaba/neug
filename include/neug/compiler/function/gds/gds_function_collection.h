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

#include "neug/compiler/function/gds/rec_joins.h"

namespace gs {
namespace function {

struct VarLenJoinsFunction {
  static constexpr const char* name = "VAR_LEN_JOINS";

  static std::unique_ptr<RJAlgorithm> getAlgorithm();
};

struct AllSPDestinationsFunction {
  static constexpr const char* name = "ALL_SP_DESTINATIONS";

  static std::unique_ptr<RJAlgorithm> getAlgorithm();
};

struct AllSPPathsFunction {
  static constexpr const char* name = "ALL_SP_PATHS";

  static std::unique_ptr<RJAlgorithm> getAlgorithm();
};

struct SingleSPDestinationsFunction {
  static constexpr const char* name = "SINGLE_SP_DESTINATIONS";

  static std::unique_ptr<RJAlgorithm> getAlgorithm();
};

struct SingleSPPathsFunction {
  static constexpr const char* name = "SINGLE_SP_PATHS";

  static std::unique_ptr<RJAlgorithm> getAlgorithm();
};

struct WeightedSPDestinationsFunction {
  static constexpr const char* name = "WEIGHTED_SP_DESTINATIONS";

  static std::unique_ptr<RJAlgorithm> getAlgorithm();
};

struct WeightedSPPathsFunction {
  static constexpr const char* name = "WEIGHTED_SP_PATHS";

  static std::unique_ptr<RJAlgorithm> getAlgorithm();
};

struct AllWeightedSPPathsFunction {
  static constexpr const char* name = "ALL_WEIGHTED_SP_PATHS";

  static std::unique_ptr<RJAlgorithm> getAlgorithm();
};

}  // namespace function
}  // namespace gs
