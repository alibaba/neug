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

#include "neug/compiler/common/enums/conflict_action.h"

#include "neug/compiler/common/assert.h"

namespace neug {
namespace common {

std::string ConflictActionUtil::toString(ConflictAction action) {
  switch (action) {
  case ConflictAction::ON_CONFLICT_THROW: {
    return "ON_CONFLICT_THROW";
  }
  case ConflictAction::ON_CONFLICT_DO_NOTHING: {
    return "ON_CONFLICT_DO_NOTHING";
  }
  default:
    NEUG_UNREACHABLE;
  }
}

}  // namespace common
}  // namespace neug
