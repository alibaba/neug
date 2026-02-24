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

#include "neug/compiler/common/enums/query_rel_type.h"

#include "neug/compiler/common/assert.h"
#include "neug/compiler/function/gds/rec_joins.h"

using namespace neug::function;

namespace neug {
namespace common {

PathSemantic QueryRelTypeUtils::getPathSemantic(QueryRelType queryRelType) {
  switch (queryRelType) {
  case QueryRelType::VARIABLE_LENGTH_WALK:
    return PathSemantic::WALK;
  case QueryRelType::VARIABLE_LENGTH_TRAIL:
    return PathSemantic::TRAIL;
  case QueryRelType::VARIABLE_LENGTH_ACYCLIC:
  case QueryRelType::SHORTEST:
  case QueryRelType::ALL_SHORTEST:
  case QueryRelType::WEIGHTED_SHORTEST:
  case QueryRelType::ALL_WEIGHTED_SHORTEST:
    return PathSemantic::ACYCLIC;
  default:
    NEUG_UNREACHABLE;
  }
}

std::unique_ptr<function::RJAlgorithm> QueryRelTypeUtils::getFunction(
    QueryRelType type) {
  // NEUG_UNREACHABLE;
  return std::make_unique<function::DefaultRJAlgorithm>();
}

}  // namespace common
}  // namespace neug
