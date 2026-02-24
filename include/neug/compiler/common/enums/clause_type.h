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

namespace neug {
namespace common {

enum class ClauseType : uint8_t {
  // updating clause
  SET = 0,
  DELETE_ = 1,  // winnt.h defines DELETE as a macro, so we use DELETE_ instead
                // of DELETE.
  INSERT = 2,
  MERGE = 3,

  // reading clause
  MATCH = 10,
  UNWIND = 11,
  IN_QUERY_CALL = 12,
  TABLE_FUNCTION_CALL = 13,
  GDS_CALL = 14,
  LOAD_FROM = 15,
};

enum class MatchClauseType : uint8_t {
  MATCH = 0,
  OPTIONAL_MATCH = 1,
};

}  // namespace common
}  // namespace neug
