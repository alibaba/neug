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

#include <regex>

#include "neug/compiler/common/vector/value_vector.h"

namespace gs {
namespace function {

struct BaseRegexpOperation {
  static inline std::string parseCypherPattern(const std::string& pattern) {
    // Cypher parses escape characters with 2 backslash eg. for expressing '.'
    // requires '\\.' Since Regular Expression requires only 1 backslash '\.' we
    // need to replace double slash with single
    return std::regex_replace(pattern, std::regex(R"(\\\\)"), "\\");
  }

  static inline void copyToKuzuString(const std::string& value,
                                      common::neug_string_t& kuString,
                                      common::ValueVector& valueVector) {
    common::StringVector::addString(&valueVector, kuString, value.data(),
                                    value.length());
  }
};

}  // namespace function
}  // namespace gs
