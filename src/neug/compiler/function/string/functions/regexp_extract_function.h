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

#include "neug/compiler/common/types/ku_string.h"
#include "neug/compiler/common/vector/value_vector.h"
#include "neug/compiler/function/string/functions/base_regexp_function.h"
#include "neug/utils/exception/exception.h"
#include "re2/include/re2.h"

namespace gs {
namespace function {

struct RegexpExtract : BaseRegexpOperation {
  static inline void operation(common::ku_string_t& value,
                               common::ku_string_t& pattern,
                               std::int64_t& group, common::ku_string_t& result,
                               common::ValueVector& resultValueVector) {
    regexExtract(value.getAsString(), pattern.getAsString(), group, result,
                 resultValueVector);
  }

  static inline void operation(common::ku_string_t& value,
                               common::ku_string_t& pattern,
                               common::ku_string_t& result,
                               common::ValueVector& resultValueVector) {
    int64_t defaultGroup = 0;
    regexExtract(value.getAsString(), pattern.getAsString(), defaultGroup,
                 result, resultValueVector);
  }

  static void regexExtract(const std::string& input, const std::string& pattern,
                           std::int64_t& group, common::ku_string_t& result,
                           common::ValueVector& resultValueVector) {
    RE2 regex(parseCypherPattern(pattern));
    auto submatchCount = regex.NumberOfCapturingGroups() + 1;
    if (group >= submatchCount) {
      THROW_RUNTIME_ERROR("Regex match group index is out of range");
    }

    std::vector<regex::StringPiece> targetSubMatches;
    targetSubMatches.resize(submatchCount);

    if (!regex.Match(regex::StringPiece(input), 0, input.length(),
                     RE2::UNANCHORED, targetSubMatches.data(), submatchCount)) {
      return;
    }

    copyToKuzuString(targetSubMatches[group].ToString(), result,
                     resultValueVector);
  }
};

}  // namespace function
}  // namespace gs
