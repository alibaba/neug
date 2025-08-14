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

#include "base_regexp_function.h"
#include "neug/compiler/common/vector/value_vector.h"
#include "neug/utils/exception/exception.h"
#include "re2/include/re2.h"

namespace gs {
namespace function {

struct RegexpExtractAll : BaseRegexpOperation {
  static inline void operation(common::ku_string_t& value,
                               common::ku_string_t& pattern,
                               std::int64_t& group,
                               common::list_entry_t& result,
                               common::ValueVector& resultVector) {
    std::vector<std::string> matches =
        regexExtractAll(value.getAsString(), pattern.getAsString(), group);
    result = common::ListVector::addList(&resultVector, matches.size());
    auto resultValues =
        common::ListVector::getListValues(&resultVector, result);
    auto resultDataVector = common::ListVector::getDataVector(&resultVector);
    auto numBytesPerValue = resultDataVector->getNumBytesPerValue();
    for (const auto& match : matches) {
      common::ku_string_t kuString;
      copyToKuzuString(match, kuString, *resultDataVector);
      resultDataVector->copyFromVectorData(
          resultValues, resultDataVector,
          reinterpret_cast<uint8_t*>(&kuString));
      resultValues += numBytesPerValue;
    }
  }

  static inline void operation(common::ku_string_t& value,
                               common::ku_string_t& pattern,
                               common::list_entry_t& result,
                               common::ValueVector& resultVector) {
    int64_t defaultGroup = 0;
    operation(value, pattern, defaultGroup, result, resultVector);
  }

  static std::vector<std::string> regexExtractAll(const std::string& value,
                                                  const std::string& pattern,
                                                  std::int64_t& group) {
    RE2 regex(parseCypherPattern(pattern));
    auto submatchCount = regex.NumberOfCapturingGroups() + 1;
    if (group >= submatchCount) {
      THROW_RUNTIME_ERROR("Regex match group index is out of range");
    }

    regex::StringPiece input(value);
    std::vector<regex::StringPiece> targetSubMatches;
    targetSubMatches.resize(submatchCount);
    uint64_t startPos = 0;

    std::vector<std::string> matches;
    while (regex.Match(input, startPos, input.length(), RE2::UNANCHORED,
                       targetSubMatches.data(), submatchCount)) {
      uint64_t consumed = static_cast<size_t>(targetSubMatches[0].end() -
                                              (input.begin() + startPos));
      if (!consumed) {
        // Empty match found, increment the position manually
        consumed++;
        while (startPos + consumed < input.length() &&
               !IsCharacter(input[startPos + consumed])) {
          consumed++;
        }
      }
      startPos += consumed;
      matches.emplace_back(targetSubMatches[group]);
    }

    return matches;
  }

  static inline bool IsCharacter(char c) {
    // Check if this character is not the middle of utf-8 character i.e. it
    // shouldn't begin with 10 XX XX XX
    return (c & 0xc0) != 0x80;
  }
};

}  // namespace function
}  // namespace gs
