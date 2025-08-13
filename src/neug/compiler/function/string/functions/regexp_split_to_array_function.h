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
#include "re2.h"

namespace gs {
namespace function {

struct RegexpSplitToArray : BaseRegexpOperation {
  static void operation(common::ku_string_t& value, common::ku_string_t& regex,
                        common::list_entry_t& result,
                        common::ValueVector& resultVector) {
    std::vector<std::string> matches =
        regexExtractAll(value.getAsString(), regex.getAsString());
    result = common::ListVector::addList(&resultVector, matches.size());
    auto resultValues =
        common::ListVector::getListValues(&resultVector, result);
    auto resultDataVector = common::ListVector::getDataVector(&resultVector);
    auto numBytesPerValue = resultDataVector->getNumBytesPerValue();
    common::ku_string_t kuString;
    for (const auto& match : matches) {
      copyToKuzuString(match, kuString, *resultDataVector);
      resultDataVector->copyFromVectorData(
          resultValues, resultDataVector,
          reinterpret_cast<uint8_t*>(&kuString));
      resultValues += numBytesPerValue;
    }
  }

  static std::vector<std::string> regexExtractAll(const std::string& value,
                                                  const std::string& pattern) {
    RE2 regex(parseCypherPattern(pattern));

    regex::StringPiece input(value);
    regex::StringPiece match;
    uint64_t startPos = 0;

    std::vector<std::string> splitParts;
    while (startPos < input.length()) {
      if (regex.Match(input, startPos, input.length(), RE2::UNANCHORED, &match,
                      1)) {
        uint64_t matchStart = match.data() - input.data();
        uint64_t matchEnd = matchStart + match.size();

        if (startPos < matchStart) {
          splitParts.emplace_back(
              value.substr(startPos, matchStart - startPos));
        }

        startPos = matchEnd;

        if (match.size() == 0) {
          // Match size is 0.
          startPos++;
        }
      } else {
        // No more regexp matches.
        if (startPos < input.length()) {
          splitParts.emplace_back(value.substr(startPos));
        }
        break;
      }
    }
    return splitParts;
  }
};

}  // namespace function
}  // namespace gs