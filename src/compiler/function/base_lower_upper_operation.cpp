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

#include "neug/compiler/common/string_format.h"
#include "neug/compiler/function/string/functions/base_lower_upper_function.h"
#include "neug/utils/exception/exception.h"
#include "utf8proc.h"

using namespace neug::common;
using namespace neug::utf8proc;

namespace neug {
namespace function {

uint32_t BaseLowerUpperFunction::getResultLen(char* inputStr, uint32_t inputLen,
                                              bool isUpper) {
  uint32_t outputLength = 0;
  for (uint32_t i = 0; i < inputLen;) {
    // For UTF-8 characters, changing case can increase / decrease total byte
    // length. Eg.: 'ß' lower case -> 'SS' upper case [more bytes + more chars]
    if (inputStr[i] & 0x80) {
      int size = 0;
      int codepoint = utf8proc_codepoint(inputStr + i, size);
      if (codepoint < 0) {
        // LCOV_EXCL_START
        // TODO(Xiyang): We shouldn't allow invalid UTF-8 to enter a string
        // column.
        std::string funcName = isUpper ? "UPPER" : "LOWER";
        THROW_RUNTIME_ERROR(common::stringFormat(
            "Failed calling {}: Invalid UTF-8.", funcName));
        // LCOV_EXCL_STOP
      }
      int convertedCodepoint =
          isUpper ? utf8proc_toupper(codepoint) : utf8proc_tolower(codepoint);
      int newSize = utf8proc_codepoint_length(convertedCodepoint);
      NEUG_ASSERT(newSize >= 0);
      outputLength += newSize;
      i += size;
    } else {
      outputLength++;
      i++;
    }
  }
  return outputLength;
}

void BaseLowerUpperFunction::convertCharCase(char* result, const char* input,
                                             int32_t charPos, bool toUpper,
                                             int& originalSize, int& newSize) {
  originalSize = 1;
  newSize = 1;
  if (input[charPos] & 0x80) {
    auto codepoint = utf8proc_codepoint(input + charPos, originalSize);
    NEUG_ASSERT(codepoint >= 0);  // Validity ensured by getResultLen.
    int convertedCodepoint =
        toUpper ? utf8proc_toupper(codepoint) : utf8proc_tolower(codepoint);
    utf8proc_codepoint_to_utf8(convertedCodepoint, newSize, result);
  } else {
    *result = toUpper ? toupper(input[charPos]) : tolower(input[charPos]);
  }
}

void BaseLowerUpperFunction::convertCase(char* result, uint32_t len,
                                         char* input, bool toUpper) {
  int originalSize = 0;
  int newSize = 0;
  for (auto i = 0u; i < len;) {
    convertCharCase(result, input, i, toUpper, originalSize, newSize);
    i += originalSize;
    result += newSize;
  }
}

}  // namespace function
}  // namespace neug
