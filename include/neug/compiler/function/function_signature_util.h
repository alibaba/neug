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

#pragma once

#include <string>
#include "neug/compiler/common/types/types.h"

namespace neug {
namespace function {
class FunctionSignatureUtil {
 public:
  static std::string getSignatureName(
      const std::string& funcName,
      const std::vector<neug::common::LogicalTypeID>& params) {
    std::string sig = funcName + "(";
    for (size_t i = 0; i < params.size(); ++i) {
      if (i)
        sig += ",";
      sig += common::LogicalTypeUtils::toString(params[i]);
    }
    sig += ")";
    return sig;
  }

  static std::string getFunctionName(const std::string& signatureName) {
    // Find the position of the first '('
    size_t pos = signatureName.find('(');
    if (pos == std::string::npos) {
      // No '(' found, return the whole string as function name
      return signatureName;
    }
    return signatureName.substr(0, pos);
  }
};
}  // namespace function
}  // namespace neug