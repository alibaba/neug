/**
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * This file is originally from the FaSTest project
 * (https://github.com/SNUCSE-CTA/FaSTest) Licensed under the MIT License.
 * Modified by Yunkai Lou and Shunyang Li in 2025 to support Neug-specific
 * features.
 */

#include "Base/base.h"

namespace neug {
namespace pattern_matching {
namespace graphlib {

deque<string> parse(string line, const string& del) {
  deque<string> ret;

  size_t pos = 0;
  string token;
  while ((pos = line.find(del)) != string::npos) {
    token = line.substr(0, pos);
    ret.push_back(token);
    line.erase(0, pos + del.length());
  }
  ret.push_back(line);
  return ret;
}

}  // namespace graphlib
}  // namespace pattern_matching
}  // namespace neug
