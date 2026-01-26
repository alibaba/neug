/** Copyright 2020 Alibaba Group Holding Limited.
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

#include "neug/utils/string_utils.h"

#include <cctype>

namespace neug {

std::string to_lower_copy(const std::string& str) {
  std::string lower_str = str;
  for (auto& c : lower_str) {
    c = static_cast<char>(std::tolower(c));
  }
  return lower_str;
}

std::vector<std::string> split_string_into_vec(const std::string& str,
                                               const std::string& delimiter) {
  std::vector<std::string> result;
  size_t pos = 0;
  size_t next_pos = 0;
  size_t delimiter_length = delimiter.length();
  while ((next_pos = str.find(delimiter, pos)) != std::string::npos) {
    result.push_back(str.substr(pos, next_pos - pos));
    pos = next_pos + delimiter_length;
  }
  result.push_back(str.substr(pos));  // Add the last segment
  return result;
}

}  // namespace neug
