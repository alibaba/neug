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

#ifndef NEUG_EXTENSION_GDS_OPTION_UTILS_H_
#define NEUG_EXTENSION_GDS_OPTION_UTILS_H_

#include <google/protobuf/map.h>

#include <cstdint>
#include <stdexcept>
#include <string>
#include <type_traits>

namespace neug {
namespace gds {

template <typename>
struct dependent_false : std::false_type {};

template <typename T>
T get_option_value(
    const google::protobuf::Map<std::string, std::string>& options,
    const std::string& key, T default_value) {
  auto it = options.find(key);
  if (it == options.end()) {
    return default_value;
  }

  if constexpr (std::is_same_v<T, int32_t>) {
    try {
      return std::stoi(it->second);
    } catch (const std::exception&) {
      throw std::runtime_error("Invalid value for " + key + ": " + it->second);
    }
  } else if constexpr (std::is_same_v<T, int64_t>) {
    try {
      return std::stoll(it->second);
    } catch (const std::exception&) {
      throw std::runtime_error("Invalid value for " + key + ": " + it->second);
    }
  } else if constexpr (std::is_same_v<T, double>) {
    try {
      return std::stod(it->second);
    } catch (const std::exception&) {
      throw std::runtime_error("Invalid value for " + key + ": " + it->second);
    }
  } else if constexpr (std::is_same_v<T, std::string>) {
    std::string s = it->second;
    std::transform(s.begin(), s.end(), s.begin(), [](unsigned char c) {
      return static_cast<char>(std::tolower(c));
    });
    return s;
  } else {
    static_assert(dependent_false<T>::value, "Unsupported option value type");
  }
}

}  // namespace gds
}  // namespace neug

#endif  // NEUG_EXTENSION_GDS_OPTION_UTILS_H_