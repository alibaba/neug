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

#include <google/protobuf/map.h>

#include <algorithm>
#include <cctype>
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
  // Use manual iteration instead of options.find() to avoid
  // protobuf static library duplication causing hash table inconsistency
  const std::string* found_value = nullptr;
  for (const auto& kv : options) {
    if (kv.first == key) {
      found_value = &kv.second;
      break;
    }
  }
  if (found_value == nullptr) {
    return default_value;
  }

  if constexpr (std::is_same_v<T, int32_t>) {
    try {
      return std::stoi(*found_value);
    } catch (const std::exception&) {
      throw std::runtime_error("Invalid value for " + key + ": " + *found_value);
    }
  } else if constexpr (std::is_same_v<T, int64_t>) {
    try {
      return std::stoll(*found_value);
    } catch (const std::exception&) {
      throw std::runtime_error("Invalid value for " + key + ": " + *found_value);
    }
  } else if constexpr (std::is_same_v<T, double>) {
    try {
      return std::stod(*found_value);
    } catch (const std::exception&) {
      throw std::runtime_error("Invalid value for " + key + ": " + *found_value);
    }
  } else if constexpr (std::is_same_v<T, std::string>) {
    // Preserve original casing: string options are used for PK values (source)
    // and property names (weight), both of which are case-sensitive.
    // Bool options that need case-insensitive matching use the bool specialization.
    return *found_value;
  } else if constexpr (std::is_same_v<T, bool>) {
    std::string s = *found_value;
    std::transform(s.begin(), s.end(), s.begin(), [](unsigned char c) {
      return static_cast<char>(std::tolower(c));
    });
    return s == "true" || s == "1" || s == "yes";
  } else {
    static_assert(dependent_false<T>::value, "Unsupported option value type");
  }
}

}  // namespace gds
}  // namespace neug
