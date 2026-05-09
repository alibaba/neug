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
#pragma once

#include <cstddef>
#include <cstdint>

#include <array>
#include <iterator>
#include <ostream>
#include <string>
#include <string_view>
#include <tuple>
#include <type_traits>
#include <unordered_map>
#include <vector>

#include "neug/utils/property/types.h"

namespace neug {

std::string to_lower_copy(const std::string& str);

std::vector<std::string> split_string_into_vec(const std::string& str,
                                               const std::string& delimiter);

template <typename T>
struct to_string_impl {
  static std::string to_string(const T& t) { return t.to_string(); }
};

template <typename T>
struct to_string_impl<std::vector<T>> {
  static inline std::string to_string(const std::vector<T>& vec) {
    std::ostringstream ss;
    //    ss << "Vec[";
    if (vec.size() > 0) {
      for (size_t i = 0; i < vec.size() - 1; ++i) {
        ss << to_string_impl<T>::to_string(vec[i]) << ",";
      }
      ss << to_string_impl<T>::to_string(vec[vec.size() - 1]);
    }
    //    ss << "]";
    return ss.str();
  }
};

template <typename K, typename V>
struct to_string_impl<std::unordered_map<K, V>> {
  static inline std::string to_string(const std::unordered_map<K, V>& vec) {
    std::ostringstream ss;
    // map{key:value, ...}
    ss << "map{";
    for (auto& [k, v] : vec) {
      ss << to_string_impl<K>::to_string(k) << ":" << to_string_impl<V>::to_string(v) << ",";
    }
    ss << "}";
    return ss.str();
  }
};

template <typename T, size_t N>
struct to_string_impl<std::array<T, N>> {
  static inline std::string to_string(const std::array<T, N>& empty) {
    std::stringstream ss;
    for (auto i : empty) {
      ss << i << ",";
    }
    return ss.str();
  }
};

template <typename T, size_t M, size_t N>
struct to_string_impl<std::array<std::array<T, N>, M>> {
  static inline std::string to_string(const std::array<std::array<T, N>, M>& empty) {
    std::stringstream ss;
    ss << "[";
    for (auto i : empty) {
      ss << to_string_impl<std::array<T, N>>::to_string(i) << ",";
    }
    ss << "]";
    return ss.str();
  }
};

template <>
struct to_string_impl<Date> {
  static inline std::string to_string(const Date& empty) { return empty.to_string(); }
};

template <>
struct to_string_impl<std::string_view> {
  static inline std::string to_string(const std::string_view& empty) { return std::string(empty); }
};

template <>
struct to_string_impl<EmptyType> {
  static inline std::string to_string(const EmptyType& empty) { return ""; }
};

template <>
struct to_string_impl<uint8_t> {
  static inline std::string to_string(const uint8_t& empty) {
    return std::to_string((int32_t) empty);
  }
};

template <>
struct to_string_impl<int64_t> {
  static inline std::string to_string(const int64_t& empty) { return std::to_string(empty); }
};

template <>
struct to_string_impl<bool> {
  static inline std::string to_string(const bool& empty) { return std::to_string(empty); }
};

template <>
struct to_string_impl<uint64_t> {
  static inline std::string to_string(const uint64_t& empty) { return std::to_string(empty); }
};

template <>
struct to_string_impl<int32_t> {
  static inline std::string to_string(const int32_t& empty) { return std::to_string(empty); }
};

template <>
struct to_string_impl<uint32_t> {
  static inline std::string to_string(const uint32_t& empty) { return std::to_string(empty); }
};

template <>
struct to_string_impl<double> {
  static inline std::string to_string(const double& empty) { return std::to_string(empty); }
};

template <>
struct to_string_impl<std::string> {
  static inline std::string to_string(const std::string& empty) { return empty; }
};

template <typename... Args>
struct to_string_impl<std::tuple<Args...>> {
  static inline std::string to_string(const std::tuple<Args...>& t) {
    std::string result;
    result += "tuple<";
    std::apply(
        [&result](const auto&... v) {
          ((result +=
            (to_string_impl<std::remove_const_t<std::remove_reference_t<decltype(v)>>>::to_string(
                v)) +
            ","),
           ...);
        },
        t);
    result += ">";
    return result;
  }
};

template <typename A, typename B>
struct to_string_impl<std::pair<A, B>> {
  static inline std::string to_string(const std::pair<A, B>& t) {
    std::stringstream ss;
    ss << "pair<" << to_string_impl<A>::to_string(t.first) << ","
       << to_string_impl<B>::to_string(t.second) << ">";
    return ss.str();
  }
};

template <typename T>
std::string to_string(const T& t) {
  return to_string_impl<T>::to_string(t);
}

}  // namespace neug
