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

#pragma once
#include <any>
#include <cassert>
#include <cstdint>
#include <cstring>
#include <deque>
#include <filesystem>
#include <fstream>
#include <limits>
#include <map>
#include <queue>
#include <random>
#include <string>
#include <vector>

#include <glog/logging.h>

namespace neug {
namespace pattern_matching {
namespace graphlib {

using std::deque;
using std::string;
using dict = std::map<std::string, std::any>;

/**
 * @brief String parsing with specified delimeter
 * @Source Folklore
 */
deque<string> parse(string line, const string& del);

}  // namespace graphlib
}  // namespace pattern_matching
}  // namespace neug

namespace std {
// from boost (functional/hash):
// see http://www.boost.org/doc/libs/1_35_0/doc/html/hash/combine.html template
template <class T>
inline void combine(std::size_t& seed, T const& v) {
  seed ^= hash<T>()(v) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
}

template <>
struct hash<std::pair<int, int>> {
  auto operator()(const std::pair<int, int>& x) const -> size_t {
    std::size_t seed = 17;
    combine(seed, x.first);
    combine(seed, x.second);
    return seed;
  }
};
}  // namespace std
