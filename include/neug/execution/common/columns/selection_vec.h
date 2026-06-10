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

#include <vector>
#include "parallel_hashmap/phmap.h"

namespace neug {
namespace execution {
using sel_t = uint32_t;
using sel_vec_t = std::vector<sel_t>;

template <typename K, typename V,
          typename H = phmap::priv::hash_default_hash<K>,
          typename E = phmap::priv::hash_default_eq<K>,
          typename A = std::allocator<std::pair<const K, V>>>
using flat_hash_map = phmap::flat_hash_map<K, V, H, E, A>;
template <typename T, typename H = phmap::priv::hash_default_hash<T>,
          typename E = phmap::priv::hash_default_eq<T>,
          typename A = std::allocator<T>>
using flat_hash_set = phmap::flat_hash_set<T, H, E, A>;

}  // namespace execution
}  // namespace neug