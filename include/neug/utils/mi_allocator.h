#pragma once

#include <memory>
#include <vector>

#include "parallel_hashmap/phmap.h"

#ifdef NEUG_WITH_MIMALLOC
#include <mimalloc.h>
#endif

namespace neug {

template <typename T>
#ifdef NEUG_WITH_MIMALLOC
using neug_allocator = mi_stl_allocator<T>;
#else
using neug_allocator = std::allocator<T>;
#endif

using sel_t = uint32_t;

template <typename T>
using vector_t = std::vector<T, neug_allocator<T>>;

using sel_vec_t = vector_t<sel_t>;

template <typename K, typename V,
          typename H = phmap::priv::hash_default_hash<K>,
          typename E = phmap::priv::hash_default_eq<K>>
using flat_hash_map_t =
    phmap::flat_hash_map<K, V, H, E, neug_allocator<std::pair<const K, V>>>;
template <typename T, typename H = phmap::priv::hash_default_hash<T>,
          typename E = phmap::priv::hash_default_eq<T>>
using flat_hash_set_t = phmap::flat_hash_set<T, H, E, neug_allocator<T>>;

using string_t =
    std::basic_string<char, std::char_traits<char>, neug_allocator<char>>;

}  // namespace neug
