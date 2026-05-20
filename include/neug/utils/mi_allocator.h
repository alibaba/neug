#pragma once

#include <memory>
#include <vector>

#ifdef WITH_MIMALLOC
#include <mimalloc.h>
#endif

namespace neug {

template <typename T>
#ifdef WITH_MIMALLOC
using NeuGAllocator = mi_stl_allocator<T>;
#else
using NeuGAllocator = std::allocator<T>;
#endif

using sel_vec_t = std::vector<size_t, NeuGAllocator<size_t>>;

}  // namespace neug
