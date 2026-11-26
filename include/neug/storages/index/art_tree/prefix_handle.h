/** Copyright 2020 Alibaba Group Holding Limited.
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

/**
 * This file is based on the DuckDB project
 * (https://github.com/duckdb/duckdb) Licensed under the MIT License.
 */

#pragma once

#include "neug/storages/index/art_tree/art.h"
#include "neug/storages/index/art_tree/fixed_size_allocator.h"
#include "neug/storages/index/art_tree/node.h"

namespace neug::art {

//! PrefixHandle provides static methods for mutable prefix operations on a
//! NodeHandle.
class PrefixHandle {
 public:
  static constexpr NType PREFIX = NType::PREFIX;

 public:
  //! Get a mutable reference to the child slot using an explicit prefix byte
  //! count.
  static NodePtr& ChildRefWithCount(const data_ptr_t data, const idx_t count) {
    return *reinterpret_cast<NodePtr*>(data + count + 1);
  }
};

}  // namespace neug::art
