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

#include "neug/storages/index/art_tree/node_handle.h"

#include "neug/storages/index/art_tree/node.h"

namespace neug::art {

NodeHandle::NodeHandle(ArtTree& art, const NodePtr node)
    : handle(NodePtr::GetAllocator(art, node.GetType()).GetHandle(node)),
      type(node.GetType()) {}

NodeHandle::NodeHandle(FixedSizeAllocator& allocator, const NodePtr node,
                       NType type)
    : handle(allocator.GetHandle(node)), type(type) {
  D_ASSERT(node.GetType() == type);
}

NodeHandle::NodeHandle(NodeHandle&& other) noexcept
    : handle(std::move(other.handle)), type(other.type) {}

NodeHandle& NodeHandle::operator=(NodeHandle&& other) noexcept {
  if (this != &other) {
    handle = std::move(other.handle);
    type = other.type;
  }
  return *this;
}

ConstNodeHandle::ConstNodeHandle(const ArtTree& art, const NodePtr node)
    : handle(NodePtr::GetAllocator(art, node.GetType()).GetHandle(node)),
      type(node.GetType()) {}

}  // namespace neug::art
