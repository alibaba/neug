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

#include "neug/storages/index/art_tree/fixed_size_allocator.h"
#include "neug/storages/index/art_tree/fixed_size_buffer.h"

namespace neug::art {

class ArtTree;
class NodePtr;
enum class NType : uint8_t;

//! NodeHandle is a mutable wrapper to access and modify a node.
//! A segment handle locates the node within its buffer.
//! For read-only access, use ConstNodeHandle instead.
class NodeHandle {
 public:
  NodeHandle(ArtTree& art, const NodePtr node);
  NodeHandle(FixedSizeAllocator& allocator, const NodePtr node, NType type);

  NodeHandle(const NodeHandle&) = delete;
  NodeHandle& operator=(const NodeHandle&) = delete;
  NodeHandle(NodeHandle&& other) noexcept;
  NodeHandle& operator=(NodeHandle&& other) noexcept;

 public:
  template <class T>
  T& Get() {
    D_ASSERT(T::TYPE == type);
    handle.MarkModified();
    return handle.GetRef<T>();
  }

  data_ptr_t GetPtr() {
    handle.MarkModified();
    return handle.GetPtr();
  }

  NType GetType() const { return type; }

 private:
  SegmentHandle handle;
  NType type;
};

//! ConstNodeHandle is a read-only wrapper to access a node.
//! A segment handle locates the node within its buffer.
//! For mutable access, use NodeHandle instead.
class ConstNodeHandle {
 public:
  ConstNodeHandle(const ArtTree& art, const NodePtr node);

  ConstNodeHandle(const ConstNodeHandle&) = delete;
  ConstNodeHandle& operator=(const ConstNodeHandle&) = delete;
  ConstNodeHandle(ConstNodeHandle&&) = delete;
  ConstNodeHandle& operator=(ConstNodeHandle&&) = delete;

 public:
  template <class T>
  const T& Get() const {
    D_ASSERT(T::TYPE == type);
    return handle.GetRef<T>();
  }

  const_data_ptr_t GetPtr() const { return handle.GetPtr<const data_t>(); }

  NType GetType() const { return type; }

 private:
  SegmentHandle handle;
  NType type;
};

}  // namespace neug::art
