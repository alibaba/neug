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

//! ConstPrefixHandle provides static methods for read-only prefix operations on
//! a ConstNodeHandle.
class ConstPrefixHandle {
 public:
  static constexpr NType PREFIX = NType::PREFIX;

  //! Get a const reference to the child slot of the prefix.
  static const NodePtr& ChildRef(const ArtTree& art,
                                 const ConstNodeHandle& handle) {
    return *reinterpret_cast<const NodePtr*>(handle.GetPtr() +
                                             art.PrefixCount() + 1);
  }

  //! Traverses and verifies the node and its subtree.
  static void Verify(ArtTree& art, const NodePtr& node);

  //! Returns the string representation of the node using ToStringOptions.
  static string ToString(ArtTree& art, const NodePtr& node,
                         const ToStringOptions& options);

 private:
  template <class F>
  static NodePtr Iterator(ArtTree& art, NodePtr node, const bool exit_gate,
                          F&& lambda) {
    while (node.HasMetadata() && node.GetType() == PREFIX) {
      ConstNodeHandle handle(art, node);
      auto data = handle.GetPtr();
      NodePtr child = ChildRef(art, handle);

      lambda(handle, data, child);

      node = child;
      if (exit_gate && node.GetGateStatus() == GateStatus::GATE_SET) {
        break;
      }
    }
    return node;
  }
};

}  // namespace neug::art
