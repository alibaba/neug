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

#include "neug/storages/index/art_tree/base_node.h"
#include "neug/storages/index/art_tree/const_prefix_handle.h"
#include "neug/storages/index/art_tree/node256.h"
#include "neug/storages/index/art_tree/node48.h"

namespace neug::art {

//! Reads child slots under the caller-held tree lock. The handler returns
//! the node to visit, or an empty OptionalNodePtr to stop that branch.
template <class NODE, class NODE_HANDLE, class ART_TYPE, class CHILD_HANDLER,
          class PUSH>
void ARTScanChildrenInternal(ART_TYPE& art, const NodePtr node,
                             CHILD_HANDLER&& child_handler, PUSH&& push) {
  NODE_HANDLE handle(art, node);
  auto& n = handle.template Get<NODE>();
  NODE::Iterator(n, [&](auto& child) {
    auto next = child_handler(child);
    if (next) {
      push(next.Get());
    }
  });
}

//! Dispatches on the node type and scans the children of current (see
//! ARTScanChildrenInternal).
template <class NODE_HANDLE, class PREFIX, class ART_TYPE, class CHILD_HANDLER,
          class PUSH>
void ARTScanChildren(ART_TYPE& art, const NodePtr current,
                     CHILD_HANDLER&& child_handler, PUSH&& push) {
  switch (current.GetType()) {
  case NType::LEAF_INLINED:
  case NType::NODE_7_LEAF:
  case NType::NODE_15_LEAF:
  case NType::NODE_256_LEAF:
    break;
  case NType::PREFIX: {
    NODE_HANDLE handle(art, current);
    auto& child = PREFIX::ChildRef(art, handle);
    auto next = child_handler(child);
    if (next) {
      push(next.Get());
    }
    break;
  }
  case NType::NODE_4:
    ARTScanChildrenInternal<Node4, NODE_HANDLE>(art, current, child_handler,
                                                push);
    break;
  case NType::NODE_16:
    ARTScanChildrenInternal<Node16, NODE_HANDLE>(art, current, child_handler,
                                                 push);
    break;
  case NType::NODE_48:
    ARTScanChildrenInternal<Node48, NODE_HANDLE>(art, current, child_handler,
                                                 push);
    break;
  case NType::NODE_256:
    ARTScanChildrenInternal<Node256, NODE_HANDLE>(art, current, child_handler,
                                                  push);
    break;
  default:
    throw InternalException("invalid node type for ARTScanChildren: %d",
                            current.GetType());
  }
}

//===--------------------------------------------------------------------===//
// ARTScanPreorder
//===--------------------------------------------------------------------===//

template <class NODE_HANDLE, class PREFIX, class ART_TYPE, class NODE_REF,
          class CHILD_HANDLER>
void ARTScanPreorderInternal(ART_TYPE& art, NODE_REF& root,
                             CHILD_HANDLER&& child_handler) {
  vector<NodePtr> stack;
  auto push = [&stack](const NodePtr node) { stack.push_back(node); };

  // The caller must keep the root slot valid while child_handler runs. This is
  // ensured by the caller-held tree lock, which protects all buffer lifetimes.
  auto next = child_handler(root);
  if (next) {
    push(next.Get());
  }

  while (!stack.empty()) {
    NodePtr current = stack.back();
    stack.pop_back();

    ARTScanChildren<NODE_HANDLE, PREFIX>(art, current, child_handler, push);
  }
}

//! Pre-order scanner over an immutable ArtTree (see ARTScanPreorder).
template <class CHILD_HANDLER>
void ARTConstScanPreorder(const ArtTree& art, const NodePtr& root,
                          CHILD_HANDLER&& child_handler) {
  ARTScanPreorderInternal<ConstNodeHandle, ConstPrefixHandle>(art, root,
                                                              child_handler);
}

}  // namespace neug::art
