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

#include <algorithm>

#include "neug/execution/common/context.h"
#include "neug/storages/graph/graph_interface.h"
#include "neug/utils/result.h"

namespace neug {

namespace execution {

class OrderBy {
 public:
  template <typename Comparer>
  static void order_by_limit_impl(const StorageReadInterface& graph,
                                  const Context& ctx, const Comparer& cmp,
                                  size_t low, size_t high,
                                  std::vector<size_t>& offsets) {
    if (low == 0 && high >= ctx.row_num()) {
      offsets.resize(ctx.row_num());
      std::iota(offsets.begin(), offsets.end(), 0);
      std::sort(offsets.begin(), offsets.end(),
                [&](size_t lhs, size_t rhs) { return cmp(lhs, rhs); });
      return;
    }
    size_t row_num = ctx.row_num();
    std::priority_queue<size_t, std::vector<size_t>, Comparer> queue(cmp);
    for (size_t i = 0; i < row_num; ++i) {
      queue.push(i);
      if (queue.size() > high) {
        queue.pop();
      }
    }
    for (size_t k = 0; k < low; ++k) {
      queue.pop();
    }
    offsets.resize(queue.size());
    size_t idx = queue.size();

    while (!queue.empty()) {
      offsets[--idx] = queue.top();
      queue.pop();
    }
  }

  template <typename Comparer>
  static neug::result<Context> order_by_with_limit(
      const StorageReadInterface& graph, Context&& ctx, const Comparer& cmp,
      size_t low, size_t high) {
    std::vector<size_t> offsets;
    order_by_limit_impl(graph, ctx, cmp, low, high, offsets);
    ctx.reshuffle(offsets);
    return ctx;
  }

  template <typename Comparer>
  static neug::result<Context> staged_order_by_with_limit(
      const StorageReadInterface& graph, Context&& ctx, const Comparer& cmp,
      size_t low, size_t high, const std::vector<size_t>& indices) {
    std::priority_queue<size_t, std::vector<size_t>, Comparer> queue(cmp);
    for (auto i : indices) {
      queue.push(i);
      if (queue.size() > high) {
        queue.pop();
      }
    }
    std::vector<size_t> offsets;
    for (size_t k = 0; k < low; ++k) {
      queue.pop();
    }
    offsets.resize(queue.size());
    size_t idx = queue.size();

    while (!queue.empty()) {
      offsets[--idx] = queue.top();
      queue.pop();
    }

    ctx.reshuffle(offsets);
    return ctx;
  }
};
}  // namespace execution

}  // namespace neug
