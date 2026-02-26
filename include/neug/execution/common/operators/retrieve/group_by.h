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

#include "neug/execution/common/context.h"
#include "neug/execution/utils/pb_parse_utils.h"
#include "neug/utils/result.h"
#include "parallel_hashmap/phmap.h"

namespace neug {
namespace execution {

struct KeyBase {
  virtual ~KeyBase() = default;
  virtual std::pair<std::vector<size_t>, std::vector<std::vector<size_t>>>
  group(const Context& ctx) = 0;
  virtual const std::vector<std::pair<int, int>>& tag_alias() const = 0;
};
template <typename EXPR>
struct Key : public KeyBase {
  Key(EXPR&& expr, const std::vector<std::pair<int, int>>& tag_alias)
      : expr(std::move(expr)), tag_alias_(tag_alias) {}
  std::pair<std::vector<size_t>, std::vector<std::vector<size_t>>> group(
      const Context& ctx) override {
    size_t row_num = ctx.row_num();
    std::vector<std::vector<size_t>> groups;
    std::vector<size_t> offsets;
    phmap::flat_hash_map<typename EXPR::V, size_t> group_map;
    for (size_t i = 0; i < row_num; ++i) {
      auto val = expr(i);
      auto iter = group_map.find(val);
      if (iter == group_map.end()) {
        size_t idx = groups.size();
        group_map[val] = idx;
        groups.emplace_back();
        groups.back().push_back(i);
        offsets.push_back(i);
      } else {
        groups[iter->second].push_back(i);
      }
    }
    return std::make_pair(std::move(offsets), std::move(groups));
  }
  const std::vector<std::pair<int, int>>& tag_alias() const override {
    return tag_alias_;
  }
  EXPR expr;
  std::vector<std::pair<int, int>> tag_alias_;
};

struct ReducerBase {
  virtual ~ReducerBase() = default;
  virtual std::shared_ptr<IContextColumn> reduce(
      const std::vector<std::vector<size_t>>& groups) = 0;
};

struct ReduceOp {
  ReduceOp(std::unique_ptr<ReducerBase>&& reducer, int alias)
      : reducer_(std::move(reducer)), alias_(alias) {}

  void reduce(const Context& ctx, Context& ret,
              const std::vector<std::vector<size_t>>& groups) {
    auto col = reducer_->reduce(groups);
    ret.set(alias_, col);
  }

  std::unique_ptr<ReducerBase> reducer_;
  int alias_;
};

class GroupBy {
 public:
  static neug::result<Context> group_by(Context&& ctx,
                                        std::unique_ptr<KeyBase>&& key,
                                        std::vector<ReduceOp>&& aggrs);
};

}  // namespace execution

}  // namespace neug
