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

#include "neug/execution/common/columns/i_context_column.h"
#include "neug/execution/common/context.h"
#include "neug/execution/common/operators/retrieve/order_by.h"
#include "neug/execution/common/params_map.h"
#include "neug/utils/top_n_generator.h"

namespace neug {

namespace execution {

struct ProjectExprBase {
  virtual ~ProjectExprBase() = default;
  virtual std::shared_ptr<IContextColumn> evaluate(const Context& ctx) = 0;
  virtual bool order_by_limit(const Context& ctx, bool asc, size_t limit,
                              std::vector<size_t>& offsets) const {
    return false;
  }
};

struct ProjectOp {
  std::unique_ptr<ProjectExprBase> expr_;
  std::unique_ptr<ProjectExprBase> fallback_expr_;
  int alias_;

  ProjectOp(std::unique_ptr<ProjectExprBase>&& expr,
            std::unique_ptr<ProjectExprBase>&& fallback_expr, int alias)
      : expr_(std::move(expr)),
        fallback_expr_(std::move(fallback_expr)),
        alias_(alias) {}
  void evaluate(const Context& ctx, Context& ret) const {
    std::shared_ptr<IContextColumn> col;
    if (expr_) {
      col = expr_->evaluate(ctx);
    }
    if (!col) {
      col = fallback_expr_->evaluate(ctx);
    }
    ret.set(alias_, col);
  }

  bool order_by_limit(const Context& ctx, bool asc, size_t limit,
                      std::vector<size_t>& offsets) const {
    if (expr_) {
      return expr_->order_by_limit(ctx, asc, limit, offsets);
    }
    return false;
  }

  int alias() const { return alias_; }
};

class Project {
 public:
  static neug::result<Context> project(Context&& ctx,
                                       const std::vector<ProjectOp>& exprs,
                                       bool is_append = false) {
    Context ret;
    if (is_append) {
      ret = ctx;
    }
    for (const auto& expr : exprs) {
      expr.evaluate(ctx, ret);
    }
    return ret;
  }

  template <typename Comparer>
  static neug::result<Context> project_order_by_fuse(
      const StorageReadInterface& graph, const ParamsMap& params, Context&& ctx,
      std::vector<ProjectOp>&& exprs,
      const std::function<Comparer(const Context&)>& cmp, size_t lower,
      size_t upper, const std::set<int>& order_index,
      std::pair<int32_t, bool> fst_idx) {
    lower = std::max(lower, static_cast<size_t>(0));
    upper = std::min(upper, ctx.row_num());

    Context ret;
    Context tmp;

    std::vector<int> alias;

    std::vector<size_t> indices;

    if (upper * 2 < ctx.row_num() && exprs[fst_idx.first].order_by_limit(
                                         ctx, fst_idx.second, upper, indices)) {
      ctx.reshuffle(indices);
      for (size_t i : order_index) {
        const auto& expr = exprs[i];
        int alias_ = expr.alias();
        expr.evaluate(ctx, tmp);
        alias.push_back(alias_);
      }
      auto cmp_ = cmp(tmp);
      std::vector<size_t> offsets;

      OrderBy::order_by_limit_impl(graph, tmp, cmp_, lower, upper, offsets);
      ctx.reshuffle(offsets);
      tmp.reshuffle(offsets);
      for (size_t i = 0; i < exprs.size(); ++i) {
        if (order_index.find(i) == order_index.end()) {
          exprs[i].evaluate(ctx, ret);
        }
      }
      for (size_t i = 0; i < tmp.col_num(); ++i) {
        if (tmp.get(i)) {
          ret.set(i, tmp.get(i));
        }
      }
    } else {
      for (size_t i = 0; i < exprs.size(); ++i) {
        auto& expr = exprs[i];
        int alias_ = expr.alias();
        expr.evaluate(ctx, ret);
        alias.push_back(alias_);
      }
      auto cmp_ = cmp(ret);
      std::vector<size_t> offsets;
      OrderBy::order_by_limit_impl(graph, ret, cmp_, lower, upper, offsets);

      ret.reshuffle(offsets);
    }

    return ret;
  }
};

}  // namespace execution

}  // namespace neug
