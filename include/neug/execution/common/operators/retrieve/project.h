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
#include "neug/utils/top_n_generator.h"

namespace gs {

namespace runtime {

struct ProjectExprBase {
  virtual ~ProjectExprBase() = default;
  virtual Context evaluate(const Context& ctx, Context&& ret) = 0;
  virtual int alias() const = 0;

  virtual bool order_by_limit(const Context& ctx, bool asc, size_t limit,
                              std::vector<size_t>& offsets) const {
    return false;
  }
};

struct DummyGetter : public ProjectExprBase {
  DummyGetter(int from, int to) : from_(from), to_(to) {}
  Context evaluate(const Context& ctx, Context&& ret) override {
    ret.set(to_, ctx.get(from_));
    return ret;
  }
  int alias() const override { return to_; }

  int from_;
  int to_;
};

template <typename EXPR, typename COLLECTOR_T>
struct ProjectExpr : public ProjectExprBase {
  EXPR expr_;
  COLLECTOR_T collector_;
  int alias_;

  ProjectExpr(EXPR&& expr, const COLLECTOR_T& collector, int alias)
      : expr_(std::move(expr)), collector_(collector), alias_(alias) {}

  Context evaluate(const Context& ctx, Context&& ret) override {
    size_t row_num = ctx.row_num();
    for (size_t i = 0; i < row_num; ++i) {
      collector_.collect(expr_, i);
    }
    ret.set(alias_, collector_.get());
    return ret;
  }

  bool order_by_limit(const Context& ctx, bool asc, size_t limit,
                      std::vector<size_t>& offsets) const override {
    size_t size = ctx.row_num();
    if (size == 0) {
      return false;
    }
    using T = typename EXPR::V;
    if constexpr (std::is_same_v<T, Date> || std::is_same_v<T, DateTime> ||
                  std::is_same_v<T, int32_t> || std::is_same_v<T, int64_t> ||
                  std::is_same_v<T, double> || std::is_same_v<T, std::string>) {
      if (asc) {
        TopNGenerator<T, TopNAscCmp<T>> generator(limit);
        for (size_t i = 0; i < size; ++i) {
          auto val = expr_(i);
          if constexpr (std::is_same_v<decltype(val), std::string_view>) {
            generator.push(std::string(val), i);
          } else {
            generator.push(val, i);
          }
        }
        generator.generate_indices(offsets);
      } else {
        TopNGenerator<T, TopNDescCmp<T>> generator(limit);
        for (size_t i = 0; i < size; ++i) {
          auto val = expr_(i);
          if constexpr (std::is_same_v<decltype(val), std::string_view>) {
            generator.push(std::string(val), i);
          } else {
            generator.push(val, i);
          }
        }
        generator.generate_indices(offsets);
      }
      return true;
    } else {
      return false;
    }
  }

  int alias() const override { return alias_; }
};

class Project {
 public:
  static gs::result<Context> project(
      Context&& ctx, const std::vector<std::unique_ptr<ProjectExprBase>>& exprs,
      bool is_append = false) {
    Context ret;
    if (is_append) {
      ret = ctx;
    }
    for (size_t i = 0; i < exprs.size(); ++i) {
      ret = exprs[i]->evaluate(ctx, std::move(ret));
    }
    return ret;
  }

  template <typename Comparer>
  static gs::result<Context> project_order_by_fuse(
      const StorageReadInterface& graph,
      const std::map<std::string, std::string>& params, Context&& ctx,
      const std::vector<std::function<std::unique_ptr<ProjectExprBase>(
          const StorageReadInterface& graph,
          const std::map<std::string, std::string>& params,
          const Context& ctx)>>& exprs,
      const std::function<Comparer(const Context&)>& cmp, size_t lower,
      size_t upper, const std::set<int>& order_index,
      const std::function<bool(const Context&, std::vector<size_t>&)>&
          index_generator) {
    lower = std::max(lower, static_cast<size_t>(0));
    upper = std::min(upper, ctx.row_num());

    Context ret;
    Context tmp;

    std::vector<int> alias;

    std::vector<size_t> indices;

    if (upper * 2 < ctx.row_num() && index_generator(ctx, indices)) {
      ctx.reshuffle(indices);
      for (size_t i : order_index) {
        auto expr = exprs[i](graph, params, ctx);
        int alias_ = expr->alias();
        tmp = expr->evaluate(ctx, std::move(tmp));
        alias.push_back(alias_);
      }
      auto cmp_ = cmp(tmp);
      std::vector<size_t> offsets;

      OrderBy::order_by_limit_impl(graph, tmp, cmp_, lower, upper, offsets);
      ctx.reshuffle(offsets);
      tmp.reshuffle(offsets);
      for (size_t i = 0; i < exprs.size(); ++i) {
        if (order_index.find(i) == order_index.end()) {
          ret = exprs[i](graph, params, ctx)->evaluate(ctx, std::move(ret));
        }
      }
      for (size_t i = 0; i < tmp.col_num(); ++i) {
        if (tmp.get(i)) {
          ret.set(i, tmp.get(i));
        }
      }
    } else {
      for (size_t i = 0; i < exprs.size(); ++i) {
        auto expr = exprs[i](graph, params, ctx);
        int alias_ = expr->alias();
        ret = expr->evaluate(ctx, std::move(ret));
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

}  // namespace runtime

}  // namespace gs
