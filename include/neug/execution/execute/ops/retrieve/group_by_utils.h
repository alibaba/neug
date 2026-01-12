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
#include <map>
#include <memory>
#include <set>
#include <string>
#include <tuple>
#include <unordered_set>
#include <utility>
#include <vector>

#include "neug/execution/common/columns/i_context_column.h"
#include "neug/execution/common/columns/value_columns.h"
#include "neug/execution/common/columns/vertex_columns.h"
#include "neug/execution/common/context.h"
#include "neug/execution/common/operators/retrieve/group_by.h"
#include "neug/execution/common/operators/retrieve/project.h"
#include "neug/execution/utils/var.h"
#include "neug/generated/proto/plan/physical.pb.h"
#include "neug/storages/graph/graph_interface.h"

namespace gs {
namespace runtime {
namespace ops {

AggrKind parse_aggregate(physical::GroupBy_AggFunc::Aggregate v);
template <typename T>
struct TypedKeyCollector {
  struct TypedKeyWrapper {
    using V = T;
    explicit TypedKeyWrapper(Var&& expr) : expr(std::move(expr)) {}
    V operator()(size_t idx) const {
      return TypedConverter<T>::to_typed(expr.get(idx));
    }
    Var expr;
  };

  TypedKeyCollector() {}
  void init(size_t size) { builder.reserve(size); }
  void collect(const TypedKeyWrapper& expr, size_t idx) {
    builder.push_back_opt(expr(idx));
  }
  auto get() { return builder.finish(); }

  ValueColumnBuilder<T> builder;
};

struct SLVertexWrapper {
  using V = vid_t;
  explicit SLVertexWrapper(const SLVertexColumn& column) : column(column) {}
  V operator()(size_t idx) const { return column.vertices()[idx]; }
  const SLVertexColumn& column;
};

struct SLVertexWrapperBeta {
  using V = VertexRecord;
  explicit SLVertexWrapperBeta(const SLVertexColumn& column) : column(column) {}
  V operator()(size_t idx) const { return column.get_vertex(idx); }
  const SLVertexColumn& column;
};

template <typename VERTEX_COL>
struct MLVertexWrapper {
  using V = VertexRecord;
  explicit MLVertexWrapper(const VERTEX_COL& vertex) : vertex(vertex) {}
  V operator()(size_t idx) const { return vertex.get_vertex(idx); }
  const VERTEX_COL& vertex;
};
template <typename T>
struct ValueWrapper {
  using V = T;
  explicit ValueWrapper(const ValueColumn<T>& column) : column(column) {}
  V operator()(size_t idx) const { return column.get_value(idx); }
  const ValueColumn<T>& column;
};

struct ColumnWrapper {
  using V = RTAny;
  explicit ColumnWrapper(const IContextColumn& column) : column(column) {}
  V operator()(size_t idx) const { return column.get_elem(idx); }
  const IContextColumn& column;
};

template <typename... EXPR>
struct KeyExpr {
  std::tuple<EXPR...> exprs;
  explicit KeyExpr(std::tuple<EXPR...>&& exprs) : exprs(std::move(exprs)) {}
  using V = std::tuple<typename EXPR::V...>;
  V operator()(size_t idx) const {
    return std::apply(
        [idx](auto&&... expr) { return std::make_tuple(expr(idx)...); }, exprs);
  }
};

template <size_t I, typename... EXPR>
struct _KeyBuilder {
  static std::unique_ptr<KeyBase> make_sp_key(
      const Context& ctx, const std::vector<std::pair<int, int>>& tag_alias,
      std::tuple<EXPR...>&& exprs) {
    if constexpr (I == 0) {
      KeyExpr<EXPR...> key(std::move(exprs));
      return std::make_unique<Key<decltype(key)>>(std::move(key), tag_alias);
    } else {
      auto [head, tail] = tag_alias[I - 1];
      auto col = ctx.get(head);
      if (col->is_optional()) {
        return nullptr;
      }
      if (col->column_type() == ContextColumnType::kVertex) {
        auto vertex_col = std::dynamic_pointer_cast<IVertexColumn>(col);
        if (vertex_col->vertex_column_type() == VertexColumnType::kSingle) {
          SLVertexWrapper wrapper(
              *dynamic_cast<const SLVertexColumn*>(vertex_col.get()));
          auto new_exprs = std::tuple_cat(std::make_tuple(std::move(wrapper)),
                                          std::move(exprs));
          return _KeyBuilder<I - 1, SLVertexWrapper, EXPR...>::make_sp_key(
              ctx, tag_alias, std::move(new_exprs));
        }

      } else if (col->column_type() == ContextColumnType::kValue) {
        if (col->elem_type().id() == DataTypeId::BIGINT) {
          ValueWrapper<int64_t> wrapper(
              *dynamic_cast<const ValueColumn<int64_t>*>(col.get()));
          auto new_exprs = std::tuple_cat(std::make_tuple(std::move(wrapper)),
                                          std::move(exprs));
          return _KeyBuilder<I - 1, ValueWrapper<int64_t>,
                             EXPR...>::make_sp_key(ctx, tag_alias,
                                                   std::move(new_exprs));
        } else if (col->elem_type().id() == DataTypeId::INTEGER) {
          ValueWrapper<int32_t> wrapper(
              *dynamic_cast<const ValueColumn<int32_t>*>(col.get()));
          auto new_exprs = std::tuple_cat(std::make_tuple(std::move(wrapper)),
                                          std::move(exprs));
          return _KeyBuilder<I - 1, ValueWrapper<int32_t>,
                             EXPR...>::make_sp_key(ctx, tag_alias,
                                                   std::move(new_exprs));
        } else {
          return nullptr;
        }
      }
    }
    return nullptr;
  }
};
template <size_t I>
struct KeyBuilder {
  static std::unique_ptr<KeyBase> make_sp_key(
      const Context& ctx, const std::vector<std::pair<int, int>>& tag_alias) {
    if (I != tag_alias.size()) {
      return nullptr;
    }
    auto col = ctx.get(tag_alias[I - 1].first);
    if (col->column_type() == ContextColumnType::kVertex) {
      auto vertex_col = std::dynamic_pointer_cast<IVertexColumn>(col);
      if (vertex_col->vertex_column_type() == VertexColumnType::kSingle) {
        SLVertexWrapper wrapper(
            *dynamic_cast<const SLVertexColumn*>(vertex_col.get()));
        auto new_exprs = std::make_tuple<SLVertexWrapper>(std::move(wrapper));
        return _KeyBuilder<I - 1, SLVertexWrapper>::make_sp_key(
            ctx, tag_alias, std::move(new_exprs));
      }
    } else if (col->column_type() == ContextColumnType::kValue) {
      if (col->elem_type().id() == DataTypeId::BIGINT) {
        ValueWrapper<int64_t> wrapper(
            *dynamic_cast<const ValueColumn<int64_t>*>(col.get()));
        auto new_exprs =
            std::make_tuple<ValueWrapper<int64_t>>(std::move(wrapper));
        return _KeyBuilder<I - 1, ValueWrapper<int64_t>>::make_sp_key(
            ctx, tag_alias, std::move(new_exprs));
      } else if (col->elem_type().id() == DataTypeId::INTEGER) {
        ValueWrapper<int32_t> wrapper(
            *dynamic_cast<const ValueColumn<int32_t>*>(col.get()));
        auto new_exprs =
            std::make_tuple<ValueWrapper<int32_t>>(std::move(wrapper));
        return _KeyBuilder<I - 1, ValueWrapper<int32_t>>::make_sp_key(
            ctx, tag_alias, std::move(new_exprs));
      } else {
        return nullptr;
      }
    }
    return nullptr;
  }
};

struct OptionalVarWrapper {
  using V = RTAny;
  std::optional<RTAny> operator()(size_t idx) const {
    auto v = vars.get(idx);
    if (v.is_null()) {
      return std::nullopt;
    } else {
      return v;
    }
  }
  explicit OptionalVarWrapper(Var&& vars) : vars(std::move(vars)) {}
  Var vars;
};
struct VarWrapper {
  using V = RTAny;
  RTAny operator()(size_t idx) const { return vars.get(idx); }
  explicit VarWrapper(Var&& vars) : vars(std::move(vars)) {}
  Var vars;
};

struct VarPairWrapper {
  using V = std::pair<RTAny, RTAny>;
  std::pair<RTAny, RTAny> operator()(size_t idx) const {
    return std::make_pair(fst.get(idx), snd.get(idx));
  }
  VarPairWrapper(Var&& fst, Var&& snd)
      : fst(std::move(fst)), snd(std::move(snd)) {}
  Var fst;
  Var snd;
};

template <typename T>
struct TypedVarWrapper {
  using V = T;
  T operator()(size_t idx) const {
    auto v = vars.get(idx);
    return TypedConverter<T>::to_typed(v);
  }
  explicit TypedVarWrapper(Var&& vars) : vars(std::move(vars)) {}
  Var vars;
};

template <typename T>
struct OptionalTypedVarWrapper {
  using V = T;
  std::optional<T> operator()(size_t idx) const {
    auto v = vars_.get(idx);
    if (v.is_null()) {
      return std::nullopt;
    }
    return TypedConverter<T>::to_typed(v);
  }
  explicit OptionalTypedVarWrapper(Var&& vars) : vars_(std::move(vars)) {}
  Var vars_;
};

template <typename EXPR, bool IS_OPTIONAL, typename Enable = void>
struct SumReducer;

template <typename EXPR, bool IS_OPTIONAL>
struct SumReducer<
    EXPR, IS_OPTIONAL,
    std::enable_if_t<std::is_arithmetic<typename EXPR::V>::value>> {
  EXPR expr;
  using V = typename EXPR::V;
  explicit SumReducer(EXPR&& expr) : expr(std::move(expr)) {}
  bool operator()(const std::vector<size_t>& group, V& sum) const {
    if constexpr (!IS_OPTIONAL) {
      sum = expr(group[0]);
      for (size_t i = 1; i < group.size(); ++i) {
        sum += expr(group[i]);
      }
      return true;
    } else {
      for (size_t i = 0; i < group.size(); ++i) {
        auto v = expr(group[i]);
        if (v.has_value()) {
          sum = v.value();
          i++;
          for (; i < group.size(); ++i) {
            auto v = expr(group[i]);
            if (v.has_value()) {
              sum += v.value();
            }
          }
          return true;
        }
      }
      return false;
    }
  }
};

template <typename T, typename = void>
struct is_hashable : std::false_type {};

template <typename T>
struct is_hashable<
    T, std::void_t<decltype(std::declval<std::hash<T>>()(std::declval<T>()))>>
    : std::true_type {};

template <typename EXPR, bool IS_OPTIONAL>
struct CountDistinctReducer {
  EXPR expr;
  using V = int64_t;
  using T = typename EXPR::V;

  explicit CountDistinctReducer(EXPR&& expr) : expr(std::move(expr)) {}
  bool operator()(const std::vector<size_t>& group, V& val) const {
    if constexpr (is_hashable<T>::value) {
      if constexpr (!IS_OPTIONAL) {
        std::unordered_set<T> set;
        for (auto idx : group) {
          set.insert(expr(idx));
        }
        val = set.size();
        return true;
      } else {
        std::unordered_set<T> set;
        for (auto idx : group) {
          auto v = expr(idx);
          if (v.has_value()) {
            set.insert(v.value());
          }
        }
        val = set.size();
        return true;
      }
    } else {
      if constexpr (!IS_OPTIONAL) {
        std::set<T> set;
        for (auto idx : group) {
          set.insert(expr(idx));
        }
        val = set.size();
        return true;
      } else {
        std::set<T> set;
        for (auto idx : group) {
          auto v = expr(idx);
          if (v.has_value()) {
            set.insert(v.value());
          }
        }
        val = set.size();
        return true;
      }
    }
  }
};

template <typename EXPR, bool IS_OPTIONAL>
struct CountReducer {
  EXPR expr;
  using V = int64_t;

  explicit CountReducer(EXPR&& expr) : expr(std::move(expr)) {}
  bool operator()(const std::vector<size_t>& group, V& val) const {
    if constexpr (!IS_OPTIONAL) {
      val = group.size();
      return true;
    } else {
      val = 0;
      for (auto idx : group) {
        if (expr(idx).has_value()) {
          val += 1;
        }
      }
      return true;
    }
  }
};

// To deal with special case count(*)
struct CountStartReducer {
  CountStartReducer() {}
  using V = int64_t;
  bool operator()(const std::vector<size_t>& group, V& val) const {
    // We will count all rows in the group
    val = group.size();
    return true;
  }
};

template <typename EXPR, bool is_optional>
struct IsCountReducer<CountReducer<EXPR, is_optional>> {
  static constexpr bool value = true;
};

template <>
struct IsCountReducer<CountStartReducer> {
  static constexpr bool value = true;
};

template <typename EXPR, bool IS_OPTIONAL>
struct MinReducer {
  EXPR expr;

  using V = typename EXPR::V;
  explicit MinReducer(EXPR&& expr) : expr(std::move(expr)) {}
  bool operator()(const std::vector<size_t>& group, V& val) const {
    if constexpr (!IS_OPTIONAL) {
      val = expr(group[0]);
      for (size_t i = 1; i < group.size(); ++i) {
        val = std::min(val, expr(group[i]));
      }
      return true;
    } else {
      for (size_t i = 0; i < group.size(); ++i) {
        auto v = expr(group[i]);
        if (v.has_value()) {
          val = v.value();
          ++i;
          for (; i < group.size(); ++i) {
            auto v = expr(group[i]);
            if (v.has_value()) {
              val = std::min(val, v.value());
            }
          }
          return true;
        }
      }
      return false;
    }
  }
};

template <typename EXPR, bool IS_OPTIONAL>
struct MaxReducer {
  EXPR expr;

  using V = typename EXPR::V;
  explicit MaxReducer(EXPR&& expr) : expr(std::move(expr)) {}
  bool operator()(const std::vector<size_t>& group, V& val) const {
    if constexpr (!IS_OPTIONAL) {
      val = expr(group[0]);
      for (size_t i = 1; i < group.size(); ++i) {
        val = std::max(val, expr(group[i]));
      }
      return true;
    } else {
      for (size_t i = 0; i < group.size(); ++i) {
        auto v = expr(group[i]);
        if (v.has_value()) {
          val = v.value();
          ++i;
          for (; i < group.size(); ++i) {
            auto v = expr(group[i]);
            if (v.has_value()) {
              val = std::max(val, v.value());
            }
          }
          return true;
        }
      }
      return false;
    }
  }
};

template <typename EXPR, bool IS_OPTIONAL>
struct FirstReducer {
  EXPR expr;
  using V = typename EXPR::V;
  explicit FirstReducer(EXPR&& expr) : expr(std::move(expr)) {}
  bool operator()(const std::vector<size_t>& group, V& val) const {
    if constexpr (!IS_OPTIONAL) {
      val = expr(group[0]);
      return true;
    } else {
      for (auto idx : group) {
        auto v = expr(idx);
        if (v.has_value()) {
          val = v.value();
          return true;
        }
      }
      return false;
    }
  }
};

template <typename EXPR, bool IS_OPTIONAL>
struct ToSetReducer {
  EXPR expr;
  using V = std::set<typename EXPR::V>;
  explicit ToSetReducer(EXPR&& expr) : expr(std::move(expr)) {}
  bool operator()(const std::vector<size_t>& group, V& val) const {
    val.clear();
    if constexpr (!IS_OPTIONAL) {
      for (auto idx : group) {
        val.insert(expr(idx));
      }
      return true;
    } else {
      for (auto idx : group) {
        auto v = expr(idx);
        if (v.has_value()) {
          val.insert(v.value());
        }
      }
      return true;
    }
  }
};

template <typename EXPR, bool IS_OPTIONAL>
struct ToListReducer {
  EXPR expr;

  using V = std::vector<typename EXPR::V>;
  explicit ToListReducer(EXPR&& expr) : expr(std::move(expr)) {}
  bool operator()(const std::vector<size_t>& group, V& list) const {
    list.clear();
    if constexpr (!IS_OPTIONAL) {
      for (auto idx : group) {
        list.push_back(expr(idx));
      }
      return true;
    } else {
      for (auto idx : group) {
        auto v = expr(idx);
        if (v.has_value()) {
          list.push_back(v.value());
        }
      }
      return true;
    }
  }
};

template <typename EXPR, bool IS_OPTIONAL, typename Enable = void>
struct AvgReducer;

template <typename EXPR, bool IS_OPTIONAL>
struct AvgReducer<
    EXPR, IS_OPTIONAL,
    std::enable_if_t<std::is_arithmetic<typename EXPR::V>::value>> {
  EXPR expr;
  using V = double;
  explicit AvgReducer(EXPR&& expr) : expr(std::move(expr)) {}

  bool operator()(const std::vector<size_t>& group, double& avg) const {
    // The result of avg is always a double, even if the input is integer.
    if constexpr (!IS_OPTIONAL) {
      avg = 0.0;
      for (auto idx : group) {
        avg += expr(idx);
      }
      avg = avg / group.size();
      return true;
    } else {
      avg = 0.0;
      size_t count = 0;
      for (auto idx : group) {
        auto v = expr(idx);
        if (v.has_value()) {
          avg += v.value();
          count += 1;
        }
      }
      if (count == 0) {
        return false;
      }
      avg = avg / count;
      return true;
    }
  }
};

template <typename T>
struct SetCollector {
  SetCollector()
      : arena(std::make_shared<Arena>()),
        builder(std::make_shared<ListValueColumnBuilder>(
            TypedConverter<T>::type())) {}
  void init(size_t size) { builder->reserve(size); }

  void collect(std::set<T>&& val) {
    std::vector<T> vec_val;
    vec_val.reserve(val.size());
    for (auto& v : val) {
      vec_val.push_back(std::move(v));
    }
    auto impl = ListImpl<T>::make_list_impl(std::move(vec_val));
    List list(impl.get());
    arena->emplace_back(std::move(impl));
    builder->push_back_opt(list);
  }
  auto get() {
    builder->set_arena(arena);
    return builder->finish();
  }
  std::shared_ptr<Arena> arena;
  std::shared_ptr<ListValueColumnBuilder> builder;
};

template <typename T>
struct SingleValueCollector {
  SingleValueCollector() = default;
  void init(size_t size) { builder.reserve(size); }

  void collect(T&& val) { builder.push_back_opt(std::move(val)); }
  auto get() { return builder.finish(); }
  ValueColumnBuilder<T> builder;
};

struct VertexCollector {
  VertexCollector() : builder() {}
  void init(size_t size) { builder.reserve(size); }
  void collect(VertexRecord&& val) { builder.push_back_vertex(std::move(val)); }
  auto get() { return builder.finish(); }
  MLVertexColumnBuilder builder;
};

template <typename T>
struct ListCollector {
  ListCollector()
      : arena(std::make_shared<Arena>()),
        builder(std::make_shared<ListValueColumnBuilder>(
            TypedConverter<T>::type())) {}
  void init(size_t size) { builder->reserve(size); }
  void collect(std::vector<T>&& val) {
    auto impl = ListImpl<T>::make_list_impl(std::move(val));
    List list(impl.get());
    arena->emplace_back(std::move(impl));
    builder->push_back_opt(list);
  }

  auto get() {
    builder->set_arena(arena);
    return builder->finish();
  }

  std::shared_ptr<Arena> arena;

  std::shared_ptr<ListValueColumnBuilder> builder;
};

template <typename EXPR, bool IS_OPTIONAL>
std::unique_ptr<ReducerBase> _make_reducer(const Context& ctx, EXPR&& expr,
                                           AggrKind kind, int alias) {
  switch (kind) {
  case AggrKind::kSum: {
    if constexpr (std::is_arithmetic<typename EXPR::V>::value) {
      SumReducer<EXPR, IS_OPTIONAL> r(std::move(expr));
      SingleValueCollector<typename EXPR::V> collector;
      return std::make_unique<Reducer<decltype(r), decltype(collector)>>(
          std::move(r), std::move(collector), alias);
    } else {
      LOG(FATAL) << "unsupport" << static_cast<int>(kind);
      return nullptr;
    }
  }
  case AggrKind::kCountDistinct: {
    CountDistinctReducer<EXPR, IS_OPTIONAL> r(std::move(expr));
    SingleValueCollector<int64_t> collector;
    return std::make_unique<Reducer<decltype(r), decltype(collector)>>(
        std::move(r), std::move(collector), alias);
  }
  case AggrKind::kCount: {
    CountReducer<EXPR, IS_OPTIONAL> r(std::move(expr));
    SingleValueCollector<int64_t> collector;
    return std::make_unique<Reducer<decltype(r), decltype(collector)>>(
        std::move(r), std::move(collector), alias);
  }
  case AggrKind::kMin: {
    if constexpr (std::is_same<typename EXPR::V, VertexRecord>::value) {
      LOG(FATAL) << "Min not support VertexRecord";
    } else {
      MinReducer<EXPR, IS_OPTIONAL> r(std::move(expr));
      SingleValueCollector<typename EXPR::V> collector;
      return std::make_unique<Reducer<decltype(r), decltype(collector)>>(
          std::move(r), std::move(collector), alias);
    }
  }
  case AggrKind::kMax: {
    if constexpr (std::is_same<typename EXPR::V, VertexRecord>::value) {
      LOG(FATAL) << "Max not support VertexRecord";
    } else {
      MaxReducer<EXPR, IS_OPTIONAL> r(std::move(expr));
      SingleValueCollector<typename EXPR::V> collector;
      return std::make_unique<Reducer<decltype(r), decltype(collector)>>(
          std::move(r), std::move(collector), alias);
    }
  }
  case AggrKind::kFirst: {
    FirstReducer<EXPR, IS_OPTIONAL> r(std::move(expr));
    if constexpr (std::is_same<typename EXPR::V, VertexRecord>::value) {
      VertexCollector collector;
      return std::make_unique<Reducer<decltype(r), decltype(collector)>>(
          std::move(r), std::move(collector), alias);
    } else {
      SingleValueCollector<typename EXPR::V> collector;
      return std::make_unique<Reducer<decltype(r), decltype(collector)>>(
          std::move(r), std::move(collector), alias);
    }
  }
  case AggrKind::kToSet: {
    ToSetReducer<EXPR, IS_OPTIONAL> r(std::move(expr));
    SetCollector<typename EXPR::V> collector;
    return std::make_unique<Reducer<decltype(r), decltype(collector)>>(
        std::move(r), std::move(collector), alias);
  }
  case AggrKind::kToList: {
    ToListReducer<EXPR, IS_OPTIONAL> r(std::move(expr));
    ListCollector<typename EXPR::V> collector;
    return std::make_unique<Reducer<decltype(r), decltype(collector)>>(
        std::move(r), std::move(collector), alias);
  }
  case AggrKind::kAvg: {
    if constexpr (std::is_arithmetic<typename EXPR::V>::value) {
      AvgReducer<EXPR, IS_OPTIONAL> r(std::move(expr));
      SingleValueCollector<typename AvgReducer<EXPR, IS_OPTIONAL>::V> collector;
      return std::make_unique<Reducer<decltype(r), decltype(collector)>>(
          std::move(r), std::move(collector), alias);
    } else {
      LOG(FATAL) << "unsupport" << static_cast<int>(kind);
      return nullptr;
    }
  }
  default:
    LOG(FATAL) << "unsupport" << static_cast<int>(kind);
    return nullptr;
  }
}

template <typename T>
std::unique_ptr<ReducerBase> make_reducer(const Context& ctx, Var&& var,
                                          AggrKind kind, int alias) {
  if (var.is_optional()) {
    OptionalTypedVarWrapper<T> wrapper(std::move(var));
    return _make_reducer<decltype(wrapper), true>(ctx, std::move(wrapper), kind,
                                                  alias);
  } else {
    TypedVarWrapper<T> wrapper(std::move(var));
    return _make_reducer<decltype(wrapper), false>(ctx, std::move(wrapper),
                                                   kind, alias);
  }
}

inline std::unique_ptr<ReducerBase> make_general_reducer(const Context& ctx,
                                                         Var&& var,
                                                         AggrKind kind,
                                                         int alias) {
  if (kind == AggrKind::kCount) {
    if (!var.is_optional()) {
      VarWrapper var_wrap(std::move(var));

      CountReducer<VarWrapper, false> r(std::move(var_wrap));
      SingleValueCollector<int64_t> collector;
      return std::make_unique<Reducer<decltype(r), decltype(collector)>>(
          std::move(r), std::move(collector), alias);
    } else {
      OptionalVarWrapper var_wrap(std::move(var));
      CountReducer<OptionalVarWrapper, true> r(std::move(var_wrap));
      SingleValueCollector<int64_t> collector;
      return std::make_unique<Reducer<decltype(r), decltype(collector)>>(
          std::move(r), std::move(collector), alias);
    }
  } else if (kind == AggrKind::kCountDistinct) {
    VarWrapper var_wrap(std::move(var));
    if (!var.is_optional()) {
      CountDistinctReducer<VarWrapper, false> r(std::move(var_wrap));
      SingleValueCollector<int64_t> collector;
      return std::make_unique<Reducer<decltype(r), decltype(collector)>>(
          std::move(r), std::move(collector), alias);
    } else {
      LOG(FATAL) << "not support optional count\n";
    }
  } else {
    LOG(FATAL) << "not support var reduce\n";
  }
  return nullptr;  // This line is unreachable but avoids compiler warning.
}

inline std::unique_ptr<ReducerBase> make_pair_reducer(const Context& ctx,
                                                      Var&& fst, Var&& snd,
                                                      AggrKind kind,
                                                      int alias) {
  if (kind == AggrKind::kCount) {
    VarPairWrapper var_wrap(std::move(fst), std::move(snd));
    if ((!fst.is_optional()) && (!snd.is_optional())) {
      CountReducer<VarPairWrapper, false> r(std::move(var_wrap));
      SingleValueCollector<int64_t> collector;
      return std::make_unique<Reducer<decltype(r), decltype(collector)>>(
          std::move(r), std::move(collector), alias);
    } else {
      LOG(FATAL) << "not support optional count\n";
    }
  } else if (kind == AggrKind::kCountDistinct) {
    VarPairWrapper var_wrap(std::move(fst), std::move(snd));
    if (!fst.is_optional() && !snd.is_optional()) {
      CountDistinctReducer<VarPairWrapper, false> r(std::move(var_wrap));
      SingleValueCollector<int64_t> collector;
      return std::make_unique<Reducer<decltype(r), decltype(collector)>>(
          std::move(r), std::move(collector), alias);
    } else {
      LOG(FATAL) << "not support optional count\n";
    }
  } else {
    LOG(FATAL) << "not support var reduce\n";
  }
  return nullptr;
}

inline std::unique_ptr<ReducerBase> make_reducer(
    const StorageReadInterface& graph, const Context& ctx,
    const common::Variable& var, AggrKind kind, int alias) {
  if (!var.has_property() && var.has_tag()) {
    int tag = var.has_tag() ? var.tag().id() : -1;
    auto col = ctx.get(tag);
    if (!col->is_optional()) {
      if (col->column_type() == ContextColumnType::kVertex) {
        auto vertex_col = std::dynamic_pointer_cast<IVertexColumn>(col);
        if (vertex_col->vertex_column_type() == VertexColumnType::kSingle) {
          SLVertexWrapperBeta wrapper(
              *dynamic_cast<const SLVertexColumn*>(vertex_col.get()));
          return _make_reducer<decltype(wrapper), false>(
              ctx, std::move(wrapper), kind, alias);
        } else if (vertex_col->vertex_column_type() ==
                   VertexColumnType::kMultiple) {
          auto typed_vertex_col =
              std::dynamic_pointer_cast<MLVertexColumn>(vertex_col);
          MLVertexWrapper<decltype(*typed_vertex_col)> wrapper(
              *typed_vertex_col);
          return _make_reducer<decltype(wrapper), false>(
              ctx, std::move(wrapper), kind, alias);
        } else {
          auto typed_vertex_col =
              std::dynamic_pointer_cast<MSVertexColumn>(vertex_col);
          MLVertexWrapper<decltype(*typed_vertex_col)> wrapper(
              *typed_vertex_col);
          return _make_reducer<decltype(wrapper), false>(
              ctx, std::move(wrapper), kind, alias);
        }
      } else if (col->column_type() == ContextColumnType::kValue) {
#define TYPE_DISPATCHER(enum_val, type)                                     \
  case DataTypeId::enum_val: {                                              \
    ValueWrapper<type> wrapper(                                             \
        *dynamic_cast<const ValueColumn<type>*>(col.get()));                \
    return _make_reducer<decltype(wrapper), false>(ctx, std::move(wrapper), \
                                                   kind, alias);            \
  }
        switch (col->elem_type().id()) {
          TYPE_DISPATCHER(BIGINT, int64_t)
          TYPE_DISPATCHER(INTEGER, int32_t)
          TYPE_DISPATCHER(VARCHAR, std::string_view)
          TYPE_DISPATCHER(TIMESTAMP_MS, DateTime)
#undef TYPE_DISPATCHER

        default:
          break;
        }
      }
    }
  }
  Var var_(&graph, ctx, var, VarType::kPathVar);
  switch (var_.type().id()) {
#define TYPE_DISPATCHER(enum_val, type) \
  case DataTypeId::enum_val:            \
    return make_reducer<type>(ctx, std::move(var_), kind, alias);
    FOR_EACH_DATA_TYPE(TYPE_DISPATCHER)
#undef TYPE_DISPATCHER
  case DataTypeId::VERTEX:
    return make_reducer<VertexRecord>(ctx, std::move(var_), kind, alias);
  case DataTypeId::STRUCT:
    return make_reducer<Tuple>(ctx, std::move(var_), kind, alias);
  default:
    return make_general_reducer(ctx, std::move(var_), kind, alias);
  }
}  // namespace ops

inline std::vector<std::unique_ptr<ProjectExprBase>> create_project_funcs(
    const std::vector<std::pair<common::Variable, int>>& vars,
    const StorageReadInterface& graph, const Context& ctx) {
  std::vector<std::unique_ptr<ProjectExprBase>> exprs;
  for (const auto& [var, alias] : vars) {
    if (!var.has_property()) {
      continue;
    }

    Var var_(&graph, ctx, var, VarType::kPathVar);
    switch (var_.type().id()) {
#define TYPE_DISPATCHER(enum_val, type)                                        \
  case DataTypeId::enum_val: {                                                 \
    TypedKeyCollector<type>::TypedKeyWrapper wrapper(std::move(var_));         \
    TypedKeyCollector<type> collector;                                         \
    exprs.emplace_back(                                                        \
        std::make_unique<ProjectExpr<decltype(wrapper), decltype(collector)>>( \
            std::move(wrapper), std::move(collector), alias));                 \
    break;                                                                     \
  }
      FOR_EACH_DATA_TYPE(TYPE_DISPATCHER)
#undef TYPE_DISPATCHER
    default: {
      THROW_NOT_SUPPORTED_EXCEPTION(
          "not support property type " +
          std::to_string(static_cast<int>(var_.type().id())));
    }
    }
  }
  return exprs;
}

inline std::unique_ptr<KeyBase> create_key_func(
    const std::vector<common::Variable>& vars,
    const std::vector<std::pair<int, int>>& mappings,
    const StorageReadInterface& graph, const Context& ctx) {
  if (mappings.size() == 1) {
    auto key = KeyBuilder<1>::make_sp_key(ctx, mappings);
    if (key) {
      return key;
    }
  }
  std::vector<VarWrapper> key_vars;
  for (const auto& var : vars) {
    Var var_(&graph, ctx, var, VarType::kPathVar);
    key_vars.emplace_back(VarWrapper(std::move(var_)));
  }

  return std::make_unique<GKey<VarWrapper>>(std::move(key_vars), mappings);
}

inline std::unique_ptr<ReducerBase> create_reducer(
    const physical::GroupBy_AggFunc& func, const StorageReadInterface& graph,
    const Context& ctx) {
  auto aggr_kind = parse_aggregate(func.aggregate());
  int alias = func.has_alias() ? func.alias().value() : -1;
  if (func.vars_size() == 0) {
    if (aggr_kind == AggrKind::kCount) {
      using count_res_t = typename CountStartReducer::V;
      return std::make_unique<
          Reducer<CountStartReducer, SingleValueCollector<count_res_t>>>(
          CountStartReducer(), SingleValueCollector<count_res_t>(), alias);

    } else {
      THROW_NOT_SUPPORTED_EXCEPTION(
          "not support reduce with no var except count");
    }
  } else if (func.vars_size() == 2) {
    auto& fst = func.vars(0);
    auto& snd = func.vars(1);

    Var fst_var(&graph, ctx, fst, VarType::kPathVar);
    Var snd_var(&graph, ctx, snd, VarType::kPathVar);
    return make_pair_reducer(ctx, std::move(fst_var), std::move(snd_var),
                             aggr_kind, alias);
  } else if (func.vars_size() == 1) {
    auto& var = func.vars(0);

    return make_reducer(graph, ctx, var, aggr_kind, alias);
  } else {
    THROW_NOT_SUPPORTED_EXCEPTION("not support reduce with more than 2 vars");
  }
}

bool BuildGroupByUtils(
    const physical::GroupBy& group_by,
    std::vector<std::pair<common::Variable, int>>& project_vars,
    std::vector<common::Variable>& key_vars,
    std::vector<std::pair<int, int>>& mappings,
    std::vector<physical::GroupBy_AggFunc>& reduce_funcs,
    std::vector<std::pair<int, int>>& dependencies);

inline gs::result<gs::runtime::Context> GroupByEvalImpl(
    const StorageReadInterface& graph,
    const std::map<std::string, std::string>& params,
    gs::runtime::Context&& ctx, const std::vector<common::Variable>& vars,
    const std::vector<std::pair<int, int>>& mappings,
    const std::vector<physical::GroupBy_AggFunc>& aggrs,
    const std::vector<std::pair<int, int>>& dependencies) {
  std::vector<std::shared_ptr<Arena>> arenas;
  if (!dependencies.empty()) {
    arenas.resize(ctx.col_num(), nullptr);
    for (size_t i = 0; i < ctx.col_num(); ++i) {
      if (ctx.get(i)) {
        arenas[i] = ctx.get(i)->get_arena();
      }
    }
  }
  auto key = create_key_func(vars, mappings, graph, ctx);
  std::vector<std::unique_ptr<ReducerBase>> reducers;
  for (auto& aggr : aggrs) {
    reducers.push_back(create_reducer(aggr, graph, ctx));
  }
  auto ret =
      GroupBy::group_by(std::move(ctx), std::move(key), std::move(reducers));
  if (!ret) {
    return ret;
  }
  for (auto& [idx, deps] : dependencies) {
    std::shared_ptr<Arena> arena = std::make_shared<Arena>();
    auto arena1 = ret.value().get(idx)->get_arena();
    if (arena1) {
      arena->emplace_back(std::make_unique<ArenaRef>(arena1));
    }
    if (arenas[deps]) {
      arena->emplace_back(std::make_unique<ArenaRef>(arenas[deps]));
    }
    ret.value().get(idx)->set_arena(arena);
  }
  return ret;
}

inline gs::result<gs::runtime::Context> GroupByBetaEvalImpl(
    const StorageReadInterface& graph,
    const std::map<std::string, std::string>& params, Context&& ctx,
    const std::vector<std::pair<common::Variable, int>>& project_var_alias,
    const std::vector<common::Variable>& vars,
    const std::vector<std::pair<int, int>>& mappings,
    const std::vector<physical::GroupBy_AggFunc>& aggrs,
    const std::vector<std::pair<int, int>>& dependencies) {
  std::vector<std::shared_ptr<Arena>> arenas;
  if (!dependencies.empty()) {
    arenas.resize(ctx.col_num(), nullptr);
    for (size_t i = 0; i < ctx.col_num(); ++i) {
      if (ctx.get(i)) {
        arenas[i] = ctx.get(i)->get_arena();
      }
    }
  }
  auto key_project = create_project_funcs(project_var_alias, graph, ctx);
  auto tmp = ctx;

  auto ret_res = Project::project(std::move(tmp), std::move(key_project));
  if (!ret_res) {
    return ret_res;
  }
  auto& ret = ret_res.value();
  for (size_t i = 0; i < ret.col_num(); ++i) {
    if (ret.get(i) != nullptr) {
      ctx.set(i, ret.get(i));
    }
  }

  auto key = create_key_func(vars, mappings, graph, ctx);
  std::vector<std::unique_ptr<ReducerBase>> reducers;
  for (auto& aggr : aggrs) {
    reducers.push_back(create_reducer(aggr, graph, ctx));
  }
  auto ret_ctx =
      GroupBy::group_by(std::move(ctx), std::move(key), std::move(reducers));
  if (!ret_ctx) {
    return ret_ctx;
  }
  for (auto& [idx, deps] : dependencies) {
    std::shared_ptr<Arena> arena = std::make_shared<Arena>();
    auto arena1 = ret_ctx.value().get(idx)->get_arena();
    if (arena1) {
      arena->emplace_back(std::make_unique<ArenaRef>(arena1));
    }
    if (arenas[deps]) {
      arena->emplace_back(std::make_unique<ArenaRef>(arenas[deps]));
    }
    ret_ctx.value().get(idx)->set_arena(arena);
  }
  return ret_ctx;
}
}  // namespace ops

}  // namespace runtime
}  // namespace gs
