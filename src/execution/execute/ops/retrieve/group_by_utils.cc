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

#include "neug/execution/execute/ops/retrieve/group_by_utils.h"
#include "neug/execution/common/columns/i_context_column.h"
#include "neug/execution/common/columns/list_columns.h"
#include "neug/execution/common/columns/value_columns.h"
#include "neug/execution/common/columns/vertex_columns.h"

namespace neug {
namespace execution {
namespace ops {

struct GKey : public KeyBase {
  GKey(const std::vector<std::pair<int, int>>& tag_alias)
      : tag_alias_(tag_alias) {}
  std::pair<std::vector<size_t>, std::vector<std::vector<size_t>>> group(
      const Context& ctx) override {
    std::vector<std::shared_ptr<IContextColumn>> exprs;
    for (size_t i = 0; i < tag_alias_.size(); ++i) {
      exprs.push_back(ctx.get(tag_alias_[i].first));
    }
    size_t row_num = ctx.row_num();
    std::vector<std::vector<size_t>> groups;
    std::vector<size_t> offsets;
    phmap::flat_hash_map<std::string_view, size_t> sig_to_root;
    std::vector<std::vector<char>> root_list;
    for (size_t i = 0; i < row_num; ++i) {
      std::vector<char> buf;
      ::neug::Encoder encoder(buf);
      for (size_t k_i = 0; k_i < exprs.size(); ++k_i) {
        auto val = exprs[k_i]->get_elem(i);
        encode_value(val, encoder);
      }
      std::string_view sv(buf.data(), buf.size());
      auto iter = sig_to_root.find(sv);
      if (iter != sig_to_root.end()) {
        groups[iter->second].push_back(i);
      } else {
        sig_to_root.emplace(sv, groups.size());
        root_list.emplace_back(std::move(buf));
        offsets.push_back(i);
        std::vector<size_t> ret_elem;
        ret_elem.push_back(i);
        groups.emplace_back(std::move(ret_elem));
      }
    }
    return std::make_pair(std::move(offsets), std::move(groups));
  }
  const std::vector<std::pair<int, int>>& tag_alias() const override {
    return tag_alias_;
  }

  std::vector<std::pair<int, int>> tag_alias_;
};

/**
 * Wrapper structs for different column types
 */

template <typename VERTEX_COL>
struct VertexWrapper {
  using V = VertexRecord;
  explicit VertexWrapper(const VERTEX_COL& vertex) : vertex(vertex) {}
  V operator()(size_t idx) const { return vertex.get_vertex(idx); }
  bool has_value(size_t idx) const { return vertex.has_value(idx); }
  const VERTEX_COL& vertex;
};
template <typename T>
struct ValueWrapper {
  using V = T;
  explicit ValueWrapper(const ValueColumn<T>& column) : column(column) {}
  V operator()(size_t idx) const { return column.get_value(idx); }
  bool has_value(size_t idx) const { return column.has_value(idx); }
  const ValueColumn<T>& column;
};

template <typename T>
struct TypedVarWrapper {
  using V = T;
  explicit TypedVarWrapper(const IContextColumn& column) : column(column) {}
  V operator()(size_t idx) const {
    return column.get_elem(idx).template GetValue<T>();
  }
  bool has_value(size_t idx) const { return column.has_value(idx); }
  const IContextColumn& column;
};
// General wrapper for Value type
struct VarWrapper {
  using V = Value;
  Value operator()(size_t idx) const { return vars->get_elem(idx); }
  explicit VarWrapper(const std::shared_ptr<IContextColumn>& vars)
      : vars(vars) {}
  bool has_value(size_t idx) const { return !vars->get_elem(idx).IsNull(); }
  const DataType& type() const { return vars->elem_type(); }
  std::shared_ptr<IContextColumn> vars;
};

struct VarPairWrapper {
  using V = std::pair<Value, Value>;
  std::pair<Value, Value> operator()(size_t idx) const {
    return std::make_pair(fst->get_elem(idx), snd->get_elem(idx));
  }
  bool has_value(size_t idx) const { return !fst->get_elem(idx).IsNull(); }
  VarPairWrapper(const std::shared_ptr<IContextColumn>& fst,
                 const std::shared_ptr<IContextColumn>& snd)
      : fst(fst), snd(snd) {}
  std::shared_ptr<IContextColumn> fst;
  std::shared_ptr<IContextColumn> snd;
};

static std::unique_ptr<KeyBase> create_sp_key(
    const Context& ctx, const std::vector<std::pair<int, int>>& tag_alias) {
  auto col = ctx.get(tag_alias[0].first);
  if (col->column_type() == ContextColumnType::kVertex) {
    auto vertex_col = std::dynamic_pointer_cast<IVertexColumn>(col);
    VertexWrapper wrapper(*vertex_col);
    return std::make_unique<Key<decltype(wrapper)>>(std::move(wrapper),
                                                    tag_alias);
    if (vertex_col->vertex_column_type() == VertexColumnType::kSingle) {
      VertexWrapper wrapper(
          *dynamic_cast<const SLVertexColumn*>(vertex_col.get()));

      return std::make_unique<Key<decltype(wrapper)>>(std::move(wrapper),
                                                      tag_alias);
    }
  } else if (col->column_type() == ContextColumnType::kValue) {
    if (col->elem_type().id() == DataTypeId::kInt64) {
      ValueWrapper<int64_t> wrapper(
          *dynamic_cast<const ValueColumn<int64_t>*>(col.get()));
      return std::make_unique<Key<decltype(wrapper)>>(std::move(wrapper),
                                                      tag_alias);
    } else if (col->elem_type().id() == DataTypeId::kInt32) {
      ValueWrapper<int32_t> wrapper(
          *dynamic_cast<const ValueColumn<int32_t>*>(col.get()));
      return std::make_unique<Key<decltype(wrapper)>>(std::move(wrapper),
                                                      tag_alias);
    } else {
      return nullptr;
    }
  }
  return nullptr;
}

/**
 * Reducers for different aggregation functions
 */
template <typename EXPR, bool IS_OPTIONAL, typename Enable = void>
struct SumReducer;

template <typename EXPR, bool IS_OPTIONAL>
struct SumReducer<EXPR, IS_OPTIONAL,
                  std::enable_if_t<std::is_arithmetic<typename EXPR::V>::value>>
    : public ReducerBase {
  EXPR expr;
  using V = typename EXPR::V;
  explicit SumReducer(EXPR&& expr) : expr(std::move(expr)) {}

  std::shared_ptr<IContextColumn> reduce(
      const std::vector<std::vector<size_t>>& groups) override {
    ValueColumnBuilder<V> builder;
    builder.reserve(groups.size());
    if constexpr (!IS_OPTIONAL) {
      for (auto& group : groups) {
        V sum = 0;
        for (size_t i = 0; i < group.size(); ++i) {
          sum += expr(group[i]);
        }
        builder.push_back_opt(sum);
      }
    } else {
      for (auto& group : groups) {
        V sum = 0;
        bool has_value = false;
        for (size_t i = 0; i < group.size(); ++i) {
          if (expr.has_value(group[i])) {
            sum += expr(group[i]);
            has_value = true;
          }
        }
        if (has_value) {
          builder.push_back_opt(sum);
        } else {
          builder.push_back_null();
        }
      }
    }
    return builder.finish();
  }
};

template <typename T, typename = void>
struct is_hashable : std::false_type {};

template <typename T>
struct is_hashable<
    T, std::void_t<decltype(std::declval<std::hash<T>>()(std::declval<T>()))>>
    : std::true_type {};

template <typename EXPR, bool IS_OPTIONAL>
struct CountDistinctReducer : public ReducerBase {
  EXPR expr;
  using V = int64_t;
  using T = typename EXPR::V;

  explicit CountDistinctReducer(EXPR&& expr) : expr(std::move(expr)) {}

  std::shared_ptr<IContextColumn> reduce(
      const std::vector<std::vector<size_t>>& groups) override {
    ValueColumnBuilder<int64_t> builder;
    builder.reserve(groups.size());
    if (groups.empty()) {
      builder.push_back_opt(0);
      return builder.finish();
    }
    if constexpr (is_hashable<T>::value) {
      if constexpr (!IS_OPTIONAL) {
        for (auto& group : groups) {
          phmap::flat_hash_set<T> set;
          for (auto idx : group) {
            set.insert(expr(idx));
          }
          builder.push_back_opt(set.size());
        }
      } else {
        for (auto& group : groups) {
          phmap::flat_hash_set<T> set;
          for (auto idx : group) {
            if (expr.has_value(idx)) {
              auto v = expr(idx);

              set.insert(v);
            }
          }
          builder.push_back_opt(set.size());
        }
      }
    } else {
      if constexpr (!IS_OPTIONAL) {
        for (auto& group : groups) {
          std::set<T> set;
          for (auto idx : group) {
            set.insert(expr(idx));
          }
          builder.push_back_opt(set.size());
        }
      } else {
        for (auto& group : groups) {
          std::set<T> set;
          for (auto idx : group) {
            if (expr.has_value(idx)) {
              auto v = expr(idx);
              set.insert(v);
            }
          }
          builder.push_back_opt(set.size());
        }
      }
    }
    return builder.finish();
  }
};

template <typename EXPR, bool IS_OPTIONAL>
struct CountReducer : public ReducerBase {
  EXPR expr;
  using V = int64_t;

  explicit CountReducer(EXPR&& expr) : expr(std::move(expr)) {}

  std::shared_ptr<IContextColumn> reduce(
      const std::vector<std::vector<size_t>>& groups) override {
    ValueColumnBuilder<int64_t> builder;
    builder.reserve(groups.size());
    if (groups.empty()) {
      builder.push_back_opt(0);
      return builder.finish();
    }
    if constexpr (!IS_OPTIONAL) {
      for (auto& group : groups) {
        builder.push_back_opt(group.size());
      }
    } else {
      for (auto& group : groups) {
        int64_t count = 0;
        for (auto idx : group) {
          if (expr.has_value(idx)) {
            count += 1;
          }
        }
        builder.push_back_opt(count);
      }
    }
    return builder.finish();
  }
};

// To deal with special case count(*)
struct CountStarReducer : public ReducerBase {
  CountStarReducer() {}
  using V = int64_t;

  std::shared_ptr<IContextColumn> reduce(
      const std::vector<std::vector<size_t>>& groups) override {
    ValueColumnBuilder<int64_t> builder;
    builder.reserve(groups.size());
    if (groups.empty()) {
      builder.push_back_opt(0);
      return builder.finish();
    }
    for (auto& group : groups) {
      builder.push_back_opt(group.size());
    }
    return builder.finish();
  }
};

template <typename EXPR, bool IS_OPTIONAL>
struct MinReducer : public ReducerBase {
  EXPR expr;

  using V = typename EXPR::V;
  explicit MinReducer(EXPR&& expr) : expr(std::move(expr)) {}

  std::shared_ptr<IContextColumn> reduce(
      const std::vector<std::vector<size_t>>& groups) override {
    ValueColumnBuilder<V> builder;
    builder.reserve(groups.size());
    if constexpr (!IS_OPTIONAL) {
      for (auto& group : groups) {
        V min_val = expr(group[0]);
        for (size_t i = 1; i < group.size(); ++i) {
          min_val = std::min(min_val, expr(group[i]));
        }
        builder.push_back_opt(min_val);
      }
    } else {
      for (auto& group : groups) {
        bool has_value = false;
        V min_val;
        for (size_t i = 0; i < group.size(); ++i) {
          auto v = expr(group[i]);
          if (expr.has_value(group[i])) {
            if (!has_value) {
              min_val = v;
              has_value = true;
            } else {
              min_val = std::min(min_val, v);
            }
          }
        }
        if (has_value) {
          builder.push_back_opt(min_val);
        } else {
          builder.push_back_null();
        }
      }
    }
    return builder.finish();
  }
};

template <typename EXPR, bool IS_OPTIONAL>
struct MaxReducer : public ReducerBase {
  EXPR expr;

  using V = typename EXPR::V;
  explicit MaxReducer(EXPR&& expr) : expr(std::move(expr)) {}

  std::shared_ptr<IContextColumn> reduce(
      const std::vector<std::vector<size_t>>& groups) override {
    ValueColumnBuilder<V> builder;
    builder.reserve(groups.size());
    if constexpr (!IS_OPTIONAL) {
      for (auto& group : groups) {
        V max_val = expr(group[0]);
        for (size_t i = 1; i < group.size(); ++i) {
          max_val = std::max(max_val, expr(group[i]));
        }
        builder.push_back_opt(max_val);
      }
    } else {
      for (auto& group : groups) {
        bool has_value = false;
        V max_val;
        for (size_t i = 0; i < group.size(); ++i) {
          auto v = expr(group[i]);
          if (expr.has_value(group[i])) {
            if (!has_value) {
              max_val = v;
              has_value = true;
            } else {
              max_val = std::max(max_val, v);
            }
          }
        }
        if (has_value) {
          builder.push_back_opt(max_val);
        } else {
          builder.push_back_null();
        }
      }
    }
    return builder.finish();
  }
};

template <typename EXPR, bool IS_OPTIONAL>
struct FirstReducer : public ReducerBase {
  EXPR expr;
  using V = typename EXPR::V;
  explicit FirstReducer(EXPR&& expr) : expr(std::move(expr)) {}

  std::shared_ptr<IContextColumn> reduce(
      const std::vector<std::vector<size_t>>& groups) override {
    ValueColumnBuilder<V> builder;
    builder.reserve(groups.size());
    if constexpr (!IS_OPTIONAL) {
      for (auto& group : groups) {
        V val = expr(group[0]);
        builder.push_back_opt(val);
      }
    } else {
      for (auto& group : groups) {
        bool has_value = false;
        V val;
        for (size_t i = 0; i < group.size(); ++i) {
          auto v = expr(group[i]);
          if (expr.has_value(group[i])) {
            val = v;
            has_value = true;
            break;
          }
        }
        if (has_value) {
          builder.push_back_opt(val);
        } else {
          builder.push_back_null();
        }
      }
    }
    return builder.finish();
  }
};

template <typename EXPR, bool IS_OPTIONAL>
struct ToSetReducer : public ReducerBase {
  EXPR expr;
  DataType type;
  explicit ToSetReducer(EXPR&& expr, const DataType& type)
      : expr(std::move(expr)), type(type) {}

  std::shared_ptr<IContextColumn> reduce(
      const std::vector<std::vector<size_t>>& groups) override {
    ListColumnBuilder builder(type);
    builder.reserve(groups.size());

    if constexpr (!IS_OPTIONAL) {
      for (auto& group : groups) {
        std::set<typename EXPR::V> temp_set;
        for (auto idx : group) {
          temp_set.insert(expr(idx));
        }
        std::vector<Value> vals;
        vals.reserve(temp_set.size());
        for (auto& v : temp_set) {
          vals.emplace_back(Value::CreateValue<typename EXPR::V>(v));
        }
        auto val = Value::LIST(type, std::move(vals));
        builder.push_back_elem(val);
      }
    } else {
      for (auto& group : groups) {
        std::set<typename EXPR::V> temp_set;
        for (auto idx : group) {
          auto v = expr(idx);
          if (expr.has_value(idx)) {
            temp_set.insert(v);
          }
        }
        std::vector<Value> vals;
        vals.reserve(temp_set.size());
        for (auto& v : temp_set) {
          vals.emplace_back(Value::CreateValue<typename EXPR::V>(v));
        }
        auto list_val = Value::LIST(type, std::move(vals));
        builder.push_back_elem(list_val);
      }
    }
    return builder.finish();
  }
};

template <typename EXPR, bool IS_OPTIONAL>
struct ToListReducer : public ReducerBase {
  EXPR expr;
  DataType type;

  explicit ToListReducer(EXPR&& expr, const DataType& type)
      : expr(std::move(expr)), type(type) {}

  std::shared_ptr<IContextColumn> reduce(
      const std::vector<std::vector<size_t>>& groups) override {
    ListColumnBuilder builder(type);
    builder.reserve(groups.size());

    if constexpr (!IS_OPTIONAL) {
      for (auto& group : groups) {
        std::vector<Value> vals;
        vals.reserve(group.size());
        for (auto idx : group) {
          vals.emplace_back(Value::CreateValue<typename EXPR::V>(expr(idx)));
        }
        auto val = Value::LIST(type, std::move(vals));
        builder.push_back_elem(val);
      }
    } else {
      for (auto& group : groups) {
        std::vector<Value> vals;
        for (auto idx : group) {
          auto v = expr(idx);
          if (expr.has_value(idx)) {
            vals.emplace_back(Value::CreateValue<typename EXPR::V>(v));
          }
        }
        auto list_val = Value::LIST(type, std::move(vals));
        builder.push_back_elem(list_val);
      }
    }
    return builder.finish();
  }
};

template <typename EXPR, bool IS_OPTIONAL, typename Enable = void>
struct AvgReducer;

template <typename EXPR, bool IS_OPTIONAL>
struct AvgReducer<EXPR, IS_OPTIONAL,
                  std::enable_if_t<std::is_arithmetic<typename EXPR::V>::value>>
    : public ReducerBase {
  EXPR expr;
  using V = double;
  explicit AvgReducer(EXPR&& expr) : expr(std::move(expr)) {}

  std::shared_ptr<IContextColumn> reduce(
      const std::vector<std::vector<size_t>>& groups) override {
    ValueColumnBuilder<double> builder;
    builder.reserve(groups.size());
    if constexpr (!IS_OPTIONAL) {
      for (auto& group : groups) {
        double avg = 0.0;
        for (auto idx : group) {
          avg += expr(idx);
        }
        avg = avg / group.size();
        builder.push_back_opt(avg);
      }
    } else {
      for (auto& group : groups) {
        double avg = 0.0;
        size_t count = 0;
        for (auto idx : group) {
          if (expr.has_value(idx)) {
            avg += expr(idx);
            count += 1;
          }
        }
        if (count == 0) {
          builder.push_back_null();
        } else {
          avg = avg / count;
          builder.push_back_opt(avg);
        }
      }
    }
    return builder.finish();
  }
};

template <typename EXPR, bool IS_OPTIONAL>
std::unique_ptr<ReducerBase> create_typed_reducer(EXPR&& expr, AggrKind kind) {
  switch (kind) {
  case AggrKind::kSum: {
    if constexpr (std::is_arithmetic<typename EXPR::V>::value) {
      return std::make_unique<SumReducer<EXPR, IS_OPTIONAL>>(std::move(expr));
    } else {
      LOG(FATAL) << "unsupport" << static_cast<int>(kind);
      return nullptr;
    }
  }
  case AggrKind::kCountDistinct: {
    return std::make_unique<CountDistinctReducer<EXPR, IS_OPTIONAL>>(
        std::move(expr));
  }
  case AggrKind::kCount: {
    return std::make_unique<CountReducer<EXPR, IS_OPTIONAL>>(std::move(expr));
  }
  case AggrKind::kMin: {
    if constexpr (std::is_same<typename EXPR::V, VertexRecord>::value) {
      LOG(FATAL) << "Min not support VertexRecord";
    } else {
      return std::make_unique<MinReducer<EXPR, IS_OPTIONAL>>(std::move(expr));
    }
  }
  case AggrKind::kMax: {
    if constexpr (std::is_same<typename EXPR::V, VertexRecord>::value) {
      LOG(FATAL) << "Max not support VertexRecord";
    } else {
      return std::make_unique<MaxReducer<EXPR, IS_OPTIONAL>>(std::move(expr));
    }
  }
  case AggrKind::kFirst: {
    if constexpr (std::is_same<typename EXPR::V, VertexRecord>::value) {
      LOG(FATAL) << "First not support VertexRecord";
    } else {
      return std::make_unique<FirstReducer<EXPR, IS_OPTIONAL>>(std::move(expr));
    }
  }
  case AggrKind::kToSet: {
    return std::make_unique<ToSetReducer<EXPR, IS_OPTIONAL>>(
        std::move(expr), ValueConverter<typename EXPR::V>::type());
  }
  case AggrKind::kToList: {
    return std::make_unique<ToListReducer<EXPR, IS_OPTIONAL>>(
        std::move(expr), ValueConverter<typename EXPR::V>::type());
  }
  case AggrKind::kAvg: {
    if constexpr (std::is_arithmetic<typename EXPR::V>::value) {
      return std::make_unique<AvgReducer<EXPR, IS_OPTIONAL>>(std::move(expr));
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

static std::unique_ptr<ReducerBase> create_general_reducer(
    const std::shared_ptr<IContextColumn>& var, AggrKind kind) {
  VarWrapper var_wrap(var);
  if (kind == AggrKind::kCount) {
    if (!var->is_optional()) {
      return std::make_unique<CountReducer<VarWrapper, false>>(
          std::move(var_wrap));
    } else {
      return std::make_unique<CountReducer<VarWrapper, true>>(
          std::move(var_wrap));
    }
  } else if (kind == AggrKind::kCountDistinct) {
    if (!var->is_optional()) {
      return std::make_unique<CountDistinctReducer<VarWrapper, false>>(
          std::move(var_wrap));
    } else {
      return std::make_unique<CountDistinctReducer<VarWrapper, true>>(
          std::move(var_wrap));
    }
  } else if (kind == AggrKind::kToList) {
    if (var->is_optional()) {
      return std::make_unique<ToListReducer<VarWrapper, true>>(
          std::move(var_wrap), var->elem_type());
    } else {
      return std::make_unique<ToListReducer<VarWrapper, false>>(
          std::move(var_wrap), var->elem_type());
    }
  } else {
    LOG(FATAL) << "not support var reduce " << static_cast<int>(kind)
               << var->elem_type().ToString() << " " << var->is_optional()
               << " " << var->column_info();
  }
  return nullptr;  // This line is unreachable but avoids compiler warning.
}

static std::unique_ptr<ReducerBase> create_pair_reducer(
    const std::shared_ptr<IContextColumn>& fst,
    const std::shared_ptr<IContextColumn>& snd, AggrKind kind) {
  if (kind == AggrKind::kCount) {
    VarPairWrapper var_wrap(std::move(fst), std::move(snd));
    if ((!fst->is_optional()) && (!snd->is_optional())) {
      auto reducer = std::make_unique<CountReducer<VarPairWrapper, false>>(
          std::move(var_wrap));
      return reducer;
    } else {
      LOG(FATAL) << "not support optional count\n";
    }
  } else if (kind == AggrKind::kCountDistinct) {
    VarPairWrapper var_wrap(std::move(fst), std::move(snd));
    if (!fst->is_optional() && !snd->is_optional()) {
      auto reducer =
          std::make_unique<CountDistinctReducer<VarPairWrapper, false>>(
              std::move(var_wrap));
      return reducer;
    } else {
      LOG(FATAL) << "not support optional count\n";
    }
  } else {
    LOG(FATAL) << "not support var reduce\n";
  }
  return nullptr;
}

static std::unique_ptr<ReducerBase> create_reducer(
    const IStorageInterface& graph, const Context& ctx,
    const common::Variable& var, AggrKind kind) {
  if (!var.has_property() && var.has_tag()) {
    int tag = var.has_tag() ? var.tag().id() : -1;
    auto col = ctx.get(tag);
    {
      if (col->column_type() == ContextColumnType::kVertex) {
        auto vertex_col = std::dynamic_pointer_cast<IVertexColumn>(col);
        VertexWrapper wrapper(*vertex_col);
        if (col->is_optional()) {
          return create_typed_reducer<decltype(wrapper), true>(
              std::move(wrapper), kind);
        } else {
          return create_typed_reducer<decltype(wrapper), false>(
              std::move(wrapper), kind);
        }
        if (vertex_col->vertex_column_type() == VertexColumnType::kSingle) {
          VertexWrapper wrapper(
              *dynamic_cast<const SLVertexColumn*>(vertex_col.get()));
          if (!col->is_optional()) {
            return create_typed_reducer<decltype(wrapper), false>(
                std::move(wrapper), kind);
          } else {
            return create_typed_reducer<decltype(wrapper), true>(
                std::move(wrapper), kind);
          }
        } else if (vertex_col->vertex_column_type() ==
                   VertexColumnType::kMultiple) {
          auto typed_vertex_col =
              std::dynamic_pointer_cast<MLVertexColumn>(vertex_col);
          VertexWrapper wrapper(*typed_vertex_col);
          if (!col->is_optional()) {
            return create_typed_reducer<decltype(wrapper), false>(
                std::move(wrapper), kind);
          } else {
            return create_typed_reducer<decltype(wrapper), true>(
                std::move(wrapper), kind);
          }
        } else {
          auto typed_vertex_col =
              std::dynamic_pointer_cast<MSVertexColumn>(vertex_col);
          VertexWrapper wrapper(*typed_vertex_col);
          if (!col->is_optional()) {
            return create_typed_reducer<decltype(wrapper), false>(
                std::move(wrapper), kind);
          } else {
            return create_typed_reducer<decltype(wrapper), true>(
                std::move(wrapper), kind);
          }
        }
      } else if (col->column_type() == ContextColumnType::kValue) {
#define TYPE_DISPATCHER(enum_val, type)                                        \
  case DataTypeId::enum_val: {                                                 \
    ValueWrapper<type> wrapper(                                                \
        *dynamic_cast<const ValueColumn<type>*>(col.get()));                   \
    if (!col->is_optional()) {                                                 \
      return create_typed_reducer<decltype(wrapper), false>(                   \
          std::move(wrapper), kind);                                           \
    } else {                                                                   \
      return create_typed_reducer<decltype(wrapper), true>(std::move(wrapper), \
                                                           kind);              \
    }                                                                          \
  }
        switch (col->elem_type().id()) {
          TYPE_DISPATCHER(kInt32, int32_t)
          TYPE_DISPATCHER(kInt64, int64_t)
          TYPE_DISPATCHER(kTimestampMs, DateTime)
          TYPE_DISPATCHER(kVarchar, std::string)
#undef TYPE_DISPATCHER

        default:
          break;
        }
      }
    }
  }
  auto tag_id = var.has_tag() ? var.tag().id() : -1;
  auto var_ = ctx.get(tag_id);
  switch (var_->elem_type().id()) {
#define TYPE_DISPATCHER(enum_val, type)                                        \
  case DataTypeId::enum_val: {                                                 \
    TypedVarWrapper<type> wrapper(*(var_.get()));                              \
    if (!var_->is_optional()) {                                                \
      return create_typed_reducer<decltype(wrapper), false>(                   \
          std::move(wrapper), kind);                                           \
    } else {                                                                   \
      return create_typed_reducer<decltype(wrapper), true>(std::move(wrapper), \
                                                           kind);              \
    }                                                                          \
  }
    FOR_EACH_DATA_TYPE(TYPE_DISPATCHER)
#undef TYPE_DISPATCHER
  default:
    break;
  }

  return create_general_reducer(var_, kind);
}

std::unique_ptr<KeyBase> create_key_func(
    const std::vector<std::pair<int, int>>& mappings,
    const IStorageInterface& graph, const Context& ctx) {
  if (mappings.size() == 1) {
    auto key = create_sp_key(ctx, mappings);
    if (key) {
      return key;
    }
  }
  return std::make_unique<GKey>(mappings);
}

ReduceOp create_reduce_op(const physical::GroupBy_AggFunc& func,
                          const IStorageInterface& graph, const Context& ctx) {
  auto aggr_kind = parse_aggregate(func.aggregate());
  int alias = func.has_alias() ? func.alias().value() : -1;
  if (func.vars_size() == 0) {
    if (aggr_kind == AggrKind::kCount) {
      auto reduce_expr = std::make_unique<CountStarReducer>();
      return ReduceOp(std::move(reduce_expr), alias);

    } else {
      THROW_NOT_SUPPORTED_EXCEPTION(
          "not support reduce with no var except count");
    }
  } else if (func.vars_size() == 2) {
    auto& fst = func.vars(0);
    auto& snd = func.vars(1);
    int32_t fst_tag = fst.has_tag() ? fst.tag().id() : -1;
    int32_t snd_tag = snd.has_tag() ? snd.tag().id() : -1;
    auto fst_var = ctx.get(fst_tag);
    auto snd_var = ctx.get(snd_tag);
    auto reducer = create_pair_reducer(fst_var, snd_var, aggr_kind);
    return ReduceOp(std::move(reducer), alias);
  } else if (func.vars_size() == 1) {
    auto& var = func.vars(0);

    auto reducer = create_reducer(graph, ctx, var, aggr_kind);
    return ReduceOp(std::move(reducer), alias);
  } else {
    THROW_NOT_SUPPORTED_EXCEPTION("not support reduce with more than 2 vars");
  }
}

bool BuildGroupByUtils(const physical::GroupBy& group_by,
                       std::vector<std::pair<int, int>>& mappings,
                       std::vector<physical::GroupBy_AggFunc>& reduce_funcs) {
  int mappings_num = group_by.mappings_size();
  int func_num = group_by.functions_size();
  for (int i = 0; i < mappings_num; ++i) {
    auto& key = group_by.mappings(i);
    if (!key.has_key() || !key.has_alias()) {
      LOG(ERROR) << "key should have key and alias";
      return false;
    }
    int tag = key.key().has_tag() ? key.key().tag().id() : -1;
    int alias = key.has_alias() ? key.alias().value() : -1;

    mappings.emplace_back(tag, alias);
  }
  for (int i = 0; i < func_num; ++i) {
    reduce_funcs.emplace_back(group_by.functions(i));
  }

  return true;
}

}  // namespace ops

}  // namespace execution
}  // namespace neug
