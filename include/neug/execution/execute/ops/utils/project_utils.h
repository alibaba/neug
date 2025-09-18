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

#ifndef EXECUTION_EXECUTE_OPS_UTILS_PROJECT_UTILS_H_
#define EXECUTION_EXECUTE_OPS_UTILS_PROJECT_UTILS_H_
#include "neug/execution/common/columns/edge_columns.h"
#include "neug/execution/common/columns/i_context_column.h"
#include "neug/execution/common/columns/value_columns.h"
#include "neug/execution/common/columns/vertex_columns.h"
#include "neug/execution/common/context.h"
#include "neug/execution/common/graph_interface.h"
#include "neug/execution/common/operators/retrieve/project.h"
#include "neug/execution/common/rt_any.h"
#include "neug/execution/execute/ops/retrieve/order_by_utils.h"
#include "neug/execution/utils/expr.h"
#include "neug/execution/utils/var.h"

namespace gs {
namespace runtime {
namespace ops {
template <typename T>
struct ValueCollector {
  struct ExprWrapper {
    using V = T;
    ExprWrapper(Expr&& expr)
        : arena(std::make_shared<Arena>()), expr(std::move(expr)) {}
    inline T operator()(size_t idx) const {
      auto val = expr.eval_path(idx, *arena);
      return TypedConverter<T>::to_typed(val);
    }

    mutable std::shared_ptr<Arena> arena;

    Expr expr;
  };
  using EXPR = ExprWrapper;
  ValueCollector(const Context& ctx, const EXPR& expr) : arena_(expr.arena) {
    builder.reserve(ctx.row_num());
  }

  void collect(const EXPR& expr, size_t idx) {
    auto val = expr(idx);
    builder.push_back_opt(val);
  }
  auto get() {
    if constexpr (gs::runtime::is_view_type<T>::value) {
      builder.set_arena(arena_);
      return builder.finish();
    } else {
      return builder.finish();
    }
  }

  std::shared_ptr<Arena> arena_;
  ValueColumnBuilder<T> builder;
};

template <typename EXPR>
struct PropertyValueCollector {
  PropertyValueCollector(const Context& ctx) { builder.reserve(ctx.row_num()); }
  void collect(const EXPR& expr, size_t idx) {
    builder.push_back_opt(expr(idx));
  }
  auto get() { return builder.finish(); }

  ValueColumnBuilder<typename EXPR::V> builder;
};

template <typename T>
struct OptionalValueCollector {
  struct OptionalExprWrapper {
    using V = std::optional<T>;
    OptionalExprWrapper(Expr&& expr)
        : arena(std::make_shared<Arena>()), expr(std::move(expr)) {}
    inline std::optional<T> operator()(size_t idx) const {
      auto val = expr.eval_path(idx, *arena);
      if (val.is_null()) {
        return std::nullopt;
      }
      return TypedConverter<T>::to_typed(val);
    }

    mutable std::shared_ptr<Arena> arena;
    Expr expr;
  };
  using EXPR = OptionalExprWrapper;
  OptionalValueCollector(const Context& ctx, const EXPR& expr)
      : arena_(expr.arena) {
    builder.reserve(ctx.row_num());
  }

  void collect(const EXPR& expr, size_t idx) {
    auto val = expr(idx);
    if (!val.has_value()) {
      builder.push_back_null();
    } else {
      builder.push_back_opt(*val, true);
    }
  }
  auto get() {
    builder.set_arena(arena_);
    return builder.finish();
  }

  std::shared_ptr<Arena> arena_;
  OptionalValueColumnBuilder<T> builder;
};

struct VertexExprWrapper {
  using V = VertexRecord;
  VertexExprWrapper(Expr&& expr) : expr(std::move(expr)) {}

  inline VertexRecord operator()(size_t idx) const {
    return expr.eval_path(idx, arena).as_vertex();
  }

  mutable Arena arena;

  Expr expr;
};

struct SLVertexCollector {
  using EXPR = VertexExprWrapper;
  SLVertexCollector(label_t v_label) : builder(v_label) {}
  void collect(const EXPR& expr, size_t idx) {
    auto v = expr(idx);
    builder.push_back_opt(v.vid_);
  }
  auto get() { return builder.finish(); }
  MSVertexColumnBuilder builder;
};

struct MLVertexCollector {
  using EXPR = VertexExprWrapper;
  MLVertexCollector() : builder() {}
  void collect(const EXPR& expr, size_t idx) {
    auto v = expr(idx);
    builder.push_back_vertex(v);
  }
  auto get() { return builder.finish(); }
  MLVertexColumnBuilder builder;
};

struct EdgeCollector {
  EdgeCollector() : builder(BDMLEdgeColumnBuilder::builder()) {}
  struct EdgeExprWrapper {
    using V = EdgeRecord;

    EdgeExprWrapper(Expr&& expr) : expr(std::move(expr)) {}
    inline EdgeRecord operator()(size_t idx) const {
      return expr.eval_path(idx, arena).as_edge();
    }

    mutable Arena arena;
    Expr expr;
  };
  using EXPR = EdgeExprWrapper;
  void collect(const EXPR& expr, size_t idx) {
    auto e = expr(idx);
    builder.push_back_opt(e);
  }
  auto get() { return builder.finish(); }
  BDMLEdgeColumnBuilder builder;
};

struct ListCollector {
  struct ListExprWrapper {
    using V = List;
    ListExprWrapper(Expr&& expr)
        : arena(std::make_shared<Arena>()), expr(std::move(expr)) {}
    inline List operator()(size_t idx) const {
      return expr.eval_path(idx, *arena).as_list();
    }

    mutable std::shared_ptr<Arena> arena;
    Expr expr;
  };
  using EXPR = ListExprWrapper;
  ListCollector(const Context& ctx, const EXPR& expr)
      : builder_(
            std::make_shared<ListValueColumnBuilder>(expr.expr.elem_type())),
        arena_(expr.arena) {}

  void collect(const EXPR& expr, size_t idx) {
    builder_->push_back_opt(expr(idx));
  }
  auto get() {
    builder_->set_arena(arena_);
    return builder_->finish();
  }

  std::shared_ptr<ListValueColumnBuilder> builder_;
  std::shared_ptr<Arena> arena_;
};

template <typename EXPR, typename RESULT_T>
struct CaseWhenCollector {
  CaseWhenCollector(const Context& ctx) : ctx_(ctx) {
    builder.reserve(ctx.row_num());
  }
  void collect(const EXPR& expr, size_t idx) {
    builder.push_back_opt(expr(idx));
  }
  auto get() { return builder.finish(); }
  const Context& ctx_;
  ValueColumnBuilder<RESULT_T> builder;
};

template <typename T>
static std::unique_ptr<ProjectExprBase> _make_project_expr(Expr&& expr,
                                                           int alias,
                                                           const Context& ctx) {
  if (!expr.is_optional()) {
    typename ValueCollector<T>::EXPR wexpr(std::move(expr));
    ValueCollector<T> collector(ctx, wexpr);
    return std::make_unique<
        ProjectExpr<typename ValueCollector<T>::EXPR, ValueCollector<T>>>(
        std::move(wexpr), collector, alias);
  } else {
    typename OptionalValueCollector<T>::EXPR wexpr(std::move(expr));
    OptionalValueCollector<T> collector(ctx, wexpr);
    return std::make_unique<ProjectExpr<
        typename OptionalValueCollector<T>::EXPR, OptionalValueCollector<T>>>(
        std::move(wexpr), collector, alias);
  }
}

inline bool check_identities(const common::Variable& var, int& tag) {
  if (var.has_property()) {
    return false;
  }
  tag = var.has_tag() ? var.tag().id() : -1;

  return false;
}

inline void parse_potential_dependencies(const common::Expression& expr,
                                         std::set<int>& dependencies) {
  if (expr.operators_size() > 1) {
    return;
  }
  if (expr.operators(0).item_case() == common::ExprOpr::kVar) {
    auto var = expr.operators(0).var();
    int tag;
    if (check_identities(var, tag)) {
      dependencies.insert(tag);
    }
  } else if (expr.operators(0).item_case() == common::ExprOpr::kVars) {
    int len = expr.operators(0).vars().keys_size();
    for (int i = 0; i < len; ++i) {
      auto var = expr.operators(0).vars().keys(i);
      int tag;
      if (check_identities(var, tag)) {
        dependencies.insert(tag);
      }
    }
  } else if (expr.operators(0).item_case() == common::ExprOpr::kCase) {
    auto opr = expr.operators(0).case_();
    int when_size = opr.when_then_expressions_size();
    for (int i = 0; i < when_size; ++i) {
      auto when = opr.when_then_expressions(i).when_expression();
      if (when.operators_size() == 1) {
        parse_potential_dependencies(when, dependencies);
      }
    }
    auto else_expr = opr.else_result_expression();
    if (else_expr.operators_size() == 1) {
      parse_potential_dependencies(else_expr, dependencies);
    }
  }
}

// in the case of data_type is not set, we need to infer the
// type from the expr

template <typename GraphInterface>
static std::unique_ptr<ProjectExprBase> make_project_expr_without_data_type(
    const common::Expression& expr, int alias, const GraphInterface& graph,
    const Context& ctx, const std::map<std::string, std::string>& params) {
  Expr e(graph, ctx, params, expr, VarType::kPathVar);

  switch (e.type()) {
  case RTAnyType::kI64Value: {
    return _make_project_expr<int64_t>(std::move(e), alias, ctx);
  } break;
  case RTAnyType::kI32Value: {
    return _make_project_expr<int32_t>(std::move(e), alias, ctx);
  } break;
  case RTAnyType::kU64Value: {
    return _make_project_expr<uint64_t>(std::move(e), alias, ctx);
  } break;
  case RTAnyType::kU32Value: {
    return _make_project_expr<uint32_t>(std::move(e), alias, ctx);
  } break;
  case RTAnyType::kStringValue: {
    return _make_project_expr<std::string_view>(std::move(e), alias, ctx);
  } break;
  case RTAnyType::kDate: {
    return _make_project_expr<Date>(std::move(e), alias, ctx);
  } break;
  case RTAnyType::kDateTime: {
    return _make_project_expr<DateTime>(std::move(e), alias, ctx);
  } break;
  case RTAnyType::kTimestamp: {
    return _make_project_expr<TimeStamp>(std::move(e), alias, ctx);
  } break;
  case RTAnyType::kInterval: {
    return _make_project_expr<Interval>(std::move(e), alias, ctx);
  } break;
  case RTAnyType::kVertex: {
    MLVertexCollector collector;
    collector.builder.reserve(ctx.row_num());
    return std::make_unique<
        ProjectExpr<typename MLVertexCollector::EXPR, MLVertexCollector>>(
        std::move(e), collector, alias);
  } break;

  case RTAnyType::kBoolValue: {
    return _make_project_expr<bool>(std::move(e), alias, ctx);
  } break;
  case RTAnyType::kF32Value: {
    return _make_project_expr<float>(std::move(e), alias, ctx);
  } break;
  case RTAnyType::kF64Value: {
    return _make_project_expr<double>(std::move(e), alias, ctx);
  } break;
  case RTAnyType::kEdge: {
    EdgeCollector collector;
    return std::make_unique<
        ProjectExpr<typename EdgeCollector::EXPR, EdgeCollector>>(
        std::move(e), collector, alias);
  } break;
  case RTAnyType::kTuple: {
    return _make_project_expr<Tuple>(std::move(e), alias, ctx);
  } break;
  case RTAnyType::kList: {
    ListCollector::EXPR expr(std::move(e));
    ListCollector collector(ctx, expr);
    return std::make_unique<
        ProjectExpr<typename ListCollector::EXPR, ListCollector>>(
        std::move(expr), collector, alias);
  } break;
  case RTAnyType::kMap: {
    return _make_project_expr<Map>(std::move(e), alias, ctx);
  } break;

  default:
    LOG(FATAL) << "not support - " << static_cast<int>(e.type());
    break;
  }
  return nullptr;
}

template <typename GraphInterface>
inline std::unique_ptr<ProjectExprBase> make_project_expr(
    const common::Expression& expr, const common::IrDataType& data_type,
    int alias, const GraphInterface& graph, const Context& ctx,
    const std::map<std::string, std::string>& params) {
  switch (data_type.type_case()) {
  case common::IrDataType::kDataType: {
    auto type = parse_from_ir_data_type(data_type);
    Expr e(graph, ctx, params, expr, VarType::kPathVar);
    switch (type) {
    case RTAnyType::kI64Value: {
      return _make_project_expr<int64_t>(std::move(e), alias, ctx);
    } break;
    case RTAnyType::kI32Value: {
      return _make_project_expr<int32_t>(std::move(e), alias, ctx);
    } break;
    case RTAnyType::kU32Value: {
      return _make_project_expr<uint32_t>(std::move(e), alias, ctx);
    } break;
    case RTAnyType::kF32Value: {
      return _make_project_expr<float>(std::move(e), alias, ctx);
    } break;
    case RTAnyType::kF64Value: {
      return _make_project_expr<double>(std::move(e), alias, ctx);
    } break;
    case RTAnyType::kBoolValue: {
      return _make_project_expr<bool>(std::move(e), alias, ctx);
    } break;
    case RTAnyType::kStringValue: {
      return _make_project_expr<std::string_view>(std::move(e), alias, ctx);
    } break;
    case RTAnyType::kDateTime: {
      return _make_project_expr<DateTime>(std::move(e), alias, ctx);
    } break;
    case RTAnyType::kTimestamp: {
      return _make_project_expr<TimeStamp>(std::move(e), alias, ctx);
    } break;
    case RTAnyType::kDate: {
      return _make_project_expr<Date>(std::move(e), alias, ctx);
    } break;
    case RTAnyType::kInterval: {
      return _make_project_expr<Interval>(std::move(e), alias, ctx);
    } break;

    // compiler bug here
    case RTAnyType::kUnknown: {
      return make_project_expr_without_data_type(expr, alias, graph, ctx,
                                                 params);
    } break;
    case RTAnyType::kU64Value: {
      return _make_project_expr<uint64_t>(std::move(e), alias, ctx);
    } break;
    case RTAnyType::kList: {
      return make_project_expr_without_data_type(expr, alias, graph, ctx,
                                                 params);
    }
    default: {
      return make_project_expr_without_data_type(expr, alias, graph, ctx,
                                                 params);
    }
    }
  }
  case common::IrDataType::kGraphType: {
    const common::GraphDataType& graph_data_type = data_type.graph_type();
    common::GraphDataType_GraphElementOpt elem_opt =
        graph_data_type.element_opt();
    int label_num = graph_data_type.graph_data_type_size();
    if (elem_opt == common::GraphDataType_GraphElementOpt::
                        GraphDataType_GraphElementOpt_VERTEX) {
      if (label_num == 1) {
        label_t v_label = static_cast<label_t>(
            graph_data_type.graph_data_type(0).label().label());

        Expr e(graph, ctx, params, expr, VarType::kPathVar);
        SLVertexCollector collector(v_label);
        collector.builder.reserve(ctx.row_num());
        return std::make_unique<
            ProjectExpr<typename SLVertexCollector::EXPR, SLVertexCollector>>(
            std::move(e), collector, alias);

      } else if (label_num > 1) {
        Expr e(graph, ctx, params, expr, VarType::kPathVar);
        MLVertexCollector collector;
        collector.builder.reserve(ctx.row_num());
        return std::make_unique<
            ProjectExpr<typename MLVertexCollector::EXPR, MLVertexCollector>>(
            std::move(e), collector, alias);

      } else {
        LOG(INFO) << "unexpected type";
      }
    } else if (elem_opt == common::GraphDataType_GraphElementOpt::
                               GraphDataType_GraphElementOpt_EDGE) {
      Expr e(graph, ctx, params, expr, VarType::kPathVar);
      EdgeCollector collector;
      return std::make_unique<
          ProjectExpr<typename EdgeCollector::EXPR, EdgeCollector>>(
          std::move(e), collector, alias);
    } else {
      LOG(INFO) << "unexpected type";
    }
  } break;
  case common::IrDataType::TYPE_NOT_SET: {
    return make_project_expr_without_data_type(expr, alias, graph, ctx, params);
  } break;

  default:
    LOG(INFO) << "unexpected type" << data_type.DebugString();
    break;
  }
  return nullptr;
}

std::unique_ptr<ProjectExprBase> parse_special_expr(
    const common::Expression& expr, int alias, const GraphReadInterface& graph,
    const Context& ctx, const std::map<std::string, std::string>& params);

inline std::unique_ptr<ProjectExprBase> create_project_expr(
    const common::Expression& expr, int alias,
    const std::optional<common::IrDataType>& data_type,
    const GraphReadInterface& graph, const Context& ctx,
    const std::map<std::string, std::string>& params) {
  auto func = parse_special_expr(expr, alias, graph, ctx, params);
  if (func != nullptr) {
    return func;
  }
  if (data_type.has_value() &&
      data_type.value().type_case() != common::IrDataType::TYPE_NOT_SET) {
    auto func =
        make_project_expr(expr, data_type.value(), alias, graph, ctx, params);
    if (func != nullptr) {
      return func;
    }
  }
  return make_project_expr_without_data_type(expr, alias, graph, ctx, params);
}

inline std::unique_ptr<ProjectExprBase> create_project_expr(
    const common::Expression& expr, int alias,
    const std::optional<common::IrDataType>& data_type,
    const GraphUpdateInterface& graph, const Context& ctx,
    const std::map<std::string, std::string>& params) {
  if (data_type.has_value() &&
      data_type.value().type_case() != common::IrDataType::TYPE_NOT_SET) {
    auto func =
        make_project_expr(expr, data_type.value(), alias, graph, ctx, params);
    if (func != nullptr) {
      return func;
    }
  }
  return make_project_expr_without_data_type(expr, alias, graph, ctx, params);
}

template <typename GraphInterface>
gs::result<Context> ProjectEvalImpl(
    const GraphInterface& graph,
    const std::map<std::string, std::string>& params, Context&& ctx,
    const std::vector<
        std::tuple<common::Expression, int, std::optional<common::IrDataType>>>&
        exprs_infos,
    const std::vector<std::pair<int, std::set<int>>>& dependencies,
    bool is_append) {
  std::vector<std::unique_ptr<ProjectExprBase>> exprs;
  std::vector<std::shared_ptr<Arena>> arenas;
  if (!dependencies.empty()) {
    arenas.resize(ctx.col_num(), nullptr);
    for (size_t i = 0; i < ctx.col_num(); ++i) {
      if (ctx.get(i)) {
        arenas[i] = ctx.get(i)->get_arena();
      }
    }
  }
  for (size_t i = 0; i < exprs_infos.size(); ++i) {
    const auto& [expr, alias, data_type] = exprs_infos[i];
    exprs.push_back(
        create_project_expr(expr, alias, data_type, graph, ctx, params));
  }

  auto ret = Project::project(std::move(ctx), exprs, is_append);
  if (!ret) {
    return ret;
  }

  for (auto& [idx, deps] : dependencies) {
    std::shared_ptr<Arena> arena = std::make_shared<Arena>();
    auto arena1 = ret.value().get(idx)->get_arena();
    if (arena1) {
      arena->emplace_back(std::make_unique<ArenaRef>(arena1));
    }
    for (auto& dep : deps) {
      if (arenas[dep]) {
        arena->emplace_back(std::make_unique<ArenaRef>(arenas[dep]));
      }
    }
    ret.value().get(idx)->set_arena(arena);
  }

  return ret;
}
}  // namespace ops
}  // namespace runtime
}  // namespace gs

#endif  // EXECUTION_EXECUTE_OPS_UTILS_PROJECT_UTILS_H_
