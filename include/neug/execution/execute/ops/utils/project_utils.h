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

#ifndef INCLUDE_NEUG_EXECUTION_EXECUTE_OPS_UTILS_PROJECT_UTILS_H_
#define INCLUDE_NEUG_EXECUTION_EXECUTE_OPS_UTILS_PROJECT_UTILS_H_

#include <map>
#include <memory>
#include <set>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "neug/execution/common/columns/edge_columns.h"
#include "neug/execution/common/columns/i_context_column.h"
#include "neug/execution/common/columns/value_columns.h"
#include "neug/execution/common/columns/vertex_columns.h"
#include "neug/execution/common/context.h"
#include "neug/execution/common/graph_interface.h"
#include "neug/execution/common/operators/retrieve/project.h"
#include "neug/execution/execute/ops/retrieve/order_by_utils.h"
#include "neug/execution/utils/expr.h"
#include "neug/execution/utils/special_predicates.h"
#include "neug/execution/utils/var.h"
#include "neug/utils/runtime/rt_any.h"

namespace gs {
namespace runtime {
namespace ops {

bool is_exchange_index(const common::Expression& expr, int& tag);

template <typename EXPR>
struct PropertyValueCollector {
  explicit PropertyValueCollector(const Context& ctx) {
    builder.reserve(ctx.row_num());
  }
  void collect(const EXPR& expr, size_t idx) {
    builder.push_back_opt(expr(idx));
  }
  auto get() { return builder.finish(); }

  ValueColumnBuilder<typename EXPR::V> builder;
};

template <typename VertexColumn, typename T>
struct SLPropertyExpr {
  using V = T;
  SLPropertyExpr(const StorageReadInterface& graph, const VertexColumn& column,
                 const std::string& property_name)
      : column(column) {
    auto labels = column.get_labels_set();
    auto& label = *labels.begin();
    property = graph.GetVertexPropColumn<T>(label, property_name);
    is_optional_ = (property == nullptr);
  }
  inline T operator()(size_t idx) const {
    auto v = column.get_vertex(idx);
    return property->get_view(v.vid_);
  }
  bool is_optional() const { return is_optional_; }
  bool is_optional_;
  const VertexColumn& column;
  std::shared_ptr<StorageReadInterface::vertex_column_t<T>> property;
};

template <typename VertexColumn, typename T>
struct MLPropertyExpr {
  using V = T;
  MLPropertyExpr(const StorageReadInterface& graph, const VertexColumn& vertex,
                 const std::string& property_name)
      : vertex(vertex) {
    auto labels = vertex.get_labels_set();
    int label_num = graph.schema().vertex_label_num();
    property.resize(label_num);
    is_optional_ = false;
    for (auto label : labels) {
      property[label] = graph.GetVertexPropColumn<T>(label, property_name);
      if (property[label] == nullptr) {
        is_optional_ = true;
      }
    }
  }
  bool is_optional() const { return is_optional_; }
  inline T operator()(size_t idx) const {
    auto v = vertex.get_vertex(idx);
    return property[v.label_]->get_view(v.vid_);
  }
  const VertexColumn& vertex;
  std::vector<std::shared_ptr<StorageReadInterface::vertex_column_t<T>>>
      property;

  bool is_optional_;
};

struct ProjectExprBuilderBase {
  virtual ~ProjectExprBuilderBase() = default;
  virtual std::unique_ptr<ProjectExprBase> build(
      const StorageReadInterface& graph, const Context& ctx,
      const std::map<std::string, std::string>& params) = 0;
  virtual bool is_general() const { return false; }
};

std::unique_ptr<ProjectExprBuilderBase> create_dummy_getter_builder(
    const common::Expression& expr, int alias);
std::unique_ptr<ProjectExprBuilderBase> create_vertex_property_expr_builder(
    const common::Expression& expr, int alias);
std::unique_ptr<ProjectExprBuilderBase> create_case_when_builder(
    const common::Expression& expr, int alias);

struct DummyGetterBuilder : public ProjectExprBuilderBase {
  DummyGetterBuilder(int from, int to) : from_(from), to_(to) {}
  std::unique_ptr<ProjectExprBase> build(
      const StorageReadInterface& graph, const Context& ctx,
      const std::map<std::string, std::string>& params) override {
    return std::make_unique<DummyGetter>(from_, to_);
  }
  int from_;
  int to_;
};

template <typename T>
struct VertexPropertyExprBuilder : public ProjectExprBuilderBase {
  VertexPropertyExprBuilder(int tag, const std::string& property_name,
                            int alias)
      : tag_(tag), property_name_(property_name), alias_(alias) {}
  std::unique_ptr<ProjectExprBase> build(
      const StorageReadInterface& graph, const Context& ctx,
      const std::map<std::string, std::string>& params) override {
    auto col = ctx.get(tag_);
    if (col->column_type() != ContextColumnType::kVertex) {
      return nullptr;
    }
    if (col->is_optional()) {
      return nullptr;
    }

    auto vertex_col = std::dynamic_pointer_cast<IVertexColumn>(col);
    if (vertex_col->vertex_column_type() == VertexColumnType::kSingle) {
      auto casted_col = std::dynamic_pointer_cast<SLVertexColumn>(col);
      SLPropertyExpr<SLVertexColumn, T> expr(graph, *casted_col,
                                             property_name_);
      if (expr.is_optional()) {
        return nullptr;
      }
      PropertyValueCollector<SLPropertyExpr<SLVertexColumn, T>> collector(ctx);
      return std::make_unique<ProjectExpr<
          SLPropertyExpr<SLVertexColumn, T>,
          PropertyValueCollector<SLPropertyExpr<SLVertexColumn, T>>>>(
          std::move(expr), collector, alias_);
    } else if (vertex_col->vertex_column_type() ==
               VertexColumnType::kMultiSegment) {
      auto casted_col = std::dynamic_pointer_cast<MSVertexColumn>(col);
      MLPropertyExpr<MSVertexColumn, T> expr(graph, *casted_col,
                                             property_name_);
      if (expr.is_optional()) {
        return nullptr;
      }
      PropertyValueCollector<MLPropertyExpr<MSVertexColumn, T>> collector(ctx);
      return std::make_unique<ProjectExpr<
          MLPropertyExpr<MSVertexColumn, T>,
          PropertyValueCollector<MLPropertyExpr<MSVertexColumn, T>>>>(
          std::move(expr), collector, alias_);

    } else {
      CHECK(vertex_col->vertex_column_type() == VertexColumnType::kMultiple);
      auto casted_col = std::dynamic_pointer_cast<MLVertexColumn>(col);
      MLPropertyExpr<MLVertexColumn, T> expr(graph, *casted_col,
                                             property_name_);
      if (expr.is_optional()) {
        return nullptr;
      }
      PropertyValueCollector<MLPropertyExpr<MLVertexColumn, T>> collector(ctx);
      return std::make_unique<ProjectExpr<
          MLPropertyExpr<MLVertexColumn, T>,
          PropertyValueCollector<MLPropertyExpr<MLVertexColumn, T>>>>(
          std::move(expr), collector, alias_);
    }
  }

  int tag_;
  std::string property_name_;
  int alias_;
};

template <typename VERTEX_COL_PTR, typename SP_PRED_T, typename RESULT_T>
struct SPOpr {
  using V = RESULT_T;
  SPOpr(const VERTEX_COL_PTR& vertex_col, SP_PRED_T&& pred, RESULT_T then_value,
        RESULT_T else_value)
      : vertex_col(vertex_col),
        pred(std::move(pred)),
        then_value(then_value),
        else_value(else_value) {}
  inline RESULT_T operator()(size_t idx) const {
    auto v = vertex_col->get_vertex(idx);
    if (pred(v.label_, v.vid_, idx)) {
      return then_value;
    } else {
      return else_value;
    }
  }

  VERTEX_COL_PTR vertex_col;
  SP_PRED_T pred;
  RESULT_T then_value;
  RESULT_T else_value;
};

template <typename EXPR, typename RESULT_T>
struct CaseWhenCollector {
  explicit CaseWhenCollector(const Context& ctx) : ctx_(ctx) {
    builder.reserve(ctx.row_num());
  }
  void collect(const EXPR& expr, size_t idx) {
    builder.push_back_opt(expr(idx));
  }
  auto get() { return builder.finish(); }
  const Context& ctx_;
  ValueColumnBuilder<RESULT_T> builder;
};

template <typename CMP_T, typename THEN_T>
struct CaseWhenExprBuilder : public ProjectExprBuilderBase {
  CaseWhenExprBuilder(const std::vector<std::string>& param_names,
                      const THEN_T& then_value, const THEN_T& else_value,
                      int tag, const std::string& property_name, int alias)
      : param_names_(param_names),
        then_value_(then_value),
        else_value_(else_value),
        tag_(tag),
        property_name_(property_name),
        alias_(alias) {}

  std::unique_ptr<ProjectExprBase> build(
      const StorageReadInterface& graph, const Context& ctx,
      const std::map<std::string, std::string>& params) override {
    auto col = ctx.get(tag_);
    if (col->column_type() != ContextColumnType::kVertex) {
      LOG(ERROR) << "Column with tag " << tag_ << " is not vertex column";
      return nullptr;
    }
    using T = typename CMP_T::data_t;
    std::vector<T> values;
    for (auto& param_name : param_names_) {
      values.push_back(
          TypedConverter<T>::typed_from_string(params.at(param_name)));
    }
    CMP_T cmp;
    cmp.reset(values);
    auto vertex_col = std::dynamic_pointer_cast<IVertexColumn>(col);
    auto labels = vertex_col->get_labels_set();
    if (labels.size() == 1) {
      label_t label = *labels.begin();
      using GETTER_T = SLVertexPropertyGetter<typename CMP_T::data_t>;
      GETTER_T getter(graph, label, property_name_);
      using PRED_T = VertexPropertyCmpPredicate<T, GETTER_T, CMP_T>;
      PRED_T pred(getter, cmp);
      auto casted_col = std::dynamic_pointer_cast<SLVertexColumn>(vertex_col);
      SPOpr sp(casted_col, std::move(pred), then_value_, else_value_);
      CaseWhenCollector<decltype(sp), THEN_T> collector(ctx);
      return std::make_unique<ProjectExpr<decltype(sp), decltype(collector)>>(
          std::move(sp), collector, alias_);
    } else {
      using GETTER_T = MLVertexPropertyGetter<T>;
      GETTER_T getter(graph, property_name_);
      using PRED_T = VertexPropertyCmpPredicate<T, GETTER_T, CMP_T>;
      PRED_T pred(getter, cmp);
      SPOpr sp(vertex_col, std::move(pred), then_value_, else_value_);
      CaseWhenCollector<decltype(sp), THEN_T> collector(ctx);
      return std::make_unique<ProjectExpr<decltype(sp), decltype(collector)>>(
          std::move(sp), collector, alias_);
    }
  }

  std::vector<std::string> param_names_;
  THEN_T then_value_;
  THEN_T else_value_;
  int tag_;
  std::string property_name_;
  int alias_;
};

struct GeneralProjectExprBuilder : public ProjectExprBuilderBase {
  GeneralProjectExprBuilder(const common::Expression& expr,
                            const std::optional<common::IrDataType>& data_type,
                            int alias)
      : expr_(expr), data_type_(data_type), alias_(alias) {}
  std::unique_ptr<ProjectExprBase> build(
      const StorageReadInterface& graph, const Context& ctx,
      const std::map<std::string, std::string>& params) override;

  bool is_general() const override { return true; }

  common::Expression expr_;
  std::optional<common::IrDataType> data_type_;
  int alias_;
};

template <typename T>
struct ValueCollector {
  struct ExprWrapper {
    using V = T;
    explicit ExprWrapper(Expr&& expr)
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

template <typename T>
struct OptionalValueCollector {
  struct OptionalExprWrapper {
    using V = std::optional<T>;
    explicit OptionalExprWrapper(Expr&& expr)
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
  explicit VertexExprWrapper(Expr&& expr) : expr(std::move(expr)) {}

  inline VertexRecord operator()(size_t idx) const {
    return expr.eval_path(idx, arena).as_vertex();
  }

  mutable Arena arena;

  Expr expr;
};

struct SLVertexCollector {
  using EXPR = VertexExprWrapper;
  explicit SLVertexCollector(label_t v_label) : builder(v_label) {}
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
  EdgeCollector() : builder({}) {}
  struct EdgeExprWrapper {
    using V = EdgeRecord;

    explicit EdgeExprWrapper(Expr&& expr) : expr(std::move(expr)) {}
    inline EdgeRecord operator()(size_t idx) const {
      return expr.eval_path(idx, arena).as_edge();
    }

    mutable Arena arena;
    Expr expr;
  };
  using EXPR = EdgeExprWrapper;
  void collect(const EXPR& expr, size_t idx) {
    auto e = expr(idx);
    int label_idx = builder.get_label_index(e.label);
    builder.push_back_opt(label_idx, e.src, e.dst, e.prop, e.dir);
  }
  auto get() { return builder.finish(); }
  BDMLEdgeColumnBuilder builder;
};

struct ListCollector {
  struct ListExprWrapper {
    using V = List;
    explicit ListExprWrapper(Expr&& expr)
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

static inline std::unique_ptr<ProjectExprBase>
make_project_expr_without_data_type(
    const common::Expression& expr, int alias,
    const StorageReadInterface& graph, const Context& ctx,
    const std::map<std::string, std::string>& params) {
  Expr e(&graph, ctx, params, expr, VarType::kPathVar);

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
  case RTAnyType::kInterval: {
    return _make_project_expr<Interval>(std::move(e), alias, ctx);
  } break;
  case RTAnyType::kVertex: {
    MLVertexCollector collector;
    collector.builder.reserve(ctx.row_num());
    return std::make_unique<
        ProjectExpr<typename MLVertexCollector::EXPR, MLVertexCollector>>(
        MLVertexCollector::EXPR(std::move(e)), collector, alias);
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
        EdgeCollector::EXPR(std::move(e)), collector, alias);
  } break;
  case RTAnyType::kTuple: {
    return _make_project_expr<Tuple>(std::move(e), alias, ctx);
  } break;
  case RTAnyType::kList: {
    ListCollector::EXPR expr(std::move(e));
    ListCollector collector(ctx, expr);
    return std::make_unique<
        ProjectExpr<typename ListCollector::EXPR, ListCollector>>(
        ListCollector::EXPR(std::move(expr)), collector, alias);
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

inline std::unique_ptr<ProjectExprBase> make_project_expr(
    const common::Expression& expr, const common::IrDataType& data_type,
    int alias, const StorageReadInterface& graph, const Context& ctx,
    const std::map<std::string, std::string>& params) {
  switch (data_type.type_case()) {
  case common::IrDataType::kDataType: {
    auto type = parse_from_ir_data_type(data_type);
    Expr e(&graph, ctx, params, expr, VarType::kPathVar);
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

        Expr e(&graph, ctx, params, expr, VarType::kPathVar);
        SLVertexCollector collector(v_label);
        collector.builder.reserve(ctx.row_num());
        return std::make_unique<
            ProjectExpr<typename SLVertexCollector::EXPR, SLVertexCollector>>(
            SLVertexCollector::EXPR(std::move(e)), collector, alias);

      } else if (label_num > 1) {
        Expr e(&graph, ctx, params, expr, VarType::kPathVar);
        MLVertexCollector collector;
        collector.builder.reserve(ctx.row_num());
        return std::make_unique<
            ProjectExpr<typename MLVertexCollector::EXPR, MLVertexCollector>>(
            MLVertexCollector::EXPR(std::move(e)), collector, alias);

      } else {
        LOG(INFO) << "unexpected type";
      }
    } else if (elem_opt == common::GraphDataType_GraphElementOpt::
                               GraphDataType_GraphElementOpt_EDGE) {
      Expr e(&graph, ctx, params, expr, VarType::kPathVar);
      EdgeCollector collector;
      return std::make_unique<
          ProjectExpr<typename EdgeCollector::EXPR, EdgeCollector>>(
          EdgeCollector::EXPR(std::move(e)), collector, alias);
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
    const common::Expression& expr, int alias,
    const StorageReadInterface& graph, const Context& ctx,
    const std::map<std::string, std::string>& params);

inline std::unique_ptr<ProjectExprBase> create_project_expr(
    const common::Expression& expr, int alias,
    const std::optional<common::IrDataType>& data_type,
    const StorageReadInterface& graph, const Context& ctx,
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

inline gs::result<Context> ProjectEvalImpl(
    const StorageReadInterface& graph,
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

#endif  // INCLUDE_NEUG_EXECUTION_EXECUTE_OPS_UTILS_PROJECT_UTILS_H_
