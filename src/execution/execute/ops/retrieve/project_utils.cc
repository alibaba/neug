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

#include "neug/execution/execute/ops/retrieve/project_utils.h"
#include "neug/execution/expression/special_predicates.h"

namespace neug {
namespace execution {
namespace ops {

/**
 * Expressions
 */
struct DummyGetter : public ProjectExprBase {
  DummyGetter(int from, int to) : from_(from), to_(to) {}
  std::shared_ptr<IContextColumn> evaluate(const Context& ctx) override {
    return ctx.get(from_);
  }

  bool order_by_limit(const Context& ctx, bool asc, size_t limit,
                      std::vector<size_t>& offsets) const override {
    return ctx.get(from_)->order_by_limit(asc, limit, offsets);
  }

  int from_;
  int to_;
};

template <typename T>
struct VertexPropertyExpr : public ProjectExprBase {
  using V = std::conditional_t<std::is_same<T, std::string_view>::value,
                               std::string, T>;
  VertexPropertyExpr(const IStorageInterface& igraph, int tag,
                     const std::string& property_name)
      : graph(dynamic_cast<const StorageReadInterface&>(igraph)),
        tag_(tag),
        property_name_(property_name) {}

  std::shared_ptr<IContextColumn> evaluate(const Context& ctx) override {
    auto col = ctx.get(tag_);
    if (col->is_optional() ||
        col->column_type() != ContextColumnType::kVertex) {
      return nullptr;
    }
    const auto& vertex_col = dynamic_cast<const IVertexColumn&>(*col);
    std::vector<std::shared_ptr<StorageReadInterface::vertex_column_t<T>>>
        property_columns;
    const auto& labels = vertex_col.get_labels_set();
    for (auto label : labels) {
      auto prop_col =
          std::dynamic_pointer_cast<StorageReadInterface::vertex_column_t<T>>(
              graph.GetVertexPropColumn(label, property_name_));
      if (label >= property_columns.size()) {
        property_columns.resize(label + 1);
      }
      if (!prop_col) {
        return nullptr;
      }
      property_columns[label] = prop_col;
    }
    ValueColumnBuilder<V> builder;
    builder.reserve(ctx.row_num());
    foreach_vertex(vertex_col, [&](size_t idx, label_t label, vid_t vid) {
      auto prop_col = property_columns[label];
      if constexpr (std::is_same_v<T, std::string_view>) {
        auto sv = prop_col->get_view(vid);
        builder.push_back_opt(std::string(sv));
      } else {
        builder.push_back_opt(prop_col->get_view(vid));
      }
    });
    return builder.finish();
  }

  bool order_by_limit(const Context& ctx, bool asc, size_t limit,
                      std::vector<size_t>& indices) const override {
    auto col = ctx.get(tag_);
    if (col->is_optional() ||
        col->column_type() != ContextColumnType::kVertex) {
      return false;
    }
    const auto vertex_col = std::dynamic_pointer_cast<IVertexColumn>(col);
    const auto& labels = vertex_col->get_labels_set();
    for (auto label : labels) {
      auto prop_col =
          std::dynamic_pointer_cast<StorageReadInterface::vertex_column_t<T>>(
              graph.GetVertexPropColumn(label, property_name_));
      if (!prop_col) {
        return false;
      }
    }
    return vertex_property_topN(asc, limit, vertex_col, graph, property_name_,
                                indices);
  }

 private:
  const StorageReadInterface& graph;
  int tag_;
  std::string property_name_;
};
template <typename CMP_T, typename RESULT_T>
struct CaseWhenExpr : public ProjectExprBase {
  using V = std::conditional_t<std::is_same<RESULT_T, std::string_view>::value,
                               std::string, RESULT_T>;
  CaseWhenExpr(const IStorageInterface& igraph, int tag,
               const std::string& property_name, std::vector<Value>&& targets,
               RESULT_T then_value, RESULT_T else_value)
      : graph(dynamic_cast<const StorageReadInterface&>(igraph)),
        tag_(tag),
        property_name_(property_name),
        targets(std::move(targets)),
        then_value(then_value),
        else_value(else_value) {}

  bool check_valid(const IVertexColumn& vertex_col) {
    auto labels = vertex_col.get_labels_set();
    using T = typename CMP_T::data_t;
    for (auto label : labels) {
      auto prop_col =
          std::dynamic_pointer_cast<StorageReadInterface::vertex_column_t<T>>(
              graph.GetVertexPropColumn(label, property_name_));
      if (!prop_col) {
        return false;
      }
    }
    return true;
  }
  std::shared_ptr<IContextColumn> evaluate(const Context& ctx) override {
    auto col = ctx.get(tag_);
    if (col->is_optional() ||
        col->column_type() != ContextColumnType::kVertex) {
      return nullptr;
    }
    const auto& vertex_col = dynamic_cast<const IVertexColumn&>(*col);
    using T = typename CMP_T::data_t;
    std::vector<T> values;
    for (auto& val : targets) {
      if constexpr (std::is_same_v<T, std::string_view>) {
        std::string_view sw = StringValue::Get(val);
        values.push_back(sw);
      } else {
        values.push_back(val.template GetValue<T>());
      }
    }
    CMP_T cmp;
    cmp.reset(values);
    auto labels = vertex_col.get_labels_set();
    if (!check_valid(vertex_col)) {
      return nullptr;
    }
    if (labels.size() == 1) {
      label_t label = *labels.begin();
      using GETTER_T = SLVertexPropertyGetter<typename CMP_T::data_t>;
      GETTER_T getter(graph, label, property_name_);
      using PRED_T = VertexPropertyCmpPredicate<T, GETTER_T, CMP_T>;
      PRED_T pred(getter, cmp);
      return eval_impl(vertex_col, std::move(pred));
    } else {
      using GETTER_T = MLVertexPropertyGetter<T>;
      GETTER_T getter(graph, property_name_);
      using PRED_T = VertexPropertyCmpPredicate<T, GETTER_T, CMP_T>;
      PRED_T pred(getter, cmp);
      return eval_impl(vertex_col, std::move(pred));
    }
  }

  bool order_by_limit(const Context& ctx, bool asc, size_t limit,
                      std::vector<size_t>& indices) const override {
    return false;
  }

 private:
  template <typename COL_T, typename PRED_T>
  std::shared_ptr<IContextColumn> eval_impl(const COL_T& vertex_col,
                                            PRED_T&& pred) {
    ValueColumnBuilder<V> builder;
    size_t num_rows = vertex_col.size();
    builder.reserve(num_rows);
    for (size_t i = 0; i < num_rows; ++i) {
      auto v = vertex_col.get_vertex(i);
      if (pred(v.label_, v.vid_)) {
        builder.push_back_opt(then_value);
      } else {
        builder.push_back_opt(else_value);
      }
    }
    return builder.finish();
  }
  const StorageReadInterface& graph;
  int tag_;
  std::string property_name_;
  std::vector<Value> targets;
  RESULT_T then_value;
  RESULT_T else_value;
};

struct GeneralExpr : public ProjectExprBase {
  using V = Value;
  GeneralExpr(const IStorageInterface& igraph,
              std::unique_ptr<BindedExprBase>&& expr, const DataType& type)
      : graph(igraph), expr(std::move(expr)), type(type) {}

  std::shared_ptr<IContextColumn> evaluate(const Context& ctx) override {
    auto column_builder = ColumnsUtils::create_builder(type);
    column_builder->reserve(ctx.row_num());
    const auto& e = expr->Cast<RecordExprBase>();

    for (size_t i = 0; i < ctx.row_num(); ++i) {
      const auto& val = e.eval_record(ctx, i);
      if (val.IsNull()) {
        column_builder->push_back_null();
      } else {
        column_builder->push_back_elem(val);
      }
    }
    return column_builder->finish();
  }

  bool order_by_limit(const Context& ctx, bool asc, size_t limit,
                      std::vector<size_t>& indices) const override {
    return false;
  }

  const IStorageInterface& graph;
  std::unique_ptr<BindedExprBase> expr;
  DataType type;
};

struct DummyGetterBuilder : public ProjectExprBuilderBase {
  DummyGetterBuilder(int from, int to) : from_(from), to_(to) {}
  std::unique_ptr<ProjectExprBase> build(const IStorageInterface& graph,
                                         const ParamsMap& params) override {
    return std::make_unique<DummyGetter>(from_, to_);
  }
  int alias() const override { return to_; }
  int from_;
  int to_;
};

template <typename T>
struct VertexPropertyExprBuilder : public ProjectExprBuilderBase {
  VertexPropertyExprBuilder(int tag, const std::string& property_name,
                            int alias)
      : tag_(tag), property_name_(property_name), alias_(alias) {}
  std::unique_ptr<ProjectExprBase> build(const IStorageInterface& graph,
                                         const ParamsMap& params) override {
    return std::make_unique<VertexPropertyExpr<T>>(graph, tag_, property_name_);
  }

  int alias() const override { return alias_; }

  int tag_;
  std::string property_name_;
  int alias_;
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

  std::unique_ptr<ProjectExprBase> build(const IStorageInterface& igraph,
                                         const ParamsMap& params) override {
    const auto& graph = dynamic_cast<const StorageReadInterface&>(igraph);

    std::vector<Value> values;
    for (auto& param_name : param_names_) {
      values.push_back(params.at(param_name));
    }
    return std::make_unique<CaseWhenExpr<CMP_T, THEN_T>>(
        graph, tag_, property_name_, std::move(values), then_value_,
        else_value_);
  }

  int alias() const override { return alias_; }

  std::vector<std::string> param_names_;
  THEN_T then_value_;
  THEN_T else_value_;
  int tag_;
  std::string property_name_;
  int alias_;
};

struct GeneralProjectExprBuilder : public ProjectExprBuilderBase {
  GeneralProjectExprBuilder(int alias, std::unique_ptr<ExprBase>&& expr_ptr)
      : alias_(alias), expr_ptr_(std::move(expr_ptr)) {}
  std::unique_ptr<ProjectExprBase> build(const IStorageInterface& graph,
                                         const ParamsMap& params) override;

  bool is_general() const override { return true; }

  int alias() const override { return alias_; }

  int alias_;
  std::unique_ptr<ExprBase> expr_ptr_;
};

std::unique_ptr<ProjectExprBase> GeneralProjectExprBuilder::build(
    const IStorageInterface& graph, const ParamsMap& params) {
  auto expr_ptr = expr_ptr_->bind(&graph, params);
  auto type = expr_ptr->type();
  return std::make_unique<GeneralExpr>(graph, std::move(expr_ptr), type);
}

bool is_exchange_index(const common::Expression& expr, int& tag) {
  if (expr.operators().size() == 1 &&
      expr.operators(0).item_case() == common::ExprOpr::kVar) {
    auto var = expr.operators(0).var();
    tag = -1;
    if (var.has_property()) {
      return false;
    }

    if (var.has_tag()) {
      tag = var.tag().id();
    }
    return true;
  }
  return false;
}

/**
 * Pattern matching for special expressions
 */
bool is_check_property_in_range(const common::Expression& expr, int& tag,
                                std::string& name, std::string& lower,
                                std::string& upper, common::Value& then_value,
                                common::Value& else_value) {
  if (expr.operators_size() == 1 &&
      expr.operators(0).item_case() == common::ExprOpr::kCase) {
    auto opr = expr.operators(0).case_();
    if (opr.when_then_expressions_size() != 1) {
      return false;
    }
    auto when = opr.when_then_expressions(0).when_expression();
    if (when.operators_size() != 7) {
      return false;
    }
    {
      if (!when.operators(0).has_var()) {
        return false;
      }
      auto var = when.operators(0).var();
      if (!var.has_tag()) {
        return false;
      }
      tag = var.tag().id();
      if (!var.has_property()) {
        return false;
      }
      if (!var.property().has_key()) {
        return false;
      }
      name = var.property().key().name();
      if (name == "label") {
        return false;
      }
    }
    {
      auto op = when.operators(1);
      if (op.item_case() != common::ExprOpr::kLogical ||
          op.logical() != common::GE) {
        return false;
      }
    }
    auto lower_param = when.operators(2);
    if (lower_param.item_case() != common::ExprOpr::kParam) {
      return false;
    }
    lower = lower_param.param().name();
    {
      auto op = when.operators(3);
      if (op.item_case() != common::ExprOpr::kLogical ||
          op.logical() != common::AND) {
        return false;
      }
    }
    {
      if (!when.operators(4).has_var()) {
        return false;
      }
      auto var = when.operators(4).var();
      if (!var.has_tag()) {
        return false;
      }
      if (var.tag().id() != tag) {
        return false;
      }
      if (!var.has_property()) {
        return false;
      }
      if (!var.property().has_key() && name != var.property().key().name()) {
        return false;
      }
    }

    auto op = when.operators(5);
    if (op.item_case() != common::ExprOpr::kLogical ||
        op.logical() != common::LT) {
      return false;
    }
    auto upper_param = when.operators(6);
    if (upper_param.item_case() != common::ExprOpr::kParam) {
      return false;
    }
    upper = upper_param.param().name();
    auto then = opr.when_then_expressions(0).then_result_expression();
    if (then.operators_size() != 1) {
      return false;
    }
    if (!then.operators(0).has_const_()) {
      return false;
    }
    then_value = then.operators(0).const_();
    auto else_expr = opr.else_result_expression();
    if (else_expr.operators_size() != 1) {
      return false;
    }
    if (!else_expr.operators(0).has_const_()) {
      return false;
    }
    else_value = else_expr.operators(0).const_();
    if (then_value.item_case() != else_value.item_case()) {
      return false;
    }

    return true;
  }
  return false;
}

bool is_check_property_cmp(const common::Expression& expr, int& tag,
                           std::string& name, std::string& target,
                           common::Value& then_value, common::Value& else_value,
                           SPPredicateType& ptype) {
  if (expr.operators_size() == 1 &&
      expr.operators(0).item_case() == common::ExprOpr::kCase) {
    auto opr = expr.operators(0).case_();
    if (opr.when_then_expressions_size() != 1) {
      return false;
    }
    auto when = opr.when_then_expressions(0).when_expression();
    if (when.operators_size() != 3) {
      return false;
    }
    {
      if (!when.operators(0).has_var()) {
        return false;
      }
      auto var = when.operators(0).var();
      if (!var.has_tag()) {
        return false;
      }
      tag = var.tag().id();
      if (!var.has_property()) {
        return false;
      }
      if (!var.property().has_key()) {
        return false;
      }
      name = var.property().key().name();
      if (name == "label") {
        return false;
      }
    }
    {
      auto op = when.operators(1);
      if (op.item_case() != common::ExprOpr::kLogical) {
        return false;
      }
      switch (op.logical()) {
      case common::LT:
        ptype = SPPredicateType::kPropertyLT;
        break;
      case common::LE:
        ptype = SPPredicateType::kPropertyLE;
        break;
      case common::GT:
        ptype = SPPredicateType::kPropertyGT;
        break;
      case common::GE:
        ptype = SPPredicateType::kPropertyGE;
        break;
      case common::EQ:
        ptype = SPPredicateType::kPropertyEQ;
        break;
      case common::NE:
        ptype = SPPredicateType::kPropertyNE;
        break;
      default:
        return false;
      }
    }
    auto upper_param = when.operators(2);
    if (upper_param.item_case() != common::ExprOpr::kParam) {
      return false;
    }
    target = upper_param.param().name();
    auto then = opr.when_then_expressions(0).then_result_expression();
    if (then.operators_size() != 1) {
      return false;
    }
    if (!then.operators(0).has_const_()) {
      return false;
    }
    then_value = then.operators(0).const_();
    auto else_expr = opr.else_result_expression();
    if (else_expr.operators_size() != 1) {
      return false;
    }
    if (!else_expr.operators(0).has_const_()) {
      return false;
    }
    else_value = else_expr.operators(0).const_();
    if (then_value.item_case() != else_value.item_case()) {
      return false;
    }

    return true;
  }
  return false;
}

bool is_property_extract(const common::Expression& expr, int& tag,
                         std::string& name, DataType& type) {
  if (expr.operators_size() == 1 &&
      expr.operators(0).item_case() == common::ExprOpr::kVar) {
    auto var = expr.operators(0).var();
    tag = -1;
    if (!var.has_property()) {
      return false;
    }

    if (var.has_tag()) {
      tag = var.tag().id();
    }
    if (var.has_property() && var.property().has_key()) {
      name = var.property().key().name();
      if (name == "label") {
        return false;
      }
      if (var.has_node_type()) {
        type = parse_from_ir_data_type(var.node_type());
      } else {
        return false;
      }
      if (type.id() == DataTypeId::kUnknown) {
        return false;
      }
      // only support pod type
      if (type.id() == DataTypeId::kTimestampMs ||
          type.id() == DataTypeId::kDate || type.id() == DataTypeId::kInt64 ||
          type.id() == DataTypeId::kInt32 || type.id() == DataTypeId::kFloat ||
          type.id() == DataTypeId::kUInt64 ||
          type.id() == DataTypeId::kUInt32 ||
          type.id() == DataTypeId::kDouble ||
          type.id() == DataTypeId::kVarchar) {
        return true;
      }
    }
  }
  return false;
}

std::unique_ptr<ProjectExprBuilderBase> create_dummy_getter_builder(
    const common::Expression& expr, int alias) {
  int tag = -1;
  if (is_exchange_index(expr, tag)) {
    return std::make_unique<DummyGetterBuilder>(tag, alias);
  }
  return nullptr;
}

std::unique_ptr<ProjectExprBuilderBase> create_vertex_property_expr_builder(
    const common::Expression& expr, int alias) {
  int tag;
  std::string name;
  DataType type;
  if (is_property_extract(expr, tag, name, type)) {
    switch (type.id()) {
#define TYPE_DISPATCHER(enum_val, type) \
  case DataTypeId::enum_val:            \
    return std::make_unique<VertexPropertyExprBuilder<type>>(tag, name, alias);
      FOR_EACH_DATA_TYPE_NO_STRING(TYPE_DISPATCHER)
      TYPE_DISPATCHER(kVarchar, std::string_view)
#undef TYPE_DISPATCHER
    default:
      return nullptr;
    }
  }
  return nullptr;
}

template <typename CMP_T>
std::unique_ptr<ProjectExprBuilderBase> create_case_when_builder_impl1(
    DataType then_type, const std::vector<std::string>& param_names,
    const common::Value& then_value, const common::Value& else_value, int tag,
    const std::string& property_name, int alias) {
  if (then_type.id() == DataTypeId::kInt64) {
    return std::make_unique<CaseWhenExprBuilder<CMP_T, int64_t>>(
        param_names, then_value.i64(), else_value.i64(), tag, property_name,
        alias);
  } else {
    LOG(ERROR) << "unsupported then type " << static_cast<int>(then_type.id());
    return nullptr;
  }
}

template <typename WHEN_T>
std::unique_ptr<ProjectExprBuilderBase> create_case_when_builder_impl0(
    SPPredicateType ptype, DataType then_type,
    const std::vector<std::string>& param_names,
    const common::Value& then_value, const common::Value& else_value, int tag,
    const std::string& property_name, int alias) {
  if (ptype == SPPredicateType::kPropertyBetween) {
    using CMP_T = BetweenCmp<WHEN_T>;
    return create_case_when_builder_impl1<CMP_T>(then_type, param_names,
                                                 then_value, else_value, tag,
                                                 property_name, alias);
  } else if (ptype == SPPredicateType::kPropertyEQ) {
    using CMP_T = EQCmp<WHEN_T>;
    return create_case_when_builder_impl1<CMP_T>(then_type, param_names,
                                                 then_value, else_value, tag,
                                                 property_name, alias);
  } else if (ptype == SPPredicateType::kPropertyGT) {
    using CMP_T = GTCmp<WHEN_T>;
    return create_case_when_builder_impl1<CMP_T>(then_type, param_names,
                                                 then_value, else_value, tag,
                                                 property_name, alias);
  } else if (ptype == SPPredicateType::kPropertyGE) {
    using CMP_T = GECmp<WHEN_T>;
    return create_case_when_builder_impl1<CMP_T>(then_type, param_names,
                                                 then_value, else_value, tag,
                                                 property_name, alias);
  } else if (ptype == SPPredicateType::kPropertyLT) {
    using CMP_T = LTCmp<WHEN_T>;
    return create_case_when_builder_impl1<CMP_T>(then_type, param_names,
                                                 then_value, else_value, tag,
                                                 property_name, alias);
  } else if (ptype == SPPredicateType::kPropertyLE) {
    using CMP_T = LECmp<WHEN_T>;
    return create_case_when_builder_impl1<CMP_T>(then_type, param_names,
                                                 then_value, else_value, tag,
                                                 property_name, alias);
  } else if (ptype == SPPredicateType::kPropertyNE) {
    using CMP_T = NECmp<WHEN_T>;
    return create_case_when_builder_impl1<CMP_T>(then_type, param_names,
                                                 then_value, else_value, tag,
                                                 property_name, alias);
  } else {
    LOG(ERROR) << "unsupported predicate type " << static_cast<int>(ptype);
    return nullptr;
  }
}

std::unique_ptr<ProjectExprBuilderBase> create_case_when_builder(
    const common::Expression& expr, int alias) {
  int tag;
  std::string name, lower, upper, target;
  common::Value then_value, else_value;

  SPPredicateType ptype = SPPredicateType::kUnknown;
  DataType when_type, then_type;
  std::vector<std::string> param_names;

  if (is_check_property_in_range(expr, tag, name, lower, upper, then_value,
                                 else_value)) {
    when_type = parse_from_ir_data_type(expr.operators(0)
                                            .case_()
                                            .when_then_expressions(0)
                                            .when_expression()
                                            .operators(2)
                                            .param()
                                            .data_type());
    ptype = SPPredicateType::kPropertyBetween;
    param_names.push_back(lower);
    param_names.push_back(upper);

    if (then_value.item_case() != else_value.item_case()) {
      LOG(ERROR) << "then and else value type mismatch"
                 << then_value.DebugString() << else_value.DebugString();
      return nullptr;
    }
    if (then_value.item_case() == common::Value::kI64) {
      then_type = DataType(DataTypeId::kInt64);
    } else {
      LOG(ERROR) << "unexpected then value type" << then_value.DebugString();
      return nullptr;
    }
  } else if (is_check_property_cmp(expr, tag, name, target, then_value,
                                   else_value, ptype)) {
    when_type = parse_from_ir_data_type(expr.operators(0)
                                            .case_()
                                            .when_then_expressions(0)
                                            .when_expression()
                                            .operators(2)
                                            .param()
                                            .data_type());
    param_names.push_back(target);
    if (then_value.item_case() != else_value.item_case()) {
      LOG(ERROR) << "then and else value type mismatch"
                 << then_value.DebugString() << else_value.DebugString();
      return nullptr;
    }
    if (then_value.item_case() == common::Value::kI64) {
      then_type = DataType(DataTypeId::kInt64);
    } else {
      LOG(ERROR) << "unexpected then value type" << then_value.DebugString();
      return nullptr;
    }
  } else {
    return nullptr;
  }
  switch (when_type.id()) {
#define TYPE_DISPATCHER(enum_val, T)                                        \
  case DataTypeId::enum_val:                                                \
    return create_case_when_builder_impl0<T>(ptype, then_type, param_names, \
                                             then_value, else_value, tag,   \
                                             name, alias);
    TYPE_DISPATCHER(kInt32, int32_t)
    TYPE_DISPATCHER(kInt64, int64_t)
    TYPE_DISPATCHER(kTimestampMs, DateTime)
    TYPE_DISPATCHER(kVarchar, std::string_view)
#undef TYPE_DISPATCHER
  default:
    LOG(ERROR) << "unsupported when type " << static_cast<int>(when_type.id());
    return nullptr;
  }
}

void create_project_expr_builders(
    std::vector<std::tuple<common::Expression, int,
                           std::unique_ptr<ExprBase>>>&& exprs_infos,
    std::vector<std::unique_ptr<ProjectExprBuilderBase>>& expr_builders,
    std::vector<std::unique_ptr<ProjectExprBuilderBase>>&
        fallback_expr_builders) {
  for (auto& expr_info : exprs_infos) {
    auto& expr = std::get<0>(expr_info);
    int alias = std::get<1>(expr_info);
    fallback_expr_builders.push_back(
        std::make_unique<GeneralProjectExprBuilder>(
            alias, std::move(std::get<2>(expr_info))));
    auto builder = create_dummy_getter_builder(expr, alias);
    if (builder != nullptr) {
      expr_builders.push_back(std::move(builder));
      continue;
    }
    builder = create_vertex_property_expr_builder(expr, alias);
    if (builder != nullptr) {
      expr_builders.push_back(std::move(builder));
      continue;
    }
    builder = create_case_when_builder(expr, alias);
    if (builder != nullptr) {
      expr_builders.push_back(std::move(builder));
      continue;
    }
    expr_builders.push_back(nullptr);
  }
}

}  // namespace ops
}  // namespace execution
}  // namespace neug
