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
#include "neug/execution/utils/special_predicates.h"

namespace neug {
namespace runtime {
namespace ops {

std::unique_ptr<ProjectExprBase> GeneralProjectExprBuilder::build(
    const IStorageInterface& graph, const Context& ctx,
    const ParamsMap& params) {
  if (data_type_.has_value() &&
      data_type_.value().type_case() != common::IrDataType::TYPE_NOT_SET) {
    auto func = make_project_expr(expr_, data_type_.value(), alias_, graph, ctx,
                                  params);
    if (func != nullptr) {
      return func;
    }
  }
  return make_project_expr_without_data_type(expr_, alias_, graph, ctx, params);
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

std::unique_ptr<ProjectExprBase> create_sl_property_expr(
    const Context& ctx, const IStorageInterface& graph,
    const SLVertexColumn& column, const std::string& property_name,
    DataType type, int alias) {
  switch (type.id()) {
#define TYPE_DISPATCHER(enum_val, type)                                       \
  case DataTypeId::enum_val: {                                                \
    auto expr =                                                               \
        SLPropertyExpr<SLVertexColumn, type>(graph, column, property_name);   \
    if (expr.is_optional()) {                                                 \
      return nullptr;                                                         \
    }                                                                         \
    PropertyValueCollector<decltype(expr)> collector(ctx);                    \
    return std::make_unique<ProjectExpr<SLPropertyExpr<SLVertexColumn, type>, \
                                        decltype(collector)>>(                \
        std::move(expr), collector, alias);                                   \
  }
    FOR_EACH_DATA_TYPE_NO_STRING(TYPE_DISPATCHER)
    TYPE_DISPATCHER(kVarchar, std::string_view)
#undef TYPE_DISPATCHER

  default:
    LOG(ERROR) << "create_sl_property_expr: not implemented for type: "
               << static_cast<int>(type.id());
  }
  return nullptr;
}

template <typename VertexColumn>
std::unique_ptr<ProjectExprBase> create_ml_property_expr(
    const Context& ctx, const IStorageInterface& graph,
    const VertexColumn& column, const std::string& property_name, DataType type,
    int alias) {
  switch (type.id()) {
#define TYPE_DISPATCHER(enum_val, type)                                        \
  case DataTypeId::enum_val: {                                                 \
    auto expr =                                                                \
        MLPropertyExpr<VertexColumn, type>(graph, column, property_name);      \
    if (expr.is_optional()) {                                                  \
      return nullptr;                                                          \
    }                                                                          \
    PropertyValueCollector<decltype(expr)> collector(ctx);                     \
    return std::make_unique<                                                   \
        ProjectExpr<MLPropertyExpr<VertexColumn, type>, decltype(collector)>>( \
        std::move(expr), collector, alias);                                    \
  }
    FOR_EACH_DATA_TYPE_NO_STRING(TYPE_DISPATCHER)
    TYPE_DISPATCHER(kVarchar, std::string_view)
#undef TYPE_DISPATCHER
  default:
    LOG(ERROR) << "create_ml_property_expr: not implemented for type: "
               << static_cast<int>(type.id());
  }
  return nullptr;
}

template <typename PRED>
std::unique_ptr<ProjectExprBase> create_case_when_project(
    const Context& ctx, const std::shared_ptr<IVertexColumn>& vertex_col,
    PRED&& pred, const common::Value& then_value,
    const common::Value& else_value, int alias) {
  if (then_value.item_case() != else_value.item_case()) {
    return nullptr;
  }
  switch (then_value.item_case()) {
  case common::Value::kI32: {
    if (vertex_col->vertex_column_type() == VertexColumnType::kSingle) {
      auto typed_vertex_col =
          std::dynamic_pointer_cast<SLVertexColumn>(vertex_col);
      SPOpr opr(typed_vertex_col, std::move(pred), then_value.i32(),
                else_value.i32());
      auto collector = CaseWhenCollector<decltype(opr), int32_t>(ctx);
      return std::make_unique<ProjectExpr<decltype(opr), decltype(collector)>>(
          std::move(opr), collector, alias);
    } else {
      SPOpr opr(vertex_col, std::move(pred), then_value.i32(),
                else_value.i32());
      auto collector = CaseWhenCollector<decltype(opr), int32_t>(ctx);
      return std::make_unique<ProjectExpr<decltype(opr), decltype(collector)>>(
          std::move(opr), collector, alias);
    }
  }
  case common::Value::kI64: {
    SPOpr opr(vertex_col, std::move(pred), then_value.i64(), else_value.i64());
    auto collector = CaseWhenCollector<decltype(opr), int64_t>(ctx);
    return std::make_unique<ProjectExpr<decltype(opr), decltype(collector)>>(
        std::move(opr), collector, alias);
  }

  default:
    LOG(ERROR) << "Unsupported type for case when collector";
    return nullptr;
  }
}

template <typename T, typename CMP_T>
static std::unique_ptr<ProjectExprBase> create_sp_pred_case_when_impl(
    const Context& ctx, const IStorageInterface& graph,
    const std::shared_ptr<IVertexColumn>& vertex,
    const std::string& property_name, const CMP_T& cmp_val,
    const common::Value& then_value, const common::Value& else_value,
    int alias) {
  auto labels = vertex->get_labels_set();
  if (labels.size() == 1) {
    label_t label = *labels.begin();
    using GETTER_T = SLVertexPropertyGetter<typename CMP_T::data_t>;
    GETTER_T getter(graph, label, property_name);
    using PRED_T =
        VertexPropertyCmpPredicate<typename CMP_T::data_t, GETTER_T, CMP_T>;
    PRED_T pred(getter, cmp_val);
    return create_case_when_project(ctx, vertex, std::move(pred), then_value,
                                    else_value, alias);
  } else {
    using GETTER_T = MLVertexPropertyGetter<typename CMP_T::data_t>;
    GETTER_T getter(graph, property_name);
    using PRED_T =
        VertexPropertyCmpPredicate<typename CMP_T::data_t, GETTER_T, CMP_T>;
    PRED_T pred(getter, cmp_val);
    return create_case_when_project(ctx, vertex, std::move(pred), then_value,
                                    else_value, alias);
  }
}

template <typename T>
static std::unique_ptr<ProjectExprBase> create_sp_pred_case_when(
    const Context& ctx, const IStorageInterface& graph, const ParamsMap& params,
    const std::shared_ptr<IVertexColumn>& vertex, SPPredicateType type,
    const std::string& name, const std::string& target,
    const common::Value& then_value, const common::Value& else_value,
    int alias) {
  if (type == SPPredicateType::kPropertyLT) {
    using CMP_T = LTCmp<T>;
    CMP_T cmp(ValueConverter<T>::typed_from_string(params.at(target)));
    return create_sp_pred_case_when_impl<T, CMP_T>(
        ctx, graph, vertex, name, cmp, then_value, else_value, alias);
  } else if (type == SPPredicateType::kPropertyGT) {
    using CMP_T = GTCmp<T>;
    CMP_T cmp(ValueConverter<T>::typed_from_string(params.at(target)));
    return create_sp_pred_case_when_impl<T, CMP_T>(
        ctx, graph, vertex, name, cmp, then_value, else_value, alias);
  } else if (type == SPPredicateType::kPropertyLE) {
    using CMP_T = LECmp<T>;
    CMP_T cmp(ValueConverter<T>::typed_from_string(params.at(target)));
    return create_sp_pred_case_when_impl<T, CMP_T>(
        ctx, graph, vertex, name, cmp, then_value, else_value, alias);
  } else if (type == SPPredicateType::kPropertyGE) {
    using CMP_T = GECmp<T>;
    CMP_T cmp(ValueConverter<T>::typed_from_string(params.at(target)));
    return create_sp_pred_case_when_impl<T, CMP_T>(
        ctx, graph, vertex, name, cmp, then_value, else_value, alias);
  } else if (type == SPPredicateType::kPropertyEQ) {
    using CMP_T = EQCmp<T>;
    CMP_T cmp(ValueConverter<T>::typed_from_string(params.at(target)));
    return create_sp_pred_case_when_impl<T, CMP_T>(
        ctx, graph, vertex, name, cmp, then_value, else_value, alias);
  } else if (type == SPPredicateType::kPropertyNE) {
    using CMP_T = NECmp<T>;
    CMP_T cmp(ValueConverter<T>::typed_from_string(params.at(target)));
    return create_sp_pred_case_when_impl<T, CMP_T>(
        ctx, graph, vertex, name, cmp, then_value, else_value, alias);
  }
  return nullptr;
}

template <typename T>
static std::unique_ptr<ProjectExprBase> parse_special_expr_between_impl(
    const IStorageInterface& graph, const Context& ctx, int alias,
    const std::shared_ptr<IVertexColumn>& vertex_col,
    const std::string& property_name, const std::string& lower_value,
    const std::string& upper_value, int then_value, int else_value) {
  BetweenCmp<T> cmp(ValueConverter<T>::typed_from_string(lower_value),
                    ValueConverter<T>::typed_from_string(upper_value));
  auto labels = vertex_col->get_labels_set();
  if (labels.size() == 1) {
    label_t label = *labels.begin();
    using GETTER_T = SLVertexPropertyGetter<T>;
    GETTER_T getter(graph, label, property_name);
    using PRED_T = VertexPropertyCmpPredicate<T, GETTER_T, BetweenCmp<T>>;
    PRED_T pred(getter, cmp);
    SPOpr sp(vertex_col, std::move(pred), then_value, else_value);
    CaseWhenCollector<decltype(sp), int32_t> collector(ctx);
    return std::make_unique<ProjectExpr<decltype(sp), decltype(collector)>>(
        std::move(sp), collector, alias);
  } else {
    using GETTER_T = MLVertexPropertyGetter<T>;
    GETTER_T getter(graph, property_name);
    using PRED_T = VertexPropertyCmpPredicate<T, GETTER_T, BetweenCmp<T>>;
    PRED_T pred(getter, cmp);
    SPOpr sp(vertex_col, std::move(pred), then_value, else_value);
    CaseWhenCollector<decltype(sp), int32_t> collector(ctx);
    return std::make_unique<ProjectExpr<decltype(sp), decltype(collector)>>(
        std::move(sp), collector, alias);
  }
}

std::unique_ptr<ProjectExprBase> parse_special_expr(
    const common::Expression& expr, int alias, const IStorageInterface& graph,
    const Context& ctx, const ParamsMap& params) {
  int tag = -1;
  if (is_exchange_index(expr, tag)) {
    return std::make_unique<DummyGetter>(tag, alias);
  }
  {
    {
      int tag;
      std::string name;
      DataType type;
      if (is_property_extract(expr, tag, name, type)) {
        auto col = ctx.get(tag);
        std::unique_ptr<ProjectExprBase> result;
        if ((!col->is_optional()) &&
            col->column_type() == ContextColumnType::kVertex) {
          auto vertex_col = std::dynamic_pointer_cast<IVertexColumn>(col);
          if (vertex_col->vertex_column_type() == VertexColumnType::kSingle) {
            auto typed_vertex_col =
                std::dynamic_pointer_cast<SLVertexColumn>(vertex_col);
            result = create_sl_property_expr(ctx, graph, *typed_vertex_col,
                                             name, type, alias);
          } else if (vertex_col->vertex_column_type() ==
                     VertexColumnType::kMultiSegment) {
            auto typed_vertex_col =
                std::dynamic_pointer_cast<MSVertexColumn>(vertex_col);
            result = create_ml_property_expr(ctx, graph, *typed_vertex_col,
                                             name, type, alias);
          } else {
            CHECK(vertex_col->vertex_column_type() ==
                  VertexColumnType::kMultiple);
            auto typed_vertex_col =
                std::dynamic_pointer_cast<MLVertexColumn>(vertex_col);
            result = create_ml_property_expr(ctx, graph, *typed_vertex_col,
                                             name, type, alias);
          }
        }
        if (result) {
          return result;
        }
        return make_project_expr_without_data_type(expr, alias, graph, ctx,
                                                   params);
      }
    }
    std::string name, lower, upper, target;
    common::Value then_value, else_value;
    if (is_check_property_in_range(expr, tag, name, lower, upper, then_value,
                                   else_value)) {
      auto col = ctx.get(tag);
      if (col->column_type() == ContextColumnType::kVertex) {
        auto vertex_col = std::dynamic_pointer_cast<IVertexColumn>(col);

        auto type = expr.operators(0)
                        .case_()
                        .when_then_expressions(0)
                        .when_expression()
                        .operators(2)
                        .param()
                        .data_type();
        auto type_ = parse_from_ir_data_type(type);
        if (then_value.item_case() != else_value.item_case() ||
            then_value.item_case() != common::Value::kI32) {
          return make_project_expr_without_data_type(expr, alias, graph, ctx,
                                                     params);
        }

        if (type_.id() == DataTypeId::kInt32) {
          return parse_special_expr_between_impl<int32_t>(
              graph, ctx, alias, vertex_col, name, params.at(lower),
              params.at(upper), then_value.i32(), else_value.i32());
        } else if (type_.id() == DataTypeId::kInt64) {
          return parse_special_expr_between_impl<int64_t>(
              graph, ctx, alias, vertex_col, name, params.at(lower),
              params.at(upper), then_value.i32(), else_value.i32());
        } else if (type_.id() == DataTypeId::kTimestampMs) {
          return parse_special_expr_between_impl<DateTime>(
              graph, ctx, alias, vertex_col, name, params.at(lower),
              params.at(upper), then_value.i32(), else_value.i32());
        }
      }
      return make_project_expr_without_data_type(expr, alias, graph, ctx,
                                                 params);
    }
    SPPredicateType ptype;
    if (is_check_property_cmp(expr, tag, name, target, then_value, else_value,
                              ptype)) {
      auto col = ctx.get(tag);
      if (col->column_type() == ContextColumnType::kVertex) {
        auto vertex_col = std::dynamic_pointer_cast<IVertexColumn>(col);
        auto type = expr.operators(0)
                        .case_()
                        .when_then_expressions(0)
                        .when_expression()
                        .operators(2)
                        .param()
                        .data_type();
        auto type_ = parse_from_ir_data_type(type);

        switch (type_.id()) {
#define TYPE_DISPATCHER(enum_val, T)                                        \
  case DataTypeId::enum_val: {                                              \
    auto ptr = create_sp_pred_case_when<T>(ctx, graph, params, vertex_col,  \
                                           ptype, name, target, then_value, \
                                           else_value, alias);              \
    if (ptr) {                                                              \
      return ptr;                                                           \
    }                                                                       \
    break;                                                                  \
  }
          TYPE_DISPATCHER(kInt32, int32_t)
          TYPE_DISPATCHER(kInt64, int64_t)
          TYPE_DISPATCHER(kTimestampMs, DateTime)
#undef TYPE_DISPATCHER
        default: {
          return make_project_expr_without_data_type(expr, alias, graph, ctx,
                                                     params);
        }
        }
      }
    }
    return nullptr;
  }
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
    TYPE_DISPATCHER(kDouble, double)
    TYPE_DISPATCHER(kTimestampMs, DateTime)
    TYPE_DISPATCHER(kVarchar, std::string_view)
#undef TYPE_DISPATCHER
  default:
    LOG(ERROR) << "unsupported when type " << static_cast<int>(when_type.id());
    return nullptr;
  }
}

}  // namespace ops
}  // namespace runtime
}  // namespace neug
