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

namespace gs {
namespace runtime {
namespace ops {

std::unique_ptr<ProjectExprBase> GeneralProjectExprBuilder::build(
    const IStorageInterface& graph, const Context& ctx,
    const std::map<std::string, std::string>& params) {
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
                         std::string& name, RTAnyType& type) {
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
      if (type == RTAnyType::kUnknown) {
        return false;
      }
      // only support pod type
      if (type == RTAnyType::kDateTime || type == RTAnyType::kDate ||
          type == RTAnyType::kI64Value || type == RTAnyType::kI32Value ||
          type == RTAnyType::kF64Value || type == RTAnyType::kU32Value ||
          type == RTAnyType::kU64Value || type == RTAnyType::kF32Value) {
        return true;
      }
    }
  }
  return false;
}

std::unique_ptr<ProjectExprBase> create_sl_property_expr(
    const Context& ctx, const IStorageInterface& graph,
    const SLVertexColumn& column, const std::string& property_name,
    RTAnyType type, int alias) {
  switch (type) {
  case RTAnyType::kI32Value: {
    auto expr =
        SLPropertyExpr<SLVertexColumn, int32_t>(graph, column, property_name);
    if (expr.is_optional()) {
      return nullptr;
    }
    PropertyValueCollector<decltype(expr)> collector(ctx);
    return std::make_unique<ProjectExpr<SLPropertyExpr<SLVertexColumn, int32_t>,
                                        decltype(collector)>>(std::move(expr),
                                                              collector, alias);
  }
  case RTAnyType::kI64Value: {
    auto expr =
        SLPropertyExpr<SLVertexColumn, int64_t>(graph, column, property_name);
    if (expr.is_optional()) {
      return nullptr;
    }
    PropertyValueCollector<decltype(expr)> collector(ctx);
    return std::make_unique<ProjectExpr<SLPropertyExpr<SLVertexColumn, int64_t>,
                                        decltype(collector)>>(std::move(expr),
                                                              collector, alias);
  }
  case RTAnyType::kF64Value: {
    auto expr =
        SLPropertyExpr<SLVertexColumn, double>(graph, column, property_name);
    if (expr.is_optional()) {
      return nullptr;
    }
    PropertyValueCollector<decltype(expr)> collector(ctx);
    return std::make_unique<ProjectExpr<SLPropertyExpr<SLVertexColumn, double>,
                                        decltype(collector)>>(std::move(expr),
                                                              collector, alias);
  }
  case RTAnyType::kStringValue: {
    auto expr = SLPropertyExpr<SLVertexColumn, std::string_view>(graph, column,
                                                                 property_name);
    PropertyValueCollector<decltype(expr)> collector(ctx);
    if (expr.is_optional()) {
      return nullptr;
    }

    return std::make_unique<ProjectExpr<
        SLPropertyExpr<SLVertexColumn, std::string_view>, decltype(collector)>>(
        std::move(expr), collector, alias);
  }
  case RTAnyType::kDate: {
    auto expr =
        SLPropertyExpr<SLVertexColumn, Date>(graph, column, property_name);
    if (expr.is_optional()) {
      return nullptr;
    }
    PropertyValueCollector<decltype(expr)> collector(ctx);
    return std::make_unique<
        ProjectExpr<SLPropertyExpr<SLVertexColumn, Date>, decltype(collector)>>(
        std::move(expr), collector, alias);
  }
  case RTAnyType::kDateTime: {
    auto expr =
        SLPropertyExpr<SLVertexColumn, DateTime>(graph, column, property_name);
    PropertyValueCollector<decltype(expr)> collector(ctx);
    if (expr.is_optional()) {
      return nullptr;
    }
    return std::make_unique<ProjectExpr<
        SLPropertyExpr<SLVertexColumn, DateTime>, decltype(collector)>>(
        std::move(expr), collector, alias);
  }
  default:
    LOG(ERROR) << "create_sl_property_expr: not implemented for type: "
               << static_cast<int>(type);
  }
  return nullptr;
}

template <typename VertexColumn>
std::unique_ptr<ProjectExprBase> create_ml_property_expr(
    const Context& ctx, const IStorageInterface& graph,
    const VertexColumn& column, const std::string& property_name,
    RTAnyType type, int alias) {
  switch (type) {
  case RTAnyType::kBoolValue: {
    auto expr =
        MLPropertyExpr<VertexColumn, bool>(graph, column, property_name);
    if (expr.is_optional()) {
      return nullptr;
    }
    PropertyValueCollector<decltype(expr)> collector(ctx);
    return std::make_unique<
        ProjectExpr<MLPropertyExpr<VertexColumn, bool>, decltype(collector)>>(
        std::move(expr), collector, alias);
  }
  case RTAnyType::kI32Value: {
    auto expr =
        MLPropertyExpr<VertexColumn, int32_t>(graph, column, property_name);
    if (expr.is_optional()) {
      return nullptr;
    }
    PropertyValueCollector<decltype(expr)> collector(ctx);
    return std::make_unique<ProjectExpr<MLPropertyExpr<VertexColumn, int32_t>,
                                        decltype(collector)>>(std::move(expr),
                                                              collector, alias);
  }
  case RTAnyType::kU32Value: {
    auto expr =
        MLPropertyExpr<VertexColumn, uint32_t>(graph, column, property_name);
    if (expr.is_optional()) {
      return nullptr;
    }
    PropertyValueCollector<decltype(expr)> collector(ctx);
    return std::make_unique<ProjectExpr<MLPropertyExpr<VertexColumn, uint32_t>,
                                        decltype(collector)>>(std::move(expr),
                                                              collector, alias);
  }
  case RTAnyType::kI64Value: {
    auto expr =
        MLPropertyExpr<VertexColumn, int64_t>(graph, column, property_name);
    if (expr.is_optional()) {
      return nullptr;
    }
    PropertyValueCollector<decltype(expr)> collector(ctx);
    return std::make_unique<ProjectExpr<MLPropertyExpr<VertexColumn, int64_t>,
                                        decltype(collector)>>(std::move(expr),
                                                              collector, alias);
  }
  case RTAnyType::kU64Value: {
    auto expr =
        MLPropertyExpr<VertexColumn, uint64_t>(graph, column, property_name);
    if (expr.is_optional()) {
      return nullptr;
    }
    PropertyValueCollector<decltype(expr)> collector(ctx);
    return std::make_unique<ProjectExpr<MLPropertyExpr<VertexColumn, uint64_t>,
                                        decltype(collector)>>(std::move(expr),
                                                              collector, alias);
  }
  case RTAnyType::kF32Value: {
    auto expr =
        MLPropertyExpr<VertexColumn, float>(graph, column, property_name);
    if (expr.is_optional()) {
      return nullptr;
    }
    PropertyValueCollector<decltype(expr)> collector(ctx);
    return std::make_unique<
        ProjectExpr<MLPropertyExpr<VertexColumn, float>, decltype(collector)>>(
        std::move(expr), collector, alias);
  }
  case RTAnyType::kF64Value: {
    auto expr =
        MLPropertyExpr<VertexColumn, double>(graph, column, property_name);
    if (expr.is_optional()) {
      return nullptr;
    }
    PropertyValueCollector<decltype(expr)> collector(ctx);
    return std::make_unique<
        ProjectExpr<MLPropertyExpr<VertexColumn, double>, decltype(collector)>>(
        std::move(expr), collector, alias);
  }
  case RTAnyType::kStringValue: {
    auto expr = MLPropertyExpr<VertexColumn, std::string_view>(graph, column,
                                                               property_name);
    PropertyValueCollector<decltype(expr)> collector(ctx);
    if (expr.is_optional()) {
      return nullptr;
    }
    return std::make_unique<ProjectExpr<
        MLPropertyExpr<VertexColumn, std::string_view>, decltype(collector)>>(
        std::move(expr), collector, alias);
  }

  case RTAnyType::kDate: {
    auto expr =
        MLPropertyExpr<VertexColumn, Date>(graph, column, property_name);
    PropertyValueCollector<decltype(expr)> collector(ctx);
    if (expr.is_optional()) {
      return nullptr;
    }
    return std::make_unique<
        ProjectExpr<MLPropertyExpr<VertexColumn, Date>, decltype(collector)>>(
        std::move(expr), collector, alias);
  }
  case RTAnyType::kDateTime: {
    auto expr =
        MLPropertyExpr<VertexColumn, DateTime>(graph, column, property_name);
    PropertyValueCollector<decltype(expr)> collector(ctx);
    if (expr.is_optional()) {
      return nullptr;
    }
    return std::make_unique<ProjectExpr<MLPropertyExpr<VertexColumn, DateTime>,
                                        decltype(collector)>>(std::move(expr),
                                                              collector, alias);
  }
  case RTAnyType::kInterval: {
    auto expr =
        MLPropertyExpr<VertexColumn, Interval>(graph, column, property_name);
    PropertyValueCollector<decltype(expr)> collector(ctx);
    if (expr.is_optional()) {
      return nullptr;
    }
    return std::make_unique<ProjectExpr<MLPropertyExpr<VertexColumn, Interval>,
                                        decltype(collector)>>(std::move(expr),
                                                              collector, alias);
  }
  default:
    LOG(ERROR) << "create_ml_property_expr: not implemented for type: "
               << static_cast<int>(type);
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
    const Context& ctx, const IStorageInterface& graph,
    const std::map<std::string, std::string>& params,
    const std::shared_ptr<IVertexColumn>& vertex, SPPredicateType type,
    const std::string& name, const std::string& target,
    const common::Value& then_value, const common::Value& else_value,
    int alias) {
  if (type == SPPredicateType::kPropertyLT) {
    using CMP_T = LTCmp<T>;
    CMP_T cmp(TypedConverter<T>::typed_from_string(params.at(target)));
    return create_sp_pred_case_when_impl<T, CMP_T>(
        ctx, graph, vertex, name, cmp, then_value, else_value, alias);
  } else if (type == SPPredicateType::kPropertyGT) {
    using CMP_T = GTCmp<T>;
    CMP_T cmp(TypedConverter<T>::typed_from_string(params.at(target)));
    return create_sp_pred_case_when_impl<T, CMP_T>(
        ctx, graph, vertex, name, cmp, then_value, else_value, alias);
  } else if (type == SPPredicateType::kPropertyLE) {
    using CMP_T = LECmp<T>;
    CMP_T cmp(TypedConverter<T>::typed_from_string(params.at(target)));
    return create_sp_pred_case_when_impl<T, CMP_T>(
        ctx, graph, vertex, name, cmp, then_value, else_value, alias);
  } else if (type == SPPredicateType::kPropertyGE) {
    using CMP_T = GECmp<T>;
    CMP_T cmp(TypedConverter<T>::typed_from_string(params.at(target)));
    return create_sp_pred_case_when_impl<T, CMP_T>(
        ctx, graph, vertex, name, cmp, then_value, else_value, alias);
  } else if (type == SPPredicateType::kPropertyEQ) {
    using CMP_T = EQCmp<T>;
    CMP_T cmp(TypedConverter<T>::typed_from_string(params.at(target)));
    return create_sp_pred_case_when_impl<T, CMP_T>(
        ctx, graph, vertex, name, cmp, then_value, else_value, alias);
  } else if (type == SPPredicateType::kPropertyNE) {
    using CMP_T = NECmp<T>;
    CMP_T cmp(TypedConverter<T>::typed_from_string(params.at(target)));
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
  BetweenCmp<T> cmp(TypedConverter<T>::typed_from_string(lower_value),
                    TypedConverter<T>::typed_from_string(upper_value));
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
    const Context& ctx, const std::map<std::string, std::string>& params) {
  int tag = -1;
  if (is_exchange_index(expr, tag)) {
    return std::make_unique<DummyGetter>(tag, alias);
  }
  {
    {
      int tag;
      std::string name;
      RTAnyType type;
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

        if (type_ == RTAnyType::kI32Value) {
          return parse_special_expr_between_impl<int32_t>(
              graph, ctx, alias, vertex_col, name, params.at(lower),
              params.at(upper), then_value.i32(), else_value.i32());
        } else if (type_ == RTAnyType::kI64Value) {
          return parse_special_expr_between_impl<int64_t>(
              graph, ctx, alias, vertex_col, name, params.at(lower),
              params.at(upper), then_value.i32(), else_value.i32());
        } else if (type_ == RTAnyType::kDateTime) {
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

        if (type_ == RTAnyType::kI32Value) {
          auto ptr = create_sp_pred_case_when<int32_t>(
              ctx, graph, params, vertex_col, ptype, name, target, then_value,
              else_value, alias);
          if (ptr) {
            return ptr;
          }
        } else if (type_ == RTAnyType::kI64Value) {
          auto ptr = create_sp_pred_case_when<int64_t>(
              ctx, graph, params, vertex_col, ptype, name, target, then_value,
              else_value, alias);
          if (ptr) {
            return ptr;
          }
        } else if (type_ == RTAnyType::kDateTime) {
          auto ptr = create_sp_pred_case_when<DateTime>(
              ctx, graph, params, vertex_col, ptype, name, target, then_value,
              else_value, alias);
          if (ptr) {
            return ptr;
          }
        } else if (type_ == RTAnyType::kStringValue) {
          auto ptr = create_sp_pred_case_when<std::string_view>(
              ctx, graph, params, vertex_col, ptype, name, target, then_value,
              else_value, alias);
          if (ptr) {
            return ptr;
          }
        }
      }
      return make_project_expr_without_data_type(expr, alias, graph, ctx,
                                                 params);
    }
  }
  return nullptr;
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
  RTAnyType type;
  if (is_property_extract(expr, tag, name, type)) {
    if (type == RTAnyType::kI32Value) {
      return std::make_unique<VertexPropertyExprBuilder<int32_t>>(tag, name,
                                                                  alias);
    } else if (type == RTAnyType::kU32Value) {
      return std::make_unique<VertexPropertyExprBuilder<uint32_t>>(tag, name,
                                                                   alias);
    } else if (type == RTAnyType::kI64Value) {
      return std::make_unique<VertexPropertyExprBuilder<int64_t>>(tag, name,
                                                                  alias);
    } else if (type == RTAnyType::kU64Value) {
      return std::make_unique<VertexPropertyExprBuilder<uint64_t>>(tag, name,
                                                                   alias);
    } else if (type == RTAnyType::kF32Value) {
      return std::make_unique<VertexPropertyExprBuilder<float>>(tag, name,
                                                                alias);
    } else if (type == RTAnyType::kF64Value) {
      return std::make_unique<VertexPropertyExprBuilder<double>>(tag, name,
                                                                 alias);
    } else if (type == RTAnyType::kStringValue) {
      return std::make_unique<VertexPropertyExprBuilder<std::string_view>>(
          tag, name, alias);
    } else if (type == RTAnyType::kDateTime) {
      return std::make_unique<VertexPropertyExprBuilder<DateTime>>(tag, name,
                                                                   alias);
    } else if (type == RTAnyType::kDate) {
      return std::make_unique<VertexPropertyExprBuilder<Date>>(tag, name,
                                                               alias);
    } else if (type == RTAnyType::kInterval) {
      return std::make_unique<VertexPropertyExprBuilder<Interval>>(tag, name,
                                                                   alias);
    }
  }
  return nullptr;
}

template <typename CMP_T>
std::unique_ptr<ProjectExprBuilderBase> create_case_when_builder_impl1(
    RTAnyType then_type, const std::vector<std::string>& param_names,
    const common::Value& then_value, const common::Value& else_value, int tag,
    const std::string& property_name, int alias) {
  if (then_type == RTAnyType::kI32Value) {
    return std::make_unique<CaseWhenExprBuilder<CMP_T, int32_t>>(
        param_names, then_value.i32(), else_value.i32(), tag, property_name,
        alias);
  } else {
    LOG(ERROR) << "unsupported then type " << static_cast<int>(then_type);
    return nullptr;
  }
}

template <typename WHEN_T>
std::unique_ptr<ProjectExprBuilderBase> create_case_when_builder_impl0(
    SPPredicateType ptype, RTAnyType then_type,
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
  RTAnyType when_type, then_type;
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
    if (then_value.item_case() == common::Value::kI32) {
      then_type = RTAnyType::kI32Value;
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
    if (then_value.item_case() == common::Value::kI32) {
      then_type = RTAnyType::kI32Value;
    } else {
      LOG(ERROR) << "unexpected then value type" << then_value.DebugString();
      return nullptr;
    }
  } else {
    return nullptr;
  }

  if (when_type == RTAnyType::kI32Value) {
    return create_case_when_builder_impl0<int32_t>(
        ptype, then_type, param_names, then_value, else_value, tag, name,
        alias);
  } else if (when_type == RTAnyType::kI64Value) {
    return create_case_when_builder_impl0<int64_t>(
        ptype, then_type, param_names, then_value, else_value, tag, name,
        alias);
  } else if (when_type == RTAnyType::kF64Value) {
    return create_case_when_builder_impl0<double>(ptype, then_type, param_names,
                                                  then_value, else_value, tag,
                                                  name, alias);
  } else if (when_type == RTAnyType::kStringValue) {
    return create_case_when_builder_impl0<std::string_view>(
        ptype, then_type, param_names, then_value, else_value, tag, name,
        alias);
  } else if (when_type == RTAnyType::kDate) {
    return create_case_when_builder_impl0<Date>(ptype, then_type, param_names,
                                                then_value, else_value, tag,
                                                name, alias);
  } else if (when_type == RTAnyType::kDateTime) {
    return create_case_when_builder_impl0<DateTime>(
        ptype, then_type, param_names, then_value, else_value, tag, name,
        alias);
  } else {
    LOG(ERROR) << "unsupported when type " << static_cast<int>(when_type);
    return nullptr;
  }
}

}  // namespace ops
}  // namespace runtime
}  // namespace gs
