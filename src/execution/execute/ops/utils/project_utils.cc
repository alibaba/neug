
#include "neug/execution/execute/ops/utils/project_utils.h"
#include "neug/execution/utils/special_predicates.h"

namespace gs {
namespace runtime {
namespace ops {

bool is_exchange_index(const common::Expression& expr, int alias, int& tag) {
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
      if (type == RTAnyType::kTimestamp || type == RTAnyType::kDate ||
          type == RTAnyType::kI64Value || type == RTAnyType::kI32Value ||
          type == RTAnyType::kF64Value || type == RTAnyType::kU32Value ||
          type == RTAnyType::kU64Value || type == RTAnyType::kF32Value) {
        return true;
      }
    }
  }
  return false;
}

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
    if (pred(v.label_, v.vid_)) {
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

template <typename VertexColoumn, typename T>
struct SLPropertyExpr {
  using V = T;
  SLPropertyExpr(const GraphReadInterface& graph, const VertexColoumn& column,
                 const std::string& property_name)
      : column(column) {
    auto labels = column.get_labels_set();
    auto& label = *labels.begin();
    property = graph.GetVertexColumn<T>(label, property_name);
    is_optional_ = property.is_null();
  }
  inline T operator()(size_t idx) const {
    auto v = column.get_vertex(idx);
    return property.get_view(v.vid_);
  }
  bool is_optional() const { return is_optional_; }
  bool is_optional_;
  const VertexColoumn& column;
  GraphReadInterface::vertex_column_t<T> property;
};

template <typename VertexColoumn, typename T>
struct MLPropertyExpr {
  using V = T;
  MLPropertyExpr(const GraphReadInterface& graph, const VertexColoumn& vertex,
                 const std::string& property_name)
      : vertex(vertex) {
    auto labels = vertex.get_labels_set();
    int label_num = graph.schema().vertex_label_num();
    property.resize(label_num);
    is_optional_ = false;
    for (auto label : labels) {
      property[label] = graph.GetVertexColumn<T>(label, property_name);
      if (property[label].is_null()) {
        is_optional_ = true;
      }
    }
  }
  bool is_optional() const { return is_optional_; }
  inline T operator()(size_t idx) const {
    auto v = vertex.get_vertex(idx);
    return property[v.label_].get_view(v.vid_);
  }
  const VertexColoumn& vertex;
  std::vector<GraphReadInterface::vertex_column_t<T>> property;

  bool is_optional_;
};

template <typename VertexColumn>
std::unique_ptr<ProjectExprBase> create_sl_property_expr(
    const Context& ctx, const GraphReadInterface& graph,
    const VertexColumn& column, const std::string& property_name,
    RTAnyType type, int alias) {
  switch (type) {
  case RTAnyType::kBoolValue: {
    auto expr =
        SLPropertyExpr<VertexColumn, bool>(graph, column, property_name);
    if (expr.is_optional()) {
      return nullptr;
    }
    PropertyValueCollector<decltype(expr)> collector(ctx);
    return std::make_unique<
        ProjectExpr<SLPropertyExpr<VertexColumn, bool>, decltype(collector)>>(
        std::move(expr), collector, alias);
  }
  case RTAnyType::kI32Value: {
    auto expr =
        SLPropertyExpr<VertexColumn, int32_t>(graph, column, property_name);
    if (expr.is_optional()) {
      return nullptr;
    }
    PropertyValueCollector<decltype(expr)> collector(ctx);
    return std::make_unique<ProjectExpr<SLPropertyExpr<VertexColumn, int32_t>,
                                        decltype(collector)>>(std::move(expr),
                                                              collector, alias);
  }
  case RTAnyType::kU32Value: {
    auto expr =
        SLPropertyExpr<VertexColumn, uint32_t>(graph, column, property_name);
    if (expr.is_optional()) {
      return nullptr;
    }
    PropertyValueCollector<decltype(expr)> collector(ctx);
    return std::make_unique<ProjectExpr<SLPropertyExpr<VertexColumn, uint32_t>,
                                        decltype(collector)>>(std::move(expr),
                                                              collector, alias);
  }
  case RTAnyType::kI64Value: {
    auto expr =
        SLPropertyExpr<VertexColumn, int64_t>(graph, column, property_name);
    if (expr.is_optional()) {
      return nullptr;
    }
    PropertyValueCollector<decltype(expr)> collector(ctx);
    return std::make_unique<ProjectExpr<SLPropertyExpr<VertexColumn, int64_t>,
                                        decltype(collector)>>(std::move(expr),
                                                              collector, alias);
  }
  case RTAnyType::kU64Value: {
    auto expr =
        SLPropertyExpr<VertexColumn, uint64_t>(graph, column, property_name);
    if (expr.is_optional()) {
      return nullptr;
    }
    PropertyValueCollector<decltype(expr)> collector(ctx);
    return std::make_unique<ProjectExpr<SLPropertyExpr<VertexColumn, uint64_t>,
                                        decltype(collector)>>(std::move(expr),
                                                              collector, alias);
  }
  case RTAnyType::kF32Value: {
    auto expr =
        SLPropertyExpr<VertexColumn, float>(graph, column, property_name);
    if (expr.is_optional()) {
      return nullptr;
    }
    PropertyValueCollector<decltype(expr)> collector(ctx);
    return std::make_unique<
        ProjectExpr<SLPropertyExpr<VertexColumn, float>, decltype(collector)>>(
        std::move(expr), collector, alias);
  }
  case RTAnyType::kF64Value: {
    auto expr =
        SLPropertyExpr<VertexColumn, double>(graph, column, property_name);
    if (expr.is_optional()) {
      return nullptr;
    }
    PropertyValueCollector<decltype(expr)> collector(ctx);
    return std::make_unique<
        ProjectExpr<SLPropertyExpr<VertexColumn, double>, decltype(collector)>>(
        std::move(expr), collector, alias);
  }
  case RTAnyType::kStringValue: {
    auto expr = SLPropertyExpr<VertexColumn, std::string_view>(graph, column,
                                                               property_name);
    PropertyValueCollector<decltype(expr)> collector(ctx);
    if (expr.is_optional()) {
      return nullptr;
    }

    return std::make_unique<ProjectExpr<
        SLPropertyExpr<VertexColumn, std::string_view>, decltype(collector)>>(
        std::move(expr), collector, alias);
  }
  case RTAnyType::kDate: {
    auto expr =
        SLPropertyExpr<VertexColumn, Date>(graph, column, property_name);
    if (expr.is_optional()) {
      return nullptr;
    }
    PropertyValueCollector<decltype(expr)> collector(ctx);
    return std::make_unique<
        ProjectExpr<SLPropertyExpr<VertexColumn, Date>, decltype(collector)>>(
        std::move(expr), collector, alias);
  }
  case RTAnyType::kDateTime: {
    auto expr =
        SLPropertyExpr<VertexColumn, DateTime>(graph, column, property_name);
    PropertyValueCollector<decltype(expr)> collector(ctx);
    if (expr.is_optional()) {
      return nullptr;
    }
    return std::make_unique<ProjectExpr<SLPropertyExpr<VertexColumn, DateTime>,
                                        decltype(collector)>>(std::move(expr),
                                                              collector, alias);
  }
  case RTAnyType::kTimestamp: {
    auto expr =
        SLPropertyExpr<VertexColumn, TimeStamp>(graph, column, property_name);
    PropertyValueCollector<decltype(expr)> collector(ctx);
    if (expr.is_optional()) {
      return nullptr;
    }
    return std::make_unique<ProjectExpr<SLPropertyExpr<VertexColumn, TimeStamp>,
                                        decltype(collector)>>(std::move(expr),
                                                              collector, alias);
  }
  case RTAnyType::kInterval: {
    auto expr =
        SLPropertyExpr<VertexColumn, Interval>(graph, column, property_name);
    PropertyValueCollector<decltype(expr)> collector(ctx);
    if (expr.is_optional()) {
      return nullptr;
    }
    return std::make_unique<ProjectExpr<SLPropertyExpr<VertexColumn, Interval>,
                                        decltype(collector)>>(std::move(expr),
                                                              collector, alias);
  }
  default:
    LOG(ERROR) << "create_sl_property_expr: not implemented for type: "
               << static_cast<int>(type);
  }
  return nullptr;
}

template <typename VertexColumn>
std::unique_ptr<ProjectExprBase> create_ml_property_expr(
    const Context& ctx, const GraphReadInterface& graph,
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
  case RTAnyType::kTimestamp: {
    auto expr =
        MLPropertyExpr<VertexColumn, TimeStamp>(graph, column, property_name);
    PropertyValueCollector<decltype(expr)> collector(ctx);
    if (expr.is_optional()) {
      return nullptr;
    }
    return std::make_unique<ProjectExpr<MLPropertyExpr<VertexColumn, TimeStamp>,
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

template <typename T>
static std::unique_ptr<ProjectExprBase> create_sp_pred_case_when(
    const Context& ctx, const GraphReadInterface& graph,
    const std::map<std::string, std::string>& params,
    const std::shared_ptr<IVertexColumn>& vertex, SPPredicateType type,
    const std::string& name, const std::string& target,
    const common::Value& then_value, const common::Value& else_value,
    int alias) {
  if (type == SPPredicateType::kPropertyLT) {
    VertexPropertyLTPredicateBeta<T> pred(graph, name, params.at(target));
    return create_case_when_project(ctx, vertex, std::move(pred), then_value,
                                    else_value, alias);
  } else if (type == SPPredicateType::kPropertyGT) {
    VertexPropertyGTPredicateBeta<T> pred(graph, name, params.at(target));
    return create_case_when_project(ctx, vertex, std::move(pred), then_value,
                                    else_value, alias);
  } else if (type == SPPredicateType::kPropertyLE) {
    VertexPropertyLEPredicateBeta<T> pred(graph, name, params.at(target));
    return create_case_when_project(ctx, vertex, std::move(pred), then_value,
                                    else_value, alias);
  } else if (type == SPPredicateType::kPropertyGE) {
    VertexPropertyGEPredicateBeta<T> pred(graph, name, params.at(target));
    return create_case_when_project(ctx, vertex, std::move(pred), then_value,
                                    else_value, alias);
  } else if (type == SPPredicateType::kPropertyEQ) {
    VertexPropertyEQPredicateBeta<T> pred(graph, name, params.at(target));
    return create_case_when_project(ctx, vertex, std::move(pred), then_value,
                                    else_value, alias);
  } else if (type == SPPredicateType::kPropertyNE) {
    VertexPropertyNEPredicateBeta<T> pred(graph, name, params.at(target));

    return create_case_when_project(ctx, vertex, std::move(pred), then_value,
                                    else_value, alias);
  }
  return nullptr;
}

std::unique_ptr<ProjectExprBase> parse_special_expr(
    const common::Expression& expr, int alias, const GraphReadInterface& graph,
    const Context& ctx, const std::map<std::string, std::string>& params) {
  int tag = -1;
  if (is_exchange_index(expr, alias, tag)) {
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
          SPOpr sp(vertex_col,
                   VertexPropertyBetweenPredicateBeta<int32_t>(
                       graph, name, params.at(lower), params.at(upper)),
                   then_value.i32(), else_value.i32());
          CaseWhenCollector<decltype(sp), int32_t> collector(ctx);
          return std::make_unique<
              ProjectExpr<decltype(sp), decltype(collector)>>(std::move(sp),
                                                              collector, alias);

        } else if (type_ == RTAnyType::kI64Value) {
          SPOpr sp(vertex_col,
                   VertexPropertyBetweenPredicateBeta<int64_t>(
                       graph, name, params.at(lower), params.at(upper)),
                   then_value.i32(), else_value.i32());
          CaseWhenCollector<decltype(sp), int32_t> collector(ctx);
          return std::make_unique<
              ProjectExpr<decltype(sp), decltype(collector)>>(std::move(sp),
                                                              collector, alias);
        } else if (type_ == RTAnyType::kTimestamp) {
          if (vertex_col->vertex_column_type() == VertexColumnType::kSingle) {
            auto typed_vertex_col =
                std::dynamic_pointer_cast<SLVertexColumn>(vertex_col);
            SPOpr sp(typed_vertex_col,
                     VertexPropertyBetweenPredicateBeta<TimeStamp>(
                         graph, name, params.at(lower), params.at(upper)),
                     then_value.i32(), else_value.i32());
            CaseWhenCollector<decltype(sp), int32_t> collector(ctx);
            return std::make_unique<
                ProjectExpr<decltype(sp), decltype(collector)>>(
                std::move(sp), collector, alias);
          } else {
            SPOpr sp(vertex_col,
                     VertexPropertyBetweenPredicateBeta<TimeStamp>(
                         graph, name, params.at(lower), params.at(upper)),
                     then_value.i32(), else_value.i32());
            CaseWhenCollector<decltype(sp), int32_t> collector(ctx);
            return std::make_unique<
                ProjectExpr<decltype(sp), decltype(collector)>>(
                std::move(sp), collector, alias);
          }
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
        } else if (type_ == RTAnyType::kTimestamp) {
          auto ptr = create_sp_pred_case_when<TimeStamp>(
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
}  // namespace ops
}  // namespace runtime
}  // namespace gs