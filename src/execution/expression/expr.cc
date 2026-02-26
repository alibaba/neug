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

#include "neug/execution/expression/expr.h"
#include <stack>

#include "neug/compiler/function/neug_scalar_function.h"
#include "neug/compiler/function/scalar_function.h"
#include "neug/compiler/gopt/g_catalog_holder.h"
#include "neug/execution/expression/accessors/const_accessor.h"
#include "neug/execution/expression/exprs/arith_expr.h"
#include "neug/execution/expression/exprs/case_when.h"
#include "neug/execution/expression/exprs/extract_expr.h"
#include "neug/execution/expression/exprs/logical_expr.h"
#include "neug/execution/expression/exprs/path_expr.h"
#include "neug/execution/expression/exprs/struct_expr.h"
#include "neug/execution/expression/exprs/udfs.h"
#include "neug/execution/expression/exprs/variable.h"

#include "neug/generated/proto/plan/expr.pb.h"

namespace neug {
namespace execution {

static std::unique_ptr<ExprBase> build_expr(
    std::stack<::common::ExprOpr>& opr_stack, const ContextMeta& ctx_meta,
    VarType var_type) {
  while (!opr_stack.empty()) {
    auto opr = opr_stack.top();
    opr_stack.pop();
    switch (opr.item_case()) {
    case ::common::ExprOpr::kConst: {
      return parse_const(opr.const_());
    }
    case ::common::ExprOpr::kParam: {
      return parse_param(opr.param());
    }
    case ::common::ExprOpr::kVar: {
      return parse_variable(opr.var(), ctx_meta, var_type);
    }
    case ::common::ExprOpr::kLogical: {
      if (opr.logical() == ::common::Logical::WITHIN) {
        auto lhs = build_expr(opr_stack, ctx_meta, var_type);
        auto rhs = build_expr(opr_stack, ctx_meta, var_type);
        return std::make_unique<WithInExpr>(std::move(lhs), std::move(rhs));
      } else if (opr.logical() == ::common::Logical::NOT ||
                 opr.logical() == ::common::Logical::ISNULL) {
        auto lhs = build_expr(opr_stack, ctx_meta, var_type);
        return std::make_unique<UnaryLogicalExpr>(std::move(lhs),
                                                  opr.logical());
      } else {
        auto lhs = build_expr(opr_stack, ctx_meta, var_type);
        auto rhs = build_expr(opr_stack, ctx_meta, var_type);
        return std::make_unique<BinaryLogicalExpr>(
            std::move(lhs), std::move(rhs), opr.logical());
      }
      break;
    }
    case ::common::ExprOpr::kArith: {
      auto lhs = build_expr(opr_stack, ctx_meta, var_type);
      auto rhs = build_expr(opr_stack, ctx_meta, var_type);
      auto type = parse_from_ir_data_type(opr.node_type());
      return std::make_unique<ArithExpr>(std::move(lhs), std::move(rhs), type,
                                         opr.arith());
    }
    case ::common::ExprOpr::kCase: {
      auto op = opr.case_();
      size_t len = op.when_then_expressions_size();
      std::vector<
          std::pair<std::unique_ptr<ExprBase>, std::unique_ptr<ExprBase>>>
          when_then_exprs;
      DataType type = DataType::SQLNULL;
      for (size_t i = 0; i < len; ++i) {
        const auto& when_expr = op.when_then_expressions(i).when_expression();
        const auto& then_expr =
            op.when_then_expressions(i).then_result_expression();
        auto when_exp = parse_expression(when_expr, ctx_meta, var_type);
        auto then_exp = parse_expression(then_expr, ctx_meta, var_type);
        if (then_exp->type().id() != DataTypeId::kNull) {
          type = then_exp->type();
        }
        when_then_exprs.emplace_back(std::move(when_exp), std::move(then_exp));
      }
      auto else_expr =
          parse_expression(op.else_result_expression(), ctx_meta, var_type);
      if (else_expr->type().id() != DataTypeId::kNull) {
        type = else_expr->type();
      }

      return std::make_unique<CaseWhenExpr>(type, std::move(when_then_exprs),
                                            std::move(else_expr));
    }
    case ::common::ExprOpr::kExtract: {
      auto hs = build_expr(opr_stack, ctx_meta, var_type);
      return std::make_unique<ExtractExpr>(std::move(hs), opr.extract());
    }

    case ::common::ExprOpr::kToTuple: {
      const auto& compisite_fields = opr.to_tuple().fields();

      std::vector<std::unique_ptr<ExprBase>> exprs_vec;
      for (int i = 0; i < compisite_fields.size(); ++i) {
        exprs_vec.emplace_back(
            parse_expression(compisite_fields[i], ctx_meta, var_type));
      }

      return std::make_unique<TupleExpr>(std::move(exprs_vec));
    }

    case ::common::ExprOpr::kToDate: {
      Date date(opr.to_date().date_str());
      return std::make_unique<ConstExpr>(Value::DATE(date));
    }

    case ::common::ExprOpr::kToDatetime: {
      DateTime datetime(opr.to_datetime().datetime_str());
      return std::make_unique<ConstExpr>(Value::TIMESTAMPMS(datetime));
    }

    case ::common::ExprOpr::kToInterval: {
      Interval interval(opr.to_interval().interval_str());
      return std::make_unique<ConstExpr>(Value::INTERVAL(interval));
    }
    case ::common::ExprOpr::kScalarFunc: {
      auto op = opr.scalar_func();
      const std::string& signature = op.unique_name();
      neug::execution::neug_func_exec_t fn = nullptr;

      auto gCatalog = catalog::GCatalogHolder::getGCatalog();
      auto func = gCatalog->getFunctionWithSignature(
          &neug::transaction::DUMMY_TRANSACTION, signature);
      if (!func) {
        THROW_RUNTIME_ERROR("Function not found in catalog for signature: " +
                            signature);
      }

      auto* scalarFunc = dynamic_cast<function::NeugScalarFunction*>(func);
      fn = scalarFunc->neugExecFunc;
      if (!fn) {
        THROW_RUNTIME_ERROR(
            "ScalarFunction neugExecFunc is null for signature: " + signature);
      }

      DataType ret_type = DataType(DataTypeId::kUnknown);
      if (opr.has_node_type()) {
        ret_type = parse_from_ir_data_type(opr.node_type());
      }
      std::vector<std::unique_ptr<ExprBase>> children;
      children.reserve(op.parameters_size());
      for (int i = 0; i < op.parameters_size(); ++i) {
        children.emplace_back(
            parse_expression(op.parameters(i), ctx_meta, var_type));
      }
      return std::make_unique<ScalarFunctionExpr>(fn, ret_type,
                                                  std::move(children));
    }

    case ::common::ExprOpr::kUdfFunc: {
      auto op = opr.udf_func();
      std::string name = op.name();
      auto expr = parse_expression(op.parameters(0), ctx_meta, var_type);
      if (name == "gs.function.relationships") {
        return std::make_unique<PathRelationsExpr>(std::move(expr));
      } else if (name == "gs.function.nodes") {
        return std::make_unique<PathNodesExpr>(std::move(expr));
      } else if (name == "gs.function.startNode") {
        return std::make_unique<StartEndNodeExpr>(std::move(expr), true);
      } else if (name == "gs.function.endNode") {
        return std::make_unique<StartEndNodeExpr>(std::move(expr), false);
      } else {
        LOG(FATAL) << "not support udf" << opr.DebugString();
      }
    }
    case ::common::ExprOpr::kPathFunc: {
      auto opt = opr.path_func().opt();
      const auto& name = opr.path_func().property().key().name();
      int tag = opr.path_func().has_tag() ? opr.path_func().tag().id() : -1;
      if (opr.node_type().data_type().item_case() !=
          ::common::DataType::kArray) {
        LOG(FATAL) << "path function node_type is not array type";
        return nullptr;
      }
      auto type = parse_from_data_type(
          opr.node_type().data_type().array().component_type());

      if (opt == ::common::PathFunction_FuncOpt::PathFunction_FuncOpt_VERTEX) {
        return std::make_unique<PathPropsExpr>(tag, name, type, true);
      } else if (opt ==
                 ::common::PathFunction_FuncOpt::PathFunction_FuncOpt_EDGE) {
        return std::make_unique<PathPropsExpr>(tag, name, type, false);
      } else {
        LOG(FATAL) << "unsupport path function opt" << opr.DebugString();
      }

      break;
    }
    default:
      LOG(FATAL) << "not support" << opr.DebugString();
      break;
    }
  }
  return nullptr;
}

static inline int get_proiority(const ::common::ExprOpr& opr) {
  switch (opr.item_case()) {
  case ::common::ExprOpr::kBrace: {
    return 17;
  }
  case ::common::ExprOpr::kExtract:
  case ::common::ExprOpr::kToDate:
  case ::common::ExprOpr::kToDatetime:
  case ::common::ExprOpr::kToInterval: {
    return 2;
  }
  case ::common::ExprOpr::kLogical: {
    switch (opr.logical()) {
    case ::common::Logical::AND:
      return 11;
    case ::common::Logical::OR:
      return 12;
    case ::common::Logical::NOT:
    case ::common::Logical::WITHIN:
    case ::common::Logical::WITHOUT:
    case ::common::Logical::REGEX:
      return 2;
    case ::common::Logical::EQ:
    case ::common::Logical::NE:
      return 7;
    case ::common::Logical::GE:
    case ::common::Logical::GT:
    case ::common::Logical::LT:
    case ::common::Logical::LE:
      return 6;
    default:
      return 16;
    }
  }
  case ::common::ExprOpr::kArith: {
    switch (opr.arith()) {
    case ::common::Arithmetic::ADD:
    case ::common::Arithmetic::SUB:
      return 4;
    case ::common::Arithmetic::MUL:
    case ::common::Arithmetic::DIV:
    case ::common::Arithmetic::MOD:
      return 3;
    default:
      return 16;
    }
  }
  default:
    return 16;
  }
}

std::unique_ptr<ExprBase> parse_expression(const ::common::Expression& expr,
                                           const ContextMeta& ctx_meta,
                                           VarType var_type) {
  std::stack<::common::ExprOpr> opr_stack;
  std::stack<::common::ExprOpr> opr_stack2;
  const auto& oprs = expr.operators();
  for (auto it = oprs.rbegin(); it != oprs.rend(); ++it) {
    switch (it->item_case()) {
    case ::common::ExprOpr::kBrace: {
      auto brace = it->brace();
      if (brace == ::common::ExprOpr::Brace::ExprOpr_Brace_LEFT_BRACE) {
        while (!opr_stack.empty() &&
               opr_stack.top().item_case() != ::common::ExprOpr::kBrace) {
          opr_stack2.push(opr_stack.top());
          opr_stack.pop();
        }
        assert(!opr_stack.empty());
        opr_stack.pop();
      } else if (brace == ::common::ExprOpr::Brace::ExprOpr_Brace_RIGHT_BRACE) {
        opr_stack.emplace(*it);
      }
      break;
    }
    case ::common::ExprOpr::kArith:
    case ::common::ExprOpr::kLogical:
    case ::common::ExprOpr::kDateTimeMinus: {
      // unary operator
      if (it->logical() == ::common::Logical::NOT ||
          it->logical() == ::common::Logical::ISNULL) {
        opr_stack2.push(*it);
        break;
      }
      while (!opr_stack.empty() &&
             get_proiority(opr_stack.top()) <= get_proiority(*it)) {
        opr_stack2.push(opr_stack.top());
        opr_stack.pop();
      }
      opr_stack.push(*it);
      break;
    }
    case ::common::ExprOpr::kConst:
    case ::common::ExprOpr::kVar:
    case ::common::ExprOpr::kParam:
    case ::common::ExprOpr::kExtract:
    case ::common::ExprOpr::kCase:
    case ::common::ExprOpr::kMap:
    case ::common::ExprOpr::kUdfFunc:
    case ::common::ExprOpr::kToInterval:
    case ::common::ExprOpr::kToDate:
    case ::common::ExprOpr::kToDatetime:
    case ::common::ExprOpr::kToTuple:
    case ::common::ExprOpr::kScalarFunc:
    case ::common::ExprOpr::kPathFunc: {
      opr_stack2.push(*it);
      break;
    }
    default: {
      std::cerr << "not support: " << (*it).DebugString() << std::endl;
      break;
    }
    }
  }
  while (!opr_stack.empty()) {
    opr_stack2.push(opr_stack.top());
    opr_stack.pop();
  }
  return build_expr(opr_stack2, ctx_meta, var_type);
}
}  // namespace execution
}  // namespace neug