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

#include "neug/compiler/gopt/g_expr_converter.h"

#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/case_conv.hpp>
#include <cstdint>
#include <ios>
#include <memory>
#include <string>
#include <vector>
#include "neug/compiler/binder/expression/expression.h"
#include "neug/compiler/binder/expression/literal_expression.h"
#include "neug/compiler/binder/expression/property_expression.h"
#include "neug/compiler/binder/expression/rel_expression.h"
#include "neug/compiler/binder/expression/scalar_function_expression.h"
#include "neug/compiler/binder/expression/variable_expression.h"
#include "neug/compiler/common/enums/expression_type.h"
#include "neug/compiler/common/exception/exception.h"
#include "neug/compiler/common/string_utils.h"
#include "neug/compiler/common/types/int128_t.h"
#include "neug/compiler/common/types/types.h"
#include "neug/compiler/common/types/value/value.h"
#include "neug/compiler/function/arithmetic/vector_arithmetic_functions.h"
#include "neug/compiler/gopt/g_alias_name.h"
#include "neug/compiler/gopt/g_scalar_type.h"
#include "neug/proto_generated_gie/common.pb.h"
#include "neug/proto_generated_gie/expr.pb.h"
#include "neug/proto_generated_gie/physical.pb.h"

namespace gs {
namespace gopt {

std::unique_ptr<::common::Expression> GExprConverter::convert(
    const binder::Expression& expr, const planner::LogicalOperator& child) {
  std::vector<gopt::GAliasName> schemaGAlias;
  aliasManager->extractGAliasNames(child, schemaGAlias);
  std::vector<std::string> schemaAlias;
  for (auto& expr : schemaGAlias) {
    schemaAlias.emplace_back(expr.uniqueName);
  }
  return convert(expr, schemaAlias);
}

std::unique_ptr<::common::Expression> GExprConverter::convert(
    const binder::Expression& expr,
    const std::vector<std::string>& schemaAlias) {
  if (!schemaAlias.empty()) {
    auto exprAlias = expr.getUniqueName();
    // if expr is PATTERN type, it will be converted to variable later in
    // function `convert(expr)`
    if (expr.expressionType != common::ExpressionType::PATTERN &&
        std::find(schemaAlias.begin(), schemaAlias.end(), exprAlias) !=
            schemaAlias.end()) {
      // the expression has been computed, convert the expr as the variable
      binder::VariableExpression var(expr.getDataType().copy(), exprAlias,
                                     exprAlias);
      return convertVariable(var);
    }
  }
  switch (expr.expressionType) {
  case common::ExpressionType::LITERAL:
    return convertLiteral(static_cast<const binder::LiteralExpression&>(
        expr));  // todo: add literal data type
  case common::ExpressionType::PROPERTY:
    return convertProperty(
        static_cast<const binder::PropertyExpression&>(expr));
  case common::ExpressionType::VARIABLE:
    return convertVariable(
        static_cast<const binder::VariableExpression&>(expr));
  case common::ExpressionType::EQUALS:
  case common::ExpressionType::NOT_EQUALS:
  case common::ExpressionType::GREATER_THAN:
  case common::ExpressionType::GREATER_THAN_EQUALS:
  case common::ExpressionType::LESS_THAN:
  case common::ExpressionType::LESS_THAN_EQUALS:
  case common::ExpressionType::AND:
  case common::ExpressionType::OR:
  case common::ExpressionType::NOT:
  case common::ExpressionType::IS_NULL:
    return convertChildren(expr, schemaAlias);
  case common::ExpressionType::PATTERN: {
    return convertPattern(expr.constCast<binder::NodeOrRelExpression>());
  }
  case common::ExpressionType::IS_NOT_NULL: {
    return convertIsNotNull(expr);  // convert to IS NOT NULL
  }
  case common::ExpressionType::FUNCTION: {
    return convertScalarFunc(expr, schemaAlias);  // convert to scalar function
  }
  default:
    throw common::Exception("Unsupported expression type: " + expr.toString());
  }
}

::physical::GroupBy_AggFunc::Aggregate convertAggregate(
    const function::AggregateFunction& func) {
  if (func.name == "COUNT" || func.name == "COUNT_STAR") {
    return func.isDistinct ? ::physical::GroupBy_AggFunc::COUNT_DISTINCT
                           : ::physical::GroupBy_AggFunc::COUNT;
  }
  if (func.name == "MIN") {
    return ::physical::GroupBy_AggFunc::MIN;
  }
  if (func.name == "MAX") {
    return ::physical::GroupBy_AggFunc::MAX;
  }
  if (func.name == "SUM") {
    return ::physical::GroupBy_AggFunc::SUM;
  }
  if (func.name == "COLLECT") {
    return ::physical::GroupBy_AggFunc::TO_LIST;
  }
  if (func.name == "AVG") {
    return ::physical::GroupBy_AggFunc::AVG;
  }
  throw common::Exception("Unsupported aggregate function: " + func.name);
}

std::unique_ptr<::physical::GroupBy_AggFunc> GExprConverter::convertAggFunc(
    const binder::AggregateFunctionExpression& expr,
    const planner::LogicalOperator& child) {
  auto aggFuncPB = std::make_unique<::physical::GroupBy_AggFunc>();
  auto exprVec = expr.getChildren();
  if (exprVec.empty()) {
    aggFuncPB->mutable_vars()->AddAllocated(
        convertDefaultVar().release());  // default variable for COUNT(*)
  } else {
    // todo: set agg function name
    // set vars in agg func
    for (auto expr : exprVec) {
      auto varPB = aggFuncPB->add_vars();
      auto exprPB = convert(*expr, child);
      *varPB = std::move(*(exprPB->mutable_operators(0)->mutable_var()));
    }
  }
  aggFuncPB->set_aggregate(convertAggregate(expr.getFunction()));
  return aggFuncPB;
}

std::unique_ptr<::common::Expression> GExprConverter::convertPattern(
    const binder::NodeOrRelExpression& expr) {
  auto variable = std::make_unique<::common::Variable>();
  auto aliasName = expr.getUniqueName();
  if (aliasName.empty()) {
    throw common::Exception(
        "Variable name cannot be empty for pattern expression.");
  }
  auto aliasId = aliasManager->getAliasId(aliasName);
  if (aliasId != DEFAULT_ALIAS_ID) {
    variable->set_allocated_tag(convertAlias(aliasId).release());
  }
  std::unique_ptr<::common::IrDataType> varType;
  switch (expr.getDataType().getLogicalTypeID()) {
  case common::LogicalTypeID::NODE: {
    auto& nodeExpr = expr.constCast<binder::NodeExpression>();
    varType = typeConverter.convertNodeType(gopt::GNodeType(nodeExpr));
    break;
  }
  case common::LogicalTypeID::REL:
  case common::LogicalTypeID::RECURSIVE_REL:
  default: {
    auto& relExpr = expr.constCast<binder::RelExpression>();
    varType = typeConverter.convertRelType(gopt::GRelType(relExpr));
    break;
  }
  }
  auto exprType = std::make_unique<::common::IrDataType>();
  exprType->CopyFrom(*varType);
  variable->set_allocated_node_type(varType.release());
  auto exprPB = std::make_unique<::common::Expression>();
  auto oprPB = exprPB->add_operators();
  oprPB->set_allocated_var(variable.release());
  oprPB->set_allocated_node_type(exprType.release());
  return exprPB;
}

std::unique_ptr<::common::Expression> GExprConverter::convertVar(
    common::alias_id_t columnId) {
  auto aliasPB = std::make_unique<::common::NameOrId>();
  aliasPB->set_id(columnId);
  auto varPB = std::make_unique<::common::Variable>();
  varPB->set_allocated_tag(aliasPB.release());
  auto result = std::make_unique<::common::Expression>();
  result->add_operators()->set_allocated_var(varPB.release());
  return result;
}

std::unique_ptr<::algebra::IndexPredicate> GExprConverter::convertPrimaryKey(
    const std::string& key, const binder::Expression& expr) {
  auto keyPB = convertPropertyExpr(key);
  auto constPB = convert(expr, {})->operators(0).const_();
  auto tripletPB = std::make_unique<::algebra::IndexPredicate_Triplet>();
  tripletPB->set_allocated_key(keyPB.release());
  *tripletPB->mutable_const_() = constPB;
  tripletPB->set_cmp(::common::Logical::EQ);
  auto andPB = std::make_unique<::algebra::IndexPredicate_AndPredicate>();
  andPB->mutable_predicates()->AddAllocated(tripletPB.release());
  auto indexPB = std::make_unique<::algebra::IndexPredicate>();
  indexPB->mutable_or_predicates()->AddAllocated(andPB.release());
  return indexPB;
}

std::unique_ptr<::common::Value> GExprConverter::convertValue(
    gs::common::Value value) {
  std::unique_ptr<::common::Value> valuePB =
      std::make_unique<::common::Value>();
  if (value.isNull()) {
    valuePB->set_allocated_none(new ::common::None());
    return valuePB;
  }
  switch (value.getDataType().getLogicalTypeID()) {
  case common::LogicalTypeID::BOOL:
    valuePB->set_boolean(value.getValue<bool>());
    break;
  case common::LogicalTypeID::INT32:
    valuePB->set_i32(value.getValue<int32_t>());
    break;
  case common::LogicalTypeID::INT64:
    valuePB->set_i64(value.getValue<int64_t>());
    break;
  case common::LogicalTypeID::FLOAT:
    valuePB->set_f32(value.getValue<float>());
    break;
  case common::LogicalTypeID::DOUBLE:
    valuePB->set_f64(value.getValue<double>());
    break;
  case common::LogicalTypeID::STRING:
    valuePB->set_str(value.getValue<std::string>());
    break;
  case common::LogicalTypeID::UINT32:
    valuePB->set_u32(value.getValue<uint32_t>());
    break;
  case common::LogicalTypeID::UINT64:
    valuePB->set_u64(value.getValue<uint64_t>());
    break;
  case common::LogicalTypeID::INT128:
    // todo: hack ways to convert int128 to uint64, int128 is unsupported in PB
    // yet.
    valuePB->set_u64(value.getValue<common::int128_t>().low);
    break;
  default:
    throw common::Exception("Unsupported value type " +
                            value.getDataType().toString());
  }
  return valuePB;
}

std::unique_ptr<::common::NameOrId> GExprConverter::convertAlias(
    common::alias_id_t aliasId) {
  auto alias = std::make_unique<::common::NameOrId>();
  alias->set_id(aliasId);
  return alias;
}

std::unique_ptr<::common::Expression> GExprConverter::convertLiteral(
    const binder::LiteralExpression& expr) {
  auto result = std::make_unique<::common::Expression>();
  auto literal = result->add_operators();
  literal->set_allocated_const_(convertValue(expr.getValue()).release());
  return result;
}

std::unique_ptr<::common::Variable> GExprConverter::convertDefaultVar() {
  auto variable = std::make_unique<::common::Variable>();
  return variable;
}

std::unique_ptr<::common::Expression> GExprConverter::convertScalarFunc(
    const binder::Expression& expr,
    const std::vector<std::string>& schemaAlias) {
  GScalarType scalarType{expr};
  if (scalarType.isArithmetic()) {
    return convertChildren(expr, schemaAlias);
  } else if (scalarType.getType() == CAST && !expr.getChildren().empty()) {
    return convertCast(expr, schemaAlias);
  } else if (scalarType.isTemporal()) {
    return convertTemporalFunc(expr);
  } else if (scalarType.getType() == DATE_PART) {
    return convertExtractFunc(expr);
  }
  throw common::Exception("Unsupported expression type: " + expr.toString());
}

std::unique_ptr<::common::Property> GExprConverter::convertPropertyExpr(
    const std::string& propName) {
  auto propPB = std::make_unique<::common::Property>();
  if (propName == common::InternalKeyword::ID) {
    propPB->set_allocated_id(new ::common::IdKey());
  } else if (propName == common::InternalKeyword::LABEL) {
    propPB->set_allocated_label(new ::common::LabelKey());
  } else if (propName == common::InternalKeyword::LENGTH) {
    propPB->set_allocated_len(new ::common::LengthKey());
  } else {
    auto namePB = std::make_unique<::common::NameOrId>();
    namePB->set_name(propName);
    propPB->set_allocated_key(namePB.release());
  }
  return propPB;
}

std::unique_ptr<::common::Expression> GExprConverter::convertProperty(
    const binder::PropertyExpression& expr) {
  auto result = std::make_unique<::common::Expression>();
  auto opr = result->add_operators();
  auto aliasName = expr.getVariableName();  // unique name
  auto propertyName = expr.getPropertyName();
  auto aliasId = aliasManager->getAliasId(std::move(aliasName));
  std::unique_ptr<::common::Variable> variable =
      std::make_unique<::common::Variable>();
  if (aliasId != DEFAULT_ALIAS_ID) {
    variable->set_allocated_tag(convertAlias(aliasId).release());
  }
  auto property = convertPropertyExpr(propertyName);
  variable->set_allocated_property(property.release());
  auto varType = typeConverter.convertLogicalType(expr.dataType, expr);
  auto exprType = std::make_unique<::common::IrDataType>();
  exprType->CopyFrom(*varType);
  variable->set_allocated_node_type(varType.release());
  opr->set_allocated_var(variable.release());
  opr->set_allocated_node_type(exprType.release());
  return result;
}

std::unique_ptr<::common::Expression> GExprConverter::convertVariable(
    const binder::VariableExpression& expr) {
  auto result = std::make_unique<::common::Expression>();
  auto opr = result->add_operators();
  auto aliasName = expr.getUniqueName();
  auto aliasId = aliasManager->getAliasId(std::move(aliasName));
  std::unique_ptr<::common::Variable> variable =
      std::make_unique<::common::Variable>();
  if (aliasId != DEFAULT_ALIAS_ID) {
    variable->set_allocated_tag(convertAlias(aliasId).release());
  }
  auto varType = typeConverter.convertLogicalType(expr.dataType, expr);
  auto exprType = std::make_unique<::common::IrDataType>();
  exprType->CopyFrom(*varType);
  variable->set_allocated_node_type(varType.release());
  opr->set_allocated_var(variable.release());
  opr->set_allocated_node_type(exprType.release());
  return result;
}

std::unique_ptr<::common::ExprOpr> GExprConverter::convertOperator(
    const binder::Expression& expr) {
  auto result = std::make_unique<::common::ExprOpr>();
  result->set_allocated_node_type(
      typeConverter.convertLogicalType(expr.getDataType(), expr).release());

  switch (expr.expressionType) {
  case common::ExpressionType::OR:
    result->set_logical(::common::Logical::OR);
    break;
  case common::ExpressionType::AND:
    result->set_logical(::common::Logical::AND);
    break;
  case common::ExpressionType::EQUALS:
    result->set_logical(::common::Logical::EQ);
    break;
  case common::ExpressionType::NOT_EQUALS:
    result->set_logical(::common::Logical::NE);
    break;
  case common::ExpressionType::GREATER_THAN:
    result->set_logical(::common::Logical::GT);
    break;
  case common::ExpressionType::GREATER_THAN_EQUALS:
    result->set_logical(::common::Logical::GE);
    break;
  case common::ExpressionType::LESS_THAN:
    result->set_logical(::common::Logical::LT);
    break;
  case common::ExpressionType::LESS_THAN_EQUALS:
    result->set_logical(::common::Logical::LE);
    break;
  case common::ExpressionType::NOT:
    result->set_logical(::common::Logical::NOT);
    break;
  case common::ExpressionType::IS_NULL:
    result->set_logical(::common::Logical::ISNULL);
    break;
  case common::ExpressionType::FUNCTION: {
    GScalarType scalarType{expr};
    switch (scalarType.getType()) {
    case ScalarType::ADD:
      result->set_arith(::common::Arithmetic::ADD);
      break;
    case ScalarType::SUBTRACT:
      result->set_arith(::common::Arithmetic::SUB);
      break;
    case ScalarType::MULTIPLY:
      result->set_arith(::common::Arithmetic::MUL);
      break;
    case ScalarType::DIVIDE:
      result->set_arith(::common::Arithmetic::DIV);
      break;
    case ScalarType::MODULO:
      result->set_arith(::common::Arithmetic::MOD);
      break;
    default:
      throw common::Exception("Unsupported scalar function: " +
                              expr.toString() + " in convertOperator");
    }
    break;
  }
  default:
    throw common::Exception("Unsupported expression: " + expr.toString() +
                            " in convertOperator");
  }
  return result;
}

::std::unique_ptr<::common::Expression> GExprConverter::convertCast(
    const binder::Expression& expr,
    const std::vector<std::string>& schemaAlias) {
  if (expr.expressionType != common::ExpressionType::FUNCTION) {
    throw common::Exception("CAST function should be a function expression");
  }
  auto& scalarExpr = expr.constCast<binder::ScalarFunctionExpression>();
  auto children = expr.getChildren();
  if (children.empty()) {
    throw common::Exception("CAST function should have at least one children");
  }
  auto sourceExpr = children[0];
  if (sourceExpr->expressionType != common::ExpressionType::LITERAL) {
    return convert(*sourceExpr, schemaAlias);
  }
  auto sourceValue =
      sourceExpr->constCast<binder::LiteralExpression>().getValue();
  auto execFunc = scalarExpr.getFunction().execFunc;
  // construct parameters of the cast function
  // construct input parameters
  auto inputVec = std::make_shared<common::ValueVector>(
      scalarExpr.getChild(0)->getDataType().copy());
  inputVec->copyFromValue(0, sourceValue);
  auto state = std::make_shared<common::DataChunkState>(1);
  state->initOriginalAndSelectedSize(1);
  inputVec->setState(state);
  std::vector<std::shared_ptr<common::ValueVector>> inputParams{inputVec};
  // construct output parameters
  common::ValueVector outputVec(scalarExpr.getBindData()->resultType.copy());
  outputVec.setState(state);
  // exec the cast function with parameters
  execFunc(inputParams, common::SelectionVector::fromValueVectors(inputParams),
           outputVec, outputVec.getSelVectorPtr(), scalarExpr.getBindData());
  // extract casted value from the ouput vector
  auto castValue = outputVec.getAsValue(0);
  auto valuePB = convertValue(*castValue);
  auto exprPB = std::make_unique<::common::Expression>();
  exprPB->add_operators()->set_allocated_const_(valuePB.release());
  return exprPB;
}

std::unique_ptr<::common::Expression> GExprConverter::convertTemporalFunc(
    const binder::Expression& expr) {
  if (expr.getChildren().size() != 1) {
    throw common::Exception("temporal function should have exactly one child");
  }
  auto child = expr.getChild(0);
  GScalarType type{expr};
  auto exprPB = std::make_unique<::common::Expression>();
  switch (type.getType()) {
  case ScalarType::TO_DATE: {
    auto date = std::make_unique<::common::ToDate>();
    date->set_date_str(child->toString());
    exprPB->add_operators()->set_allocated_to_date(date.release());
    break;
  }
  case ScalarType::TO_DATETIME: {
    auto datetime = std::make_unique<::common::ToDatetime>();
    datetime->set_datetime_str(child->toString());
    exprPB->add_operators()->set_allocated_to_datetime(datetime.release());
    break;
  }
  case ScalarType::TO_INTERVAL: {
    auto interval = std::make_unique<::common::ToInterval>();
    interval->set_interval_str(child->toString());
    exprPB->add_operators()->set_allocated_to_interval(interval.release());
    break;
  }
  default:
    throw common::Exception("Unsupported scalar function " + expr.toString() +
                            " in temporal func");
  }
  auto typePB = typeConverter.convertLogicalType(expr.getDataType(), expr);
  exprPB->mutable_operators(0)->set_allocated_node_type(typePB.release());
  return exprPB;
}

::common::Extract::Interval GExprConverter::convertTemporalField(
    const binder::Expression& field) {
  std::string fieldName = field.toString();
  common::StringUtils::toLower(fieldName);
  if (fieldName == "year") {
    return ::common::Extract::YEAR;
  } else if (fieldName == "month") {
    return ::common::Extract::MONTH;
  } else if (fieldName == "day") {
    return ::common::Extract::DAY;
  } else if (fieldName == "hour") {
    return ::common::Extract::HOUR;
  } else if (fieldName == "minute") {
    return ::common::Extract::MINUTE;
  } else if (fieldName == "second") {
    return ::common::Extract::SECOND;
  } else if (fieldName == "millisecond") {
    return ::common::Extract::MILLISECOND;
  }
  throw common::Exception("invalid interval field " + fieldName);
}

std::unique_ptr<::common::Expression> GExprConverter::convertExtractFunc(
    const binder::Expression& expr) {
  GScalarType type{expr};
  if (type.getType() != ScalarType::DATE_PART) {
    throw common::Exception("Unsupport scalar function " + expr.toString() +
                            "in extract func");
  }
  auto children = expr.getChildren();
  if (children.size() != 2) {
    throw common::Exception(
        "extract function should have exactly two children, but is " +
        children.size());
  }
  auto extractPB = std::make_unique<::common::Extract>();
  extractPB->set_interval(convertTemporalField(*expr.getChild(0)));
  auto exprPB = std::make_unique<::common::Expression>();
  exprPB->add_operators()->set_allocated_extract(extractPB.release());
  auto extractFrom = convert(*expr.getChild(1), {});
  *exprPB->add_operators() = std::move(*extractFrom->mutable_operators(0));
  return exprPB;
}

std::unique_ptr<::common::Expression> GExprConverter::convertChildren(
    const binder::Expression& expr,
    const std::vector<std::string>& schemaAlias) {
  bool leftAssociate = preced.isLeftAssociative(expr);
  auto children = expr.getChildren();
  auto result = std::make_unique<::common::Expression>();
  for (size_t i = 0; i < children.size(); ++i) {
    size_t idx = leftAssociate ? i : (children.size() - 1 - i);
    auto& child = children[idx];

    if (children.size() == 1  // unary operator, i.e. IS_NULL, NOT
        || (leftAssociate && i > 0) ||
        (!leftAssociate && idx < children.size() - 1)) {
      auto opPB = convertOperator(expr);
      if (opPB &&
          opPB->item_case() != ::common::ExprOpr::ItemCase::ITEM_NOT_SET) {
        *result->add_operators() = std::move(*opPB);
      }
    }

    bool needBrace = preced.needBrace(expr, *child);
    if (needBrace) {
      auto leftBrace = result->add_operators();
      leftBrace->set_brace(::common::ExprOpr::Brace::ExprOpr_Brace_LEFT_BRACE);
    }

    auto childExpr = convert(*child, schemaAlias);
    for (size_t j = 0; j < childExpr->operators_size(); ++j) {
      auto& childOp = *result->add_operators();
      childOp = std::move(*childExpr->mutable_operators(j));
    }

    if (needBrace) {
      auto rightBrace = result->add_operators();
      rightBrace->set_brace(
          ::common::ExprOpr::Brace::ExprOpr_Brace_RIGHT_BRACE);
    }
  }
  return result;
}

std::unique_ptr<::common::Expression> GExprConverter::convertIsNotNull(
    const binder::Expression& expr) {
  if (expr.getNumChildren() != 1) {
    throw common::Exception(
        "IS_NOT_NULL expressions must have exactly one child.");
  }
  auto result = std::make_unique<::common::Expression>();
  auto notOp = result->add_operators();
  notOp->set_logical(::common::Logical::NOT);
  notOp->set_allocated_node_type(
      typeConverter.convertLogicalType(expr.getDataType(), expr).release());
  auto leftBrace = result->add_operators();
  leftBrace->set_brace(::common::ExprOpr::Brace::ExprOpr_Brace_LEFT_BRACE);
  auto isnullOp = result->add_operators();
  isnullOp->set_allocated_node_type(
      typeConverter.convertLogicalType(expr.getDataType(), expr).release());
  isnullOp->set_logical(::common::Logical::ISNULL);
  auto childExpr = convert(*expr.getChild(0), {});
  auto childOp = result->add_operators();
  *childOp = std::move(*childExpr->mutable_operators(0));
  auto rightBrace = result->add_operators();
  rightBrace->set_brace(::common::ExprOpr::Brace::ExprOpr_Brace_RIGHT_BRACE);
  return result;
}

}  // namespace gopt
}  // namespace gs