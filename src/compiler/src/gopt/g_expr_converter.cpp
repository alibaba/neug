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

#include "gopt/g_expr_converter.h"

#include <cstdint>
#include <memory>
#include <vector>
#include "binder/expression/expression.h"
#include "binder/expression/literal_expression.h"
#include "binder/expression/property_expression.h"
#include "binder/expression/rel_expression.h"
#include "binder/expression/variable_expression.h"
#include "common/enums/expression_type.h"
#include "common/exception/exception.h"
#include "common/types/types.h"
#include "common/types/value/value.h"
#include "src/proto_generated_gie/common.pb.h"
#include "src/proto_generated_gie/expr.pb.h"
#include "src/proto_generated_gie/physical.pb.h"

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
  return convert(expr);
}

std::unique_ptr<::common::Expression> GExprConverter::convert(
    const binder::Expression& expr) {
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
    return convertComparison(expr);
  case common::ExpressionType::AND:
    return convertAnd(expr);
  case common::ExpressionType::PATTERN: {
    return convertPattern(expr.constCast<binder::NodeOrRelExpression>());
  }
  default:
    throw common::Exception(
        "Unsupported expression type: " +
        std::to_string(static_cast<uint8_t>(expr.expressionType)));
  }
}

::physical::GroupBy_AggFunc::Aggregate convertAggregate(
    const function::AggregateFunction& func) {
  if (func.name == "COUNT" || func.name == "COUNT_STAR") {
    return func.isDistinct ? ::physical::GroupBy_AggFunc::COUNT_DISTINCT
                           : ::physical::GroupBy_AggFunc::COUNT;
  }
  throw common::Exception("Unsupported aggregate function: " + func.name);
}

std::unique_ptr<::physical::GroupBy_AggFunc> GExprConverter::convertAggFunc(
    const binder::AggregateFunctionExpression& expr,
    const planner::LogicalOperator& child) {
  auto aggFuncPB = std::make_unique<::physical::GroupBy_AggFunc>();
  // todo: set agg function name
  // set vars in agg func
  for (auto expr : expr.getChildren()) {
    auto varPB = aggFuncPB->add_vars();
    auto exprPB = convert(*expr, child);
    *varPB = std::move(*(exprPB->mutable_operators(0)->mutable_var()));
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
  auto constPB = convert(expr)->operators(0).const_();
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

::common::Logical GExprConverter::convertCompare(common::ExpressionType type) {
  switch (type) {
  case common::ExpressionType::EQUALS:
    return ::common::Logical::EQ;
  case common::ExpressionType::NOT_EQUALS:
    return ::common::Logical::NE;
  case common::ExpressionType::GREATER_THAN:
    return ::common::Logical::GT;
  case common::ExpressionType::GREATER_THAN_EQUALS:
    return ::common::Logical::GE;
  case common::ExpressionType::LESS_THAN:
    return ::common::Logical::LT;
  case common::ExpressionType::LESS_THAN_EQUALS:
    return ::common::Logical::LE;
  default:
    throw common::Exception("Unsupported logical type: " +
                            std::to_string(static_cast<uint8_t>(type)));
  }
}

std::unique_ptr<::common::Value> GExprConverter::convertValue(
    gs::common::Value value) {
  std::unique_ptr<::common::Value> valuePB =
      std::make_unique<::common::Value>();
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
  case common::LogicalTypeID::DOUBLE:
    valuePB->set_f64(value.getValue<double>());
    break;
  case common::LogicalTypeID::STRING:
    valuePB->set_str(value.getValue<std::string>());
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
  auto varType = typeConverter.convertLogicalType(expr.dataType);
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
  auto varType = typeConverter.convertLogicalType(expr.dataType);
  auto exprType = std::make_unique<::common::IrDataType>();
  exprType->CopyFrom(*varType);
  variable->set_allocated_node_type(varType.release());
  opr->set_allocated_var(variable.release());
  opr->set_allocated_node_type(exprType.release());
  return result;
}

std::unique_ptr<::common::Expression> GExprConverter::convertComparison(
    const binder::Expression& expr) {
  std::vector<std::unique_ptr<::common::Expression>> children;
  for (size_t i = 0; i < expr.getNumChildren(); ++i) {
    children.push_back(convert(*expr.getChild(i)));
  }
  if (children.size() != 2) {
    throw common::Exception(
        "Comparison expressions must have exactly two children.");
  }
  auto result = std::make_unique<::common::Expression>();
  auto leftOp = result->add_operators();
  *leftOp = children[0]->operators(0);
  auto comparison = result->add_operators();
  comparison->set_logical(convertCompare(expr.expressionType));
  // todo: set comparison data type
  auto rightOp = result->add_operators();
  *rightOp = children[1]->operators(0);
  return result;
}

std::unique_ptr<::common::Expression> GExprConverter::convertAnd(
    const binder::Expression& expr) {
  std::vector<std::unique_ptr<::common::Expression>> children;
  for (size_t i = 0; i < expr.getNumChildren(); ++i) {
    children.push_back(convert(*expr.getChild(i)));
  }
  if (children.size() < 2) {
    throw common::Exception("AND expressions must have at least two children.");
  }
  auto result = std::make_unique<::common::Expression>();
  auto counter = 0;
  for (auto& child : children) {
    if (counter++ > 0) {
      auto andOp = result->add_operators();
      andOp->set_logical(::common::Logical::AND);
      // todo: set and data type
    }
    auto childOp = result->add_operators();
    *childOp = child->operators(0);
  }
  return result;
}
}  // namespace gopt
}  // namespace gs