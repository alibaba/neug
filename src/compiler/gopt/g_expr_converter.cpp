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

#include <cstdint>
#include <ios>
#include <memory>
#include <ostream>
#include <string>
#include <vector>
#include "neug/compiler/binder/ddl/property_definition.h"
#include "neug/compiler/binder/expression/expression.h"
#include "neug/compiler/binder/expression/literal_expression.h"
#include "neug/compiler/binder/expression/property_expression.h"
#include "neug/compiler/binder/expression/rel_expression.h"
#include "neug/compiler/binder/expression/scalar_function_expression.h"
#include "neug/compiler/binder/expression/variable_expression.h"
#include "neug/compiler/common/enums/expression_type.h"
#include "neug/compiler/common/string_utils.h"
#include "neug/compiler/common/types/date_t.h"
#include "neug/compiler/common/types/int128_t.h"
#include "neug/compiler/common/types/interval_t.h"
#include "neug/compiler/common/types/timestamp_t.h"
#include "neug/compiler/common/types/types.h"
#include "neug/compiler/common/types/value/value.h"
#include "neug/compiler/function/arithmetic/vector_arithmetic_functions.h"
#include "neug/compiler/function/cast/vector_cast_functions.h"
#include "neug/compiler/function/neug_scalar_function.h"
#include "neug/compiler/function/struct/vector_struct_functions.h"
#include "neug/compiler/gopt/g_alias_manager.h"
#include "neug/compiler/gopt/g_alias_name.h"
#include "neug/compiler/gopt/g_scalar_type.h"
#include "neug/compiler/parser/expression/parsed_expression.h"
#include "neug/compiler/parser/expression/parsed_function_expression.h"
#include "neug/compiler/parser/expression/parsed_literal_expression.h"
#include "neug/generated/proto/plan/common.pb.h"
#include "neug/generated/proto/plan/expr.pb.h"
#include "neug/generated/proto/plan/physical.pb.h"
#include "neug/utils/exception/exception.h"

namespace neug {
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
    return convertScalarFunc(expr,
                             schemaAlias);  // convert to scalar function
  }
  case common::ExpressionType::CASE_ELSE: {
    return convertCaseExpression(expr.constCast<binder::CaseExpression>(),
                                 schemaAlias);
  }
  case common::ExpressionType::PARAMETER: {
    return convertParam(expr.constCast<binder::ParameterExpression>());
  }
  default:
    THROW_EXCEPTION_WITH_FILE_LINE("Unsupported expression type: " +
                                   expr.toString());
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
    return func.isDistinct ? ::physical::GroupBy_AggFunc::TO_SET
                           : ::physical::GroupBy_AggFunc::TO_LIST;
  }
  if (func.name == "AVG") {
    return ::physical::GroupBy_AggFunc::AVG;
  }
  THROW_EXCEPTION_WITH_FILE_LINE("Unsupported aggregate function: " +
                                 func.name);
}

std::unique_ptr<::physical::GroupBy_AggFunc> GExprConverter::convertAggFunc(
    const binder::AggregateFunctionExpression& expr,
    const planner::LogicalOperator& child) {
  auto aggFuncPB = std::make_unique<::physical::GroupBy_AggFunc>();
  auto exprVec = expr.getChildren();
  if (!exprVec.empty()) {
    // todo: set agg function name
    // set vars in agg func
    for (auto expr : exprVec) {
      auto varPB = aggFuncPB->add_vars();
      auto exprPB = convert(*expr, child);
      *varPB = std::move(*(exprPB->mutable_operators(0)->mutable_var()));
    }
  }
  // For count(*), we use empty vars to represent '*'
  aggFuncPB->set_aggregate(convertAggregate(expr.getFunction()));
  return aggFuncPB;
}

std::unique_ptr<::common::Expression> GExprConverter::convertPattern(
    const binder::NodeOrRelExpression& expr) {
  auto variable = std::make_unique<::common::Variable>();
  auto aliasName = expr.getUniqueName();
  if (aliasName.empty()) {
    THROW_EXCEPTION_WITH_FILE_LINE(
        "Variable name cannot be empty for pattern expression.");
  }
  auto aliasId = aliasManager->getAliasId(aliasName);
  if (aliasId != DEFAULT_ALIAS_ID) {
    variable->set_allocated_tag(convertAlias(aliasId).release());
  }
  auto varType = typeConverter.convertLogicalType(expr.getDataType());
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
  auto valuePB = convert(expr, {})->operators(0);
  auto tripletPB = std::make_unique<::algebra::IndexPredicate_Triplet>();
  tripletPB->set_allocated_key(keyPB.release());
  if (valuePB.has_const_()) {
    tripletPB->set_allocated_const_(valuePB.release_const_());
  } else if (valuePB.has_param()) {
    tripletPB->set_allocated_param(valuePB.release_param());
  } else {
    THROW_EXCEPTION_WITH_FILE_LINE("Unsupported value type in primary key: " +
                                   expr.getDataType().toString());
  }
  tripletPB->set_cmp(::common::Logical::EQ);
  auto andPB = std::make_unique<::algebra::IndexPredicate_AndPredicate>();
  andPB->mutable_predicates()->AddAllocated(tripletPB.release());
  auto indexPB = std::make_unique<::algebra::IndexPredicate>();
  indexPB->mutable_or_predicates()->AddAllocated(andPB.release());
  return indexPB;
}

std::unique_ptr<::common::Value> GExprConverter::castLiteral(
    const binder::Expression& castExpr) {
  GScalarType type(castExpr);
  if (type.getType() != ScalarType::CAST) {
    return nullptr;
  }
  if (castExpr.getChildren().empty()) {
    return nullptr;
  }
  if (castExpr.getChild(0)->expressionType != common::ExpressionType::LITERAL) {
    return nullptr;
  }
  auto& scalarExpr = castExpr.constCast<binder::ScalarFunctionExpression>();
  auto execFunc = scalarExpr.getFunction().execFunc;
  if (!execFunc) {
    return nullptr;
  }
  auto sourceExpr =
      castExpr.getChild(0)->constPtrCast<binder::LiteralExpression>();
  auto& targetType = castExpr.getDataType();
  // construct parameters of the cast function
  // construct input parameters
  auto inputVec =
      std::make_shared<common::ValueVector>(sourceExpr->getDataType().copy());
  inputVec->copyFromValue(0, sourceExpr->getValue());
  auto state = std::make_shared<common::DataChunkState>(1);
  state->initOriginalAndSelectedSize(1);
  inputVec->setState(state);
  std::vector<std::shared_ptr<common::ValueVector>> inputParams{inputVec};
  // construct output parameters
  common::ValueVector outputVec(targetType.copy());
  outputVec.setState(state);
  // exec the cast function with parameters
  execFunc(inputParams, common::SelectionVector::fromValueVectors(inputParams),
           outputVec, outputVec.getSelVectorPtr(), scalarExpr.getBindData());
  // extract casted value from the ouput vector
  auto castValue = outputVec.getAsValue(0);
  return convertValue(*castValue);
}

// set default value for property definition
std::unique_ptr<::common::Value> GExprConverter::convertDefaultValue(
    const binder::PropertyDefinition& propertyDef) {
  std::shared_ptr<binder::Expression> defaultExpr = propertyDef.boundExpr;
  // the query default value of temporal type (date, datetime, interval) is
  // set by scalar function, here we extract the default value expression from
  // the scalar function
  if (defaultExpr->expressionType == common::ExpressionType::FUNCTION) {
    auto funcExpr =
        defaultExpr->constPtrCast<binder::ScalarFunctionExpression>();
    auto funcName = funcExpr->getFunction().name;
    if (funcName == function::CastToDateFunction::name ||
        funcName == function::CastToTimestampFunction::name ||
        funcName == function::CastToIntervalFunction::name ||
        funcName == function::DateFunction::name ||
        funcName == function::IntervalFunctionAlias::name) {
      if (!funcExpr->getNumChildren()) {
        THROW_EXCEPTION_WITH_FILE_LINE(
            "Temporal function expression should have at least one "
            "child");
      }
      defaultExpr = funcExpr->getChild(0);
    }
  }
  auto valuePB = convert(*defaultExpr, {});
  if (valuePB->operators_size() == 0) {
    THROW_EXCEPTION_WITH_FILE_LINE(
        "Default value expression should not be empty");
  }
  auto oprPB = valuePB->operators(0);
  if (!oprPB.has_const_()) {
    THROW_EXCEPTION_WITH_FILE_LINE(
        "Default value expression should be a constant");
  }
  return std::unique_ptr<::common::Value>(oprPB.release_const_());
}

std::unique_ptr<::common::Value> GExprConverter::convertValue(
    const neug::common::Value& value) {
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
  case common::LogicalTypeID::DATE:
    valuePB->set_str(
        neug::common::Date::toString(value.getValue<neug::common::date_t>()));
    break;
  case common::LogicalTypeID::TIMESTAMP:
    valuePB->set_str(neug::common::Timestamp::toString(
        value.getValue<neug::common::timestamp_t>()));
    break;
  case common::LogicalTypeID::INTERVAL:
    valuePB->set_str(neug::common::Interval::toString(
        value.getValue<neug::common::interval_t>()));
    break;
  case common::LogicalTypeID::ARRAY: {
    auto extraInfo = value.getDataType().getExtraTypeInfo();
    if (extraInfo == nullptr) {
      THROW_EXCEPTION_WITH_FILE_LINE("List type should have extra info");
    }
    auto arrayInfo = extraInfo->constPtrCast<common::ArrayTypeInfo>();
    auto& childType = arrayInfo->getChildType();
    return convertToLiteralArray(value, childType);
  }
  case common::LogicalTypeID::LIST: {
    auto extraInfo = value.getDataType().getExtraTypeInfo();
    if (extraInfo == nullptr) {
      THROW_EXCEPTION_WITH_FILE_LINE("List type should have extra info");
    }
    auto listInfo = extraInfo->constPtrCast<common::ListTypeInfo>();
    auto& childType = listInfo->getChildType();
    return convertToLiteralArray(value, childType);
  }
  default:
    THROW_EXCEPTION_WITH_FILE_LINE("Unsupported value type " +
                                   value.getDataType().toString());
  }
  return valuePB;
}

std::string GExprConverter::convertRegexValue(const std::string& regex,
                                              const GScalarType& scalarType) {
  std::string updateRegex;
  switch (scalarType.getType()) {
  case ScalarType::STARTS_WITH:
    return "^" + regex + ".*";
  case ScalarType::ENDS_WITH:
    return ".*" + regex + "$";
  case ScalarType::CONTAINS:
    return ".*" + regex + ".*";
  default:
    THROW_EXCEPTION_WITH_FILE_LINE("Unsupported regex type " +
                                   scalarType.getType());
  }
}

std::unique_ptr<::common::Expression> GExprConverter::convertListContainsFunc(
    const binder::Expression& expr, const GScalarType& scalarType,
    const std::vector<std::string>& schemaAlias) {
  if (expr.expressionType != common::ExpressionType::FUNCTION) {
    THROW_EXCEPTION_WITH_FILE_LINE(
        "List Contains function should be a function expression");
  }
  auto& scalarExpr = expr.constCast<binder::ScalarFunctionExpression>();
  if (expr.getChildren().size() < 2) {
    THROW_EXCEPTION_WITH_FILE_LINE(
        "List Contains function should have at least two children");
  }
  return convertChildren(expr, schemaAlias);
}

std::unique_ptr<::common::Expression> GExprConverter::convertRegexFunc(
    const binder::Expression& expr, const GScalarType& scalarType,
    const std::vector<std::string>& schemaAlias) {
  if (expr.getNumChildren() != 2) {
    THROW_EXCEPTION_WITH_FILE_LINE("Regex function should have two children");
  }
  auto right = expr.getChild(1);
  if (right->expressionType != common::ExpressionType::LITERAL) {
    THROW_EXCEPTION_WITH_FILE_LINE(
        "Right child of regex function should be a literal");
  }
  auto* literalExpr = right->ptrCast<binder::LiteralExpression>();
  std::string pattern = literalExpr->getValue().getValue<std::string>();
  std::string regexPattern = convertRegexValue(pattern, scalarType);
  literalExpr->value = common::Value(regexPattern);
  return convertChildren(expr, schemaAlias);
}

std::unique_ptr<::common::Value> GExprConverter::convertToLiteralArray(
    const common::Value& value, const common::LogicalType& childType) {
  if (value.children.empty()) {
    THROW_EXCEPTION_WITH_FILE_LINE(
        "Array function should have at least one child");
  }
  auto valuePB = std::make_unique<::common::Value>();
  switch (childType.getLogicalTypeID()) {
  case common::LogicalTypeID::INT32: {
    auto i32Array = valuePB->mutable_i32_array();
    for (auto& child : value.children) {
      i32Array->add_item(child->getValue<int32_t>());
    }
    break;
  }
  case common::LogicalTypeID::INT64: {
    auto i64Array = valuePB->mutable_i64_array();
    for (auto& child : value.children) {
      i64Array->add_item(child->getValue<int64_t>());
    }
    break;
  }
  case common::LogicalTypeID::FLOAT: {
    auto f32Array = valuePB->mutable_f64_array();
    for (auto& child : value.children) {
      f32Array->add_item(child->getValue<float>());
    }
    break;
  }
  case common::LogicalTypeID::DOUBLE: {
    auto f64Array = valuePB->mutable_f64_array();
    for (auto& child : value.children) {
      f64Array->add_item(child->getValue<double>());
    }
    break;
  }
  case common::LogicalTypeID::STRING: {
    auto strArray = valuePB->mutable_str_array();
    for (auto& child : value.children) {
      strArray->add_item(child->getValue<std::string>());
    }
    break;
  }
  default:
    THROW_EXCEPTION_WITH_FILE_LINE("Unsupported value type " +
                                   childType.toString());
  }
  return valuePB;
}

std::unique_ptr<::common::NameOrId> GExprConverter::convertAlias(
    common::alias_id_t aliasId) {
  auto alias = std::make_unique<::common::NameOrId>();
  alias->set_id(aliasId);
  return alias;
}

std::unique_ptr<::common::Expression> GExprConverter::convertParam(
    const binder::ParameterExpression& expr) {
  auto paramPB = std::make_unique<::common::DynamicParam>();
  paramPB->set_name(expr.getName());
  paramPB->set_allocated_data_type(
      typeConverter.convertLogicalType(expr.getDataType().copy()).release());
  // todo: Engine get parameter value by its name (not index) during dynamic
  // parameter execution, here we just set a default 0 for all parameters.
  paramPB->set_index(0);
  auto exprPB = std::make_unique<::common::Expression>();
  exprPB->add_operators()->set_allocated_param(paramPB.release());
  return exprPB;
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

std::unique_ptr<::common::Expression> GExprConverter::convertLabel(
    const binder::Expression& expr) {
  auto children = expr.getChildren();
  if (children.empty()) {
    THROW_EXCEPTION_WITH_FILE_LINE(
        "Label function should have at least one child");
  }
  auto child = children[0];
  if (child->expressionType != common::ExpressionType::PROPERTY) {
    THROW_EXCEPTION_WITH_FILE_LINE(
        "The first child of Label function should be NodeId, but is " +
        child->toString());
  }
  auto nodeID = child->ptrCast<binder::PropertyExpression>();
  binder::PropertyExpression labelExpr(
      expr.getDataType().copy(), common::InternalKeyword::LABEL,
      nodeID->getVariableName(), nodeID->getRawVariableName(), {});
  return convertProperty(labelExpr);
}

std::unique_ptr<::common::Expression> GExprConverter::convertUDFFunc(
    const std::string& funcName, const binder::Expression& expr,
    size_t paramNum, const std::vector<std::string>& schemaAlias) {
  auto udfFuncPB = std::make_unique<::common::UserDefinedFunction>();
  udfFuncPB->set_name(funcName);
  for (size_t i = 0; i < paramNum; i++) {
    auto paramExpr = convert(*expr.getChild(i), schemaAlias);
    udfFuncPB->mutable_parameters()->AddAllocated(paramExpr.release());
  }
  auto exprPB = std::make_unique<::common::Expression>();
  exprPB->add_operators()->set_allocated_udf_func(udfFuncPB.release());
  return exprPB;
}

std::unique_ptr<::common::Expression> GExprConverter::convertPropertiesFunc(
    const binder::Expression& expr,
    const std::vector<std::string>& schemaAlias) {
  if (expr.expressionType != common::ExpressionType::FUNCTION) {
    THROW_EXCEPTION_WITH_FILE_LINE(
        "Properties function should be a function expression");
  }
  auto& scalarExpr = expr.constCast<binder::ScalarFunctionExpression>();
  if (expr.getChildren().size() < 2) {
    THROW_EXCEPTION_WITH_FILE_LINE(
        "Properties function should have at least two children");
  }
  auto pathFuncPB = std::make_unique<::common::PathFunction>();
  // convert property key
  auto literalExpr =
      expr.getChild(1)->constPtrCast<binder::LiteralExpression>();
  auto key = literalExpr->getValue().getValue<std::string>();
  pathFuncPB->set_allocated_property(convertPropertyExpr(key).release());

  // convert path tag
  auto nodeOrRelExpr = expr.getChild(0);
  if (nodeOrRelExpr->getChildren().empty()) {
    THROW_EXCEPTION_WITH_FILE_LINE(
        "The first child of Properties function should be a list of nodes or "
        "rels, but is " +
        expr.getChild(0)->toString());
  }
  auto pathExpr = nodeOrRelExpr->getChild(0);
  auto pathAlias = aliasManager->getAliasId(pathExpr->getUniqueName());
  if (pathAlias != DEFAULT_ALIAS_ID) {
    pathFuncPB->set_allocated_tag(convertAlias(pathAlias).release());
  }

  // convert function opt: vertex or edge
  const auto& listType = expr.getChild(0)->getDataType();
  const auto& childType = common::ListType::getChildType(listType);
  // project properties for each node in path expand
  if (childType.getLogicalTypeID() == common::LogicalTypeID::NODE) {
    pathFuncPB->set_opt(
        ::common::PathFunction::FuncOpt::PathFunction_FuncOpt_VERTEX);
  } else if (childType.getLogicalTypeID() == common::LogicalTypeID::REL) {
    pathFuncPB->set_opt(
        ::common::PathFunction::FuncOpt::PathFunction_FuncOpt_EDGE);
  } else {
    THROW_EXCEPTION_WITH_FILE_LINE(
        "The first child of Properties function should be a list of nodes or "
        "rels, but is " +
        expr.getChild(0)->toString());
  }
  auto oprPB = std::make_unique<::common::ExprOpr>();
  oprPB->set_allocated_path_func(pathFuncPB.release());
  oprPB->set_allocated_node_type(
      typeConverter.convertLogicalType(expr.getDataType()).release());
  auto exprPB = std::make_unique<::common::Expression>();
  *exprPB->add_operators() = std::move(*oprPB);
  return exprPB;
}

std::unique_ptr<::common::Expression> GExprConverter::convertPatternExtractFunc(
    const binder::Expression& expr,
    const std::vector<std::string>& schemaAlias) {
  if (expr.expressionType != common::ExpressionType::FUNCTION) {
    THROW_EXCEPTION_WITH_FILE_LINE(
        "Pattern extract function should be a function expression");
  }
  if (expr.getChildren().empty()) {
    THROW_EXCEPTION_WITH_FILE_LINE(
        "Pattern extract function should have at least one child");
  }
  auto& scalarExpr = expr.constCast<binder::ScalarFunctionExpression>();
  std::string extractKey;
  if (scalarExpr.getFunction().name == function::NodesFunction::name) {
    extractKey = common::InternalKeyword::NODES;
  } else if (scalarExpr.getFunction().name == function::RelsFunction::name) {
    extractKey = common::InternalKeyword::RELS;
  } else if (expr.getChildren().size() > 1 &&
             expr.getChild(1)->expressionType ==
                 common::ExpressionType::LITERAL) {
    extractKey = expr.getChild(1)
                     ->constCast<binder::LiteralExpression>()
                     .getValue()
                     .getValue<std::string>();
  } else {
    THROW_EXCEPTION_WITH_FILE_LINE(
        "cannot evaluate <extract key> from pattern extract function: " +
        expr.toString());
  }
  if (extractKey == common::InternalKeyword::SRC) {
    return convertUDFFunc("gs.function.startNode", expr, 1, schemaAlias);
  } else if (extractKey == common::InternalKeyword::DST) {
    return convertUDFFunc("gs.function.endNode", expr, 1, schemaAlias);
  } else if (extractKey == common::InternalKeyword::NODES) {
    return convertUDFFunc("gs.function.nodes", expr, 1, schemaAlias);
  } else if (extractKey == common::InternalKeyword::RELS) {
    return convertUDFFunc("gs.function.relationships", expr, 1, schemaAlias);
  }
  THROW_EXCEPTION_WITH_FILE_LINE("Unsupported struct extract key: " +
                                 extractKey);
}

bool isVariable(const binder::Expression& expr) {
  if (expr.expressionType == common::ExpressionType::FUNCTION) {
    auto& funcExpr = expr.constCast<binder::ScalarFunctionExpression>();
    GScalarType type{funcExpr};
    if (type.getType() == ScalarType::CAST) {
      return isVariable(*funcExpr.getChild(0));
    }
  }
  return expr.expressionType == common::ExpressionType::VARIABLE ||
         expr.expressionType == common::ExpressionType::PROPERTY;
}

bool isLiteralOrVariable(const binder::Expression& expr) {
  if (expr.expressionType == common::ExpressionType::FUNCTION) {
    auto& funcExpr = expr.constCast<binder::ScalarFunctionExpression>();
    GScalarType type{funcExpr};
    if (type.getType() == ScalarType::CAST) {
      return isLiteralOrVariable(*funcExpr.getChild(0));
    }
  }
  return expr.expressionType == common::ExpressionType::LITERAL ||
         expr.expressionType == common::ExpressionType::VARIABLE ||
         expr.expressionType == common::ExpressionType::PROPERTY;
}

std::unique_ptr<::common::Expression> GExprConverter::convertExtensionFunc(
    const binder::ScalarFunctionExpression& expr,
    const std::vector<std::string>& schemaAlias) {
  // acquire unqiue name
  const auto& func = expr.getFunction();
  // only neug scalar functions can be converted to extension
  if (!dynamic_cast<const function::NeugScalarFunction*>(&func)) {
    THROW_EXCEPTION_WITH_FILE_LINE(
        expr.toString() +
        "' is not a NeuG scalar function, can not convert to extension");
  }
  const auto& signature = func.signatureName;

  auto scalarPB = std::make_unique<::common::ScalarFunction>();
  scalarPB->set_unique_name(signature);

  // convert arguments
  for (auto child : expr.getChildren()) {
    auto childExprPB = convert(*child, schemaAlias);
    scalarPB->mutable_parameters()->AddAllocated(childExprPB.release());
  }

  auto exprPB = std::make_unique<::common::Expression>();
  auto opr = exprPB->add_operators();
  opr->set_allocated_scalar_func(scalarPB.release());
  opr->set_allocated_node_type(
      typeConverter.convertLogicalType(expr.getDataType()).release());
  return exprPB;
}

std::unique_ptr<::common::Expression> GExprConverter::convertToTupleFunc(
    const binder::Expression& expr,
    const std::vector<std::string>& schemaAlias) {
  if (expr.getChildren().empty()) {
    THROW_EXCEPTION_WITH_FILE_LINE(
        "Array function should have at least one child");
  }
  auto tuplePB = std::make_unique<::common::ToTuple>();
  for (auto child : expr.getChildren()) {
    auto exprPB = convert(*child, schemaAlias);
    if (exprPB->operators_size() == 0) {
      THROW_EXCEPTION_WITH_FILE_LINE(
          "convert child of array function failed, empty expression");
    }
    auto fieldPB = tuplePB->add_fields();
    *fieldPB = std::move(*exprPB);
  }
  auto exprPB = std::make_unique<::common::Expression>();
  auto opr = exprPB->add_operators();
  opr->set_allocated_to_tuple(tuplePB.release());
  opr->set_allocated_node_type(
      typeConverter.convertLogicalType(expr.getDataType().copy()).release());
  return exprPB;
}

std::unique_ptr<::common::Expression> GExprConverter::convertToListFunc(
    const binder::Expression& expr,
    const std::vector<std::string>& schemaAlias) {
  if (expr.getChildren().empty()) {
    THROW_EXCEPTION_WITH_FILE_LINE(
        "Array function should have at least one child");
  }
  auto listPB = std::make_unique<::common::ToList>();
  for (auto child : expr.getChildren()) {
    auto exprPB = convert(*child, schemaAlias);
    if (exprPB->operators_size() == 0) {
      THROW_EXCEPTION_WITH_FILE_LINE(
          "convert child of array function failed, empty expression");
    }
    auto fieldPB = listPB->add_fields();
    *fieldPB = std::move(*exprPB);
  }
  auto exprPB = std::make_unique<::common::Expression>();
  auto opr = exprPB->add_operators();
  opr->set_allocated_to_list(listPB.release());
  opr->set_allocated_node_type(
      typeConverter.convertLogicalType(expr.getDataType().copy()).release());
  return exprPB;
}

std::unique_ptr<::common::Expression> GExprConverter::convertCaseExpression(
    const binder::CaseExpression& expr,
    const std::vector<std::string>& schemaAlias) {
  size_t caseNum = expr.getNumCaseAlternatives();
  if (caseNum == 0) {
    THROW_EXCEPTION_WITH_FILE_LINE(
        "Case expression should have at least one case "
        "alternative");
  }
  auto casePB = std::make_unique<::common::Case>();
  for (size_t i = 0; i < caseNum; i++) {
    auto caseAlternative = expr.getCaseAlternative(i);
    auto whenExprPB = convert(*caseAlternative->whenExpression, schemaAlias);
    auto thenExprPB = convert(*caseAlternative->thenExpression, schemaAlias);
    auto alterPB = casePB->add_when_then_expressions();
    alterPB->set_allocated_when_expression(whenExprPB.release());
    alterPB->set_allocated_then_result_expression(thenExprPB.release());
  }
  auto elseExprPB = convert(*expr.getElseExpression(), schemaAlias);
  casePB->set_allocated_else_result_expression(elseExprPB.release());
  auto exprPB = std::make_unique<::common::Expression>();
  exprPB->add_operators()->set_allocated_case_(casePB.release());
  return exprPB;
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
  } else if (scalarType.getType() == LABEL) {
    return convertLabel(expr);
  } else if (scalarType.getType() == PATTERN_EXTRACT) {
    return convertPatternExtractFunc(expr, schemaAlias);
  } else if (scalarType.getType() == PROPERTIES) {
    return convertPropertiesFunc(expr, schemaAlias);
  } else if (scalarType.getType() == TO_LIST) {
    return convertToListFunc(expr, schemaAlias);
  } else if (scalarType.getType() == TO_TUPLE) {
    return convertToTupleFunc(expr, schemaAlias);
  } else if (scalarType.getType() == STARTS_WITH ||
             scalarType.getType() == ENDS_WITH ||
             scalarType.getType() == CONTAINS) {
    return convertRegexFunc(expr, scalarType, schemaAlias);
  } else if (scalarType.getType() == LIST_CONTAINS) {
    return convertListContainsFunc(expr, scalarType, schemaAlias);
  }
  auto& sfExpr = expr.constCast<binder::ScalarFunctionExpression>();
  return convertExtensionFunc(sfExpr, schemaAlias);
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
  bool useName = expr.getUseName();
  if (!useName) {
    if (aliasId != DEFAULT_ALIAS_ID) {
      variable->set_allocated_tag(convertAlias(aliasId).release());
    }
  } else {
    auto namePB = std::make_unique<::common::NameOrId>();
    namePB->set_name(expr.getVariableName());
    variable->set_allocated_tag(namePB.release());
  }
  auto varType = typeConverter.convertLogicalType(expr.dataType);
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
      typeConverter.convertLogicalType(expr.getDataType()).release());

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
    case ScalarType::STARTS_WITH:
    case ScalarType::ENDS_WITH:
    case ScalarType::CONTAINS:
      result->set_logical(::common::Logical::REGEX);
      break;
    case ScalarType::LIST_CONTAINS:
      result->set_logical(::common::Logical::WITHIN);
      break;
    default:
      THROW_EXCEPTION_WITH_FILE_LINE("Unsupported scalar function: " +
                                     expr.toString() + " in convertOperator");
    }
    break;
  }
  default:
    THROW_EXCEPTION_WITH_FILE_LINE(
        "Unsupported expression: " + expr.toString() + " in convertOperator");
  }
  return result;
}

::std::unique_ptr<::common::Expression> GExprConverter::convertCast(
    const binder::Expression& expr,
    const std::vector<std::string>& schemaAlias) {
  if (expr.expressionType != common::ExpressionType::FUNCTION) {
    THROW_EXCEPTION_WITH_FILE_LINE(
        "CAST function should be a function expression");
  }
  auto& scalarExpr = expr.constCast<binder::ScalarFunctionExpression>();
  auto children = expr.getChildren();
  if (children.empty()) {
    THROW_EXCEPTION_WITH_FILE_LINE(
        "CAST function should have at least one children");
  }
  auto sourceExpr = children[0];
  switch (sourceExpr->expressionType) {
  case common::ExpressionType::LITERAL: {
    auto valuePB = castLiteral(expr);
    if (valuePB) {
      auto exprPB = std::make_unique<::common::Expression>();
      exprPB->add_operators()->set_allocated_const_(valuePB.release());
      return exprPB;
    }
  }
  case common::ExpressionType::PARAMETER: {  // cast dynamic param by
                                             // setting its meta data with
                                             // target type
    return convert(*sourceExpr, schemaAlias);
  }
  default: {  // convert to cast extension function for other conditions
    return convertExtensionFunc(scalarExpr, schemaAlias);
  }
  }
}

std::unique_ptr<::common::Expression> GExprConverter::convertTemporalFunc(
    const binder::Expression& expr) {
  if (expr.getChildren().size() != 1) {
    THROW_EXCEPTION_WITH_FILE_LINE(
        "temporal function should have exactly one child");
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
    THROW_EXCEPTION_WITH_FILE_LINE("Unsupported scalar function " +
                                   expr.toString() + " in temporal func");
  }
  auto typePB = typeConverter.convertLogicalType(expr.getDataType());
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
  THROW_EXCEPTION_WITH_FILE_LINE("invalid interval field " + fieldName);
}

std::unique_ptr<::common::Expression> GExprConverter::convertExtractFunc(
    const binder::Expression& expr) {
  GScalarType type{expr};
  if (type.getType() != ScalarType::DATE_PART) {
    THROW_EXCEPTION_WITH_FILE_LINE("Unsupport scalar function " +
                                   expr.toString() + "in extract func");
  }
  auto children = expr.getChildren();
  if (children.size() != 2) {
    THROW_EXCEPTION_WITH_FILE_LINE(
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
    THROW_EXCEPTION_WITH_FILE_LINE(
        "IS_NOT_NULL expressions must have exactly one child.");
  }
  auto result = std::make_unique<::common::Expression>();
  auto notOp = result->add_operators();
  notOp->set_logical(::common::Logical::NOT);
  notOp->set_allocated_node_type(
      typeConverter.convertLogicalType(expr.getDataType()).release());
  auto leftBrace = result->add_operators();
  leftBrace->set_brace(::common::ExprOpr::Brace::ExprOpr_Brace_LEFT_BRACE);
  auto isnullOp = result->add_operators();
  isnullOp->set_allocated_node_type(
      typeConverter.convertLogicalType(expr.getDataType()).release());
  isnullOp->set_logical(::common::Logical::ISNULL);
  auto childExpr = convert(*expr.getChild(0), {});
  auto childOp = result->add_operators();
  *childOp = std::move(*childExpr->mutable_operators(0));
  auto rightBrace = result->add_operators();
  rightBrace->set_brace(::common::ExprOpr::Brace::ExprOpr_Brace_RIGHT_BRACE);
  return result;
}

}  // namespace gopt
}  // namespace neug