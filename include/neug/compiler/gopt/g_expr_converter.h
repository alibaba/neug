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

#pragma once

#include <memory>
#include <string>
#include <vector>
#include "neug/compiler/binder/expression/aggregate_function_expression.h"
#include "neug/compiler/binder/expression/case_expression.h"
#include "neug/compiler/binder/expression/expression.h"
#include "neug/compiler/binder/expression/literal_expression.h"
#include "neug/compiler/binder/expression/node_rel_expression.h"
#include "neug/compiler/binder/expression/parameter_expression.h"
#include "neug/compiler/binder/expression/property_expression.h"
#include "neug/compiler/binder/expression/scalar_function_expression.h"
#include "neug/compiler/binder/expression/variable_expression.h"
#include "neug/compiler/common/types/types.h"
#include "neug/compiler/common/types/value/value.h"
#include "neug/compiler/gopt/g_alias_manager.h"
#include "neug/compiler/gopt/g_precedence.h"
#include "neug/compiler/gopt/g_scalar_type.h"
#include "neug/compiler/gopt/g_type_converter.h"
#include "neug/config.h"
#include "neug/generated/proto/plan/algebra.pb.h"
#include "neug/generated/proto/plan/common.pb.h"
#include "neug/generated/proto/plan/expr.pb.h"
#include "neug/generated/proto/plan/physical.pb.h"

namespace neug {
namespace gopt {

class GExprConverter {
 public:
  GExprConverter(const std::shared_ptr<gopt::GAliasManager> aliasManager)
      : aliasManager{std::move(aliasManager)} {}

  // Main conversion function
  std::unique_ptr<::common::Expression> convert(
      const binder::Expression& expr,
      const std::vector<std::string>& schemaAlias);
  std::unique_ptr<::common::Expression> convert(
      const binder::Expression& expr, const planner::LogicalOperator& child);
  std::unique_ptr<::algebra::IndexPredicate> convertPrimaryKey(
      const std::string& key, const binder::Expression& expr);
  std::unique_ptr<::common::Expression> convertVar(common::alias_id_t columnId);
  std::unique_ptr<::common::NameOrId> convertAlias(common::alias_id_t aliasId);
  std::unique_ptr<::physical::GroupBy_AggFunc> convertAggFunc(
      const binder::AggregateFunctionExpression& expr,
      const planner::LogicalOperator& child);
  std::unique_ptr<::common::Variable> convertDefaultVar();
  std::unique_ptr<::common::Value> convertDefaultValue(
      const binder::PropertyDefinition& propertyDef);
  std::unique_ptr<::common::Property> convertPropertyExpr(
      const std::string& propName);

 private:
  // Core expression type converters
  // convert dynamic parameter
  std::unique_ptr<::common::Expression> convertParam(
      const binder::ParameterExpression& expr);
  std::unique_ptr<::common::Expression> convertLiteral(
      const binder::LiteralExpression& expr);
  std::unique_ptr<::common::Expression> convertProperty(
      const binder::PropertyExpression& expr);
  std::unique_ptr<::common::Expression> convertVariable(
      const binder::VariableExpression& expr);
  std::unique_ptr<::common::Expression> convertComparison(
      const binder::Expression& expr);
  std::unique_ptr<::common::Expression> convertAnd(
      const binder::Expression& expr);
  std::unique_ptr<::common::Expression> convertIsNull(
      const binder::Expression& expr);
  std::unique_ptr<::common::Expression> convertIsNotNull(
      const binder::Expression& expr);
  std::unique_ptr<::common::Expression> convertChildren(
      const binder::Expression& expr,
      const std::vector<std::string>& schemaAlias);
  std::unique_ptr<::common::ExprOpr> convertOperator(
      const binder::Expression& expr);

  std::unique_ptr<::common::Expression> convertTemporalFunc(
      const binder::Expression& expr);

  std::unique_ptr<::common::Expression> convertExtensionFunc(
      const binder::ScalarFunctionExpression& expr,
      const std::vector<std::string>& schemaAlias);

  std::unique_ptr<::common::Expression> convertExtractFunc(
      const binder::Expression& expr);

  ::common::Extract::Interval convertTemporalField(
      const binder::Expression& field);
  ::std::unique_ptr<::common::Expression> convertCast(
      const binder::Expression& expr,
      const std::vector<std::string>& schemaAlias);
  std::unique_ptr<::common::Expression> convertLabel(
      const binder::Expression& expr);

  std::unique_ptr<::common::Expression> convertToTupleFunc(
      const binder::Expression& expr,
      const std::vector<std::string>& schemaAlias);

  std::unique_ptr<::common::Expression> convertToListFunc(
      const binder::Expression& expr,
      const std::vector<std::string>& schemaAlias);

  std::unique_ptr<::common::Expression> convertCaseExpression(
      const binder::CaseExpression& expr,
      const std::vector<std::string>& schemaAlias);

  // helper functions
  std::unique_ptr<::common::Value> convertValue(
      const neug::common::Value& value);
  std::unique_ptr<::common::Variable> convertVarProperty(
      const std::string& aliasName, const std::string& propertyName,
      common::LogicalType& type);
  std::unique_ptr<::common::Variable> convertSingleVar(
      const std::string& aliasName, const common::LogicalType& type);
  ::common::Logical convertCompare(common::ExpressionType type);
  std::unique_ptr<::common::Expression> convertPattern(
      const binder::NodeOrRelExpression& expr);
  std::unique_ptr<::common::Expression> convertScalarFunc(
      const binder::Expression& expr,
      const std::vector<std::string>& schemaAlias);
  std::unique_ptr<::common::Expression> convertPatternExtractFunc(
      const binder::Expression& expr,
      const std::vector<std::string>& schemaAlias);
  // project properties for each node or rel in path expand
  std::unique_ptr<::common::Expression> convertPropertiesFunc(
      const binder::Expression& expr,
      const std::vector<std::string>& schemaAlias);
  std::unique_ptr<::common::Expression> convertUDFFunc(
      const std::string& funcName, const binder::Expression& expr,
      size_t paramNum, const std::vector<std::string>& schemaAlias);
  std::unique_ptr<::common::Value> convertToLiteralArray(
      const common::Value& value, const common::LogicalType& childType);
  std::unique_ptr<::common::Expression> convertRegexFunc(
      const binder::Expression& expr, const GScalarType& scalarType,
      const std::vector<std::string>& schemaAlias);
  std::string convertRegexValue(const std::string& regex,
                                const GScalarType& scalarType);
  std::unique_ptr<::common::Expression> convertListContainsFunc(
      const binder::Expression& expr, const GScalarType& scalarType,
      const std::vector<std::string>& schemaAlias);

  std::unique_ptr<::common::Value> castLiteral(
      const binder::Expression& castExpr);

 private:
  const std::shared_ptr<gopt::GAliasManager> aliasManager;
  gopt::GPhysicalTypeConverter typeConverter;
  gopt::GPrecedence preced;
};

}  // namespace gopt
}  // namespace neug