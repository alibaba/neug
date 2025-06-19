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
#include "binder/expression/aggregate_function_expression.h"
#include "binder/expression/expression.h"
#include "binder/expression/literal_expression.h"
#include "binder/expression/node_rel_expression.h"
#include "binder/expression/property_expression.h"
#include "binder/expression/variable_expression.h"
#include "common/types/types.h"
#include "gopt/g_alias_manager.h"
#include "gopt/g_precedence.h"
#include "gopt/g_type_converter.h"
#include "src/proto_generated_gie/algebra.pb.h"
#include "src/proto_generated_gie/expr.pb.h"
#include "src/proto_generated_gie/physical.pb.h"

namespace gs {
namespace gopt {

class GExprConverter {
 public:
  GExprConverter(const std::shared_ptr<gopt::GAliasManager> aliasManager)
      : aliasManager{std::move(aliasManager)} {}

  // Main conversion function
  std::unique_ptr<::common::Expression> convert(const binder::Expression& expr);
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

 private:
  // Core expression type converters
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
      const binder::Expression& expr);
  std::unique_ptr<::common::ExprOpr> convertOperator(
      const binder::Expression& expr);

  // helper functions
  std::unique_ptr<::common::Value> convertValue(gs::common::Value value);
  std::unique_ptr<::common::Variable> convertVarProperty(
      const std::string& aliasName, const std::string& propertyName,
      common::LogicalType& type);
  std::unique_ptr<::common::Variable> convertSingleVar(
      const std::string& aliasName, const common::LogicalType& type);
  ::common::Logical convertCompare(common::ExpressionType type);
  std::unique_ptr<::common::Expression> convertPattern(
      const binder::NodeOrRelExpression& expr);
  std::unique_ptr<::common::Property> convertPropertyExpr(
      const std::string& propName);
  std::unique_ptr<::common::Expression> convertScalarFunc(
      const binder::Expression& expr);

 private:
  const std::shared_ptr<gopt::GAliasManager> aliasManager;
  gopt::GTypeConverter typeConverter;
  gopt::GPrecedence preced;
};

}  // namespace gopt
}  // namespace gs