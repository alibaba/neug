/**
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include "neug/compiler/binder/expression/expression.h"
#include "neug/compiler/binder/expression/scalar_function_expression.h"
#include "neug/compiler/function/arithmetic/vector_arithmetic_functions.h"
#include "neug/compiler/function/cast/vector_cast_functions.h"
#include "neug/compiler/function/date/vector_date_functions.h"
#include "neug/compiler/function/list/vector_list_functions.h"
#include "neug/compiler/function/path/vector_path_functions.h"
#include "neug/compiler/function/schema/vector_node_rel_functions.h"
#include "neug/compiler/function/string/vector_string_functions.h"
#include "neug/compiler/function/struct/vector_struct_functions.h"

namespace neug {
namespace gopt {

enum ScalarType {
  NONE,  // not a valid scalar operation
  ADD,
  SUBTRACT,
  MULTIPLY,
  DIVIDE,
  MODULO,
  POWER,
  CAST,
  TO_DATE,
  TO_DATETIME,
  TO_INTERVAL,
  DATE_PART,
  LABEL,
  PATTERN_EXTRACT,  // startNode, endNode, nodes, rels
  PROPERTIES,       // properties(nodes(), 'name')
  TO_ARRAY,
  UPPER,
  LOWER,
  REVERSE,
  STARTS_WITH,
  ENDS_WITH,
  CONTAINS,
  LIST_CONTAINS
};

class GScalarType {
 public:
  GScalarType(const binder::Expression& expr) : type{analyze(expr)} {}

  ScalarType getType() const { return type; }

  bool isArithmetic() const {
    return type == ScalarType::ADD || type == ScalarType::SUBTRACT ||
           type == ScalarType::MULTIPLY || type == ScalarType::DIVIDE ||
           type == ScalarType::MODULO;
  }

  bool isTemporal() const {
    return type == ScalarType::TO_DATE || type == ScalarType::TO_DATETIME ||
           type == ScalarType::TO_INTERVAL;
  }

 private:
  ScalarType analyze(const binder::Expression& expr) {
    if (expr.expressionType != common::ExpressionType::FUNCTION) {
      return ScalarType::NONE;
    }

    auto& funcExpr = expr.constCast<binder::ScalarFunctionExpression>();
    auto func = funcExpr.getFunction();

    if (func.name == function::AddFunction::name) {
      return ScalarType::ADD;
    } else if (func.name == function::SubtractFunction::name) {
      return ScalarType::SUBTRACT;
    } else if (func.name == function::MultiplyFunction::name) {
      return ScalarType::MULTIPLY;
    } else if (func.name == function::DivideFunction::name) {
      return ScalarType::DIVIDE;
    } else if (func.name == function::ModuloFunction::name) {
      return ScalarType::MODULO;
    } else if (func.name == function::PowerFunction::name) {
      return ScalarType::POWER;
    } else if (func.name.starts_with(function::CastAnyFunction::name)) {
      return ScalarType::CAST;
    } else if (func.name == function::CastToDateFunction::name) {
      return ScalarType::TO_DATE;
    } else if (func.name == function::CastToTimestampFunction::name) {
      return ScalarType::TO_DATETIME;
    } else if (func.name == function::CastToIntervalFunction::name) {
      return ScalarType::TO_INTERVAL;
    } else if (func.name == function::DatePartFunction::name) {
      return ScalarType::DATE_PART;
    } else if (func.name == function::LabelFunction::name) {
      return ScalarType::LABEL;
    } else if (func.name == function::StructExtractFunctions::name ||
               func.name == function::NodesFunction::name ||
               func.name == function::RelsFunction::name) {
      return ScalarType::PATTERN_EXTRACT;
    } else if (func.name == function::PropertiesFunction::name) {
      return ScalarType::PROPERTIES;
    } else if (func.name == function::ListCreationFunction::name) {
      return ScalarType::TO_ARRAY;
    } else if (func.name == function::UpperFunction::name) {
      return ScalarType::UPPER;
    } else if (func.name == function::LowerFunction::name) {
      return ScalarType::LOWER;
    } else if (func.name == function::ReverseFunction::name) {
      return ScalarType::REVERSE;
    } else if (func.name == function::StartsWithFunction::name) {
      return ScalarType::STARTS_WITH;
    } else if (func.name == function::EndsWithFunction::name) {
      return ScalarType::ENDS_WITH;
    } else if (func.name == function::ContainsFunction::name) {
      return ScalarType::CONTAINS;
    } else if (func.name == function::ListContainsFunction::name) {
      return ScalarType::LIST_CONTAINS;
    }

    // todo: support more scalar functions

    return ScalarType::NONE;
  }

  ScalarType type;
};
}  // namespace gopt
}  // namespace neug