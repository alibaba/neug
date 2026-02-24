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

/**
 * This file is originally from the Kùzu project
 * (https://github.com/kuzudb/kuzu) Licensed under the MIT License. Modified by
 * Zhou Xiaoli in 2025 to support Neug-specific features.
 */

#include "neug/compiler/binder/binder.h"
#include "neug/compiler/binder/bound_standalone_call.h"
#include "neug/compiler/binder/expression/expression_util.h"
#include "neug/compiler/binder/expression_visitor.h"
#include "neug/compiler/common/cast.h"
#include "neug/compiler/main/client_context.h"
#include "neug/compiler/main/option_config.h"
#include "neug/compiler/parser/standalone_call.h"
#include "neug/utils/exception/exception.h"

using namespace neug::common;

namespace neug {
namespace binder {

std::unique_ptr<BoundStatement> Binder::bindStandaloneCall(
    const parser::Statement& statement) {
  auto& callStatement =
      neug_dynamic_cast<const parser::StandaloneCall&>(statement);
  const main::Option* option =
      clientContext->getExtensionOption(callStatement.getOptionName());
  if (option == nullptr) {
    THROW_BINDER_EXCEPTION(
        "Invalid option name: " + callStatement.getOptionName() + ".");
  }
  auto optionValue =
      expressionBinder.bindExpression(*callStatement.getOptionValue());
  ExpressionUtil::validateExpressionType(*optionValue, ExpressionType::LITERAL);
  if (LogicalTypeUtils::isFloatingPoint(
          optionValue->dataType.getLogicalTypeID()) &&
      LogicalTypeUtils::isIntegral(LogicalType(option->parameterType))) {
    THROW_BINDER_EXCEPTION(stringFormat(
        "Expression {} has data type {} but expected {}. Implicit cast is not "
        "supported.",
        optionValue->toString(),
        LogicalTypeUtils::toString(optionValue->dataType.getLogicalTypeID()),
        LogicalTypeUtils::toString(option->parameterType)));
  }
  optionValue = expressionBinder.implicitCastIfNecessary(
      optionValue, LogicalType(option->parameterType));
  if (ConstantExpressionVisitor::needFold(*optionValue)) {
    optionValue = expressionBinder.foldExpression(optionValue);
  }
  return std::make_unique<BoundStandaloneCall>(option, std::move(optionValue));
}

}  // namespace binder
}  // namespace neug
