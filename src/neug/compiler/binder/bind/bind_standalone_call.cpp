#include "neug/compiler/binder/binder.h"
#include "neug/compiler/binder/bound_standalone_call.h"
#include "neug/compiler/binder/expression/expression_util.h"
#include "neug/compiler/binder/expression_visitor.h"
#include "neug/compiler/common/cast.h"
#include "neug/compiler/main/client_context.h"
#include "neug/compiler/main/db_config.h"
#include "neug/compiler/parser/standalone_call.h"
#include "neug/utils/exception/exception.h"

using namespace gs::common;

namespace gs {
namespace binder {

std::unique_ptr<BoundStatement> Binder::bindStandaloneCall(
    const parser::Statement& statement) {
  auto& callStatement =
      ku_dynamic_cast<const parser::StandaloneCall&>(statement);
  const main::Option* option =
      main::DBConfig::getOptionByName(callStatement.getOptionName());
  if (option == nullptr) {
    option = clientContext->getExtensionOption(callStatement.getOptionName());
  }
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
}  // namespace gs
