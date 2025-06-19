#include "src/include/binder/binder.h"
#include "src/include/binder/bound_standalone_call.h"
#include "src/include/binder/expression/expression_util.h"
#include "src/include/binder/expression_visitor.h"
#include "src/include/common/cast.h"
#include "src/include/common/exception/binder.h"
#include "src/include/main/client_context.h"
#include "src/include/main/db_config.h"
#include "src/include/parser/standalone_call.h"

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
    throw BinderException{
        "Invalid option name: " + callStatement.getOptionName() + "."};
  }
  auto optionValue =
      expressionBinder.bindExpression(*callStatement.getOptionValue());
  ExpressionUtil::validateExpressionType(*optionValue, ExpressionType::LITERAL);
  if (LogicalTypeUtils::isFloatingPoint(
          optionValue->dataType.getLogicalTypeID()) &&
      LogicalTypeUtils::isIntegral(LogicalType(option->parameterType))) {
    throw BinderException{stringFormat(
        "Expression {} has data type {} but expected {}. Implicit cast is not "
        "supported.",
        optionValue->toString(),
        LogicalTypeUtils::toString(optionValue->dataType.getLogicalTypeID()),
        LogicalTypeUtils::toString(option->parameterType))};
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
