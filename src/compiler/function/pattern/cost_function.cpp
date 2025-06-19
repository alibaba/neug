#include "src/include/binder/expression/rel_expression.h"
#include "src/include/common/exception/binder.h"
#include "src/include/function/rewrite_function.h"
#include "src/include/function/schema/vector_node_rel_functions.h"

using namespace gs::common;
using namespace gs::binder;

namespace gs {
namespace function {

static std::shared_ptr<Expression> rewriteFunc(
    const RewriteFunctionBindInput& input) {
  KU_ASSERT(input.arguments.size() == 1);
  auto param = input.arguments[0].get();
  KU_ASSERT(param->getDataType().getLogicalTypeID() ==
            LogicalTypeID::RECURSIVE_REL);
  auto recursiveInfo = param->ptrCast<RelExpression>()->getRecursiveInfo();
  if (recursiveInfo->bindData->weightOutputExpr == nullptr) {
    throw BinderException(
        stringFormat("Cost function is not defined for {}", param->toString()));
  }
  return recursiveInfo->bindData->weightOutputExpr;
}

function_set CostFunction::getFunctionSet() {
  function_set functionSet;
  auto function = std::make_unique<RewriteFunction>(
      name, std::vector<LogicalTypeID>{LogicalTypeID::RECURSIVE_REL},
      rewriteFunc);
  functionSet.push_back(std::move(function));
  return functionSet;
}

}  // namespace function
}  // namespace gs
