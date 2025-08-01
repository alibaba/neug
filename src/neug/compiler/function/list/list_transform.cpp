#include "neug/compiler/function/list/vector_list_functions.h"
#include "neug/compiler/function/scalar_function.h"
#include "neug/utils/exception/binder.h"

namespace gs {
namespace function {

using namespace common;

static std::unique_ptr<FunctionBindData> bindFunc(
    const ScalarBindFuncInput& input) {
  if (input.arguments[1]->expressionType != ExpressionType::LAMBDA) {
    throw BinderException(stringFormat(
        "The second argument of LIST_TRANSFORM should be a lambda expression "
        "but got {}.",
        ExpressionTypeUtil::toString(input.arguments[1]->expressionType)));
  }
  std::vector<LogicalType> paramTypes;
  paramTypes.push_back(input.arguments[0]->getDataType().copy());
  paramTypes.push_back(input.arguments[1]->getDataType().copy());
  return std::make_unique<FunctionBindData>(
      std::move(paramTypes),
      LogicalType::LIST(input.arguments[1]->getDataType().copy()));
}

static void copyListEntriesToResult(
    const common::ValueVector& inputVector,
    const common::SelectionVector& inputSelVector,
    common::ValueVector& result) {
  for (uint64_t i = 0; i < inputSelVector.getSelSize(); ++i) {
    auto pos = inputSelVector[i];
    result.setNull(pos, inputVector.isNull(pos));

    auto inputList = inputVector.getValue<list_entry_t>(pos);
    ListVector::addList(&result, inputList.size);
    result.setValue(pos, inputList);
  }
}

static void execFunc(
    const std::vector<std::shared_ptr<common::ValueVector>>& input,
    const std::vector<common::SelectionVector*>& inputSelVectors,
    common::ValueVector& result, common::SelectionVector* resultSelVector,
    void* bindData) {}

function_set ListTransformFunction::getFunctionSet() {
  function_set result;
  auto function = std::make_unique<ScalarFunction>(
      name, std::vector<LogicalTypeID>{LogicalTypeID::LIST, LogicalTypeID::ANY},
      LogicalTypeID::LIST, execFunc);
  function->bindFunc = bindFunc;
  function->isListLambda = true;
  result.push_back(std::move(function));
  return result;
}

}  // namespace function
}  // namespace gs
