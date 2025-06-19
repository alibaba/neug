#include "src/include/function/list/functions/list_position_function.h"

#include "src/include/common/type_utils.h"
#include "src/include/function/list/vector_list_functions.h"
#include "src/include/function/scalar_function.h"

using namespace gs::common;

namespace gs {
namespace function {

static std::unique_ptr<FunctionBindData> bindFunc(
    const ScalarBindFuncInput& input) {
  auto scalarFunction = input.definition->ptrCast<ScalarFunction>();
  TypeUtils::visit(
      input.arguments[1]->getDataType().getPhysicalType(),
      [&scalarFunction]<typename T>(T) {
        scalarFunction->execFunc =
            ScalarFunction::BinaryExecListStructFunction<list_entry_t, T,
                                                         int64_t, ListPosition>;
      });
  return FunctionBindData::getSimpleBindData(input.arguments,
                                             LogicalType::INT64());
}

function_set ListPositionFunction::getFunctionSet() {
  function_set result;
  auto func = std::make_unique<ScalarFunction>(
      name, std::vector<LogicalTypeID>{LogicalTypeID::LIST, LogicalTypeID::ANY},
      LogicalTypeID::INT64);
  func->bindFunc = bindFunc;
  result.push_back(std::move(func));
  return result;
}

}  // namespace function
}  // namespace gs
