#include "common/types/types.h"
#include "common/vector/value_vector.h"
#include "function/function.h"
#include "function/list/vector_list_functions.h"

using namespace gs::common;

namespace gs {
namespace function {

void execQuantifierFunc(
    quantifier_handler handler,
    const std::vector<std::shared_ptr<common::ValueVector>>& input,
    const std::vector<common::SelectionVector*>& inputSelVectors,
    common::ValueVector& result, common::SelectionVector* resultSelVector,
    void* bindData) {}

std::unique_ptr<FunctionBindData> bindQuantifierFunc(
    const ScalarBindFuncInput& input) {
  std::vector<common::LogicalType> paramTypes;
  paramTypes.push_back(input.arguments[0]->getDataType().copy());
  paramTypes.push_back(input.arguments[1]->getDataType().copy());
  return std::make_unique<FunctionBindData>(std::move(paramTypes),
                                            common::LogicalType::BOOL());
}

}  // namespace function
}  // namespace gs
