#include "neug/compiler/function/scalar_function.h"
#include "neug/compiler/function/struct/vector_struct_functions.h"
#include "neug/compiler/function/union/vector_union_functions.h"

using namespace gs::common;

namespace gs {
namespace function {

function_set UnionExtractFunction::getFunctionSet() {
  function_set functionSet;
  auto function = std::make_unique<ScalarFunction>(
      name,
      std::vector<LogicalTypeID>{LogicalTypeID::UNION, LogicalTypeID::STRING},
      LogicalTypeID::ANY);
  function->bindFunc = StructExtractFunctions::bindFunc;
  function->compileFunc = StructExtractFunctions::compileFunc;
  functionSet.push_back(std::move(function));
  return functionSet;
}

}  // namespace function
}  // namespace gs
