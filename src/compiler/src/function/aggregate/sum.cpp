#include "function/aggregate/sum.h"

namespace gs {
namespace function {

using namespace gs::common;

function_set AggregateSumFunction::getFunctionSet() {
  function_set result;
  for (auto typeID : LogicalTypeUtils::getNumericalLogicalTypeIDs()) {
    AggregateFunctionUtils::appendSumOrAvgFuncs<SumFunction>(name, typeID,
                                                             result);
  }
  return result;
}

}  // namespace function
}  // namespace gs
