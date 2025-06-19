#include "src/include/function/aggregate/avg.h"

namespace gs {
namespace function {

using namespace gs::common;

function_set AggregateAvgFunction::getFunctionSet() {
  function_set result;
  for (auto typeID : LogicalTypeUtils::getNumericalLogicalTypeIDs()) {
    AggregateFunctionUtils::appendSumOrAvgFuncs<AvgFunction>(name, typeID,
                                                             result);
  }
  return result;
}

}  // namespace function
}  // namespace gs
