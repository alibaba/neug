#include "neug/compiler/binder/expression/parameter_expression.h"

#include "neug/compiler/common/exception/binder.h"

namespace gs {
using namespace common;

namespace binder {

void ParameterExpression::cast(const LogicalType& type) {
  if (!dataType.containsAny()) {
    // LCOV_EXCL_START
    throw BinderException(stringFormat(
        "Cannot change parameter expression data type from {} to {}.",
        dataType.toString(), type.toString()));
    // LCOV_EXCL_STOP
  }
  dataType = type.copy();
  value.setDataType(type);
}

}  // namespace binder
}  // namespace gs
