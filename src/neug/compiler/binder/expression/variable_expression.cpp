#include "neug/compiler/binder/expression/variable_expression.h"

#include "neug/utils/exception/exception.h"

using namespace gs::common;

namespace gs {
namespace binder {

void VariableExpression::cast(const LogicalType& type) {
  if (!dataType.containsAny()) {
    // LCOV_EXCL_START
    throw BinderException(stringFormat(
        "Cannot change variable expression data type from {} to {}.",
        dataType.toString(), type.toString()));
    // LCOV_EXCL_STOP
  }
  dataType = type.copy();
}

}  // namespace binder
}  // namespace gs
