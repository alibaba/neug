#include "neug/compiler/binder/expression/literal_expression.h"

#include "neug/utils/exception/exception.h"

using namespace gs::common;

namespace gs {
namespace binder {

void LiteralExpression::cast(const LogicalType& type) {
  // The following is a safeguard to make sure we are not changing literal type
  // unexpectedly.
  if (!value.allowTypeChange()) {
    // LCOV_EXCL_START
    THROW_BINDER_EXCEPTION(stringFormat(
        "Cannot change literal expression data type from {} to {}.",
        dataType.toString(), type.toString()));
    // LCOV_EXCL_STOP
  }
  dataType = type.copy();
  value.setDataType(type);
}

}  // namespace binder
}  // namespace gs
