#include "neug/compiler/common/types/value/recursive_rel.h"

#include "neug/compiler/common/string_format.h"
#include "neug/compiler/common/types/types.h"
#include "neug/compiler/common/types/value/value.h"
#include "neug/utils/exception/exception.h"

namespace gs {
namespace common {

Value* RecursiveRelVal::getNodes(const Value* val) {
  throwIfNotRecursiveRel(val);
  return val->children[0].get();
}

Value* RecursiveRelVal::getRels(const Value* val) {
  throwIfNotRecursiveRel(val);
  return val->children[1].get();
}

void RecursiveRelVal::throwIfNotRecursiveRel(const Value* val) {
  // LCOV_EXCL_START
  if (val->dataType.getLogicalTypeID() != LogicalTypeID::RECURSIVE_REL) {
    throw exception::Exception(
        stringFormat("Expected RECURSIVE_REL type, but got {} type",
                     val->dataType.toString()));
  }
  // LCOV_EXCL_STOP
}

}  // namespace common
}  // namespace gs
