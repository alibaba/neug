#include "neug/compiler/common/types/value/nested.h"

#include "neug/compiler/common/types/value/value.h"
#include "neug/utils/exception/exception.h"

namespace gs {
namespace common {

uint32_t NestedVal::getChildrenSize(const Value* val) {
  return val->childrenSize;
}

Value* NestedVal::getChildVal(const Value* val, uint32_t idx) {
  if (idx > val->childrenSize) {
    throw RuntimeException("NestedVal::getChildVal index out of bound.");
  }
  return val->children[idx].get();
}

}  // namespace common
}  // namespace gs
