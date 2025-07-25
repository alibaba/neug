#include "neug/compiler/common/enums/drop_type.h"

#include "neug/compiler/common/assert.h"

namespace gs {
namespace common {

std::string DropTypeUtils::toString(DropType type) {
  switch (type) {
  case DropType::TABLE:
    return "Table";
  case DropType::SEQUENCE:
    return "Sequence";
  default:
    KU_UNREACHABLE;
  }
}

}  // namespace common
}  // namespace gs
