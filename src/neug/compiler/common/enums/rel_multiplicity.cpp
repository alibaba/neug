#include "neug/compiler/common/enums/rel_multiplicity.h"

#include "neug/compiler/common/assert.h"
#include "neug/compiler/common/exception/binder.h"
#include "neug/compiler/common/string_format.h"
#include "neug/compiler/common/string_utils.h"

namespace gs {
namespace common {

RelMultiplicity RelMultiplicityUtils::getFwd(const std::string& str) {
  auto normStr = common::StringUtils::getUpper(str);
  if ("ONE_ONE" == normStr || "ONE_MANY" == normStr) {
    return RelMultiplicity::ONE;
  } else if ("MANY_ONE" == normStr || "MANY_MANY" == normStr) {
    return RelMultiplicity::MANY;
  }
  throw BinderException(
      stringFormat("Cannot bind {} as relationship multiplicity.", str));
}

RelMultiplicity RelMultiplicityUtils::getBwd(const std::string& str) {
  auto normStr = common::StringUtils::getUpper(str);
  if ("ONE_ONE" == normStr || "MANY_ONE" == normStr) {
    return RelMultiplicity::ONE;
  } else if ("ONE_MANY" == normStr || "MANY_MANY" == normStr) {
    return RelMultiplicity::MANY;
  }
  throw BinderException(
      stringFormat("Cannot bind {} as relationship multiplicity.", str));
}

std::string RelMultiplicityUtils::toString(RelMultiplicity multiplicity) {
  switch (multiplicity) {
  case RelMultiplicity::ONE:
    return "ONE";
  case RelMultiplicity::MANY:
    return "MANY";
  default:
    KU_UNREACHABLE;
  }
}

}  // namespace common
}  // namespace gs
