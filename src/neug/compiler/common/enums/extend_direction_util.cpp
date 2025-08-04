#include "neug/compiler/common/enums/extend_direction_util.h"

#include "neug/compiler/common/string_utils.h"
#include "neug/utils/exception/exception.h"

namespace gs {
namespace common {

ExtendDirection ExtendDirectionUtil::fromString(const std::string& str) {
  auto normalizedString = StringUtils::getUpper(str);
  if (normalizedString == "FWD") {
    return ExtendDirection::FWD;
  } else if (normalizedString == "BWD") {
    return ExtendDirection::BWD;
  } else if (normalizedString == "BOTH") {
    return ExtendDirection::BOTH;
  } else {
    throw RuntimeException(
        stringFormat("Cannot parse {} as ExtendDirection.", str));
  }
}

std::string ExtendDirectionUtil::toString(ExtendDirection direction) {
  switch (direction) {
  case ExtendDirection::FWD:
    return "fwd";
  case ExtendDirection::BWD:
    return "bwd";
  case ExtendDirection::BOTH:
    return "both";
  default:
    KU_UNREACHABLE;
  }
}

}  // namespace common
}  // namespace gs
