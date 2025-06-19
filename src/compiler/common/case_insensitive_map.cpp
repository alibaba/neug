#include "src/include/common/case_insensitive_map.h"

#include "src/include/common/string_utils.h"

namespace gs {
namespace common {

uint64_t CaseInsensitiveStringHashFunction::operator()(
    const std::string& str) const {
  return common::StringUtils::caseInsensitiveHash(str);
}

bool CaseInsensitiveStringEquality::operator()(const std::string& lhs,
                                               const std::string& rhs) const {
  return common::StringUtils::caseInsensitiveEquals(lhs, rhs);
}

}  // namespace common
}  // namespace gs
