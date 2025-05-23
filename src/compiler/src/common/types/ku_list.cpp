#include "common/types/ku_list.h"

#include <cstring>

namespace kuzu {
namespace common {

void ku_list_t::set(const uint8_t* values, const LogicalType& dataType) const {}

void ku_list_t::set(const std::vector<uint8_t*>& parameters, LogicalTypeID childTypeId) {}

} // namespace common
} // namespace kuzu
