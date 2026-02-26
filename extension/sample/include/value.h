#pragma once

#include "neug/execution/common/types/value.h"

namespace neug {
namespace execution {

// Define missing comparison operators for Value
// Value class already has operator< and operator==, we only add the missing ones

inline bool operator>(const Value& lhs, const Value& rhs) {
    // a > b  <==>  b < a
    return rhs < lhs;
}

inline bool operator<=(const Value& lhs, const Value& rhs) {
    // a <= b  <==>  !(b < a)
    return !(rhs < lhs);
}

inline bool operator>=(const Value& lhs, const Value& rhs) {
    // a >= b  <==>  !(a < b)
    return !(lhs < rhs);
}

inline bool operator!=(const Value& lhs, const Value& rhs) {
    return !(lhs == rhs);
}

}  // namespace execution
}  // namespace neug
