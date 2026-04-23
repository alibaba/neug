/**
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include "neug/execution/common/types/value.h"

namespace neug {
namespace execution {

// Value already defines operator< and operator==. FaSTest code expects the
// full set of relational operators, so round out the interface here using the
// two primitives the upstream class provides.

inline bool operator>(const Value& lhs, const Value& rhs) {
    return rhs < lhs;
}

inline bool operator<=(const Value& lhs, const Value& rhs) {
    return !(rhs < lhs);
}

inline bool operator>=(const Value& lhs, const Value& rhs) {
    return !(lhs < rhs);
}

inline bool operator!=(const Value& lhs, const Value& rhs) {
    return !(lhs == rhs);
}

}  // namespace execution
}  // namespace neug
