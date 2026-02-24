/**
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * This file is originally from the Kùzu project
 * (https://github.com/kuzudb/kuzu) Licensed under the MIT License. Modified by
 * Zhou Xiaoli in 2025 to support Neug-specific features.
 */

#pragma once

#include <typeinfo>

#include "neug/compiler/common/assert.h"

namespace neug {
namespace common {

template <typename TO, typename FROM>
TO neug_dynamic_cast(FROM* old) {
#if defined(NEUG_RUNTIME_CHECKS) || !defined(NDEBUG)
  static_assert(std::is_pointer<TO>());
  TO newVal = dynamic_cast<TO>(old);
  NEUG_ASSERT(newVal != nullptr);
  return newVal;
#else
  return reinterpret_cast<TO>(old);
#endif
}

template <typename TO, typename FROM>
TO neug_dynamic_cast(FROM& old) {
#if defined(NEUG_RUNTIME_CHECKS) || !defined(NDEBUG)
  static_assert(std::is_reference<TO>());
  try {
    TO newVal = dynamic_cast<TO>(old);
    return newVal;
  } catch (std::bad_cast& e) { NEUG_ASSERT(false); }
#else
  return reinterpret_cast<TO>(old);
#endif
}

}  // namespace common
}  // namespace neug
