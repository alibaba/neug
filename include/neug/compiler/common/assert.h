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

#include "neug/compiler/common/string_format.h"
#include "neug/utils/exception/exception.h"

namespace gs {
namespace common {

[[noreturn]] inline void kuAssertFailureInternal(const char* condition_name,
                                                 const char* file, int linenr) {
  // LCOV_EXCL_START
  THROW_INTERNAL_EXCEPTION(
      stringFormat("Assertion failed in file \"{}\" on line {}: {}", file,
                   linenr, condition_name));
  // LCOV_EXCL_STOP
}

#if defined(KUZU_RUNTIME_CHECKS) || !defined(NDEBUG)
#define RUNTIME_CHECK(code) code
#define KU_ASSERT(condition)   \
  static_cast<bool>(condition) \
      ? void(0)                \
      : gs::common::kuAssertFailureInternal(#condition, __FILE__, __LINE__)
#else
#define KU_ASSERT(condition) void(0)
#define RUNTIME_CHECK(code) void(0)
#endif

#define KU_UNREACHABLE                                                    \
  /* LCOV_EXCL_START */ [[unlikely]] gs::common::kuAssertFailureInternal( \
      "KU_UNREACHABLE", __FILE__, __LINE__) /* LCOV_EXCL_STOP */
#define KU_UNUSED(expr) (void) (expr)

}  // namespace common
}  // namespace gs
