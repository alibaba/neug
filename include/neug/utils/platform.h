/** Copyright 2020 Alibaba Group Holding Limited.
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

#include <stdexcept>

/// Cross-platform hint to force a function to be inlined.
#ifdef _WIN32
#define NEUG_ALWAYS_INLINE __forceinline
// MSVC does not support GCC's __attribute__ syntax. This blanket define is
// kept as a fallback for third-party headers that use __attribute__ directly.
// Internal code MUST use platform-independent macros (NEUG_ALWAYS_INLINE,
// NEUG_API, NEUG_DEPRECATED, etc.) instead of raw __attribute__.
#define __attribute__(x)
// MSVC does not have __builtin_prefetch; use _mm_prefetch instead.
#include <xmmintrin.h>
#define __builtin_prefetch(ptr, rw, loc) \
  _mm_prefetch((const char*) (ptr), (loc))
// Windows headers define GetObject as a macro (GetObjectA/GetObjectW),
// which conflicts with rapidjson::Value::GetObject(). Undefine it.
#ifdef GetObject
#undef GetObject
#endif
// Windows headers (winnt.h) define DELETE, OPTIONAL, NONE as macros,
// which conflict with enum values in the codebase.
#ifdef DELETE
#undef DELETE
#endif
#ifdef OPTIONAL
#undef OPTIONAL
#endif
#ifdef NONE
#undef NONE
#endif
#else
#define NEUG_ALWAYS_INLINE __attribute__((always_inline))
#endif
