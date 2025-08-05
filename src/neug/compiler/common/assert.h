#pragma once

#include "neug/compiler/common/string_format.h"
#include "neug/utils/exception/exception.h"

namespace gs {
namespace common {

[[noreturn]] inline void kuAssertFailureInternal(const char* condition_name,
                                                 const char* file, int linenr) {
  // LCOV_EXCL_START
  throw exception::InternalException(
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
