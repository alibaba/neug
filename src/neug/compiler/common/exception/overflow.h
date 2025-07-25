#pragma once

#include "exception.h"
#include "neug/compiler/common/api.h"

namespace gs {
namespace common {

class KUZU_API OverflowException : public Exception {
 public:
  explicit OverflowException(const std::string& msg)
      : Exception("Overflow exception: " + msg) {}
};

}  // namespace common
}  // namespace gs
