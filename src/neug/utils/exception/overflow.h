#pragma once

#include "neug/compiler/common/api.h"
#include "neug/utils/exception/exception.h"

namespace gs {
namespace common {

class KUZU_API OverflowException : public Exception {
 public:
  explicit OverflowException(const std::string& msg)
      : Exception("Overflow exception: " + msg) {}
};

}  // namespace common
}  // namespace gs
