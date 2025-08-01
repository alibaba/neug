#pragma once

#include "neug/compiler/common/api.h"
#include "neug/utils/exception/exception.h"

namespace gs {
namespace common {

class KUZU_API ConversionException : public Exception {
 public:
  explicit ConversionException(const std::string& msg)
      : Exception("Conversion exception: " + msg) {}
};

}  // namespace common
}  // namespace gs
