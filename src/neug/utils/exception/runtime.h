#pragma once

#include "neug/compiler/common/api.h"
#include "neug/utils/exception/exception.h"

namespace gs {
namespace common {

class KUZU_API RuntimeException : public Exception {
 public:
  explicit RuntimeException(const std::string& msg)
      : Exception("Runtime exception: " + msg){};
};

}  // namespace common
}  // namespace gs
