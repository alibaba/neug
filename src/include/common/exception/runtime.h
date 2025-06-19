#pragma once

#include "exception.h"
#include "src/include/common/api.h"

namespace gs {
namespace common {

class KUZU_API RuntimeException : public Exception {
 public:
  explicit RuntimeException(const std::string& msg)
      : Exception("Runtime exception: " + msg){};
};

}  // namespace common
}  // namespace gs
