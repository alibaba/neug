#pragma once

#include "exception.h"
#include "src/include/common/api.h"

namespace gs {
namespace common {

class KUZU_API ConnectionException : public Exception {
 public:
  explicit ConnectionException(const std::string& msg)
      : Exception("Connection exception: " + msg){};
};

}  // namespace common
}  // namespace gs
