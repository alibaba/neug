#pragma once

#include "exception.h"
#include "src/include/common/api.h"

namespace gs {
namespace common {

class KUZU_API BufferManagerException : public Exception {
 public:
  explicit BufferManagerException(const std::string& msg)
      : Exception("Buffer manager exception: " + msg){};
};

}  // namespace common
}  // namespace gs
