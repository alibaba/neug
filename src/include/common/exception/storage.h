#pragma once

#include "exception.h"
#include "src/include/common/api.h"

namespace gs {
namespace common {

class KUZU_API StorageException : public Exception {
 public:
  explicit StorageException(const std::string& msg)
      : Exception("Storage exception: " + msg){};
};

}  // namespace common
}  // namespace gs
