#pragma once

#include "neug/compiler/common/api.h"
#include "neug/utils/exception/exception.h"

namespace gs {
namespace common {

class KUZU_API StorageException : public Exception {
 public:
  explicit StorageException(const std::string& msg)
      : Exception("Storage exception: " + msg){};
};

}  // namespace common
}  // namespace gs
