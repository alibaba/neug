#pragma once

#include "exception.h"
#include "src/include/common/api.h"

namespace gs {
namespace common {

class KUZU_API CopyException : public Exception {
 public:
  explicit CopyException(const std::string& msg)
      : Exception("Copy exception: " + msg){};
};

}  // namespace common
}  // namespace gs
