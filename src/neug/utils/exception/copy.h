#pragma once

#include "neug/compiler/common/api.h"
#include "neug/utils/exception/exception.h"

namespace gs {
namespace common {

class KUZU_API CopyException : public Exception {
 public:
  explicit CopyException(const std::string& msg)
      : Exception("Copy exception: " + msg){};
};

}  // namespace common
}  // namespace gs
