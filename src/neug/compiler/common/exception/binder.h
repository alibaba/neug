#pragma once

#include "exception.h"
#include "neug/compiler/common/api.h"

namespace gs {
namespace common {

class KUZU_API BinderException : public Exception {
 public:
  explicit BinderException(const std::string& msg)
      : Exception("Binder exception: " + msg){};
};

}  // namespace common
}  // namespace gs
