#pragma once

#include "exception.h"
#include "neug/compiler/common/api.h"

namespace gs {
namespace common {

class KUZU_API TransactionManagerException : public Exception {
 public:
  explicit TransactionManagerException(const std::string& msg)
      : Exception(msg){};
};

}  // namespace common
}  // namespace gs
