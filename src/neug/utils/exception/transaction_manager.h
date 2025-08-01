#pragma once

#include "neug/compiler/common/api.h"
#include "neug/utils/exception/exception.h"

namespace gs {
namespace common {

class KUZU_API TransactionManagerException : public Exception {
 public:
  explicit TransactionManagerException(const std::string& msg)
      : Exception(msg){};
};

}  // namespace common
}  // namespace gs
