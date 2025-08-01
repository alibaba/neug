#pragma once

#include "neug/compiler/common/api.h"
#include "neug/utils/exception/exception.h"

namespace gs {
namespace common {

class KUZU_API NotImplementedException : public Exception {
 public:
  explicit NotImplementedException(const std::string& msg) : Exception(msg){};
};

}  // namespace common
}  // namespace gs
