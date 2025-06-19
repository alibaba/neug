#pragma once

#include "exception.h"
#include "src/include/common/api.h"

namespace gs {
namespace common {

class KUZU_API NotImplementedException : public Exception {
 public:
  explicit NotImplementedException(const std::string& msg) : Exception(msg){};
};

}  // namespace common
}  // namespace gs
