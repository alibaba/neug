#pragma once

#include "neug/compiler/common/api.h"
#include "neug/utils/exception/exception.h"

namespace gs {
namespace common {

class KUZU_API InterruptException : public Exception {
 public:
  explicit InterruptException() : Exception("Interrupted."){};
};

}  // namespace common
}  // namespace gs
