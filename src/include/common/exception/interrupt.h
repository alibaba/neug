#pragma once

#include "exception.h"
#include "src/include/common/api.h"

namespace gs {
namespace common {

class KUZU_API InterruptException : public Exception {
 public:
  explicit InterruptException() : Exception("Interrupted."){};
};

}  // namespace common
}  // namespace gs
