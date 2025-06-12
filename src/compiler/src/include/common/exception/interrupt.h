#pragma once

#include "common/api.h"
#include "exception.h"

namespace gs {
namespace common {

class KUZU_API InterruptException : public Exception {
 public:
  explicit InterruptException() : Exception("Interrupted."){};
};

}  // namespace common
}  // namespace gs
