#pragma once

#include "neug/utils/exception/exception.h"

namespace gs {
namespace common {

class TestException : public Exception {
 public:
  explicit TestException(const std::string& msg)
      : Exception("Test exception: " + msg){};
};

}  // namespace common
}  // namespace gs
