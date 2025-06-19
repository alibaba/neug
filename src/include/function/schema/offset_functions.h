#pragma once

#include "src/include/common/types/types.h"

namespace gs {
namespace function {

struct Offset {
  static void operation(common::internalID_t& input, int64_t& result) {
    result = input.offset;
  }
};

}  // namespace function
}  // namespace gs
