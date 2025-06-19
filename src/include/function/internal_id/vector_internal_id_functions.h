#pragma once

#include "src/include/function/function.h"

namespace gs {
namespace function {

struct InternalIDCreationFunction {
  static constexpr const char* name = "internal_id";

  static function_set getFunctionSet();
};

}  // namespace function
}  // namespace gs
