#pragma once

#include "neug/compiler/common/types/ku_string.h"
#include "neug/compiler/common/vector/value_vector.h"
#include "neug/compiler/function/string/functions/base_lower_upper_function.h"

namespace gs {
namespace function {

struct Upper {
 public:
  static inline void operation(common::ku_string_t& input,
                               common::ku_string_t& result,
                               common::ValueVector& resultValueVector) {
    BaseLowerUpperFunction::operation(input, result, resultValueVector,
                                      true /* isUpper */);
  }
};

}  // namespace function
}  // namespace gs
