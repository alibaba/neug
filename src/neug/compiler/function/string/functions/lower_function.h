#pragma once

#include "base_lower_upper_function.h"
#include "neug/compiler/common/types/ku_string.h"

namespace gs {
namespace function {

struct Lower {
 public:
  static inline void operation(common::ku_string_t& input,
                               common::ku_string_t& result,
                               common::ValueVector& resultValueVector) {
    BaseLowerUpperFunction::operation(input, result, resultValueVector,
                                      false /* isUpper */);
  }
};

}  // namespace function
}  // namespace gs
