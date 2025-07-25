#pragma once

#include "neug/compiler/common/types/ku_string.h"
#include "neug/compiler/function/string/functions/find_function.h"

namespace gs {
namespace function {

struct Contains {
  static inline void operation(common::ku_string_t& left,
                               common::ku_string_t& right, uint8_t& result) {
    int64_t pos = 0;
    Find::operation(left, right, pos);
    result = (pos != 0);
  }
};

}  // namespace function
}  // namespace gs
