#pragma once

#include "src/include/common/types/types.h"
#include "src/include/common/vector/value_vector.h"

namespace gs {
namespace function {

struct ListConcat {
 public:
  static void operation(common::list_entry_t& left, common::list_entry_t& right,
                        common::list_entry_t& result,
                        common::ValueVector& leftVector,
                        common::ValueVector& rightVector,
                        common::ValueVector& resultVector);
};

}  // namespace function
}  // namespace gs
