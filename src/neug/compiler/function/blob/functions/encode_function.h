#pragma once

#include "neug/compiler/common/types/blob.h"
#include "neug/compiler/common/vector/value_vector.h"

namespace gs {
namespace function {

struct Encode {
  static inline void operation(common::ku_string_t& input,
                               common::blob_t& result,
                               common::ValueVector& resultVector) {
    common::StringVector::addString(&resultVector, result.value, input);
  }
};

}  // namespace function
}  // namespace gs
