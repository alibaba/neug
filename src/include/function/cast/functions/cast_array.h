#pragma once

#include "src/include/common/types/types.h"
#include "src/include/common/vector/value_vector.h"

using namespace gs::common;

namespace gs {
namespace function {

struct CastArrayHelper {
  static bool checkCompatibleNestedTypes(LogicalTypeID sourceTypeID,
                                         LogicalTypeID targetTypeID);

  static bool containsListToArray(const LogicalType& srcType,
                                  const LogicalType& dstType);

  static void validateListEntry(ValueVector* inputVector,
                                const LogicalType& resultType, uint64_t pos);
};

}  // namespace function
}  // namespace gs
