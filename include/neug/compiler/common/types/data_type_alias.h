#pragma once

// This header brings engine DataType/DataTypeId into the neug::common namespace
// for use in compiler code. Include this OR types.h — types.h already includes
// the engine types and the using declarations at its end.

#include "neug/common/extra_type_info.h"
#include "neug/common/types.h"

namespace neug {
namespace common {

using DataType = neug::DataType;
using DataTypeId = neug::DataTypeId;

}  // namespace common
}  // namespace neug
