#pragma once

#include <vector>
#include <cstddef>
#include "neug/execution/common/rt_any.h"

namespace gs {
namespace runtime {

using neug_func_exec_t = RTAny (*)(size_t, Arena&, const std::vector<RTAny>&);

} // namespace runtime
} // namespace gs