#ifndef GLOBAL_GLOBAL_H_
#define GLOBAL_GLOBAL_H_

#include <cassert>
#include <cstdint>
#include <limits>

#include "../global/log.h"

namespace daf {
using Size = uint32_t;
using Vertex = uint32_t;
using Label = uint32_t;
using QueryDegree = uint8_t;

constexpr Size INVALID_SZ = std::numeric_limits<Size>::max();
constexpr Vertex INVALID_VTX = std::numeric_limits<Vertex>::max();
constexpr Label INVALID_LB = std::numeric_limits<Label>::max();
}  // namespace daf

#endif  // GLOBAL_GLOBAL_H_
