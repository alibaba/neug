#pragma once

#include <cstdint>

namespace gs {
namespace common {

enum class ZoneMapCheckResult : uint8_t {
  ALWAYS_SCAN = 0,
  SKIP_SCAN = 1,
};

}  // namespace common
}  // namespace gs
