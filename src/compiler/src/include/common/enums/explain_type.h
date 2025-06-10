#pragma once

#include <cstdint>

namespace gs {
namespace common {

enum class ExplainType : uint8_t {
  PROFILE = 0,
  LOGICAL_PLAN = 1,
  PHYSICAL_PLAN = 2,
};

}  // namespace common
}  // namespace gs
