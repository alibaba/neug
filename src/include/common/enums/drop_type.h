#pragma once

#include <cstdint>
#include <string>

namespace gs {
namespace common {

enum class DropType : uint8_t {
  TABLE = 0,
  SEQUENCE = 1,
};

struct DropTypeUtils {
  static std::string toString(DropType type);
};

}  // namespace common
}  // namespace gs
