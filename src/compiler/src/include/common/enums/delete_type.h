#pragma once

#include <cstdint>

namespace gs {
namespace common {

enum class DeleteNodeType : uint8_t {
  DELETE = 0,
  DETACH_DELETE = 1,
};

}  // namespace common
}  // namespace gs
