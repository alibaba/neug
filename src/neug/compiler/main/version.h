#pragma once
#include <cstdint>

#include "neug/compiler/common/api.h"
namespace gs {
namespace main {

struct Version {
 public:
  /**
   * @brief Get the version of the Kuzu library.
   * @return const char* The version of the Kuzu library.
   */
  KUZU_API static const char* getVersion();

  /**
   * @brief Get the storage version of the Kuzu library.
   * @return uint64_t The storage version of the Kuzu library.
   */
  KUZU_API static uint64_t getStorageVersion();
};
}  // namespace main
}  // namespace gs
