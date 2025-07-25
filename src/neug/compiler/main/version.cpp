#include "neug/compiler/main/version.h"

#include "neug/compiler/storage/storage_version_info.h"

namespace gs {
namespace main {
const char* Version::getVersion() { return NEUG_CMAKE_VERSION; }

uint64_t Version::getStorageVersion() { return 0; }
}  // namespace main
}  // namespace gs
