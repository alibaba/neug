#include "main/version.h"

#include "storage/storage_version_info.h"

namespace gs {
namespace main {
const char* Version::getVersion() { return NEXG_CMAKE_VERSION; }

uint64_t Version::getStorageVersion() { return 0; }
}  // namespace main
}  // namespace gs
