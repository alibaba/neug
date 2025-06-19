#include "src/include/main/settings.h"

#include "src/include/common/exception/runtime.h"
#include "src/include/common/file_system/virtual_file_system.h"
#include "src/include/main/client_context.h"

namespace gs {
namespace main {

void SpillToDiskSetting::setContext(ClientContext* context,
                                    const common::Value& parameter) {}

}  // namespace main
}  // namespace gs
