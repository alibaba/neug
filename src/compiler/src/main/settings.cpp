#include "main/settings.h"

#include "common/exception/runtime.h"
#include "common/file_system/virtual_file_system.h"
#include "main/client_context.h"

namespace kuzu {
namespace main {

void SpillToDiskSetting::setContext(ClientContext* context,
                                    const common::Value& parameter) {}

}  // namespace main
}  // namespace kuzu
