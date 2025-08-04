#include "neug/compiler/main/settings.h"

#include "neug/compiler/common/file_system/virtual_file_system.h"
#include "neug/compiler/main/client_context.h"
#include "neug/utils/exception/exception.h"

namespace gs {
namespace main {

void SpillToDiskSetting::setContext(ClientContext* context,
                                    const common::Value& parameter) {}

}  // namespace main
}  // namespace gs
