#pragma once

#include "neug/compiler/common/types/uuid.h"
#include "neug/compiler/function/function.h"
#include "neug/compiler/main/client_context.h"

namespace gs {
namespace function {

struct GenRandomUUID {
  static void operation(common::ku_uuid_t& input, void* dataPtr) {
    input = common::UUID::generateRandomUUID(
        static_cast<FunctionBindData*>(dataPtr)
            ->clientContext->getRandomEngine());
  }
};

}  // namespace function
}  // namespace gs
