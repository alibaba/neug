#pragma once

#include "src/include/common/types/uuid.h"
#include "src/include/function/function.h"
#include "src/include/main/client_context.h"

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
