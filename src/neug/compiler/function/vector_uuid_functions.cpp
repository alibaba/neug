#include "neug/compiler/function/uuid/vector_uuid_functions.h"

#include "neug/compiler/function/scalar_function.h"
#include "neug/compiler/function/uuid/functions/gen_random_uuid.h"

using namespace gs::common;

namespace gs {
namespace function {

function_set GenRandomUUIDFunction::getFunctionSet() {
  function_set definitions;
  definitions.push_back(make_unique<ScalarFunction>(
      name, std::vector<LogicalTypeID>{}, LogicalTypeID::UUID,
      ScalarFunction::NullaryAuxilaryExecFunction<ku_uuid_t, GenRandomUUID>));
  return definitions;
}

}  // namespace function
}  // namespace gs
