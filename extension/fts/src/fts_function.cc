#include "fts_function.h"

#include <memory>
#include <vector>

#include "neug/compiler/function/neug_scalar_function.h"
#include "neug/utils/exception/exception.h"

namespace neug::fts_ext {

function::function_set FTSBM25Function::getFunctionSet() {
  function::function_set functions;
  functions.push_back(std::make_unique<function::NeugScalarFunction>(
      name, std::vector<DataTypeId>{DataTypeId::kVarchar, DataTypeId::kVarchar},
      DataTypeId::kDouble, Exec));
  return functions;
}

Value FTSBM25Function::Exec(const std::vector<Value>&) {
  THROW_NOT_SUPPORTED_EXCEPTION(
      "BM25 is only supported with ORDER BY score ASC and LIMIT");
}

}  // namespace neug::fts_ext
