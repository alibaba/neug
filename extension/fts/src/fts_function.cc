/**
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
