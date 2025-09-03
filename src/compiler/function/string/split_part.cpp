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

/**
 * This file is originally from the Kùzu project
 * (https://github.com/kuzudb/kuzu) Licensed under the MIT License. Modified by
 * Zhou Xiaoli in 2025 to support Neug-specific features.
 */

#include "neug/compiler/common/string_utils.h"
#include "neug/compiler/function/string/vector_string_functions.h"

namespace gs {
namespace function {

using namespace gs::common;

struct SplitPart {
  static void operation(ku_string_t& strToSplit, ku_string_t& separator,
                        int64_t idx, ku_string_t& result,
                        ValueVector& resultVector) {
    auto splitStrVec =
        StringUtils::split(strToSplit.getAsString(), separator.getAsString());
    bool idxOutOfRange = idx <= 0 || (uint64_t) idx > splitStrVec.size();
    std::string resultStr = idxOutOfRange ? "" : splitStrVec[idx - 1];
    StringVector::addString(&resultVector, result, resultStr);
  }
};

static std::unique_ptr<FunctionBindData> bindFunc(
    const ScalarBindFuncInput& input) {
  return FunctionBindData::getSimpleBindData(input.arguments,
                                             LogicalType::STRING());
}

function_set SplitPartFunction::getFunctionSet() {
  function_set functionSet;
  auto function = std::make_unique<ScalarFunction>(
      name,
      std::vector<LogicalTypeID>{LogicalTypeID::STRING, LogicalTypeID::STRING,
                                 LogicalTypeID::INT64},
      LogicalTypeID::STRING,
      ScalarFunction::TernaryStringExecFunction<
          ku_string_t, ku_string_t, int64_t, ku_string_t, SplitPart>);
  function->bindFunc = bindFunc;
  functionSet.emplace_back(std::move(function));
  return functionSet;
}

}  // namespace function
}  // namespace gs
