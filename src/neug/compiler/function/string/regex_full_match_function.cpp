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

#include "neug/compiler/binder/expression/expression_util.h"
#include "neug/compiler/function/string/functions/base_regexp_function.h"
#include "neug/compiler/function/string/vector_string_functions.h"
#include "re2.h"

namespace gs {
namespace function {

using namespace common;

struct RegexFullMatchBindData : public FunctionBindData {
  regex::RE2 pattern;

  explicit RegexFullMatchBindData(common::logical_type_vec_t paramTypes,
                                  std::string patternInStr)
      : FunctionBindData{std::move(paramTypes), common::LogicalType::BOOL()},
        pattern{patternInStr} {}

  std::unique_ptr<FunctionBindData> copy() const override {
    return std::make_unique<RegexFullMatchBindData>(copyVector(paramTypes),
                                                    pattern.pattern());
  }
};

struct RegexpFullMatch {
  static void operation(common::ku_string_t& left, common::ku_string_t& right,
                        uint8_t& result) {
    result = RE2::FullMatch(
        left.getAsString(),
        BaseRegexpOperation::parseCypherPattern(right.getAsString()));
  }
};

struct RegexpFullMatchStaticPattern : BaseRegexpOperation {
  static void operation(common::ku_string_t& left,
                        common::ku_string_t& /*right*/, uint8_t& result,
                        common::ValueVector& /*leftValueVector*/,
                        common::ValueVector& /*rightValueVector*/,
                        common::ValueVector& /*resultValueVector*/,
                        void* dataPtr) {
    auto regexFullMatchBindData =
        reinterpret_cast<RegexFullMatchBindData*>(dataPtr);
    result =
        RE2::FullMatch(left.getAsString(), regexFullMatchBindData->pattern);
  }
};

static std::unique_ptr<FunctionBindData> regexFullMatchBindFunc(
    const ScalarBindFuncInput& input) {
  return nullptr;
}

function_set RegexpFullMatchFunction::getFunctionSet() {
  function_set functionSet;
  auto scalarFunc = make_unique<ScalarFunction>(
      name,
      std::vector<LogicalTypeID>{LogicalTypeID::STRING, LogicalTypeID::STRING},
      LogicalTypeID::BOOL,
      ScalarFunction::BinaryExecFunction<ku_string_t, ku_string_t, uint8_t,
                                         RegexpFullMatch>,
      ScalarFunction::BinarySelectFunction<ku_string_t, ku_string_t,
                                           RegexpFullMatch>);
  scalarFunc->bindFunc = regexFullMatchBindFunc;
  functionSet.emplace_back(std::move(scalarFunc));
  return functionSet;
}

}  // namespace function
}  // namespace gs
