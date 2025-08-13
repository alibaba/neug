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
#include "neug/utils/exception/exception.h"
#include "re2.h"

namespace gs {
namespace function {

using namespace common;

using re2_replace_func_t = std::function<void(
    std::string* str, const RE2& re, const regex::StringPiece& rewrite)>;

struct RegexReplaceBindData : public FunctionBindData {
  re2_replace_func_t replaceFunc;

  RegexReplaceBindData(common::logical_type_vec_t paramTypes,
                       re2_replace_func_t replaceFunc)
      : FunctionBindData{std::move(paramTypes), common::LogicalType::STRING()},
        replaceFunc{std::move(replaceFunc)} {}

  std::unique_ptr<FunctionBindData> copy() const override {
    return std::make_unique<RegexReplaceBindData>(copyVector(paramTypes),
                                                  replaceFunc);
  }
};

struct RegexpReplace {
  static void operation(common::ku_string_t& value,
                        common::ku_string_t& pattern,
                        common::ku_string_t& replacement,
                        common::ku_string_t& result,
                        common::ValueVector& resultValueVector, void* dataPtr) {
    auto bindData = reinterpret_cast<RegexReplaceBindData*>(dataPtr);
    std::string resultStr = value.getAsString();
    RE2 re2Pattern{pattern.getAsString()};
    bindData->replaceFunc(&resultStr, re2Pattern, replacement.getAsString());
    BaseRegexpOperation::copyToKuzuString(resultStr, result, resultValueVector);
  }
};

struct RegexReplaceBindDataStaticPattern : public RegexReplaceBindData {
  regex::RE2 pattern;

  RegexReplaceBindDataStaticPattern(common::logical_type_vec_t paramTypes,
                                    re2_replace_func_t replaceFunc,
                                    std::string patternInStr)
      : RegexReplaceBindData{std::move(paramTypes), std::move(replaceFunc)},
        pattern{patternInStr} {}

  std::unique_ptr<FunctionBindData> copy() const override {
    return std::make_unique<RegexReplaceBindDataStaticPattern>(
        copyVector(paramTypes), replaceFunc, pattern.pattern());
  }
};

struct RegexpReplaceStaticPattern {
  static void operation(common::ku_string_t& value,
                        common::ku_string_t& /*pattern*/,
                        common::ku_string_t& replacement,
                        common::ku_string_t& result,
                        common::ValueVector& resultValueVector, void* dataPtr) {
    auto bindData =
        reinterpret_cast<RegexReplaceBindDataStaticPattern*>(dataPtr);
    auto resultStr = value.getAsString();
    bindData->replaceFunc(&resultStr, bindData->pattern,
                          replacement.getAsString());
    BaseRegexpOperation::copyToKuzuString(resultStr, result, resultValueVector);
  }
};

static re2_replace_func_t bindReplaceFunc(
    const binder::expression_vector& expr) {
  re2_replace_func_t result;
  switch (expr.size()) {
  case 3: {
    result = RE2::Replace;
  } break;
  case 4: {
    result = RE2::GlobalReplace;
  } break;
  default:
    KU_UNREACHABLE;
  }
  return result;
}

template <typename OP>
scalar_func_exec_t getExecFunc(const binder::expression_vector& expr) {
  scalar_func_exec_t execFunc;
  switch (expr.size()) {
  case 3: {
    execFunc =
        ScalarFunction::TernaryRegexExecFunction<ku_string_t, ku_string_t,
                                                 ku_string_t, ku_string_t, OP>;
  } break;
  case 4: {
    auto option = expr[3];
    binder::ExpressionUtil::validateExpressionType(*option,
                                                   ExpressionType::LITERAL);
    binder::ExpressionUtil::validateDataType(*option, LogicalType::STRING());
    auto optionVal =
        binder::ExpressionUtil::getLiteralValue<std::string>(*option);
    if (optionVal != RegexpReplaceFunction::GLOBAL_REPLACE_OPTION) {
      THROW_BINDER_EXCEPTION(
          "regex_replace can only support global replace option: g.");
    }
    execFunc =
        ScalarFunction::TernaryRegexExecFunction<ku_string_t, ku_string_t,
                                                 ku_string_t, ku_string_t, OP>;
  } break;
  default:
    KU_UNREACHABLE;
  }
  return execFunc;
}

std::unique_ptr<FunctionBindData> bindFunc(ScalarBindFuncInput input) {
  return nullptr;
}

function_set RegexpReplaceFunction::getFunctionSet() {
  function_set functionSet;
  std::unique_ptr<ScalarFunction> func;
  func = std::make_unique<ScalarFunction>(
      name,
      std::vector<LogicalTypeID>{LogicalTypeID::STRING, LogicalTypeID::STRING,
                                 LogicalTypeID::STRING, LogicalTypeID::STRING},
      LogicalTypeID::STRING);
  func->bindFunc = bindFunc;
  functionSet.emplace_back(std::move(func));
  func = std::make_unique<ScalarFunction>(
      name,
      std::vector<LogicalTypeID>{LogicalTypeID::STRING, LogicalTypeID::STRING,
                                 LogicalTypeID::STRING},
      LogicalTypeID::STRING);
  func->bindFunc = bindFunc;
  functionSet.emplace_back(std::move(func));
  return functionSet;
}

}  // namespace function
}  // namespace gs
