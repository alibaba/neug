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

#include "neug/compiler/function/string/vector_string_functions.h"

#include "neug/compiler/function/neug_scalar_function.h"
#include "neug/compiler/function/string/functions/array_extract_function.h"
#include "neug/compiler/function/string/functions/contains_function.h"
#include "neug/compiler/function/string/functions/ends_with_function.h"
#include "neug/compiler/function/string/functions/left_operation.h"
#include "neug/compiler/function/string/functions/lpad_function.h"
#include "neug/compiler/function/string/functions/regexp_extract_all_function.h"
#include "neug/compiler/function/string/functions/regexp_extract_function.h"
#include "neug/compiler/function/string/functions/regexp_matches_function.h"
#include "neug/compiler/function/string/functions/regexp_split_to_array_function.h"
#include "neug/compiler/function/string/functions/repeat_function.h"
#include "neug/compiler/function/string/functions/right_function.h"
#include "neug/compiler/function/string/functions/rpad_function.h"
#include "neug/compiler/function/string/functions/starts_with_function.h"
#include "neug/compiler/function/string/functions/substr_function.h"

#include "neug/utils/runtime/rt_any.h"

using namespace gs::common;

namespace gs {
namespace function {

void BaseLowerUpperFunction::operation(neug_string_t& input,
                                       neug_string_t& result,
                                       ValueVector& resultValueVector,
                                       bool isUpper) {
  uint32_t resultLen =
      getResultLen((char*) input.getData(), input.len, isUpper);
  result.len = resultLen;
  if (resultLen <= neug_string_t::SHORT_STR_LENGTH) {
    convertCase((char*) result.prefix, input.len, (char*) input.getData(),
                isUpper);
  } else {
    StringVector::reserveString(&resultValueVector, result, resultLen);
    auto buffer = reinterpret_cast<char*>(result.overflowPtr);
    convertCase(buffer, input.len, (char*) input.getData(), isUpper);
    memcpy(result.prefix, buffer, neug_string_t::PREFIX_LENGTH);
  }
}

void BaseStrOperation::operation(neug_string_t& input, neug_string_t& result,
                                 ValueVector& resultValueVector,
                                 uint32_t (*strOperation)(char* data,
                                                          uint32_t len)) {
  if (input.len <= neug_string_t::SHORT_STR_LENGTH) {
    memcpy(result.prefix, input.prefix, input.len);
    result.len = strOperation((char*) result.prefix, input.len);
  } else {
    StringVector::reserveString(&resultValueVector, result, input.len);
    auto buffer = reinterpret_cast<char*>(result.overflowPtr);
    memcpy(buffer, input.getData(), input.len);
    result.len = strOperation(buffer, input.len);
    memcpy(result.prefix, buffer,
           result.len < neug_string_t::PREFIX_LENGTH
               ? result.len
               : neug_string_t::PREFIX_LENGTH);
  }
}

void Repeat::operation(neug_string_t& left, int64_t& right,
                       neug_string_t& result, ValueVector& resultValueVector) {
  result.len = left.len * right;
  if (result.len <= neug_string_t::SHORT_STR_LENGTH) {
    repeatStr((char*) result.prefix, left.getAsString(), right);
  } else {
    StringVector::reserveString(&resultValueVector, result, result.len);
    auto buffer = reinterpret_cast<char*>(result.overflowPtr);
    repeatStr(buffer, left.getAsString(), right);
    memcpy(result.prefix, buffer, neug_string_t::PREFIX_LENGTH);
  }
}

void Reverse::operation(neug_string_t& input, neug_string_t& result,
                        ValueVector& resultValueVector) {
  bool isAscii = true;
  std::string inputStr = input.getAsString();
  for (uint32_t i = 0; i < input.len; i++) {
    if (inputStr[i] & 0x80) {
      isAscii = false;
      break;
    }
  }
  if (isAscii) {
    BaseStrOperation::operation(input, result, resultValueVector, reverseStr);
  } else {
    result.len = input.len;
    if (result.len > neug_string_t::SHORT_STR_LENGTH) {
      StringVector::reserveString(&resultValueVector, result, input.len);
    }
    auto resultBuffer = result.len <= neug_string_t::SHORT_STR_LENGTH
                            ? reinterpret_cast<char*>(result.prefix)
                            : reinterpret_cast<char*>(result.overflowPtr);
    utf8proc::utf8proc_grapheme_callback(
        inputStr.c_str(), input.len, [&](size_t start, size_t end) {
          memcpy(resultBuffer + input.len - end, input.getData() + start,
                 end - start);
          return true;
        });
    if (result.len > neug_string_t::SHORT_STR_LENGTH) {
      memcpy(result.prefix, resultBuffer, neug_string_t::PREFIX_LENGTH);
    }
  }
}

function_set ContainsFunction::getFunctionSet() {
  function_set functionSet;
  functionSet.emplace_back(make_unique<ScalarFunction>(
      name,
      std::vector<LogicalTypeID>{LogicalTypeID::STRING, LogicalTypeID::STRING},
      LogicalTypeID::BOOL,
      ScalarFunction::BinaryExecFunction<neug_string_t, neug_string_t, uint8_t,
                                         Contains>,
      ScalarFunction::BinarySelectFunction<neug_string_t, neug_string_t,
                                           Contains>));
  return functionSet;
}

function_set EndsWithFunction::getFunctionSet() {
  function_set functionSet;
  functionSet.emplace_back(make_unique<ScalarFunction>(
      name,
      std::vector<LogicalTypeID>{LogicalTypeID::STRING, LogicalTypeID::STRING},
      LogicalTypeID::BOOL,
      ScalarFunction::BinaryExecFunction<neug_string_t, neug_string_t, uint8_t,
                                         EndsWith>,
      ScalarFunction::BinarySelectFunction<neug_string_t, neug_string_t,
                                           EndsWith>));
  return functionSet;
}

function_set StartsWithFunction::getFunctionSet() {
  function_set functionSet;
  functionSet.emplace_back(make_unique<ScalarFunction>(
      name,
      std::vector<LogicalTypeID>{LogicalTypeID::STRING, LogicalTypeID::STRING},
      LogicalTypeID::BOOL,
      ScalarFunction::BinaryExecFunction<neug_string_t, neug_string_t, uint8_t,
                                         StartsWith>,
      ScalarFunction::BinarySelectFunction<neug_string_t, neug_string_t,
                                           StartsWith>));
  return functionSet;
}

function_set UpperFunction::getFunctionSet() {
  function_set functionSet;
  functionSet.emplace_back(std::make_unique<NeugScalarFunction>(
      name, std::vector<LogicalTypeID>{LogicalTypeID::STRING},
      LogicalTypeID::STRING, UpperFunction::Exec));
  return functionSet;
}

runtime::RTAny UpperFunction::Exec(runtime::Arena& arena,
                                   const std::vector<runtime::RTAny>& args) {
  if (args.size() != 1) {
    THROW_RUNTIME_ERROR("UPPER: expect exactly 1 argument, got " +
                        std::to_string(args.size()));
  }
  const auto& val = args[0];
  if (val.type().id() != DataTypeId::VARCHAR) {
    THROW_RUNTIME_ERROR("UPPER: input value is not a string");
  }
  std::string str(val.as_string());
  std::transform(str.begin(), str.end(), str.begin(), ::toupper);
  auto ptr = runtime::StringImpl::make_string_impl(str);
  auto str_view = ptr->str_view();
  arena.emplace_back(std::move(ptr));
  return runtime::RTAny::from_string(str_view);
}

function_set LowerFunction::getFunctionSet() {
  function_set functionSet;
  functionSet.emplace_back(std::make_unique<NeugScalarFunction>(
      name, std::vector<LogicalTypeID>{LogicalTypeID::STRING},
      LogicalTypeID::STRING, LowerFunction::Exec));
  return functionSet;
}

runtime::RTAny LowerFunction::Exec(runtime::Arena& arena,
                                   const std::vector<runtime::RTAny>& args) {
  if (args.size() != 1) {
    THROW_RUNTIME_ERROR("LOWER: expect exactly 1 argument, got " +
                        std::to_string(args.size()));
  }
  const auto& val = args[0];
  if (val.type().id() != DataTypeId::VARCHAR) {
    THROW_RUNTIME_ERROR("LOWER: input value is not a string");
  }
  std::string str(val.as_string());
  std::transform(str.begin(), str.end(), str.begin(), ::tolower);
  auto ptr = runtime::StringImpl::make_string_impl(str);
  auto str_view = ptr->str_view();
  arena.emplace_back(std::move(ptr));
  return runtime::RTAny::from_string(str_view);
}

function_set ReverseFunction::getFunctionSet() {
  function_set functionSet;
  functionSet.emplace_back(std::make_unique<NeugScalarFunction>(
      name, std::vector<LogicalTypeID>{LogicalTypeID::STRING},
      LogicalTypeID::STRING, ReverseFunction::Exec));
  return functionSet;
}

runtime::RTAny ReverseFunction::Exec(runtime::Arena& arena,
                                     const std::vector<runtime::RTAny>& args) {
  if (args.size() != 1) {
    THROW_RUNTIME_ERROR("REVERSE: expect exactly 1 argument, got " +
                        std::to_string(args.size()));
  }
  const auto& val = args[0];
  if (val.type().id() != DataTypeId::VARCHAR) {
    THROW_RUNTIME_ERROR("REVERSE: input value is not a string");
  }
  std::string str(val.as_string());
  std::reverse(str.begin(), str.end());
  auto ptr = runtime::StringImpl::make_string_impl(str);
  auto str_view = ptr->str_view();
  arena.emplace_back(std::move(ptr));
  return runtime::RTAny::from_string(str_view);
}

function_set SubStrFunction::getFunctionSet() {
  function_set functionSet;
  functionSet.emplace_back(make_unique<ScalarFunction>(
      name,
      std::vector<LogicalTypeID>{LogicalTypeID::STRING, LogicalTypeID::INT64,
                                 LogicalTypeID::INT64},
      LogicalTypeID::STRING,
      ScalarFunction::TernaryStringExecFunction<neug_string_t, int64_t, int64_t,
                                                neug_string_t, SubStr>));
  return functionSet;
}

}  // namespace function
}  // namespace gs
