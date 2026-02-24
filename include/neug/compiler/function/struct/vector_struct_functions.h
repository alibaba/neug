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

#pragma once

#include "neug/compiler/common/vector/value_vector.h"
#include "neug/compiler/function/function.h"

namespace neug {
namespace function {

struct StructExtractBindData : public FunctionBindData {
  common::idx_t childIdx;

  StructExtractBindData(common::LogicalType dataType, common::idx_t childIdx)
      : FunctionBindData{std::move(dataType)}, childIdx{childIdx} {}

  std::unique_ptr<FunctionBindData> copy() const override {
    return std::make_unique<StructExtractBindData>(resultType.copy(), childIdx);
  }
};

struct StructExtractFunctions {
  static constexpr const char* name = "STRUCT_EXTRACT";

  static function_set getFunctionSet();

  static std::unique_ptr<FunctionBindData> bindFunc(
      const ScalarBindFuncInput& input);

  static void compileFunc(
      FunctionBindData* bindData,
      const std::vector<std::shared_ptr<common::ValueVector>>& parameters,
      std::shared_ptr<common::ValueVector>& result);
};

}  // namespace function
}  // namespace neug
