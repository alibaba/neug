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

#include "neug/compiler/function/function.h"

namespace neug {
namespace function {

struct NodesFunction {
  static constexpr const char* name = "NODES";

  static function_set getFunctionSet();
};

struct RelsFunction {
  static constexpr const char* name = "RELS";

  static function_set getFunctionSet();
};

struct RelationshipsFunction {
  using alias = RelsFunction;

  static constexpr const char* name = "RELATIONSHIPS";
};

struct PropertiesBindData : public FunctionBindData {
  common::idx_t childIdx;

  PropertiesBindData(common::LogicalType dataType, common::idx_t childIdx)
      : FunctionBindData{std::move(dataType)}, childIdx{childIdx} {}

  inline std::unique_ptr<FunctionBindData> copy() const override {
    return std::make_unique<PropertiesBindData>(resultType.copy(), childIdx);
  }
};

struct PropertiesFunction {
  static constexpr const char* name = "PROPERTIES";

  static function_set getFunctionSet();
};

struct IsTrailFunction {
  static constexpr const char* name = "IS_TRAIL";

  static function_set getFunctionSet();
};

struct IsACyclicFunction {
  static constexpr const char* name = "IS_ACYCLIC";

  static function_set getFunctionSet();
};

struct LengthFunction {
  static constexpr const char* name = "LENGTH";

  static function_set getFunctionSet();
};

}  // namespace function
}  // namespace neug
