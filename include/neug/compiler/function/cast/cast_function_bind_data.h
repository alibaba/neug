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

#include "neug/compiler/common/copier_config/csv_reader_config.h"
#include "neug/compiler/function/function.h"

namespace neug {
namespace function {

struct CastFunctionBindData : public FunctionBindData {
  // We don't allow configuring delimiters, ... in CAST function.
  // For performance purpose, we generate a default option object during binding
  // time.
  common::CSVOption option;
  // TODO(Mahn): the following field should be removed once we refactor fixed
  // list.
  uint64_t numOfEntries;

  explicit CastFunctionBindData(common::LogicalType dataType)
      : FunctionBindData{std::move(dataType)}, numOfEntries(0) {}

  inline std::unique_ptr<FunctionBindData> copy() const override {
    auto result = std::make_unique<CastFunctionBindData>(resultType.copy());
    result->numOfEntries = numOfEntries;
    result->option = option.copy();
    return result;
  }
};

}  // namespace function
}  // namespace neug
