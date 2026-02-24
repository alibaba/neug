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

#include <vector>

#include "neug/compiler/binder/expression/expression.h"
#include "neug/compiler/common/case_insensitive_map.h"
#include "neug/compiler/common/copier_config/file_scan_info.h"
#include "neug/compiler/common/types/value/value.h"
#include "neug/compiler/parser/query/reading_clause/yield_variable.h"

namespace neug {
namespace binder {
class LiteralExpression;
class Binder;
}  // namespace binder
namespace main {
class ClientContext;
}

namespace common {
class Value;
}

namespace function {

using optional_params_t = common::case_insensitive_map_t<common::Value>;

struct TableFunction;

struct ExtraTableFuncBindInput {
  virtual ~ExtraTableFuncBindInput() = default;

  template <class TARGET>
  const TARGET* constPtrCast() const {
    return common::neug_dynamic_cast<const TARGET*>(this);
  }
};

struct NEUG_API TableFuncBindInput {
  binder::expression_vector params;
  optional_params_t optionalParams;
  binder::expression_vector optionalParamsLegacy;
  std::unique_ptr<ExtraTableFuncBindInput> extraInput = nullptr;
  binder::Binder* binder = nullptr;
  std::vector<parser::YieldVariable> yieldVariables;

  TableFuncBindInput() = default;

  void addLiteralParam(common::Value value);

  std::shared_ptr<binder::Expression> getParam(common::idx_t idx) const {
    return params[idx];
  }
  common::Value getValue(common::idx_t idx) const;
  template <typename T>
  T getLiteralVal(common::idx_t idx) const;
};

struct NEUG_API ExtraScanTableFuncBindInput : ExtraTableFuncBindInput {
  common::FileScanInfo fileScanInfo;
  std::vector<std::string> expectedColumnNames;
  std::vector<common::LogicalType> expectedColumnTypes;
  TableFunction* tableFunction = nullptr;
};

}  // namespace function
}  // namespace neug
