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

#include "neug/compiler/function/table/bind_data.h"

#include "neug/compiler/common/constants.h"

namespace neug {
namespace function {

std::vector<bool> TableFuncBindData::getColumnSkips() const {
  if (columnSkips
          .empty()) {  // If not specified, all columns should be scanned.
    std::vector<bool> skips;
    for (auto i = 0u; i < getNumColumns(); ++i) {
      skips.push_back(false);
    }
    return skips;
  }
  return columnSkips;
}

bool TableFuncBindData::getIgnoreErrorsOption() const {
  return common::CopyConstants::DEFAULT_IGNORE_ERRORS;
}

std::unique_ptr<TableFuncBindData> TableFuncBindData::copy() const {
  return std::make_unique<TableFuncBindData>(*this);
}

}  // namespace function
}  // namespace neug
