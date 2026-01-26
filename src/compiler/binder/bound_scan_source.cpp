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

#include "neug/compiler/binder/bound_scan_source.h"

using namespace neug::common;

namespace neug {
namespace binder {

expression_vector BoundTableScanSource::getWarningColumns() const {
  expression_vector warningDataExprs;
  auto& columns = info.bindData->columns;
  switch (type) {
  case ScanSourceType::FILE: {
    auto bindData = info.bindData->constPtrCast<function::ScanFileBindData>();
    for (auto i = bindData->numWarningDataColumns; i >= 1; --i) {
      NEUG_ASSERT(i < columns.size());
      warningDataExprs.push_back(columns[columns.size() - i]);
    }
  } break;
  default:
    break;
  }
  return warningDataExprs;
}

bool BoundTableScanSource::getIgnoreErrorsOption() const {
  return info.bindData->getIgnoreErrorsOption();
}

bool BoundQueryScanSource::getIgnoreErrorsOption() const {
  return info.options.contains(CopyConstants::IGNORE_ERRORS_OPTION_NAME)
             ? info.options.at(CopyConstants::IGNORE_ERRORS_OPTION_NAME)
                   .getValue<bool>()
             : CopyConstants::DEFAULT_IGNORE_ERRORS;
}

}  // namespace binder
}  // namespace neug
