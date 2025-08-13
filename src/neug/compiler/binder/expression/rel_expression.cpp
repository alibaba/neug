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

#include "neug/compiler/binder/expression/rel_expression.h"

#include "neug/compiler/catalog/catalog_entry/rel_table_catalog_entry.h"
#include "neug/compiler/catalog/catalog_entry/table_catalog_entry.h"
#include "neug/compiler/common/enums/extend_direction_util.h"
#include "neug/compiler/common/utils.h"
#include "neug/utils/exception/exception.h"

using namespace gs::common;

namespace gs {
namespace binder {

std::string RelExpression::detailsToString() const {
  std::string result = toString();
  switch (relType) {
  case QueryRelType::SHORTEST: {
    result += "SHORTEST";
  } break;
  case QueryRelType::ALL_SHORTEST: {
    result += "ALL SHORTEST";
  } break;
  case QueryRelType::WEIGHTED_SHORTEST: {
    result += "WEIGHTED SHORTEST";
  } break;
  case QueryRelType::ALL_WEIGHTED_SHORTEST: {
    result += "ALL WEIGHTED SHORTEST";
  } break;
  default:
    break;
  }
  if (QueryRelTypeUtils::isRecursive(relType)) {
    result += std::to_string(recursiveInfo->bindData->lowerBound);
    result += "..";
    result += std::to_string(recursiveInfo->bindData->upperBound);
  }
  return result;
}

std::vector<common::ExtendDirection> RelExpression::getExtendDirections()
    const {
  std::vector<ExtendDirection> ret;
  for (const auto direction : {ExtendDirection::FWD, ExtendDirection::BWD}) {
    const bool addDirection = std::all_of(
        entries.begin(), entries.end(),
        [direction](const catalog::TableCatalogEntry* tableEntry) {
          const auto* relTableEntry =
              tableEntry->constPtrCast<catalog::RelTableCatalogEntry>();
          return common::containsValue(
              relTableEntry->getRelDataDirections(),
              ExtendDirectionUtil::getRelDataDirection(direction));
        });
    if (addDirection) {
      ret.push_back(direction);
    }
  }
  if (ret.empty()) {
    THROW_BINDER_EXCEPTION(stringFormat(
        "There are no common storage directions among the rel "
        "tables matched by pattern '{}' (some tables have storage direction "
        "'fwd' "
        "while others have storage direction 'bwd'). Scanning different tables "
        "matching the "
        "same pattern in different directions is currently unsupported.",
        toString()));
  }
  return ret;
}

}  // namespace binder
}  // namespace gs
