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

#include "neug/compiler/common/enums/extend_direction_util.h"
#include "neug/compiler/common/types/types.h"
#include "neug/compiler/common/utils.h"
#include "neug/compiler/gopt/g_graph_type.h"
#include "neug/storages/graph/schema.h"
#include "neug/utils/exception/exception.h"

using namespace neug::common;

namespace neug {
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
    const bool addDirection =
        std::all_of(entries.begin(), entries.end(),
                    [direction](const SchemaEntry* tableEntry) {
                      const auto* relTableEntry =
                          dynamic_cast<const EdgeSchema*>(tableEntry);
                      NEUG_ASSERT(relTableEntry != nullptr);
                      const bool hasDirection =
                          (direction == ExtendDirection::FWD &&
                           relTableEntry->oe_strategy != EdgeStrategy::kNone) ||
                          (direction == ExtendDirection::BWD &&
                           relTableEntry->ie_strategy != EdgeStrategy::kNone);
                      return hasDirection;
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

void RelExpression::setEntries(std::vector<SchemaEntry*> entries_) {
  entries = std::move(entries_);
  auto extraTypeInfo = getDataType().getExtraTypeInfoRef();
  auto relTypeInfo = dynamic_cast<common::GRelTypeInfo*>(extraTypeInfo);
  if (!relTypeInfo) {
    return;
  }
  // update rel labels using new entries
  std::vector<EdgeSchema*> relEntries;
  for (auto& entry : entries) {
    relEntries.emplace_back(dynamic_cast<EdgeSchema*>(entry));
  }
  auto relType = relTypeInfo->getRelType();
  if (relType) {
    relType->setRelTables(std::move(relEntries));
  }
}

}  // namespace binder
}  // namespace neug
