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

#include "neug/compiler/common/copy_constructors.h"
#include "neug/compiler/common/enums/query_rel_type.h"
#include "node_pattern.h"

namespace neug {
namespace parser {

enum class ArrowDirection : uint8_t { LEFT = 0, RIGHT = 1, BOTH = 2 };

struct RecursiveRelPatternInfo {
  std::string lowerBound;
  std::string upperBound;
  std::string weightPropertyName;
  std::string relName;
  std::string nodeName;
  std::unique_ptr<ParsedExpression> whereExpression = nullptr;
  bool hasProjection = false;
  parsed_expr_vector relProjectionList;
  parsed_expr_vector nodeProjectionList;

  RecursiveRelPatternInfo() = default;
  DELETE_COPY_DEFAULT_MOVE(RecursiveRelPatternInfo);
};

class RelPattern : public NodePattern {
 public:
  RelPattern(std::string name, std::vector<std::string> tableNames,
             common::QueryRelType relType, ArrowDirection arrowDirection,
             std::vector<s_parsed_expr_pair> propertyKeyValPairs,
             RecursiveRelPatternInfo recursiveInfo)
      : NodePattern{std::move(name), std::move(tableNames),
                    std::move(propertyKeyValPairs)},
        relType{relType},
        arrowDirection{arrowDirection},
        recursiveInfo{std::move(recursiveInfo)} {}
  DELETE_COPY_DEFAULT_MOVE(RelPattern);

  common::QueryRelType getRelType() const { return relType; }

  ArrowDirection getDirection() const { return arrowDirection; }

  const RecursiveRelPatternInfo* getRecursiveInfo() const {
    return &recursiveInfo;
  }

 private:
  common::QueryRelType relType;
  ArrowDirection arrowDirection;
  RecursiveRelPatternInfo recursiveInfo;
};

}  // namespace parser
}  // namespace neug
