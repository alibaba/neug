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

#include "neug/compiler/binder/expression/expression.h"

namespace neug {
namespace binder {

class PathExpression final : public Expression {
 public:
  PathExpression(common::LogicalType dataType, std::string uniqueName,
                 std::string variableName, common::LogicalType nodeType,
                 common::LogicalType relType, expression_vector children)
      : Expression{common::ExpressionType::PATH, std::move(dataType),
                   std::move(children), std::move(uniqueName)},
        variableName{std::move(variableName)},
        nodeType{std::move(nodeType)},
        relType{std::move(relType)} {}

  inline std::string getVariableName() const { return variableName; }
  inline const common::LogicalType& getNodeType() const { return nodeType; }
  inline const common::LogicalType& getRelType() const { return relType; }

  inline std::string toStringInternal() const override { return variableName; }

 private:
  std::string variableName;
  common::LogicalType nodeType;
  common::LogicalType relType;
};

}  // namespace binder
}  // namespace neug
