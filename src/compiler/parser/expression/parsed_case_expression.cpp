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

#include "neug/compiler/parser/expression/parsed_case_expression.h"

#include "neug/compiler/common/serializer/deserializer.h"
#include "neug/compiler/common/serializer/serializer.h"

using namespace neug::common;

namespace neug {
namespace parser {

void ParsedCaseAlternative::serialize(Serializer& serializer) const {
  whenExpression->serialize(serializer);
  thenExpression->serialize(serializer);
}

ParsedCaseAlternative ParsedCaseAlternative::deserialize(
    Deserializer& deserializer) {
  auto whenExpression = ParsedExpression::deserialize(deserializer);
  auto thenExpression = ParsedExpression::deserialize(deserializer);
  return ParsedCaseAlternative(std::move(whenExpression),
                               std::move(thenExpression));
}

std::unique_ptr<ParsedCaseExpression> ParsedCaseExpression::deserialize(
    Deserializer& deserializer) {
  std::unique_ptr<ParsedExpression> caseExpression;
  deserializer.deserializeOptionalValue(caseExpression);
  std::vector<ParsedCaseAlternative> caseAlternatives;
  deserializer.deserializeVector<ParsedCaseAlternative>(caseAlternatives);
  std::unique_ptr<ParsedExpression> elseExpression;
  deserializer.deserializeOptionalValue(elseExpression);
  return std::make_unique<ParsedCaseExpression>(std::move(caseExpression),
                                                std::move(caseAlternatives),
                                                std::move(elseExpression));
}

std::unique_ptr<ParsedExpression> ParsedCaseExpression::copy() const {
  std::vector<ParsedCaseAlternative> caseAlternativesCopy;
  caseAlternativesCopy.reserve(caseAlternatives.size());
  for (auto& caseAlternative : caseAlternatives) {
    caseAlternativesCopy.push_back(caseAlternative);
  }
  return std::make_unique<ParsedCaseExpression>(
      alias, rawName, copyVector(children),
      caseExpression ? caseExpression->copy() : nullptr,
      std::move(caseAlternativesCopy),
      elseExpression ? elseExpression->copy() : nullptr);
}

void ParsedCaseExpression::serializeInternal(Serializer& serializer) const {
  serializer.serializeOptionalValue(caseExpression);
  serializer.serializeVector(caseAlternatives);
  serializer.serializeOptionalValue(elseExpression);
}

}  // namespace parser
}  // namespace neug
