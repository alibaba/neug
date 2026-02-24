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

#include "join_hint.h"
#include "neug/compiler/parser/query/graph_pattern/pattern_element.h"
#include "reading_clause.h"

namespace neug {
namespace parser {

class MatchClause : public ReadingClause {
  static constexpr common::ClauseType clauseType_ = common::ClauseType::MATCH;

 public:
  MatchClause(std::vector<PatternElement> patternElements,
              common::MatchClauseType matchClauseType)
      : ReadingClause{clauseType_},
        patternElements{std::move(patternElements)},
        matchClauseType{matchClauseType} {}

  const std::vector<PatternElement>& getPatternElementsRef() const {
    return patternElements;
  }

  common::MatchClauseType getMatchClauseType() const { return matchClauseType; }

  void setHint(std::shared_ptr<JoinHintNode> root) {
    hintRoot = std::move(root);
  }
  bool hasHint() const { return hintRoot != nullptr; }
  std::shared_ptr<JoinHintNode> getHint() const { return hintRoot; }

 private:
  std::vector<PatternElement> patternElements;
  common::MatchClauseType matchClauseType;
  std::shared_ptr<JoinHintNode> hintRoot = nullptr;
};

}  // namespace parser
}  // namespace neug
