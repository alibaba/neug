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

#include "pattern_element_chain.h"

namespace neug {
namespace parser {

class PatternElement {
 public:
  explicit PatternElement(NodePattern nodePattern)
      : nodePattern{std::move(nodePattern)} {}
  DELETE_COPY_DEFAULT_MOVE(PatternElement);

  inline void setPathName(std::string name) { pathName = std::move(name); }
  inline bool hasPathName() const { return !pathName.empty(); }
  inline std::string getPathName() const { return pathName; }

  inline const NodePattern* getFirstNodePattern() const { return &nodePattern; }

  inline void addPatternElementChain(PatternElementChain chain) {
    patternElementChains.push_back(std::move(chain));
  }
  inline uint32_t getNumPatternElementChains() const {
    return patternElementChains.size();
  }
  inline const PatternElementChain* getPatternElementChain(uint32_t idx) const {
    return &patternElementChains[idx];
  }

 private:
  std::string pathName;
  NodePattern nodePattern;
  std::vector<PatternElementChain> patternElementChains;
};

}  // namespace parser
}  // namespace neug
