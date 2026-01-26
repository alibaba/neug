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

#include "rel_pattern.h"

namespace neug {
namespace parser {

class PatternElementChain {
 public:
  PatternElementChain(RelPattern relPattern, NodePattern nodePattern)
      : relPattern{std::move(relPattern)},
        nodePattern{std::move(nodePattern)} {}
  DELETE_COPY_DEFAULT_MOVE(PatternElementChain);

  inline const RelPattern* getRelPattern() const { return &relPattern; }

  inline const NodePattern* getNodePattern() const { return &nodePattern; }

 private:
  RelPattern relPattern;
  NodePattern nodePattern;
};

}  // namespace parser
}  // namespace neug
