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

#include "neug/utils/api.h"

namespace neug {
namespace common {

class Value;

/**
 * @brief RecursiveRelVal represents a path in the graph and stores the
 * corresponding rels and nodes of that path.
 */
class RecursiveRelVal {
 public:
  /**
   * @return the list of nodes in the recursive rel as a Value.
   */
  NEUG_API static Value* getNodes(const Value* val);

  /**
   * @return the list of rels in the recursive rel as a Value.
   */
  NEUG_API static Value* getRels(const Value* val);

 private:
  static void throwIfNotRecursiveRel(const Value* val);
};

}  // namespace common
}  // namespace neug
