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

#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "neug/utils/api.h"

namespace neug {
namespace common {

class Value;

/**
 * @brief NodeVal represents a node in the graph and stores the nodeID, label
 * and properties of that node.
 */
class NodeVal {
 public:
  /**
   * @return all properties of the NodeVal.
   * @note this function copies all the properties into a vector, which is not
   * efficient. use `getPropertyName` and `getPropertyVal` instead if possible.
   */
  NEUG_API static std::vector<std::pair<std::string, std::unique_ptr<Value>>>
  getProperties(const Value* val);
  /**
   * @return number of properties of the RelVal.
   */
  NEUG_API static uint64_t getNumProperties(const Value* val);

  /**
   * @return the name of the property at the given index.
   */
  NEUG_API static std::string getPropertyName(const Value* val, uint64_t index);

  /**
   * @return the value of the property at the given index.
   */
  NEUG_API static Value* getPropertyVal(const Value* val, uint64_t index);
  /**
   * @return the nodeID as a Value.
   */
  NEUG_API static Value* getNodeIDVal(const Value* val);
  /**
   * @return the name of the node as a Value.
   */
  NEUG_API static Value* getLabelVal(const Value* val);
  /**
   * @return the current node values in string format.
   */
  NEUG_API static std::string toString(const Value* val);

 private:
  static void throwIfNotNode(const Value* val);
  // 2 offsets for id and label.
  static constexpr uint64_t OFFSET = 2;
};

}  // namespace common
}  // namespace neug
