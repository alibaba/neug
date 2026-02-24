/** Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <cstdint>
#include <string>

namespace neug {
namespace common {

// Add exception only if you need to throw it in multiple places.
struct ExceptionMessage {
  // Primary key.
  static std::string duplicatePKException(const std::string& pkString);
  static std::string nonExistentPKException(const std::string& pkString);
  static std::string invalidPKType(const std::string& type);
  static std::string nullPKException();
  // Long string.
  static std::string overLargeStringPKValueException(uint64_t length);
  static std::string overLargeStringValueException(uint64_t length);
  // Foreign key.
  static std::string violateDeleteNodeWithConnectedEdgesConstraint(
      const std::string& tableName, const std::string& offset,
      const std::string& direction);
  static std::string violateRelMultiplicityConstraint(
      const std::string& tableName, const std::string& offset,
      const std::string& direction);
  // Binding exception
  static std::string variableNotInScope(const std::string& varName);
  static std::string listFunctionIncompatibleChildrenType(
      const std::string& functionName, const std::string& leftType,
      const std::string& rightType);
};

}  // namespace common
}  // namespace neug
