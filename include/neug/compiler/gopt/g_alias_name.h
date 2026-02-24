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

#include <optional>
#include <string>
#include "neug/utils/exception/exception.h"

namespace neug {
namespace gopt {
struct GAliasName {
  GAliasName() = default;
  GAliasName(const std::string& uniqueName,
             std::optional<std::string> queryName = std::nullopt) {
    if (uniqueName.empty()) {
      THROW_EXCEPTION_WITH_FILE_LINE("Unique name cannot be empty.");
    }
    if (queryName.has_value() && queryName.value().empty()) {
      THROW_EXCEPTION_WITH_FILE_LINE("Query name cannot be empty if provided.");
    }
    this->uniqueName = std::move(uniqueName);
    this->queryName = std::move(queryName);
  }
  GAliasName(const std::string& uniqueName, const std::string& queryName) {
    if (uniqueName.empty()) {
      THROW_EXCEPTION_WITH_FILE_LINE("Unique name cannot be empty.");
    }
    if (queryName.empty()) {
      THROW_EXCEPTION_WITH_FILE_LINE("Query name cannot be empty.");
    }
    this->uniqueName = std::move(uniqueName);
    this->queryName = queryName;
  }
  // system generated unique name
  std::string uniqueName;
  // query given name
  std::optional<std::string> queryName;
};
}  // namespace gopt
}  // namespace neug