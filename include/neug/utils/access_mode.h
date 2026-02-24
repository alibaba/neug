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

#include "neug/utils/exception/exception.h"
#include "neug/utils/string_utils.h"

namespace neug {

enum class AccessMode {
  kUnKnown,  // Unset access mode
  kRead,     // Read-only access
  kInsert,   // Insert-only access
  kUpdate,   // Update graph data, read existing data, insert new data
  kSchema    // Modify schema,
};
inline AccessMode ParseAccessMode(const std::string& access_mode_str) {
  std::string mode_upper = to_lower_copy(access_mode_str);
  if (mode_upper == "" || mode_upper == "unknown") {
    return AccessMode::kUnKnown;
  } else if (mode_upper == "read" || mode_upper == "r") {
    return AccessMode::kRead;
  } else if (mode_upper == "insert" || mode_upper == "i") {
    return AccessMode::kInsert;
  } else if (mode_upper == "update" || mode_upper == "u") {
    return AccessMode::kUpdate;
  } else if (mode_upper == "schema" || mode_upper == "s") {
    return AccessMode::kSchema;
  } else {
    THROW_INVALID_ARGUMENT_EXCEPTION("Unknown access mode: " + access_mode_str);
  }
}

inline std::string AccessModeToString(AccessMode mode) {
  switch (mode) {
  case AccessMode::kRead:
    return "read";
  case AccessMode::kInsert:
    return "insert";
  case AccessMode::kUpdate:
    return "update";
  case AccessMode::kSchema:
    return "schema";
  case AccessMode::kUnKnown:
    return "unknown";
  default:
    return "unknown";
  }
}

}  // namespace neug
