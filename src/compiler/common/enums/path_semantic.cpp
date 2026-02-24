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

#include "neug/compiler/common/enums/path_semantic.h"

#include "neug/compiler/common/assert.h"
#include "neug/compiler/common/string_format.h"
#include "neug/compiler/common/string_utils.h"
#include "neug/utils/exception/exception.h"

namespace neug {
namespace common {

PathSemantic PathSemanticUtils::fromString(const std::string& str) {
  auto normalizedStr = StringUtils::getUpper(str);
  if (normalizedStr == "WALK") {
    return PathSemantic::WALK;
  }
  if (normalizedStr == "TRAIL") {
    return PathSemantic::TRAIL;
  }
  if (normalizedStr == "ACYCLIC") {
    return PathSemantic::ACYCLIC;
  }
  THROW_BINDER_EXCEPTION(
      stringFormat("Cannot parse {} as a path semantic. Supported inputs are "
                   "[WALK, TRAIL, ACYCLIC]",
                   str));
}

std::string PathSemanticUtils::toString(PathSemantic semantic) {
  switch (semantic) {
  case PathSemantic::WALK:
    return "WALK";
  case PathSemantic::TRAIL:
    return "TRAIL";
  case PathSemantic::ACYCLIC:
    return "ACYCLIC";
  default:
    NEUG_UNREACHABLE;
  }
}

}  // namespace common
}  // namespace neug
