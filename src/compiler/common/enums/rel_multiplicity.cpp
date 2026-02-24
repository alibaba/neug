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

#include "neug/compiler/common/enums/rel_multiplicity.h"

#include "neug/compiler/common/assert.h"
#include "neug/compiler/common/string_format.h"
#include "neug/compiler/common/string_utils.h"
#include "neug/utils/exception/exception.h"

namespace neug {
namespace common {

RelMultiplicity RelMultiplicityUtils::getFwd(const std::string& str) {
  auto normStr = common::StringUtils::getUpper(str);
  if ("ONE_ONE" == normStr || "ONE_MANY" == normStr) {
    return RelMultiplicity::ONE;
  } else if ("MANY_ONE" == normStr || "MANY_MANY" == normStr) {
    return RelMultiplicity::MANY;
  }
  THROW_BINDER_EXCEPTION(
      stringFormat("Cannot bind {} as relationship multiplicity.", str));
}

RelMultiplicity RelMultiplicityUtils::getBwd(const std::string& str) {
  auto normStr = common::StringUtils::getUpper(str);
  if ("ONE_ONE" == normStr || "MANY_ONE" == normStr) {
    return RelMultiplicity::ONE;
  } else if ("ONE_MANY" == normStr || "MANY_MANY" == normStr) {
    return RelMultiplicity::MANY;
  }
  THROW_BINDER_EXCEPTION(
      stringFormat("Cannot bind {} as relationship multiplicity.", str));
}

std::string RelMultiplicityUtils::toString(RelMultiplicity multiplicity) {
  switch (multiplicity) {
  case RelMultiplicity::ONE:
    return "ONE";
  case RelMultiplicity::MANY:
    return "MANY";
  default:
    NEUG_UNREACHABLE;
  }
}

}  // namespace common
}  // namespace neug
