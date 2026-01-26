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

#include <string>

#include "neug/compiler/common/copy_constructors.h"
#include "neug/compiler/common/enums/conflict_action.h"

namespace neug {
namespace parser {

enum class SequenceInfoType {
  START,
  INCREMENT,
  MINVALUE,
  MAXVALUE,
  CYCLE,
  INVALID,
};

struct CreateSequenceInfo {
  std::string sequenceName;
  std::string startWith = "";
  std::string increment = "1";
  std::string minValue = "";
  std::string maxValue = "";
  bool cycle = false;
  common::ConflictAction onConflict;

  explicit CreateSequenceInfo(std::string sequenceName,
                              common::ConflictAction onConflict)
      : sequenceName{std::move(sequenceName)}, onConflict{onConflict} {}
  EXPLICIT_COPY_DEFAULT_MOVE(CreateSequenceInfo);

 private:
  CreateSequenceInfo(const CreateSequenceInfo& other)
      : sequenceName{other.sequenceName},
        startWith{other.startWith},
        increment{other.increment},
        minValue{other.minValue},
        maxValue{other.maxValue},
        cycle{other.cycle},
        onConflict{other.onConflict} {}
};

}  // namespace parser
}  // namespace neug
