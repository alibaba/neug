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
namespace binder {

struct BoundCreateSequenceInfo {
  std::string sequenceName;
  int64_t startWith;
  int64_t increment;
  int64_t minValue;
  int64_t maxValue;
  bool cycle;
  common::ConflictAction onConflict;
  bool hasParent = false;
  bool isInternal;

  BoundCreateSequenceInfo(std::string sequenceName, int64_t startWith,
                          int64_t increment, int64_t minValue, int64_t maxValue,
                          bool cycle, common::ConflictAction onConflict,
                          bool isInternal)
      : sequenceName{std::move(sequenceName)},
        startWith{startWith},
        increment{increment},
        minValue{minValue},
        maxValue{maxValue},
        cycle{cycle},
        onConflict{onConflict},
        isInternal{isInternal} {}
  EXPLICIT_COPY_DEFAULT_MOVE(BoundCreateSequenceInfo);

 private:
  BoundCreateSequenceInfo(const BoundCreateSequenceInfo& other)
      : sequenceName{other.sequenceName},
        startWith{other.startWith},
        increment{other.increment},
        minValue{other.minValue},
        maxValue{other.maxValue},
        cycle{other.cycle},
        onConflict{other.onConflict},
        hasParent{other.hasParent},
        isInternal{other.isInternal} {}
};

}  // namespace binder
}  // namespace neug
