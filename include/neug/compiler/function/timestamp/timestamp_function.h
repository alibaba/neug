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

#include "neug/compiler/common/types/interval_t.h"
#include "neug/compiler/common/types/timestamp_t.h"
#include "neug/compiler/function/cast/functions/numeric_cast.h"
#include "neug/utils/exception/exception.h"

namespace neug {
namespace function {

struct Century {
  static inline void operation(common::timestamp_t& timestamp,
                               int64_t& result) {
    result = common::Timestamp::getTimestampPart(
        common::DatePartSpecifier::CENTURY, timestamp);
  }
};

struct EpochMs {
  static inline void operation(int64_t& ms, common::timestamp_t& result) {
    result = common::Timestamp::fromEpochMilliSeconds(ms);
  }
};

struct ToTimestamp {
  static inline void operation(double& sec, common::timestamp_t& result) {
    int64_t ms = 0;
    if (!tryCastWithOverflowCheck(sec * common::Interval::MICROS_PER_SEC, ms)) {
      THROW_CONVERSION_EXCEPTION(
          "Could not convert epoch seconds to TIMESTAMP");
    }
    result = common::timestamp_t(ms);
  }
};

}  // namespace function
}  // namespace neug
