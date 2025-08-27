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

namespace gs {
namespace function {

struct ToYears {
  static inline void operation(int64_t& input, common::interval_t& result) {
    result.days = result.micros = 0;
    result.months = input * common::Interval::MONTHS_PER_YEAR;
  }
};

struct ToMonths {
  static inline void operation(int64_t& input, common::interval_t& result) {
    result.days = result.micros = 0;
    result.months = input;
  }
};

struct ToDays {
  static inline void operation(int64_t& input, common::interval_t& result) {
    result.micros = result.months = 0;
    result.days = input;
  }
};

struct ToHours {
  static inline void operation(int64_t& input, common::interval_t& result) {
    result.months = result.days = 0;
    result.micros = input * common::Interval::MICROS_PER_HOUR;
  }
};

struct ToMinutes {
  static inline void operation(int64_t& input, common::interval_t& result) {
    result.months = result.days = 0;
    result.micros = input * common::Interval::MICROS_PER_MINUTE;
  }
};

struct ToSeconds {
  static inline void operation(int64_t& input, common::interval_t& result) {
    result.months = result.days = 0;
    result.micros = input * common::Interval::MICROS_PER_SEC;
  }
};

struct ToMilliseconds {
  static inline void operation(int64_t& input, common::interval_t& result) {
    result.months = result.days = 0;
    result.micros = input * common::Interval::MICROS_PER_MSEC;
  }
};

struct ToMicroseconds {
  static inline void operation(int64_t& input, common::interval_t& result) {
    result.months = result.days = 0;
    result.micros = input;
  }
};

}  // namespace function
}  // namespace gs
