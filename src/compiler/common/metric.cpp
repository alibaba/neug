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

#include "neug/compiler/common/metric.h"

namespace neug {
namespace common {

TimeMetric::TimeMetric(bool enable) : Metric(enable) {
  accumulatedTime = 0;
  isStarted = false;
  timer = Timer();
}

void TimeMetric::start() {
  if (!enabled) {
    return;
  }
  isStarted = true;
  timer.start();
}

void TimeMetric::stop() {
  if (!enabled) {
    return;
  }
  if (!isStarted) {
    THROW_EXCEPTION_WITH_FILE_LINE("Timer metric has not started.");
  }
  timer.stop();
  accumulatedTime += timer.getDuration();
  isStarted = false;
}

double TimeMetric::getElapsedTimeMS() const { return accumulatedTime / 1000; }

NumericMetric::NumericMetric(bool enable) : Metric(enable) {
  accumulatedValue = 0u;
}

void NumericMetric::increase(uint64_t value) {
  if (!enabled) {
    return;
  }
  accumulatedValue += value;
}

void NumericMetric::incrementByOne() {
  if (!enabled) {
    return;
  }
  accumulatedValue++;
}

}  // namespace common
}  // namespace neug
