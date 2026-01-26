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

#include <chrono>
#include <string>

#include "neug/compiler/common/assert.h"
#include "neug/utils/exception/exception.h"

namespace neug {
namespace common {

class Timer {
 public:
  void start() {
    finished = false;
    startTime = std::chrono::high_resolution_clock::now();
  }

  void stop() {
    stopTime = std::chrono::high_resolution_clock::now();
    finished = true;
  }

  double getDuration() const {
    if (finished) {
      auto duration = stopTime - startTime;
      return (double) std::chrono::duration_cast<std::chrono::microseconds>(
                 duration)
          .count();
    }
    THROW_EXCEPTION_WITH_FILE_LINE("Timer is still running.");
  }

  uint64_t getElapsedTimeInMS() const {
    auto now = std::chrono::high_resolution_clock::now();
    auto duration = now - startTime;
    auto count =
        std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();
    NEUG_ASSERT(count >= 0);
    return count;
  }

 private:
  std::chrono::time_point<std::chrono::high_resolution_clock> startTime;
  std::chrono::time_point<std::chrono::high_resolution_clock> stopTime;
  bool finished = false;
};

}  // namespace common
}  // namespace neug
