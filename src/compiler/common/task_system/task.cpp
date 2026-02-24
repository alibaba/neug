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

#include "neug/compiler/common/task_system/task.h"

namespace neug {
namespace common {

Task::Task(uint64_t maxNumThreads)
    : parent{nullptr},
      maxNumThreads{maxNumThreads},
      numThreadsFinished{0},
      numThreadsRegistered{0},
      exceptionsPtr{nullptr},
      ID{UINT64_MAX} {}

bool Task::registerThread() {
  lock_t lck{taskMtx};
  if (!hasExceptionNoLock() && canRegisterNoLock()) {
    numThreadsRegistered++;
    return true;
  }
  return false;
}

void Task::deRegisterThreadAndFinalizeTask() {
  lock_t lck{taskMtx};
  ++numThreadsFinished;
  if (!hasExceptionNoLock() && isCompletedNoLock()) {
    try {
      finalizeIfNecessary();
    } catch (std::exception& e) {
      setExceptionNoLock(std::current_exception());
    }
  }
  if (isCompletedNoLock()) {
    lck.unlock();
    cv.notify_all();
  }
}

}  // namespace common
}  // namespace neug
