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

#include "neug/compiler/common/task_system/terminal_progress_bar_display.h"

namespace neug {
namespace common {

void TerminalProgressBarDisplay::updateProgress(
    uint64_t /*queryID*/, double newPipelineProgress,
    uint32_t newNumPipelinesFinished) {
  // There can still be data races as the comparison + update of cur/old
  // progress is not done atomically However this error does not build up over
  // time and we don't require perfect progress bar accuracy So we implement it
  // this way with atomics (instead of mutexes) for better performance
  uint32_t curPipelineProgress = (uint32_t)(newPipelineProgress * 100.0);
  uint32_t oldPipelineProgress = (uint32_t)(pipelineProgress * 100.0);
  if (curPipelineProgress > oldPipelineProgress ||
      newNumPipelinesFinished > numPipelinesFinished) {
    pipelineProgress.store(newPipelineProgress);
    numPipelinesFinished.store(newNumPipelinesFinished);
    printProgressBar();
  }
}

void TerminalProgressBarDisplay::finishProgress(uint64_t /*queryID*/) {
  if (printing) {
    std::cout << "\033[2A\033[2K\033[1B\033[2K\033[1A";
    std::cout.flush();
  }
  printing = false;
  numPipelines = 0;
  numPipelinesFinished = 0;
  pipelineProgress = 0;
}

void TerminalProgressBarDisplay::printProgressBar() {
  // If a different thread is already printing the progress skip the current
  // update As we do not require the displayed value to be perfectly up to date
  bool falseValue{false};
  if (currentlyPrintingProgress.compare_exchange_strong(falseValue, true)) {
    setGreenFont();
    if (printing) {
      if (pipelineProgress == 0) {
        std::cout << "\033[1A\033[2K\033[1A";
        printing = false;
      } else {
        std::cout << "\033[1A";
      }
    }
    if (!printing) {
      std::cout << "Pipelines Finished: " << numPipelinesFinished << "/"
                << numPipelines << "\n";
      printing = true;
    }
    std::cout << "Current Pipeline Progress: "
              << uint32_t(pipelineProgress * 100.0) << "%"
              << "\n";
    setDefaultFont();
    std::cout.flush();
    currentlyPrintingProgress.store(false);
  }
}

}  // namespace common
}  // namespace neug
