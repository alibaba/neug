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

#include "neug/compiler/common/task_system/progress_bar.h"

#include "neug/compiler/common/task_system/terminal_progress_bar_display.h"

namespace neug {
namespace common {

ProgressBar::ProgressBar(bool enableProgressBar) {
  display = DefaultProgressBarDisplay();
  numPipelines = 0;
  numPipelinesFinished = 0;
  trackProgress = enableProgressBar;
}

std::shared_ptr<ProgressBarDisplay> ProgressBar::DefaultProgressBarDisplay() {
  return std::make_shared<TerminalProgressBarDisplay>();
}

void ProgressBar::setDisplay(
    std::shared_ptr<ProgressBarDisplay> progressBarDipslay) {
  display = progressBarDipslay;
}

void ProgressBar::startProgress(uint64_t queryID) {
  if (!trackProgress) {
    return;
  }
  std::lock_guard<std::mutex> lock(progressBarLock);
  updateDisplay(queryID, 0.0);
}

void ProgressBar::endProgress(uint64_t queryID) {
  std::lock_guard<std::mutex> lock(progressBarLock);
  resetProgressBar(queryID);
}

void ProgressBar::addPipeline() {
  if (!trackProgress) {
    return;
  }
  numPipelines++;
  display->setNumPipelines(numPipelines);
}

void ProgressBar::finishPipeline(uint64_t queryID) {
  if (!trackProgress) {
    return;
  }
  numPipelinesFinished++;
  updateProgress(queryID, 0.0);
}

void ProgressBar::updateProgress(uint64_t queryID, double curPipelineProgress) {
  if (!trackProgress) {
    return;
  }
  updateDisplay(queryID, curPipelineProgress);
}

void ProgressBar::resetProgressBar(uint64_t queryID) {
  numPipelines = 0;
  numPipelinesFinished = 0;
  display->finishProgress(queryID);
}

void ProgressBar::updateDisplay(uint64_t queryID, double curPipelineProgress) {
  display->updateProgress(queryID, curPipelineProgress, numPipelinesFinished);
}

void ProgressBar::toggleProgressBarPrinting(bool enable) {
  trackProgress = enable;
}

}  // namespace common
}  // namespace neug
