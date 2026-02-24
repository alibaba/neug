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

#include <memory>
#include <mutex>

#include "progress_bar_display.h"

namespace neug {
namespace common {

typedef std::unique_ptr<ProgressBarDisplay> (
    *progress_bar_display_create_func_t)();

/**
 * @brief Progress bar for tracking the progress of a pipeline. Prints the
 * progress of each query pipeline and the overall progress.
 */
class ProgressBar {
 public:
  explicit ProgressBar(bool enableProgressBar);

  static std::shared_ptr<ProgressBarDisplay> DefaultProgressBarDisplay();

  void addPipeline();

  void finishPipeline(uint64_t queryID);

  void endProgress(uint64_t queryID);

  void startProgress(uint64_t queryID);

  void toggleProgressBarPrinting(bool enable);

  void updateProgress(uint64_t queryID, double curPipelineProgress);

  void setDisplay(std::shared_ptr<ProgressBarDisplay> progressBarDipslay);

  std::shared_ptr<ProgressBarDisplay> getDisplay() { return display; }

  bool getProgressBarPrinting() const { return trackProgress; }

 private:
  void resetProgressBar(uint64_t queryID);

  void updateDisplay(uint64_t queryID, double curPipelineProgress);

 private:
  uint32_t numPipelines;
  uint32_t numPipelinesFinished;
  std::mutex progressBarLock;
  bool trackProgress;
  std::shared_ptr<ProgressBarDisplay> display;
};

}  // namespace common
}  // namespace neug
