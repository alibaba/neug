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

#include <iostream>

#include "progress_bar_display.h"

namespace neug {
namespace common {

/**
 * @brief A class that displays a progress bar in the terminal.
 */
class TerminalProgressBarDisplay final : public ProgressBarDisplay {
 public:
  void updateProgress(uint64_t queryID, double newPipelineProgress,
                      uint32_t newNumPipelinesFinished) override;

  void finishProgress(uint64_t queryID) override;

 private:
  void setGreenFont() const { std::cerr << "\033[1;32m"; }

  void setDefaultFont() const { std::cerr << "\033[0m"; }

  void printProgressBar();

 private:
  bool printing = false;
  std::atomic<bool> currentlyPrintingProgress;
};

}  // namespace common
}  // namespace neug
