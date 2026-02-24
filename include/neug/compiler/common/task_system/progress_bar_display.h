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

#include <stdint.h>

#include <atomic>

namespace neug {
namespace common {

/**
 * @brief Interface for displaying progress of a pipeline and a query.
 */
class ProgressBarDisplay {
 public:
  ProgressBarDisplay()
      : pipelineProgress{0}, numPipelines{0}, numPipelinesFinished{0} {};

  virtual ~ProgressBarDisplay() = default;

  // Update the progress of the pipeline and the number of finished pipelines.
  // queryID is used to identify the query when we track progress of multiple
  // queries asynchronously This function should work even if called
  // concurrently by multiple threads
  virtual void updateProgress(uint64_t queryID, double newPipelineProgress,
                              uint32_t newNumPipelinesFinished) = 0;

  // Finish the progress display. queryID is used to identify the query when we
  // track progress of multiple queries asynchronously
  virtual void finishProgress(uint64_t queryID) = 0;

  void setNumPipelines(uint32_t newNumPipelines) {
    numPipelines = newNumPipelines;
  };

 protected:
  std::atomic<double> pipelineProgress;
  uint32_t numPipelines;
  std::atomic<uint32_t> numPipelinesFinished;
};

}  // namespace common
}  // namespace neug
