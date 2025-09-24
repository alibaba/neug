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

#include <atomic>

#include "neug/compiler/common/types/types.h"

namespace gs {
namespace function {

class FrontierMorsel {
 public:
  FrontierMorsel() = default;

  common::offset_t getBeginOffset() const { return beginOffset; }
  common::offset_t getEndOffset() const { return endOffset; }

  void init(common::offset_t beginOffset_, common::offset_t endOffset_) {
    beginOffset = beginOffset_;
    endOffset = endOffset_;
  }

 private:
  common::offset_t beginOffset = common::INVALID_OFFSET;
  common::offset_t endOffset = common::INVALID_OFFSET;
};

class NEUG_API FrontierMorselDispatcher {
  static constexpr uint64_t MIN_FRONTIER_MORSEL_SIZE = 512;
  // Note: MIN_NUMBER_OF_FRONTIER_MORSELS is the minimum number of morsels we
  // aim to have but we can have fewer than this. See the
  // beginFrontierComputeBetweenTables to see the actual morselSize computation
  // for details.
  static constexpr uint64_t MIN_NUMBER_OF_FRONTIER_MORSELS = 128;

 public:
  explicit FrontierMorselDispatcher(uint64_t maxThreads);

  void init(common::offset_t _maxOffset);

  bool getNextRangeMorsel(FrontierMorsel& frontierMorsel);

 private:
  common::offset_t maxOffset;
  std::atomic<common::offset_t> nextOffset;
  uint64_t maxThreads;
  uint64_t morselSize;
};

}  // namespace function
}  // namespace gs
