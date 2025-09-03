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

#include "neug/compiler/function/gds/frontier_morsel.h"

using namespace gs::common;

namespace gs {
namespace function {

FrontierMorselDispatcher::FrontierMorselDispatcher(uint64_t maxThreads)
    : maxOffset{INVALID_OFFSET},
      maxThreads{maxThreads},
      morselSize(UINT64_MAX) {
  nextOffset.store(INVALID_OFFSET);
}

void FrontierMorselDispatcher::init(offset_t _maxOffset) {
  maxOffset = _maxOffset;
  nextOffset.store(0u);
  // Frontier size calculation: The ideal scenario is to have k^2 many morsels
  // where k the number of maximum threads that could be working on this
  // frontier. However, if that is too small then we default to
  // MIN_FRONTIER_MORSEL_SIZE.
  auto idealMorselSize = maxOffset / std::max(MIN_NUMBER_OF_FRONTIER_MORSELS,
                                              maxThreads * maxThreads);
  morselSize = std::max(MIN_FRONTIER_MORSEL_SIZE, idealMorselSize);
}

bool FrontierMorselDispatcher::getNextRangeMorsel(
    FrontierMorsel& frontierMorsel) {
  auto beginOffset =
      nextOffset.fetch_add(morselSize, std::memory_order_acq_rel);
  if (beginOffset >= maxOffset) {
    return false;
  }
  auto endOffset = beginOffset + morselSize > maxOffset
                       ? maxOffset
                       : beginOffset + morselSize;
  frontierMorsel.init(beginOffset, endOffset);
  return true;
}

}  // namespace function
}  // namespace gs
