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

#include "neug/compiler/common/data_chunk/sel_vector.h"

#include <array>
#include <numeric>

#include "neug/compiler/common/system_config.h"
#include "neug/compiler/common/types/types.h"
#include "neug/compiler/common/vector/value_vector.h"

namespace neug {
namespace common {

// NOLINTNEXTLINE(cert-err58-cpp): always evaluated at compile time, and even
// not it would not throw
static const std::array<sel_t, DEFAULT_VECTOR_CAPACITY>
    INCREMENTAL_SELECTED_POS = []() constexpr noexcept {
  std::array<sel_t, DEFAULT_VECTOR_CAPACITY> selectedPos{};
  std::iota(selectedPos.begin(), selectedPos.end(), 0);
  return selectedPos;
}
();

SelectionView::SelectionView(sel_t startPos, sel_t selectedSize)
    : selectedPositions{INCREMENTAL_SELECTED_POS.data() + startPos},
      selectedSize{selectedSize},
      state{State::STATIC} {}

SelectionVector::SelectionVector() : SelectionVector{DEFAULT_VECTOR_CAPACITY} {}

void SelectionVector::setToUnfiltered() {
  selectedPositions = INCREMENTAL_SELECTED_POS.data();
  state = State::STATIC;
}
void SelectionVector::setToUnfiltered(sel_t size) {
  NEUG_ASSERT(size <= capacity);
  selectedPositions = INCREMENTAL_SELECTED_POS.data();
  selectedSize = size;
  state = State::STATIC;
}

std::vector<SelectionVector*> SelectionVector::fromValueVectors(
    const std::vector<std::shared_ptr<ValueVector>>& vec) {
  std::vector<SelectionVector*> ret(vec.size());
  for (size_t i = 0; i < vec.size(); ++i) {
    ret[i] = vec[i]->getSelVectorPtr();
  }
  return ret;
}

}  // namespace common
}  // namespace neug
