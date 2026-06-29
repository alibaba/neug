/** Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "neug/execution/common/columns/array_columns.h"

#include <glog/logging.h>

namespace neug {
namespace execution {

std::pair<std::shared_ptr<IContextColumn>, sel_vec_t> ArrayColumn::unfold()
    const {
  sel_vec_t offsets;
  offsets.reserve(size() * array_size_);
  for (size_t i = 0; i < size(); ++i) {
    for (uint64_t j = 0; j < array_size_; ++j) {
      offsets.push_back(i);
    }
  }
  return {datas_, offsets};
}

std::shared_ptr<IContextColumn> ArrayColumn::shuffle(
    const sel_vec_t& offsets) const {
  if (!datas_)
    return nullptr;

  auto result = std::make_shared<ArrayColumn>(type_);
  sel_vec_t data_offsets;
  data_offsets.reserve(offsets.size() * array_size_);

  for (size_t row_idx : offsets) {
    size_t base = row_idx * array_size_;
    for (uint64_t j = 0; j < array_size_; ++j) {
      data_offsets.push_back(base + j);
    }
  }

  result->datas_ = datas_->shuffle(data_offsets);
  return result;
}

}  // namespace execution
}  // namespace neug
