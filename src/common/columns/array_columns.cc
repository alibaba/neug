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
 * See the License for permissions and limitations under the License.
 */

#include "neug/common/columns/array_columns.h"

#include <limits>

#include <glog/logging.h>

namespace neug {

std::pair<std::shared_ptr<IContextColumn>, sel_vec_t>
ContextArrayColumn::unfold() const {
  sel_vec_t offsets;
  offsets.reserve(size() * array_size_);
  if (!is_optional_) {
    for (size_t i = 0; i < size(); ++i) {
      for (uint64_t j = 0; j < array_size_; ++j) {
        offsets.push_back(i);
      }
    }
    return {datas_, offsets};
  }

  sel_vec_t data_offsets;
  data_offsets.reserve(size() * array_size_);
  for (size_t i = 0; i < size(); ++i) {
    if (!valids_[i]) {
      continue;
    }
    size_t base = i * array_size_;
    for (uint64_t j = 0; j < array_size_; ++j) {
      data_offsets.push_back(base + j);
      offsets.push_back(i);
    }
  }
  return {datas_->shuffle(data_offsets), offsets};
}

std::shared_ptr<IContextColumn> ContextArrayColumn::shuffle(
    const sel_vec_t& offsets) const {
  if (!datas_)
    return nullptr;

  auto result = std::make_shared<ContextArrayColumn>(type_);
  sel_vec_t data_offsets;
  data_offsets.reserve(offsets.size() * array_size_);

  for (size_t row_idx : offsets) {
    size_t base = row_idx * array_size_;
    for (uint64_t j = 0; j < array_size_; ++j) {
      data_offsets.push_back(base + j);
    }
  }

  result->datas_ = datas_->shuffle(data_offsets);
  result->is_optional_ = is_optional_;
  if (is_optional_) {
    result->valids_.reserve(offsets.size());
    for (auto offset : offsets) {
      result->valids_.push_back(valids_[offset]);
    }
  }
  return result;
}

std::shared_ptr<IContextColumn> ContextArrayColumn::optional_shuffle(
    const sel_vec_t& offsets) const {
  if (!datas_) {
    return nullptr;
  }

  auto result = std::make_shared<ContextArrayColumn>(type_);
  sel_vec_t data_offsets;
  data_offsets.reserve(offsets.size() * array_size_);
  result->valids_.reserve(offsets.size());

  for (auto row_idx : offsets) {
    if (row_idx == std::numeric_limits<sel_t>::max()) {
      for (uint64_t j = 0; j < array_size_; ++j) {
        data_offsets.push_back(std::numeric_limits<sel_t>::max());
      }
      result->valids_.push_back(false);
      continue;
    }

    size_t base = row_idx * array_size_;
    for (uint64_t j = 0; j < array_size_; ++j) {
      data_offsets.push_back(base + j);
    }
    result->valids_.push_back(has_value(row_idx));
  }

  result->datas_ = datas_->optional_shuffle(data_offsets);
  result->is_optional_ = true;
  return result;
}

}  // namespace neug
