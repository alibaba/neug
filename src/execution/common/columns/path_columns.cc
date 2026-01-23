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

#include "neug/execution/common/columns/path_columns.h"

#include <limits>

namespace gs {
namespace runtime {

std::shared_ptr<IContextColumn> PathColumn::shuffle(
    const std::vector<size_t>& offsets) const {
  PathColumnBuilder builder;
  builder.reserve(offsets.size());
  for (auto& offset : offsets) {
    builder.push_back_opt(data_[offset]);
  }
  return builder.finish();
}

std::shared_ptr<IContextColumn> PathColumn::optional_shuffle(
    const std::vector<size_t>& offsets) const {
  PathColumnBuilder builder(true);
  builder.reserve(offsets.size());
  for (auto& offset : offsets) {
    if (offset == std::numeric_limits<size_t>::max()) {
      builder.push_back_null();
    } else {
      builder.push_back_opt(data_[offset]);
    }
  }
  return builder.finish();
}

std::shared_ptr<IContextColumn> OptionalPathColumn::shuffle(
    const std::vector<size_t>& offsets) const {
  PathColumnBuilder builder(true);
  builder.reserve(offsets.size());
  for (auto& offset : offsets) {
    if (!valids_[offset]) {
      builder.push_back_null();
    } else {
      builder.push_back_opt(data_[offset]);
    }
  }
  return builder.finish();
}

}  // namespace runtime
}  // namespace gs
