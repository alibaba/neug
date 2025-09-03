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

#include "neug/execution/runtime/common/columns/path_columns.h"

#include <limits>

namespace gs {
namespace runtime {

std::shared_ptr<IContextColumn> GeneralPathColumn::shuffle(
    const std::vector<size_t>& offsets) const {
  GeneralPathColumnBuilder builder;
  builder.reserve(offsets.size());
  for (auto& offset : offsets) {
    builder.push_back_opt(data_[offset]);
  }
  builder.set_arena(this->get_arena());
  return builder.finish();
}

std::shared_ptr<IContextColumn> GeneralPathColumn::optional_shuffle(
    const std::vector<size_t>& offsets) const {
  OptionalGeneralPathColumnBuilder builder;
  builder.reserve(offsets.size());
  for (auto& offset : offsets) {
    if (offset == std::numeric_limits<size_t>::max()) {
      builder.push_back_null();
    } else {
      builder.push_back_opt(data_[offset], true);
    }
  }
  builder.set_arena(this->get_arena());
  return builder.finish();
}

std::shared_ptr<IContextColumn> OptionalGeneralPathColumn::shuffle(
    const std::vector<size_t>& offsets) const {
  OptionalGeneralPathColumnBuilder builder;
  builder.reserve(offsets.size());
  for (auto& offset : offsets) {
    builder.push_back_opt(data_[offset], valids_[offset]);
  }
  builder.set_arena(this->get_arena());
  return builder.finish();
}

}  // namespace runtime
}  // namespace gs