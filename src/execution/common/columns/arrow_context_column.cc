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

#include "neug/execution/common/columns/arrow_context_column.h"

#include <arrow/type.h>

namespace gs {
namespace runtime {

std::shared_ptr<IContextColumn> ArrowArrayContextColumnBuilder::finish() {
  return std::make_shared<ArrowArrayContextColumn>(columns_);
}

void ArrowArrayContextColumnBuilder::push_back(
    const std::shared_ptr<arrow::Array>& column) {
  if (columns_.size() > 0) {
    if (columns_[0]->type()->Equals(column->type())) {
      columns_.push_back(column);
      return;
    } else {
      LOG(FATAL) << "Expect the same type of columns, but got "
                 << columns_[0]->type()->ToString() << " and "
                 << column->type()->ToString();
    }
  }
  columns_.push_back(column);
}
}  // namespace runtime
}  // namespace gs
