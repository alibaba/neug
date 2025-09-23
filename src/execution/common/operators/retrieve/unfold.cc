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

#include "neug/execution/common/operators/retrieve/unfold.h"

#include <glog/logging.h>
#include <memory>
#include <ostream>

#include "neug/execution/common/columns/i_context_column.h"
#include "neug/execution/common/columns/value_columns.h"
#include "neug/execution/common/context.h"
#include "neug/execution/common/rt_any.h"
#include "neug/utils/result.h"

namespace gs {

namespace runtime {

gs::result<Context> Unfold::unfold(Context&& ctxs, int key, int alias) {
  auto col = ctxs.get(key);
  if (col->elem_type() != RTAnyType::kList) {
    LOG(ERROR) << "Unfold column type is not list";
    RETURN_INVALID_ARGUMENT_ERROR("Unfold column type is not list");
  }
  auto list_col = std::dynamic_pointer_cast<ListValueColumnBase>(col);
  auto [ptr, offsets] = list_col->unfold();

  ctxs.set_with_reshuffle(alias, ptr, offsets);

  return ctxs;
}

}  // namespace runtime

}  // namespace gs
