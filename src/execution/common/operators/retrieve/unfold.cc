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
#include "neug/utils/result.h"
#include "neug/utils/runtime/rt_any.h"

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

template <typename T>
Context unfold_impl(Context&& ctx, int alias, const Expr& key,
                    std::shared_ptr<Arena> arena) {
  ValueColumnBuilder<T> builder;
  size_t row_num = ctx.row_num();
  std::vector<size_t> offsets;
  for (size_t i = 0; i < row_num; ++i) {
    RTAny val = key.eval_path(i, *arena);
    auto list = val.as_list();
    size_t list_size = list.size();
    for (size_t j = 0; j < list_size; ++j) {
      builder.push_back_elem(list.get(j));
      offsets.push_back(i);
    }
  }
  ctx.set_with_reshuffle(alias, builder.finish(), offsets);
  return ctx;
}
template <>
Context unfold_impl<List>(Context&& ctx, int alias, const Expr& key,
                          std::shared_ptr<Arena> arena) {
  ValueColumnBuilder<List> builder;
  size_t row_num = ctx.row_num();
  std::vector<size_t> offsets;
  for (size_t i = 0; i < row_num; ++i) {
    RTAny val = key.eval_path(i, *arena);
    auto list = val.as_list();
    size_t list_size = list.size();
    for (size_t j = 0; j < list_size; ++j) {
      builder.push_back_elem(list.get(j));
      offsets.push_back(i);
    }
  }
  builder.set_arena(arena);
  ctx.set_with_reshuffle(alias, builder.finish(), offsets);
  return ctx;
}

gs::result<Context> Unfold::unfold(Context&& ctxs, const Expr& key, int alias) {
  std::shared_ptr<Arena> arena = std::make_shared<Arena>();
  auto type = key.eval_path(0, *arena).as_list().elem_type();
  if (type == RTAnyType::kI64Value) {
    return unfold_impl<int64_t>(std::move(ctxs), alias, key, arena);
  } else if (type == RTAnyType::kF64Value) {
    return unfold_impl<double>(std::move(ctxs), alias, key, arena);
  } else if (type == RTAnyType::kBoolValue) {
    return unfold_impl<bool>(std::move(ctxs), alias, key, arena);
  } else if (type == RTAnyType::kStringValue) {
    return unfold_impl<std::string_view>(std::move(ctxs), alias, key, arena);
  } else if (type == RTAnyType::kI32Value) {
    return unfold_impl<int32_t>(std::move(ctxs), alias, key, arena);
  } else if (type == RTAnyType::kF32Value) {
    return unfold_impl<float>(std::move(ctxs), alias, key, arena);
  } else if (type == RTAnyType::kU32Value) {
    return unfold_impl<uint32_t>(std::move(ctxs), alias, key, arena);
  } else if (type == RTAnyType::kU64Value) {
    return unfold_impl<uint64_t>(std::move(ctxs), alias, key, arena);
  } else if (type == RTAnyType::kList) {
    return unfold_impl<List>(std::move(ctxs), alias, key, arena);
  } else {
    LOG(ERROR) << "Unfold column type is not supported: "
               << static_cast<int>(type);
    RETURN_INVALID_ARGUMENT_ERROR("Unfold column type is not supported");
  }

  return ctxs;
}

}  // namespace runtime

}  // namespace gs
