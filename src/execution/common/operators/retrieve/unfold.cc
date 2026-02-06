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

#include "neug/execution/common/columns/list_columns.h"
#include "neug/execution/common/context.h"
#include "neug/execution/expression/expr.h"
#include "neug/utils/result.h"

namespace neug {

namespace execution {

neug::result<Context> Unfold::unfold(Context&& ctxs, int key, int alias) {
  auto col = ctxs.get(key);
  if (col->elem_type().id() != DataTypeId::kList) {
    LOG(ERROR) << "Unfold column type is not list";
    RETURN_INVALID_ARGUMENT_ERROR("Unfold column type is not list");
  }
  auto list_col = std::dynamic_pointer_cast<ListColumnBase>(col);
  auto [ptr, offsets] = list_col->unfold();

  ctxs.set_with_reshuffle(alias, ptr, offsets);

  return ctxs;
}

template <typename T>
Context unfold_impl(Context&& ctx, int alias, const RecordExprBase& key) {
  ValueColumnBuilder<T> builder;
  size_t row_num = ctx.row_num();
  std::vector<size_t> offsets;
  for (size_t i = 0; i < row_num; ++i) {
    Value val = key.eval_record(ctx, i);
    const auto& list = ListValue::GetChildren(val);
    for (const auto& elem : list) {
      builder.push_back_elem(elem);
      offsets.push_back(i);
    }
  }
  ctx.set_with_reshuffle(alias, builder.finish(), offsets);
  return ctx;
}

Context unfold_list(Context&& ctx, int alias, const RecordExprBase& key) {
  const auto& elem_type = ListType::GetChildType(key.type());

  ListColumnBuilder builder(ListType::GetChildType(elem_type));
  size_t row_num = ctx.row_num();
  std::vector<size_t> offsets;
  for (size_t i = 0; i < row_num; ++i) {
    Value val = key.eval_record(ctx, i);
    const auto& list = ListValue::GetChildren(val);
    for (const auto& elem : list) {
      builder.push_back_elem(elem);
      offsets.push_back(i);
    }
  }
  ctx.set_with_reshuffle(alias, builder.finish(), offsets);
  return ctx;
}

neug::result<Context> Unfold::unfold(Context&& ctxs, const RecordExprBase& key,
                                     int alias) {
  auto type = ListType::GetChildType(key.type());
  switch (type.id()) {
#define TYPE_DISPATCHER(enum_val, type) \
  case DataTypeId::enum_val:            \
    return unfold_impl<type>(std::move(ctxs), alias, key);
    FOR_EACH_DATA_TYPE(TYPE_DISPATCHER)
#undef TYPE_DISPATCHER
  case DataTypeId::kList:
    return unfold_list(std::move(ctxs), alias, key);
  default:
    LOG(ERROR) << "Unfold column type is not supported: "
               << static_cast<int>(type.id());
    RETURN_INVALID_ARGUMENT_ERROR("Unfold column type is not supported");
  }
  return ctxs;
}

}  // namespace execution

}  // namespace neug
