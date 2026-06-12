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
#include "neug/execution/expression/expr.h"
#include "neug/utils/result.h"

namespace neug {

namespace execution {

neug::result<ContextChunk> Unfold::unfold(ContextChunk&& chunk, int key,
                                          int alias) {
  auto col = chunk.get(key);
  if (col->elem_type().id() != DataTypeId::kList) {
    LOG(ERROR) << "Unfold column type is not list";
    RETURN_INVALID_ARGUMENT_ERROR("Unfold column type is not list");
  }
  auto list_col = std::dynamic_pointer_cast<ListColumn>(col);
  auto [ptr, offsets] = list_col->unfold();

  chunk.set_with_reshuffle(alias, ptr, offsets);

  return chunk;
}

template <typename T>
void unfold_impl(ContextChunk& chunk, int alias, const RecordExprBase& key) {
  ValueColumnBuilder<T> builder;
  size_t row_num = chunk.row_num();
  std::vector<size_t> offsets;
  for (size_t i = 0; i < row_num; ++i) {
    Value val = key.eval_record(chunk.chunk(), i);
    const auto& list = ListValue::GetChildren(val);
    for (const auto& elem : list) {
      builder.push_back_elem(elem);
      offsets.push_back(i);
    }
  }
  chunk.set_with_reshuffle(alias, builder.finish(), offsets);
}

void unfold_list(ContextChunk& chunk, int alias, const RecordExprBase& key) {
  const auto& elem_type = ListType::GetChildType(key.type());

  ListColumnBuilder builder(ListType::GetChildType(elem_type));
  size_t row_num = chunk.row_num();
  std::vector<size_t> offsets;
  for (size_t i = 0; i < row_num; ++i) {
    Value val = key.eval_record(chunk.chunk(), i);
    const auto& list = ListValue::GetChildren(val);
    for (const auto& elem : list) {
      builder.push_back_elem(elem);
      offsets.push_back(i);
    }
  }
  chunk.set_with_reshuffle(alias, builder.finish(), offsets);
}

neug::result<ContextChunk> Unfold::unfold(ContextChunk&& chunk,
                                          const RecordExprBase& key,
                                          int alias) {
  auto type = ListType::GetChildType(key.type());
  switch (type.id()) {
#define TYPE_DISPATCHER(enum_val, type)   \
  case DataTypeId::enum_val:              \
    unfold_impl<type>(chunk, alias, key); \
    return chunk;
    FOR_EACH_DATA_TYPE(TYPE_DISPATCHER)
#undef TYPE_DISPATCHER
  case DataTypeId::kList:
    unfold_list(chunk, alias, key);
    return chunk;
  default:
    LOG(ERROR) << "Unfold column type is not supported: "
               << static_cast<int>(type.id());
    RETURN_INVALID_ARGUMENT_ERROR("Unfold column type is not supported");
  }
  return chunk;
}

}  // namespace execution

}  // namespace neug
