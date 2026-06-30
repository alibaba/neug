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

#include "neug/execution/common/columns/array_columns.h"
#include "neug/execution/common/columns/columns_utils.h"
#include "neug/execution/common/columns/list_columns.h"
#include "neug/execution/expression/expr.h"
#include "neug/utils/exception/exception.h"
#include "neug/utils/result.h"

namespace neug {

namespace execution {

namespace {

bool isListLikeType(DataTypeId type_id) {
  return type_id == DataTypeId::kList || type_id == DataTypeId::kArray;
}

const DataType& getListLikeChildType(const DataType& type) {
  if (type.id() == DataTypeId::kList) {
    return ListType::GetChildType(type);
  }
  if (type.id() == DataTypeId::kArray) {
    return ArrayType::GetChildType(type);
  }
  THROW_INVALID_ARGUMENT_EXCEPTION("Unfold column type is not list or array");
}

const std::vector<Value>& getListLikeChildren(const Value& value) {
  if (value.type().id() == DataTypeId::kList) {
    return ListValue::GetChildren(value);
  }
  if (value.type().id() == DataTypeId::kArray) {
    return ArrayValue::GetChildren(value);
  }
  THROW_INVALID_ARGUMENT_EXCEPTION("Unfold value type is not list or array");
}

}  // namespace

neug::result<ContextChunk> Unfold::unfold(ContextChunk&& chunk, int key,
                                          int alias) {
  auto col = chunk.get(key);
  if (!isListLikeType(col->elem_type().id())) {
    LOG(ERROR) << "Unfold column type is not list or array";
    RETURN_INVALID_ARGUMENT_ERROR("Unfold column type is not list or array");
  }
  if (col->elem_type().id() == DataTypeId::kArray) {
    auto array_col = std::dynamic_pointer_cast<ArrayColumn>(col);
    auto [ptr, offsets] = array_col->unfold();
    chunk.set_with_reshuffle(alias, ptr, offsets);
    return chunk;
  }
  auto list_col = std::dynamic_pointer_cast<ListColumn>(col);
  auto [ptr, offsets] = list_col->unfold();

  chunk.set_with_reshuffle(alias, ptr, offsets);

  return chunk;
}

void unfold_list_like(ContextChunk& chunk, int alias,
                      const RecordExprBase& key) {
  const auto& elem_type = getListLikeChildType(key.type());
  auto builder = ColumnsUtils::create_builder(elem_type);
  size_t row_num = chunk.row_num();
  sel_vec_t offsets;
  for (size_t i = 0; i < row_num; ++i) {
    Value val = key.eval_record(chunk.chunk(), i);
    const auto& children = getListLikeChildren(val);
    for (const auto& elem : children) {
      builder->push_back_elem(elem);
      offsets.push_back(i);
    }
  }
  chunk.set_with_reshuffle(alias, builder->finish(), offsets);
}

neug::result<ContextChunk> Unfold::unfold(ContextChunk&& chunk,
                                          const RecordExprBase& key,
                                          int alias) {
  if (!isListLikeType(key.type().id())) {
    LOG(ERROR) << "Unfold column type is not list or array";
    RETURN_INVALID_ARGUMENT_ERROR("Unfold column type is not list or array");
  }
  unfold_list_like(chunk, alias, key);
  return chunk;
}

}  // namespace execution

}  // namespace neug
