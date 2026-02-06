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

#include "neug/execution/common/columns/list_columns.h"
#include "neug/execution/common/columns/edge_columns.h"
#include "neug/execution/common/columns/struct_columns.h"
#include "neug/execution/common/columns/vertex_columns.h"

namespace neug {
namespace execution {

std::pair<std::shared_ptr<IContextColumn>, std::vector<size_t>>
ListColumn::unfold() const {
  switch (elem_type_.id()) {
#define TYPE_DISPATCHER(enum_val, type) \
  case DataTypeId::enum_val:            \
    return unfold_impl<type>();
    FOR_EACH_DATA_TYPE(TYPE_DISPATCHER)
#undef TYPE_DISPATCHER
  case DataTypeId::kStruct: {
    StructColumnBuilder builder(elem_type_);
    std::vector<size_t> offsets;
    size_t i = 0;
    for (const auto& list : items_) {
      for (size_t j = list.offset; j < list.offset + list.length; ++j) {
        auto elem = datas_->get_elem(j);
        builder.push_back_elem(elem);
        offsets.push_back(i);
      }
      ++i;
    }
    return {builder.finish(), offsets};
  }
  case DataTypeId::kList: {
    ListColumnBuilder builder(elem_type_);
    std::vector<size_t> offsets;
    size_t i = 0;
    for (const auto& list : items_) {
      for (size_t j = list.offset; j < list.offset + list.length; ++j) {
        auto elem = datas_->get_elem(j);
        builder.push_back_elem(elem);
        offsets.push_back(i);
      }
      ++i;
    }
    return {builder.finish(), offsets};
  }
  case DataTypeId::kVertex: {
    std::vector<size_t> offsets;
    MLVertexColumnBuilder builder;
    size_t i = 0;
    for (const auto& list : items_) {
      for (size_t j = list.offset; j < list.offset + list.length; ++j) {
        auto elem = datas_->get_elem(j);
        builder.push_back_elem(elem);
        offsets.push_back(i);
      }
      ++i;
    }
    return {builder.finish(), offsets};
  }
  case DataTypeId::kEdge: {
    std::vector<size_t> offsets;
    BDMLEdgeColumnBuilder builder({});
    size_t i = 0;
    for (const auto& list : items_) {
      for (size_t j = list.offset; j < list.offset + list.length; ++j) {
        auto elem = datas_->get_elem(j);
        auto edge = elem.GetValue<edge_t>();
        builder.insert_label(edge.label);
        builder.push_back_opt(edge.label, edge.src, edge.dst, edge.prop,
                              edge.dir);
        offsets.push_back(i);
      }
      ++i;
    }
    return {builder.finish(), offsets};
  }
  default:
    LOG(FATAL) << "not implemented for " << this->column_info() << " "
               << static_cast<int>(elem_type_.id());
    return {nullptr, std::vector<size_t>()};
  }
}
std::shared_ptr<IContextColumn> ListColumn::shuffle(
    const std::vector<size_t>& offsets) const {
  auto ptr = std::make_shared<ListColumn>(elem_type_);
  std::vector<list_item> new_items(offsets.size());
  for (size_t i = 0; i < offsets.size(); ++i) {
    new_items[i] = items_[offsets[i]];
  }
  ptr->items_.swap(new_items);
  ptr->datas_ = datas_;
  return ptr;
}

}  // namespace execution
}  // namespace neug