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

#include "neug/execution/execute/ops/retrieve/order_by_utils.h"

#include "neug/execution/common/columns/vertex_columns.h"
#include "neug/storages/graph/graph_interface.h"
namespace neug {
namespace execution {
namespace ops {
template <typename T>
bool vertex_property_topN_impl(bool asc, size_t limit,
                               const std::shared_ptr<IVertexColumn>& col,
                               const StorageReadInterface& graph,
                               const std::string& prop_name,
                               std::vector<size_t>& offsets) {
  std::vector<std::shared_ptr<StorageReadInterface::vertex_column_t<T>>>
      property_columns;
  label_t label_num = graph.schema().vertex_label_frontier();
  for (label_t i = 0; i < label_num; ++i) {
    if (!graph.schema().vertex_label_valid(i)) {
      continue;
    }
    property_columns.emplace_back(
        std::dynamic_pointer_cast<StorageReadInterface::vertex_column_t<T>>(
            graph.GetVertexPropColumn(i, prop_name)));
  }
  bool success = true;
  if (asc) {
    TopNGenerator<T, TopNAscCmp<T>> gen(limit);
    foreach_vertex(*col, [&](size_t idx, label_t label, vid_t v) {
      if (!(property_columns[label] == nullptr)) {
        gen.push(property_columns[label]->get_view(v), idx);
      } else {
        success = false;
      }
    });
    if (success) {
      gen.generate_indices(offsets);
    }
  } else {
    TopNGenerator<T, TopNDescCmp<T>> gen(limit);
    foreach_vertex(*col, [&](size_t idx, label_t label, vid_t v) {
      if (!(property_columns[label] == nullptr)) {
        gen.push(property_columns[label]->get_view(v), idx);
      } else {
        success = false;
      }
    });
    if (success) {
      gen.generate_indices(offsets);
    }
  }

  return success;
}

bool vertex_property_topN(bool asc, size_t limit,
                          const std::shared_ptr<IVertexColumn>& col,
                          const StorageReadInterface& graph,
                          const std::string& prop_name,
                          std::vector<size_t>& offsets) {
  std::vector<DataTypeId> prop_types;
  const auto& labels = col->get_labels_set();
  for (auto l : labels) {
    const auto& pk = graph.schema().get_vertex_primary_key(l)[0];
    if (prop_name == std::get<1>(pk)) {
      prop_types.emplace_back(std::get<0>(pk).id());
      break;
    }

    const auto& prop_names = graph.schema().get_vertex_property_names(l);
    int prop_names_size = prop_names.size();
    for (int prop_id = 0; prop_id < prop_names_size; ++prop_id) {
      if (prop_names[prop_id] == prop_name) {
        prop_types.push_back(
            graph.schema().get_vertex_properties(l)[prop_id].id());
        break;
      }
    }
  }
  if (prop_types.size() != labels.size()) {
    return false;
  }
  for (size_t k = 1; k < prop_types.size(); ++k) {
    if (prop_types[k] != prop_types[0]) {
      return false;
    }
  }
  switch (prop_types[0]) {
#define TYPE_DISPATCHER(enum_val, type)                                       \
  case DataTypeId::enum_val:                                                  \
    return vertex_property_topN_impl<type>(asc, limit, col, graph, prop_name, \
                                           offsets);
    FOR_EACH_DATA_TYPE_NO_STRING(TYPE_DISPATCHER)
#undef TYPE_DISPATCHER
  case DataTypeId::kVarchar:
    return vertex_property_topN_impl<std::string_view>(asc, limit, col, graph,
                                                       prop_name, offsets);
  default:
    LOG(ERROR) << "prop type not support..." << static_cast<int>(prop_types[0]);
    return false;
  }
}
}  // namespace ops
}  // namespace execution
}  // namespace neug