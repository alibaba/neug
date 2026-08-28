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

#include "neug/storages/graph/cow_detach_state.h"

#include "neug/storages/graph/schema.h"

namespace neug {

CowDetachState CowDetachState::FromSchema(const Schema& schema) {
  CowDetachState state;

  // Build vertex table COW states from schema — one per vertex label slot.
  const auto& vertex_schemas = schema.get_all_vertex_schemas();
  state.vertex_tables.resize(vertex_schemas.size());
  for (size_t i = 0; i < vertex_schemas.size(); ++i) {
    if (vertex_schemas[i] && !vertex_schemas[i]->empty()) {
      size_t col_count = vertex_schemas[i]->property_names.size();
      state.vertex_tables[i].columns_detached.resize(col_count, false);
    }
  }

  for (const auto& [key, edge_schema] : schema.get_all_edge_schemas()) {
    EdgeTableDetachState edge_state;
    if (edge_schema) {
      edge_state.columns_detached.resize(edge_schema->property_names.size(),
                                         false);
    }
    state.edge_tables.emplace(key, std::move(edge_state));
  }

  return state;
}

}  // namespace neug
