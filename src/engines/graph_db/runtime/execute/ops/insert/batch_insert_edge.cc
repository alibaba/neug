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

#include "src/engines/graph_db/runtime/execute/ops/insert/batch_insert_edge.h"

#include <glog/logging.h>
#include <stddef.h>

#include <ostream>
#include <string_view>
#include <tuple>

#include "src/engines/graph_db/database/update_transaction.h"
#include "src/engines/graph_db/runtime/common/context.h"
#include "src/engines/graph_db/runtime/common/graph_interface.h"
#include "src/engines/graph_db/runtime/execute/ops/insert/batch_insert_utils.h"
#include "src/proto_generated_gie/common.pb.h"
#include "src/proto_generated_gie/cypher_ddl.pb.h"
#include "src/proto_generated_gie/cypher_dml.pb.h"
#include "src/storages/rt_mutable_graph/schema.h"

namespace gs {
class IRecordBatchSupplier;
class MutablePropertyFragment;

namespace runtime {
class OprTimer;

namespace ops {

template <typename SRC_PK_T, typename DST_PK_T>
void batch_load_edges_helper(
    MutablePropertyFragment& frag, label_t src_label_id, label_t edge_label_id,
    label_t dst_label_id, PropertyType edge_prop_type,
    std::vector<std::shared_ptr<IRecordBatchSupplier>>& suppliers) {
  if (edge_prop_type == PropertyType::Int64()) {
    frag.batch_load_edges<SRC_PK_T, DST_PK_T, int64_t,
                          std::vector<std::tuple<vid_t, vid_t, int64_t>>>(
        src_label_id, dst_label_id, edge_label_id, suppliers);
  } else if (edge_prop_type == PropertyType::UInt64()) {
    frag.batch_load_edges<SRC_PK_T, DST_PK_T, uint64_t,
                          std::vector<std::tuple<vid_t, vid_t, uint64_t>>>(
        src_label_id, dst_label_id, edge_label_id, suppliers);
  } else if (edge_prop_type == PropertyType::Int32()) {
    frag.batch_load_edges<SRC_PK_T, DST_PK_T, int32_t,
                          std::vector<std::tuple<vid_t, vid_t, int32_t>>>(
        src_label_id, dst_label_id, edge_label_id, suppliers);
  } else if (edge_prop_type == PropertyType::UInt32()) {
    frag.batch_load_edges<SRC_PK_T, DST_PK_T, uint32_t,
                          std::vector<std::tuple<vid_t, vid_t, uint32_t>>>(
        src_label_id, dst_label_id, edge_label_id, suppliers);
  } else if (edge_prop_type == PropertyType::StringView()) {
    frag.batch_load_edges<
        SRC_PK_T, DST_PK_T, std::string_view,
        std::vector<std::tuple<vid_t, vid_t, std::string_view>>>(
        src_label_id, dst_label_id, edge_label_id, suppliers);
  } else if (edge_prop_type == PropertyType::RecordView()) {
    frag.batch_load_edges<SRC_PK_T, DST_PK_T, RecordView,
                          std::vector<std::tuple<vid_t, vid_t, size_t>>>(
        src_label_id, dst_label_id, edge_label_id, suppliers);
  } else if (edge_prop_type == PropertyType::Double()) {
    frag.batch_load_edges<SRC_PK_T, DST_PK_T, double,
                          std::vector<std::tuple<vid_t, vid_t, double>>>(
        src_label_id, dst_label_id, edge_label_id, suppliers);
  } else {
    LOG(FATAL) << "BatchInsertEdgeOpr::Eval: unsupported edge prop type: "
               << edge_prop_type.ToString();
  }
}

template <typename SRC_PK_T>
void batch_load_edges_helper(
    MutablePropertyFragment& frag, label_t src_label_id, label_t edge_label_id,
    label_t dst_label_id, PropertyType dst_pk_prop, PropertyType edge_prop_type,
    std::vector<std::shared_ptr<IRecordBatchSupplier>>& suppliers) {
  if (dst_pk_prop == PropertyType::Int64()) {
    batch_load_edges_helper<SRC_PK_T, int64_t>(frag, src_label_id,
                                               edge_label_id, dst_label_id,
                                               edge_prop_type, suppliers);
  } else if (dst_pk_prop == PropertyType::UInt64()) {
    batch_load_edges_helper<SRC_PK_T, uint64_t>(frag, src_label_id,
                                                edge_label_id, dst_label_id,
                                                edge_prop_type, suppliers);
  } else if (dst_pk_prop == PropertyType::Int32()) {
    batch_load_edges_helper<SRC_PK_T, int32_t>(frag, src_label_id,
                                               edge_label_id, dst_label_id,
                                               edge_prop_type, suppliers);
  } else if (dst_pk_prop == PropertyType::UInt32()) {
    batch_load_edges_helper<SRC_PK_T, uint32_t>(frag, src_label_id,
                                                edge_label_id, dst_label_id,
                                                edge_prop_type, suppliers);
  } else if (dst_pk_prop == PropertyType::StringView()) {
    batch_load_edges_helper<SRC_PK_T, std::string_view>(
        frag, src_label_id, edge_label_id, dst_label_id, edge_prop_type,
        suppliers);
  } else {
    LOG(FATAL) << "BatchInsertEdgeOpr::Eval: unsupported dst pk type: "
               << dst_pk_prop.ToString();
  }
}

bl::result<Context> BatchInsertEdgeOpr::Eval(
    GraphUpdateInterface& graph,
    const std::map<std::string, std::string>& params, Context&& ctx,
    OprTimer& timer) {
  auto& frag = graph.GetTransaction().GetGraph();
  std::vector<std::pair<int32_t, std::string>>
      total_mappings;  // include prop_mappings and src/dst vertex bindings
  for (const auto& mapping : src_vertex_bindings_) {
    total_mappings.emplace_back(mapping);
  }
  for (const auto& mapping : dst_vertex_bindings_) {
    total_mappings.emplace_back(mapping);
  }
  for (const auto& mapping : prop_mappings_) {
    total_mappings.emplace_back(mapping);
  }
  auto suppliers = create_record_batch_supplier(ctx, total_mappings);
  if (src_pk_prop_ == PropertyType::Int64()) {
    batch_load_edges_helper<int64_t>(frag, src_label_id_, edge_label_id_,
                                     dst_label_id_, dst_pk_prop_, e_prop_,
                                     suppliers);
  } else if (src_pk_prop_ == PropertyType::UInt64()) {
    batch_load_edges_helper<uint64_t>(frag, src_label_id_, edge_label_id_,
                                      dst_label_id_, dst_pk_prop_, e_prop_,
                                      suppliers);
  } else if (src_pk_prop_ == PropertyType::Int32()) {
    batch_load_edges_helper<int32_t>(frag, src_label_id_, edge_label_id_,
                                     dst_label_id_, dst_pk_prop_, e_prop_,
                                     suppliers);
  } else if (src_pk_prop_ == PropertyType::UInt32()) {
    batch_load_edges_helper<uint32_t>(frag, src_label_id_, edge_label_id_,
                                      dst_label_id_, dst_pk_prop_, e_prop_,
                                      suppliers);
  } else if (src_pk_prop_ == PropertyType::StringView()) {
    batch_load_edges_helper<std::string_view>(frag, src_label_id_,
                                              edge_label_id_, dst_label_id_,
                                              dst_pk_prop_, e_prop_, suppliers);
  } else {
    LOG(FATAL) << "BatchInsertEdgeOpr::Eval: unsupported src pk type: "
               << src_pk_prop_.ToString();
  }
  return bl::result<Context>(std::move(ctx));
}

bool get_edge_triplet_label_ids(const Schema& schema,
                                const physical::EdgeType& edge_type,
                                label_t& edge_label, label_t& src_type,
                                label_t& dst_type) {
  switch (edge_type.type_name().item_case()) {
  case common::NameOrId::kId: {
    edge_label = edge_type.type_name().id();
    break;
  }
  case common::NameOrId::kName: {
    edge_label = schema.get_edge_label_id(edge_type.type_name().name());
    break;
  }
  default:
    LOG(ERROR) << "Unknown edge type: " << edge_type.type_name().DebugString();
    return false;
  }
  switch (edge_type.src_type_name().item_case()) {
  case common::NameOrId::kId: {
    src_type = edge_type.src_type_name().id();
    break;
  }
  case common::NameOrId::kName: {
    src_type = schema.get_vertex_label_id(edge_type.src_type_name().name());
    break;
  }
  default:
    LOG(ERROR) << "Unknown edge type: " << edge_type.type_name().DebugString();
    return false;
  }
  switch (edge_type.dst_type_name().item_case()) {
  case common::NameOrId::kId: {
    dst_type = edge_type.dst_type_name().id();
    break;
  }
  case common::NameOrId::kName: {
    dst_type = schema.get_vertex_label_id(edge_type.dst_type_name().name());
    break;
  }
  default:
    LOG(ERROR) << "Unknown edge type: " << edge_type.type_name().DebugString();
    return false;
  }
  return true;
}

std::unique_ptr<IUpdateOperator> BatchInsertEdgeOprBuilder::Build(
    const Schema& schema, const physical::PhysicalPlan& plan, int op_idx) {
  const auto& opr = plan.query_plan().plan(op_idx).opr().load_edge();
  // before BatchInsertEdgeOpr, we assume the raw data has already been loaded
  // into memory, with each tag points to a column.

  if (!opr.has_edge_type()) {
    LOG(FATAL) << "BatchInsertEdgeOprBuilder::Build: edge type is not set";
  }
  label_t edge_type, src_type, dst_type;
  get_edge_triplet_label_ids(schema, opr.edge_type(), edge_type, src_type,
                             dst_type);

  // parse property mappings
  std::vector<std::pair<int32_t, std::string>> prop_mappings,
      src_vertex_bindings, dst_vertex_binds;
  parse_property_mappings(opr.property_mappings(), prop_mappings);
  parse_property_mappings(opr.source_vertex_binding(), src_vertex_bindings);
  parse_property_mappings(opr.destination_vertex_binding(), dst_vertex_binds);

  PropertyType edge_prop_type, src_pk_type, dst_pk_type;
  auto& edge_props = schema.get_edge_properties(src_type, dst_type, edge_type);
  if (edge_props.empty()) {
    edge_prop_type = PropertyType::Empty();
  } else if (edge_props.size() == 1) {
    edge_prop_type = edge_props[0];
  } else {
    edge_prop_type = PropertyType::RecordView();
  }
  src_pk_type = get_the_pk_type_from_schema(schema, src_type);
  dst_pk_type = get_the_pk_type_from_schema(schema, dst_type);

  std::vector<std::tuple<PropertyType, std::string, size_t>> src_primary_key,
      dst_primary_key;

  return std::make_unique<BatchInsertEdgeOpr>(
      edge_type, src_type, dst_type, edge_prop_type, src_pk_type, dst_pk_type,
      prop_mappings, src_vertex_bindings, dst_vertex_binds);
}

}  // namespace ops
}  // namespace runtime
}  // namespace gs
