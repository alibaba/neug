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

#include <glog/logging.h>
#include <ostream>

#include "src/engines/graph_db/database/update_transaction.h"
#include "src/engines/graph_db/runtime/common/context.h"
#include "src/engines/graph_db/runtime/common/graph_interface.h"
#include "src/engines/graph_db/runtime/execute/ops/batch/batch_insert_vertex.h"
#include "src/engines/graph_db/runtime/execute/ops/batch/batch_update_utils.h"
#include "src/proto_generated_gie/common.pb.h"
#include "src/proto_generated_gie/cypher_dml.pb.h"
#include "src/storages/rt_mutable_graph/mutable_property_fragment.h"
#include "src/storages/rt_mutable_graph/schema.h"
#include "src/utils/pb_utils.h"

namespace gs {
namespace runtime {
class OprTimer;

namespace ops {

bl::result<Context> BatchInsertVertexOpr::Eval(
    GraphUpdateInterface& graph,
    const std::map<std::string, std::string>& params, Context&& ctx,
    OprTimer& timer) {
  auto& frag = graph.GetTransaction().GetGraph();

  auto suppliers = create_record_batch_supplier(ctx, prop_mappings_);
  if (pk_type_ == PropertyType::kInt64) {
    frag.template batch_load_vertices<int64_t>(vertex_label_id_, suppliers);
  } else if (pk_type_ == PropertyType::kInt32) {
    frag.template batch_load_vertices<int32_t>(vertex_label_id_, suppliers);
  } else if (pk_type_ == PropertyType::kString) {
    frag.template batch_load_vertices<std::string>(vertex_label_id_, suppliers);
  } else {
    LOG(FATAL) << "Unsupported primary key type: " << pk_type_;
  }
  return bl::result<Context>(std::move(ctx));
}

std::unique_ptr<IUpdateOperator> BatchInsertVertexOprBuilder::Build(
    const Schema& schema, const physical::PhysicalPlan& plan, int op_idx) {
  const auto& opr = plan.query_plan().plan(op_idx).opr().load_vertex();
  // before BatchInsertVertexOpr, we assume the raw data has already been loaded
  // into memory, with each tag points to a column.

  if (!opr.has_vertex_type()) {
    LOG(FATAL) << "BatchInsertVertexOpr must have vertex type";
  }
  label_t vertex_label_id = 0;
  switch (opr.vertex_type().item_case()) {
  case common::NameOrId::kId: {
    vertex_label_id = opr.vertex_type().id();
    break;
  }
  case common::NameOrId::kName: {
    vertex_label_id = schema.get_vertex_label_id(opr.vertex_type().name());
    break;
  }
  default:
    LOG(FATAL) << "Unknown vertex type: " << opr.vertex_type().DebugString();
  }
  LOG(INFO) << "vertex_label_id: " << (int32_t) vertex_label_id;
  // <tag_id, property_name>
  std::vector<std::pair<int32_t, std::string>> prop_mappings;
  parse_property_mappings(opr.property_mappings(), prop_mappings);

  return std::make_unique<BatchInsertVertexOpr>(
      vertex_label_id, get_the_pk_type_from_schema(schema, vertex_label_id),
      prop_mappings);
}

std::pair<Any, std::vector<Any>> get_pk_and_prop_values(
    const std::vector<std::pair<std::string, Any>>& properties,
    const std::string& pk_name, const PropertyType& pk_type,
    const std::vector<std::string>& properties_name,
    const std::vector<PropertyType>& properties_type) {
  if (properties.empty()) {
    throw std::runtime_error("No properties provided for vertex insertion");
  }
  if (properties_name.size() != properties_type.size()) {
    throw std::runtime_error("Properties name and type size mismatch");
  }

  Any pk_value;
  std::vector<Any> prop_values;
  // The order should match the schema
  bool pk_found = false;
  for (size_t i = 0; i < properties.size(); ++i) {
    const auto& prop = properties[i];
    const auto& prop_name = prop.first;
    const auto& prop_value = prop.second;
    if (!pk_found && prop_name == pk_name) {
      pk_found = true;
      if (prop_value.type != pk_type) {
        throw std::runtime_error("Primary key type mismatch for property " +
                                 prop_name);
      }
      pk_value = prop_value;
    } else {
      auto it =
          std::find(properties_name.begin(), properties_name.end(), prop_name);
      if (it != properties_name.end()) {
        size_t idx = std::distance(properties_name.begin(), it);
        if (idx < properties_type.size()) {
          if (properties_type[idx] == PropertyType::kEmpty) {
            LOG(WARNING) << "Property " << prop_name
                         << " has empty type, skipping.";
            continue;
          }
          if (prop_value.type != properties_type[idx]) {
            throw std::runtime_error("Property type mismatch for property " +
                                     prop_name);
          }
          prop_values.push_back(prop_value);
        } else {
          LOG(WARNING) << "Property " << prop_name
                       << " has no corresponding type, skipping.";
        }
      } else {
        LOG(WARNING) << "Property " << prop_name
                     << " not found in schema, skipping.";
      }
    }
  }
  if (!pk_found) {
    throw std::runtime_error("Primary key " + pk_name +
                             " not found in provided properties");
  }
  return std::make_pair(
      pk_value, prop_values);  // pk_value is not set here, it will be set later
}

template <typename GraphInterface>
bl::result<Context> InsertVertexOpr::eval_impl(
    GraphInterface& graph, const std::map<std::string, std::string>& params,
    Context&& ctx, OprTimer& timer) {
  for (auto& entry : vertex_data_) {
    label_t vertex_label_id = entry.first;
    auto& properties = entry.second;

    auto properties_name =
        graph.schema().get_vertex_property_names(vertex_label_id);
    auto properties_type =
        graph.schema().get_vertex_properties(vertex_label_id);
    if (properties_name.size() != properties_type.size()) {
      throw std::runtime_error("Vertex label " +
                               std::to_string(vertex_label_id) +
                               " has different number of properties: " +
                               std::to_string(properties_name.size()) + " vs " +
                               std::to_string(properties_type.size()));
    }
    const auto& pks = graph.schema().get_vertex_primary_key(vertex_label_id);
    if (pks.size() != 1) {
      throw std::runtime_error(
          "Vertex label " + std::to_string(vertex_label_id) +
          " must have exactly one primary key, but found: " +
          std::to_string(pks.size()));
    }
    const auto& pk = pks[0];
    if (properties.size() != properties_name.size() + 1) {
      throw std::runtime_error("Provided properties size " +
                               std::to_string(properties.size()) +
                               " does not match schema size: " +
                               std::to_string(properties_name.size() + 1));
    }
    Any pk_value;
    std::vector<Any> prop_values;
    std::tie(pk_value, prop_values) =
        get_pk_and_prop_values(properties, std::get<1>(pk), std::get<0>(pk),
                               properties_name, properties_type);
    VLOG(10) << "Inserting vertex with label " << (int32_t) vertex_label_id
             << " and primary key " << pk_value.to_string();
    if (!graph.AddVertex(vertex_label_id, pk_value, prop_values)) {
      LOG(ERROR) << "Failed to add vertex with label "
                 << (int32_t) vertex_label_id << " and primary key "
                 << pk_value.to_string();
      RETURN_FLEX_LEAF_ERROR(gs::StatusCode::ERR_INTERNAL_ERROR,
                             "Failed to add vertex with label " +
                                 std::to_string(vertex_label_id) +
                                 " and primary key " + pk_value.to_string());
    }
  }
  VLOG(10) << "Inserted " << vertex_data_.size() << " vertices successfully.";
  return bl::result<Context>(std::move(ctx));
}

bl::result<Context> InsertVertexOpr::Eval(
    GraphUpdateInterface& graph,
    const std::map<std::string, std::string>& params, Context&& ctx,
    OprTimer& timer) {
  return eval_impl(graph, params, std::move(ctx), timer);
}

std::unique_ptr<IUpdateOperator> InsertVertexOprBuilder::Build(
    const Schema& schema, const physical::PhysicalPlan& plan, int op_idx) {
  const auto& opr = plan.query_plan().plan(op_idx).opr().create_vertex();
  using vertex_prop_vec_t = typename InsertVertexOpr::vertex_prop_vec_t;
  std::vector<std::pair<label_t, vertex_prop_vec_t>> vertex_data;
  for (auto& entry : opr.entries()) {
    label_t vertex_label_id = 0;
    switch (entry.vertex_type().item_case()) {
    case common::NameOrId::kId: {
      vertex_label_id = entry.vertex_type().id();
      break;
    }
    case common::NameOrId::kName: {
      vertex_label_id = schema.get_vertex_label_id(entry.vertex_type().name());
      break;
    }
    default:
      LOG(FATAL) << "Unknown vertex type: "
                 << entry.vertex_type().DebugString();
    }
    vertex_prop_vec_t properties;
    for (const auto& prop : entry.property_mappings()) {
      if (!prop.property().has_key()) {
        throw std::runtime_error("Property must have a key");
      }

      if (prop.data().operators_size() != 1) {
        throw std::runtime_error(
            "Property value must have exactly one operator");
      }
      if (prop.data().operators(0).item_case() !=
          common::ExprOpr::ItemCase::kConst) {
        throw std::runtime_error("Property value must be a constant value");
      }
      properties.emplace_back(
          prop.property().key().name(),
          const_value_to_any(prop.data().operators(0).const_()));
    }
    vertex_data.emplace_back(vertex_label_id, properties);
  }
  return std::make_unique<InsertVertexOpr>(std::move(vertex_data));
}

}  // namespace ops
}  // namespace runtime
}  // namespace gs
