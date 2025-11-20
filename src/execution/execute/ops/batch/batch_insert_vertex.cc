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

#include "neug/execution/common/context.h"
#include "neug/execution/common/graph_interface.h"
#include "neug/execution/execute/ops/batch/batch_insert_vertex.h"
#include "neug/execution/execute/ops/batch/batch_update_utils.h"
#include "neug/storages/graph/property_graph.h"
#include "neug/storages/graph/schema.h"
#include "neug/transaction/update_transaction.h"
#include "neug/utils/exception/exception.h"
#include "neug/utils/pb_utils.h"
#include "neug/utils/property/property.h"

namespace gs {

namespace runtime {
class OprTimer;

namespace ops {

class BatchInsertVertexOpr : public IUpdateOperator {
 public:
  BatchInsertVertexOpr(
      const label_t& vertex_label_id, const PropertyType& pk_type,
      const std::vector<std::pair<int32_t, std::string>>& prop_mappings)
      : vertex_label_id_(vertex_label_id),
        pk_type_(pk_type),
        prop_mappings_(prop_mappings) {}

  std::string get_operator_name() const override {
    return "BatchInsertVertexOpr";
  }

  gs::result<Context> Eval(GraphUpdateInterface& graph,
                           const std::map<std::string, std::string>& params,
                           Context&& ctx, OprTimer* timer) override;

 private:
  label_t vertex_label_id_;
  PropertyType pk_type_;
  std::vector<std::pair<int32_t, std::string>> prop_mappings_;
};

class InsertVertexOpr : public IUpdateOperator {
 public:
  using vertex_prop_vec_t = std::vector<std::pair<std::string, Property>>;
  InsertVertexOpr(std::vector<std::tuple<label_t, vertex_prop_vec_t, int32_t>>&&
                      vertex_data)
      : vertex_data_(std::move(vertex_data)) {}

  std::string get_operator_name() const override { return "InsertVertexOpr"; }

  gs::result<Context> eval_impl(
      GraphUpdateInterface& graph,
      const std::map<std::string, std::string>& params, Context&& ctx,
      OprTimer* timer);

  gs::result<Context> Eval(GraphUpdateInterface& graph,
                           const std::map<std::string, std::string>& params,
                           Context&& ctx, OprTimer* timer) override;

 private:
  std::vector<std::tuple<label_t, vertex_prop_vec_t, int32_t>> vertex_data_;
};

gs::result<Context> BatchInsertVertexOpr::Eval(
    GraphUpdateInterface& graph,
    const std::map<std::string, std::string>& params, Context&& ctx,
    OprTimer* timer) {
  auto suppliers = create_record_batch_supplier(ctx, prop_mappings_);
  for (auto supplier : suppliers) {
    if (!graph.BatchAddVertices(vertex_label_id_, supplier).ok()) {
      THROW_INTERNAL_EXCEPTION("Failed to add vertices");
    }
  }
  return gs::result<Context>(std::move(ctx));
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

std::pair<Property, std::vector<Property>> get_pk_and_prop_values(
    const std::vector<std::pair<std::string, Property>>& properties,
    const std::string& pk_name, const PropertyType& pk_type,
    const std::vector<std::string>& properties_name,
    const std::vector<PropertyType>& properties_type) {
  if (properties.empty()) {
    THROW_RUNTIME_ERROR("No properties provided for vertex insertion");
  }
  if (properties_name.size() != properties_type.size()) {
    THROW_RUNTIME_ERROR("Properties name and type size mismatch");
  }

  Property pk_value;
  std::vector<Property> prop_values;
  // The order should match the schema
  bool pk_found = false;
  for (size_t i = 0; i < properties.size(); ++i) {
    const auto& prop = properties[i];
    const auto& prop_name = prop.first;
    const auto& prop_value = prop.second;
    if (!pk_found && prop_name == pk_name) {
      pk_found = true;
      if (prop_value.type() != pk_type) {
        THROW_RUNTIME_ERROR("Primary key type mismatch for property " +
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
          if (prop_value.type() != properties_type[idx]) {
            THROW_RUNTIME_ERROR("Property type mismatch for property " +
                                prop_name + ": expected " +
                                properties_type[idx].ToString() + ", got " +
                                prop_value.type().ToString());
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
    THROW_RUNTIME_ERROR("Primary key " + pk_name +
                        " not found in provided properties");
  }
  return std::make_pair(
      pk_value, prop_values);  // pk_value is not set here, it will be set later
}

gs::result<Context> InsertVertexOpr::eval_impl(
    GraphUpdateInterface& graph,
    const std::map<std::string, std::string>& params, Context&& ctx,
    OprTimer* timer) {
  for (auto& entry : vertex_data_) {
    label_t vertex_label_id = std::get<0>(entry);
    auto& properties = std::get<1>(entry);
    auto res_alias = std::get<2>(entry);

    auto properties_name =
        graph.schema().get_vertex_property_names(vertex_label_id);
    auto properties_type =
        graph.schema().get_vertex_properties(vertex_label_id);
    if (properties_name.size() != properties_type.size()) {
      THROW_RUNTIME_ERROR("Vertex label " + std::to_string(vertex_label_id) +
                          " has different number of properties: " +
                          std::to_string(properties_name.size()) + " vs " +
                          std::to_string(properties_type.size()));
    }
    const auto& pks = graph.schema().get_vertex_primary_key(vertex_label_id);
    if (pks.size() != 1) {
      THROW_RUNTIME_ERROR("Vertex label " + std::to_string(vertex_label_id) +
                          " must have exactly one primary key, but found: " +
                          std::to_string(pks.size()));
    }
    const auto& pk = pks[0];
    if (properties.size() != properties_name.size() + 1) {
      THROW_RUNTIME_ERROR("Provided properties size " +
                          std::to_string(properties.size()) +
                          " does not match schema size: " +
                          std::to_string(properties_name.size() + 1));
    }
    Property pk_value;
    std::vector<Property> prop_values;
    std::tie(pk_value, prop_values) =
        get_pk_and_prop_values(properties, std::get<1>(pk), std::get<0>(pk),
                               properties_name, properties_type);
    VLOG(10) << "Inserting vertex with label " << (int32_t) vertex_label_id
             << " and primary key " << pk_value.to_string();
    vid_t existing_vid;
    if (graph.GetVertexIndex(vertex_label_id, pk_value, existing_vid)) {
      LOG(ERROR) << "Vertex with label " << (int32_t) vertex_label_id
                 << " and primary key " << pk_value.to_string()
                 << " already exists.";
      RETURN_STATUS_ERROR(
          gs::StatusCode::ERR_INVALID_ARGUMENT,
          "Vertex with label " + std::to_string(vertex_label_id) +
              " and primary key " + pk_value.to_string() + " already exists.");
    }
    vid_t vid;
    if (!graph.AddVertex(vertex_label_id, pk_value, prop_values, vid)) {
      LOG(ERROR) << "Failed to add vertex with label "
                 << (int32_t) vertex_label_id << " and primary key "
                 << pk_value.to_string();
      RETURN_STATUS_ERROR(gs::StatusCode::ERR_INTERNAL_ERROR,
                          "Failed to add vertex with label " +
                              std::to_string(vertex_label_id) +
                              " and primary key " + pk_value.to_string());
    }
    MSVertexColumnBuilder builder(vertex_label_id);
    builder.push_back_opt(vid);
    auto col = builder.finish();
    ctx.set(res_alias, std::move(col));
  }
  VLOG(10) << "Inserted " << vertex_data_.size()
           << " vertices successfully, col num: " << ctx.col_num();
  return gs::result<Context>(std::move(ctx));
}

gs::result<Context> InsertVertexOpr::Eval(
    GraphUpdateInterface& graph,
    const std::map<std::string, std::string>& params, Context&& ctx,
    OprTimer* timer) {
  return eval_impl(graph, params, std::move(ctx), timer);
}

std::unique_ptr<IUpdateOperator> InsertVertexOprBuilder::Build(
    const Schema& schema, const physical::PhysicalPlan& plan, int op_idx) {
  const auto& opr = plan.query_plan().plan(op_idx).opr().create_vertex();
  using vertex_prop_vec_t = typename InsertVertexOpr::vertex_prop_vec_t;
  std::vector<std::tuple<label_t, vertex_prop_vec_t, int32_t>> vertex_data;
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
        THROW_RUNTIME_ERROR("Property must have a key");
      }

      if (prop.data().operators_size() != 1) {
        THROW_RUNTIME_ERROR("Property value must have exactly one operator");
      }
      properties.emplace_back(prop.property().key().name(),
                              expr_opr_value_to_prop(prop.data().operators(0)));
    }
    vertex_data.emplace_back(vertex_label_id, properties, entry.alias().id());
  }
  return std::make_unique<InsertVertexOpr>(std::move(vertex_data));
}

}  // namespace ops
}  // namespace runtime
}  // namespace gs
