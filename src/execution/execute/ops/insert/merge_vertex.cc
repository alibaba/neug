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

#include "neug/execution/execute/ops/insert/merge_vertex.h"

#include <glog/logging.h>
#include <algorithm>
#include <unordered_map>
#include <utility>
#include <vector>

#include "neug/execution/common/columns/vertex_columns.h"
#include "neug/execution/common/context.h"
#include "neug/execution/common/types/value.h"
#include "neug/execution/expression/expr.h"
#include "neug/generated/proto/plan/cypher_dml.pb.h"
#include "neug/storages/graph/graph_interface.h"
#include "neug/utils/exception/exception.h"
#include "neug/utils/result.h"

namespace neug {
namespace execution {
namespace ops {

namespace {

std::vector<std::pair<std::string, std::unique_ptr<ExprBase>>> parse_mappings(
    const google::protobuf::RepeatedPtrField<::physical::PropertyMapping>&
        mappings,
    const ContextMeta& ctx_meta) {
  std::vector<std::pair<std::string, std::unique_ptr<ExprBase>>> props;
  for (const auto& prop : mappings) {
    if (!prop.has_property()) {
      LOG(FATAL) << "PropertyMapping has no property: " << prop.DebugString();
    }
    if (!prop.has_data()) {
      LOG(FATAL) << "PropertyMapping has no data: " << prop.DebugString();
    }
    props.emplace_back(
        prop.property().key().name(),
        parse_expression(prop.data(), ctx_meta, VarType::kRecord));
  }
  return props;
}

std::vector<std::pair<std::string, std::unique_ptr<BindedExprBase>>>
merge_pattern_and_on_create(
    std::vector<std::pair<std::string, std::unique_ptr<BindedExprBase>>>
        pattern,
    std::vector<std::pair<std::string, std::unique_ptr<BindedExprBase>>>
        on_create) {
  flat_hash_map<std::string, size_t> pos;
  std::vector<std::pair<std::string, std::unique_ptr<BindedExprBase>>> merged;
  merged.reserve(pattern.size() + on_create.size());
  for (auto& p : pattern) {
    pos[p.first] = merged.size();
    merged.push_back(std::move(p));
  }
  for (auto& o : on_create) {
    auto it = pos.find(o.first);
    if (it != pos.end()) {
      merged[it->second].second = std::move(o.second);
    } else {
      pos[o.first] = merged.size();
      merged.push_back(std::move(o));
    }
  }
  return merged;
}

void apply_on_match_vertex(
    StorageUpdateInterface& graph, DataChunk& chunk, size_t row, label_t label,
    vid_t vid,
    const std::vector<std::pair<std::string, std::unique_ptr<BindedExprBase>>>&
        on_match) {
  const auto& property_names = graph.schema().get_vertex_property_names(label);
  const auto& property_types = graph.schema().get_vertex_properties(label);
  const auto& pks = graph.schema().get_vertex_primary_key(label);
  for (const auto& [prop_name, expr] : on_match) {
    if (prop_name == std::get<1>(pks[0])) {
      THROW_RUNTIME_ERROR("Cannot ON MATCH set primary key on vertex");
    }
    Value prop = expr->Cast<RecordExprBase>().eval_record(chunk, row);
    auto pos =
        std::find(property_names.begin(), property_names.end(), prop_name);
    if (pos == property_names.end()) {
      THROW_RUNTIME_ERROR("Property " + prop_name +
                          " not found in vertex label " +
                          std::to_string(label));
    }
    int32_t col_id =
        static_cast<int32_t>(std::distance(property_names.begin(), pos));
    if (property_types[col_id] != prop.type()) {
      THROW_RUNTIME_ERROR("Property type mismatch for property " + prop_name);
    }
    graph.UpdateVertexProperty(label, vid, col_id, prop);
  }
}

neug::result<vid_t> insert_vertex_row(
    StorageInsertInterface& graph, DataChunk& chunk, size_t row, label_t label,
    const std::vector<std::pair<std::string, std::unique_ptr<BindedExprBase>>>&
        properties) {
  const auto& schema = graph.schema();
  auto properties_name = schema.get_vertex_property_names(label);
  auto properties_type = schema.get_vertex_properties(label);
  const auto& v_default_values =
      schema.get_vertex_default_property_values(label);
  const auto& pks = schema.get_vertex_primary_key(label);
  if (pks.size() != 1) {
    THROW_RUNTIME_ERROR("Vertex label " + std::to_string(label) +
                        " must have exactly one primary key");
  }
  const auto& pk = pks[0];
  if (properties.size() != properties_name.size() + 1) {
    THROW_RUNTIME_ERROR("Provided properties size " +
                        std::to_string(properties.size()) +
                        " does not match schema size: " +
                        std::to_string(properties_name.size() + 1));
  }

  Value pk_value;
  std::vector<execution::Value> property_values(properties.size() - 1);
  for (size_t j = 0; j < properties.size(); ++j) {
    const auto& [prop_name, prop_expr] = properties[j];
    Value value = prop_expr->Cast<RecordExprBase>().eval_record(chunk, row);
    if (prop_name == std::get<1>(pk)) {
      pk_value = value;
    } else {
      auto it =
          std::find(properties_name.begin(), properties_name.end(), prop_name);
      if (it == properties_name.end()) {
        THROW_RUNTIME_ERROR("Property " + prop_name +
                            " not found in vertex label " +
                            std::to_string(label));
      }
      size_t index = std::distance(properties_name.begin(), it);
      if (value.IsNull()) {
        property_values[index] = v_default_values[index];
      } else {
        if (properties_type[index] != value.type()) {
          THROW_RUNTIME_ERROR("Property type mismatch for property " +
                              properties_name[index]);
        }
        property_values[index] = value;
      }
    }
  }
  vid_t existing_vid;
  if (graph.GetVertexIndex(label, pk_value, existing_vid)) {
    LOG(ERROR) << "Vertex with label " << (int32_t) label << " and primary key "
               << pk_value.to_string() << " already exists.";
    RETURN_STATUS_ERROR(neug::StatusCode::ERR_INVALID_ARGUMENT,
                        "Vertex with label " + std::to_string(label) +
                            " and primary key " + pk_value.to_string() +
                            " already exists.");
  }
  vid_t vid;
  if (!graph.AddVertex(label, pk_value, property_values, vid)) {
    LOG(ERROR) << "Failed to add vertex with label " << (int32_t) label
               << " and primary key " << pk_value.to_string();
    RETURN_STATUS_ERROR(neug::StatusCode::ERR_INTERNAL_ERROR,
                        "Failed to add vertex with label " +
                            std::to_string(label) + " and primary key " +
                            pk_value.to_string());
  }
  return vid;
}

struct VertexEntryPlan {
  label_t label;
  int32_t alias_id;
  std::vector<std::pair<std::string, std::unique_ptr<ExprBase>>> pattern_props;
  std::vector<std::pair<std::string, std::unique_ptr<ExprBase>>>
      on_create_props;
  std::vector<std::pair<std::string, std::unique_ptr<ExprBase>>> on_match_props;
};

class MergeVertexOpr : public IOperator {
 public:
  explicit MergeVertexOpr(std::vector<VertexEntryPlan>&& entries)
      : entries_(std::move(entries)) {}

  std::string get_operator_name() const override { return "MergeVertexOpr"; }

  neug::result<Context> Eval(IStorageInterface& graph_interface,
                             const ParamsMap& params, Context&& ctx,
                             OprTimer* timer) override {
    (void) timer;
    auto& graph = dynamic_cast<StorageUpdateInterface&>(graph_interface);
    const StorageReadInterface* graph_read = nullptr;
    if (graph_interface.readable()) {
      graph_read = dynamic_cast<const StorageReadInterface*>(&graph_interface);
    }

    return ctx.apply_chunks(
        [&](ContextChunk&& chunk) -> neug::result<ContextChunk> {
          for (const auto& plan : entries_) {
            std::vector<std::pair<std::string, std::unique_ptr<BindedExprBase>>>
                pattern_binded;
            std::vector<std::pair<std::string, std::unique_ptr<BindedExprBase>>>
                on_create_binded;
            std::vector<std::pair<std::string, std::unique_ptr<BindedExprBase>>>
                on_match_binded;
            for (const auto& [n, e] : plan.pattern_props) {
              pattern_binded.emplace_back(n, e->bind(graph_read, params));
            }
            for (const auto& [n, e] : plan.on_create_props) {
              on_create_binded.emplace_back(n, e->bind(graph_read, params));
            }
            for (const auto& [n, e] : plan.on_match_props) {
              on_match_binded.emplace_back(n, e->bind(graph_read, params));
            }
            auto merged_binded = merge_pattern_and_on_create(
                std::move(pattern_binded), std::move(on_create_binded));

            MSVertexColumnBuilder builder(plan.label);

            // Standalone MERGE after OPTIONAL MATCH can yield row_num() == 0
            // when the inner scan finds no row. MERGE write semantics still
            // need exactly one logical row (CREATE/MATCH branch once).
            std::shared_ptr<IContextColumn> alias_col;
            if (chunk.exist(plan.alias_id)) {
              auto c = chunk.get(plan.alias_id);
              if (c != nullptr && c->size() > 0) {
                alias_col = std::move(c);
              }
            }
            size_t num_rows = chunk.row_num();
            if (num_rows == 0) {
              num_rows = 1;
            }

            for (size_t row = 0; row < num_rows; ++row) {
              bool matched = false;
              vid_t matched_vid = 0;
              if (alias_col) {
                auto vc = std::dynamic_pointer_cast<IVertexColumn>(alias_col);
                if (vc && row < vc->size() && vc->has_value(row)) {
                  auto vr = vc->get_vertex(row);
                  if (vr.label_ == plan.label) {
                    matched = true;
                    matched_vid = vr.vid_;
                  }
                }
              }
              if (matched) {
                apply_on_match_vertex(graph, chunk.chunk(), row, plan.label,
                                      matched_vid, on_match_binded);
                builder.push_back_opt(matched_vid);
              } else {
                vid_t vid;
                GS_ASSIGN(vid, insert_vertex_row(graph, chunk.chunk(), row,
                                                 plan.label, merged_binded));
                builder.push_back_opt(vid);
              }
            }
            if (chunk.exist(plan.alias_id)) {
              chunk.remove(plan.alias_id);
            }
            chunk.set(plan.alias_id, builder.finish());
          }
          return chunk;
        });
  }

 private:
  std::vector<VertexEntryPlan> entries_;
};

}  // namespace

neug::result<OpBuildResultT> MergeVertexOprBuilder::Build(
    const Schema& schema, const ContextMeta& ctx_meta,
    const physical::PhysicalPlan& plan, int op_idx) {
  ContextMeta ret_meta = ctx_meta;
  const auto& opr = plan.plan(op_idx).opr().merge_vertex();
  std::vector<VertexEntryPlan> entries;
  for (const auto& entry : opr.entries()) {
    VertexEntryPlan e;
    switch (entry.vertex_type().item_case()) {
    case common::NameOrId::kId:
      e.label = entry.vertex_type().id();
      break;
    case common::NameOrId::kName:
      e.label = schema.get_vertex_label_id(entry.vertex_type().name());
      break;
    default:
      LOG(FATAL) << "Unknown vertex type: "
                 << entry.vertex_type().DebugString();
    }
    e.alias_id = entry.alias().id();
    ret_meta.set(e.alias_id, DataType::VERTEX);
    e.pattern_props = parse_mappings(entry.property_mappings(), ctx_meta);
    e.on_create_props = parse_mappings(entry.on_create(), ctx_meta);
    e.on_match_props = parse_mappings(entry.on_match(), ctx_meta);
    entries.push_back(std::move(e));
  }
  return std::make_pair(std::make_unique<MergeVertexOpr>(std::move(entries)),
                        ret_meta);
}

}  // namespace ops
}  // namespace execution
}  // namespace neug
