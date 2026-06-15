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

#include "leiden_function.h"

#include <sstream>
#include <string>
#include <unordered_map>
#include <vector>

#include "glog/logging.h"
#include "leiden_algorithm.h"
#include "neug/common/types.h"
#include "neug/execution/common/columns/value_columns.h"
#include "neug/execution/common/context.h"
#include "neug/generated/proto/plan/physical.pb.h"
#include "neug/generated/proto/plan/stored_procedure.pb.h"
#include "neug/storages/graph/graph_interface.h"
#include "neug/storages/graph/schema.h"
#include "neug/utils/exception/exception.h"
#include "neug/utils/property/property.h"

namespace neug {
namespace function {

// ---------------------------------------------------------------------------
// Bind data
// ---------------------------------------------------------------------------
struct LeidenBindData : public CallFuncInputBase {
  double resolution = 1.0;
  bool directed = false;
  double threshold = 1e-7;
  std::vector<std::string> extra_props;
  std::vector<int32_t> output_aliases;
};

// ---------------------------------------------------------------------------
// Helper: extract parameters from protobuf Argument.
// ---------------------------------------------------------------------------
static double get_double_param(const ::procedure::Argument& arg,
                               double default_val = 1.0) {
  if (arg.has_const_()) {
    const auto& val = arg.const_();
    if (val.has_f64()) return val.f64();
    if (val.has_i64()) return static_cast<double>(val.i64());
    if (val.has_i32()) return static_cast<double>(val.i32());
  }
  return default_val;
}

static bool get_bool_param(const ::procedure::Argument& arg,
                           bool default_val = false) {
  if (arg.has_const_()) {
    const auto& val = arg.const_();
    if (val.has_boolean()) return val.boolean();
    if (val.has_i64()) return val.i64() != 0;
    if (val.has_i32()) return val.i32() != 0;
  }
  return default_val;
}

static std::string get_string_param(const ::procedure::Argument& arg,
                                    const std::string& default_val = "") {
  if (arg.has_const_()) {
    const auto& val = arg.const_();
    if (val.has_str()) return val.str();
  }
  return default_val;
}

static std::vector<std::string> parse_prop_names(const std::string& s) {
  std::vector<std::string> result;
  if (s.empty()) return result;
  std::istringstream iss(s);
  std::string token;
  while (std::getline(iss, token, ',')) {
    size_t start = token.find_first_not_of(" \t");
    size_t end = token.find_last_not_of(" \t");
    if (start != std::string::npos) {
      result.push_back(token.substr(start, end - start + 1));
    }
  }
  return result;
}

// ---------------------------------------------------------------------------
// Bind function
// ---------------------------------------------------------------------------
static std::unique_ptr<CallFuncInputBase> leiden_bind(
    const Schema& schema, const execution::ContextMeta& ctx_meta,
    const ::physical::PhysicalPlan& plan, int op_idx) {
  auto data = std::make_unique<LeidenBindData>();

  const auto& proc = plan.plan(op_idx).opr().procedure_call();
  const auto& args = proc.query().arguments();

  if (args.size() > 0) {
    data->resolution = get_double_param(args[0], 1.0);
  }
  if (args.size() > 1) {
    data->directed = get_bool_param(args[1], false);
  }
  if (args.size() > 2) {
    data->threshold = get_double_param(args[2], 1e-7);
  }
  if (args.size() > 3) {
    std::string prop_str = get_string_param(args[3], "");
    data->extra_props = parse_prop_names(prop_str);
  }

  const auto& meta = plan.plan(op_idx).meta_data();
  for (int i = 0; i < meta.size(); ++i) {
    data->output_aliases.push_back(meta[i].alias());
  }

  LOG(INFO) << "[leiden] bind: resolution=" << data->resolution
            << " directed=" << data->directed
            << " threshold=" << data->threshold
            << " extra_props=" << data->extra_props.size();

  return data;
}

// ---------------------------------------------------------------------------
// Helper: composite key for (label, vid) -> global node id mapping.
// ---------------------------------------------------------------------------
struct VertexKey {
  label_t label;
  vid_t vid;
  bool operator==(const VertexKey& o) const {
    return label == o.label && vid == o.vid;
  }
};

struct VertexKeyHash {
  size_t operator()(const VertexKey& k) const {
    return std::hash<uint64_t>()(
        (static_cast<uint64_t>(k.label) << 32) | k.vid);
  }
};

// ---------------------------------------------------------------------------
// Exec function
// ---------------------------------------------------------------------------
static execution::Context leiden_exec(const CallFuncInputBase& input_base,
                                      IStorageInterface& graph) {
  const auto& input = dynamic_cast<const LeidenBindData&>(input_base);
  const auto& schema = graph.schema();

  auto* reader = dynamic_cast<StorageReadInterface*>(&graph);
  if (!reader) {
    THROW_RUNTIME_ERROR("Leiden: graph is not readable");
  }

  // Step 1: Collect all vertices and build global id mapping.
  std::unordered_map<VertexKey, int64_t, VertexKeyHash> key_to_global;
  struct GlobalVertex {
    label_t label;
    vid_t vid;
  };
  std::vector<GlobalVertex> global_to_key;

  label_t v_label_num = schema.vertex_label_frontier();
  for (label_t vl = 0; vl < v_label_num; ++vl) {
    if (!schema.is_vertex_label_valid(vl)) continue;
    auto vset = reader->GetVertexSet(vl);
    for (auto it = vset.begin(); it != vset.end(); ++it) {
      vid_t vid = *it;
      int64_t gid = static_cast<int64_t>(global_to_key.size());
      key_to_global[{vl, vid}] = gid;
      global_to_key.push_back({vl, vid});
    }
  }

  int64_t total_vertices = static_cast<int64_t>(global_to_key.size());
  LOG(INFO) << "[leiden] total vertices across all labels: "
            << total_vertices;

  // Step 2: Build internal Leiden graph.
  leiden::LeidenGraph leiden_graph;

  for (int64_t i = 0; i < total_vertices; ++i) {
    leiden_graph.AddVertex(i);
  }

  label_t e_label_num = schema.edge_label_frontier();
  int64_t total_edges = 0;

  for (label_t src_vl = 0; src_vl < v_label_num; ++src_vl) {
    if (!schema.is_vertex_label_valid(src_vl)) continue;
    for (label_t dst_vl = 0; dst_vl < v_label_num; ++dst_vl) {
      if (!schema.is_vertex_label_valid(dst_vl)) continue;
      for (label_t el = 0; el < e_label_num; ++el) {
        if (!schema.is_edge_label_valid(el)) continue;
        if (!schema.is_edge_triplet_valid(src_vl, dst_vl, el)) continue;

        auto out_view =
            reader->GetGenericOutgoingGraphView(src_vl, dst_vl, el);
        auto src_vset = reader->GetVertexSet(src_vl);

        for (auto vit = src_vset.begin(); vit != src_vset.end(); ++vit) {
          vid_t src_vid = *vit;
          auto edges = out_view.get_edges(src_vid);
          for (auto eit = edges.begin(); eit != edges.end(); ++eit) {
            vid_t dst_vid = eit.get_vertex();

            auto src_it = key_to_global.find({src_vl, src_vid});
            auto dst_it = key_to_global.find({dst_vl, dst_vid});
            if (src_it == key_to_global.end() ||
                dst_it == key_to_global.end()) {
              continue;
            }

            leiden_graph.AddEdge(src_it->second, dst_it->second, 1.0,
                                 input.directed);
            total_edges++;
          }
        }
      }
    }
  }

  LOG(INFO) << "[leiden] total edges across all triplets: " << total_edges;

  // Step 3: Run Leiden algorithm.
  auto result = leiden::RunLeiden(leiden_graph, input.directed,
                                  input.resolution, input.threshold);

  // Step 4: Build output columns.
  execution::Context ctx;
  execution::ValueColumnBuilder<int64_t> node_id_builder;
  execution::ValueColumnBuilder<int64_t> community_builder;

  const bool has_extra_props = !input.extra_props.empty();

  std::unordered_map<label_t,
      std::vector<std::shared_ptr<RefColumnBase>>> prop_columns;
  if (has_extra_props) {
    for (label_t vl = 0; vl < v_label_num; ++vl) {
      if (!schema.is_vertex_label_valid(vl)) continue;
      auto& cols = prop_columns[vl];
      for (const auto& prop_name : input.extra_props) {
        if (schema.vertex_has_property(vl, prop_name)) {
          cols.push_back(reader->GetVertexPropColumn(vl, prop_name));
        } else {
          cols.push_back(nullptr);
        }
      }
    }
  }

  std::unique_ptr<execution::ValueColumnBuilder<std::string>> props_builder;
  if (has_extra_props) {
    props_builder = std::make_unique<execution::ValueColumnBuilder<std::string>>();
    props_builder->reserve(total_vertices);
  }

  node_id_builder.reserve(total_vertices);
  community_builder.reserve(total_vertices);

  for (int64_t gid = 0; gid < total_vertices; ++gid) {
    const auto& gv = global_to_key[gid];
    auto com_it = result.communities.find(gid);
    int64_t com_id = (com_it != result.communities.end()) ? com_it->second : -1;

    int64_t node_id =
        (static_cast<int64_t>(gv.label) << 56) | static_cast<int64_t>(gv.vid);
    node_id_builder.push_back_opt(node_id);
    community_builder.push_back_opt(com_id);

    if (has_extra_props) {
      std::string json = "{";
      bool first = true;
      auto col_it = prop_columns.find(gv.label);
      if (col_it != prop_columns.end()) {
        for (size_t pi = 0; pi < input.extra_props.size(); ++pi) {
          auto& col = col_it->second[pi];
          if (!col) continue;
          Property prop = col->get(static_cast<size_t>(gv.vid));
          if (prop.type() == DataTypeId::kEmpty) continue;

          if (!first) json += ",";
          first = false;

          json += "\"";
          json += input.extra_props[pi];
          json += "\":";

          if (prop.type() == DataTypeId::kVarchar) {
            json += "\"";
            std::string sv = prop.to_string();
            for (char c : sv) {
              switch (c) {
              case '"':  json += "\\\""; break;
              case '\\': json += "\\\\"; break;
              case '\n': json += "\\n";  break;
              case '\t': json += "\\t";  break;
              default:   json += c;      break;
              }
            }
            json += "\"";
          } else {
            json += prop.to_string();
          }
        }
      }
      json += "}";
      props_builder->push_back_opt(std::move(json));
    }
  }

  // Set output columns using DataChunk
  execution::DataChunk chunk;
  size_t num_outputs = has_extra_props ? 3 : 2;
  if (input.output_aliases.size() >= num_outputs) {
    chunk.set(input.output_aliases[0], node_id_builder.finish());
    chunk.set(input.output_aliases[1], community_builder.finish());
    if (has_extra_props) {
      chunk.set(input.output_aliases[2], props_builder->finish());
    }
  } else {
    chunk.set(0, node_id_builder.finish());
    chunk.set(1, community_builder.finish());
    if (has_extra_props) {
      chunk.set(2, props_builder->finish());
    }
  }
  ctx.append_chunk(std::move(chunk));
  ctx.tag_ids = {0, 1};

  LOG(INFO) << "[leiden] found " << result.num_communities
            << " communities, modularity=" << result.modularity;

  return ctx;
}

// ---------------------------------------------------------------------------
// Function registration
// ---------------------------------------------------------------------------
function_set LeidenFunction::getFunctionSet() {
  call_output_columns outputCols = {
      {"node_id", common::DataTypeId::kInt64},
      {"community", common::DataTypeId::kInt64},
  };

  function_set set;

  // Overload 0: CALL leiden()
  {
    auto func = std::make_unique<NeugCallFunction>(
        name, std::vector<common::DataTypeId>{}, outputCols);
    func->bindFunc = leiden_bind;
    func->execFunc = leiden_exec;
    set.push_back(std::move(func));
  }
  // Overload 1: CALL leiden(resolution)
  {
    auto func = std::make_unique<NeugCallFunction>(
        name,
        std::vector<common::DataTypeId>{common::DataTypeId::kDouble},
        outputCols);
    func->bindFunc = leiden_bind;
    func->execFunc = leiden_exec;
    set.push_back(std::move(func));
  }
  // Overload 2: CALL leiden(resolution, directed)
  {
    auto func = std::make_unique<NeugCallFunction>(
        name,
        std::vector<common::DataTypeId>{common::DataTypeId::kDouble,
                                        common::DataTypeId::kBoolean},
        outputCols);
    func->bindFunc = leiden_bind;
    func->execFunc = leiden_exec;
    set.push_back(std::move(func));
  }
  // Overload 3: CALL leiden(resolution, directed, threshold)
  {
    auto func = std::make_unique<NeugCallFunction>(
        name,
        std::vector<common::DataTypeId>{common::DataTypeId::kDouble,
                                        common::DataTypeId::kBoolean,
                                        common::DataTypeId::kDouble},
        outputCols);
    func->bindFunc = leiden_bind;
    func->execFunc = leiden_exec;
    set.push_back(std::move(func));
  }
  // Overload 4: CALL leiden(resolution, directed, threshold, prop_names)
  {
    call_output_columns outputColsWithProps = {
        {"node_id", common::DataTypeId::kInt64},
        {"community", common::DataTypeId::kInt64},
        {"properties", common::DataTypeId::kVarchar},
    };
    auto func = std::make_unique<NeugCallFunction>(
        name,
        std::vector<common::DataTypeId>{common::DataTypeId::kDouble,
                                        common::DataTypeId::kBoolean,
                                        common::DataTypeId::kDouble,
                                        common::DataTypeId::kVarchar},
        outputColsWithProps);
    func->bindFunc = leiden_bind;
    func->execFunc = leiden_exec;
    set.push_back(std::move(func));
  }

  return set;
}

}  // namespace function
}  // namespace neug
