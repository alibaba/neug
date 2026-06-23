/**
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "utils/subgraph_utils.h"

#include <cstdlib>
#include <stdexcept>
#include <string>

namespace neug {
namespace gds {

bool parse_subgraph_entries(const ::physical::Subgraph& subgraph,
                            const execution::ContextMeta& ctx_meta,
                            ParsedSubgraph& parsed) {
  parsed.vertex_entries.clear();
  parsed.edge_entries.clear();

  parsed.vertex_entries.reserve(subgraph.vertex_entries_size());
  for (const auto& vertex_entry : subgraph.vertex_entries()) {
    if (vertex_entry.label_id() < 0) {
      LOG(ERROR) << "Vertex label ID must be non-negative.";
      return false;
    }

    ParsedSubgraphEntry parsed_entry;
    parsed_entry.label = static_cast<label_t>(vertex_entry.label_id());
    if (vertex_entry.has_predicate()) {
      parsed_entry.predicate = execution::parse_expression(
          vertex_entry.predicate(), ctx_meta, execution::VarType::kVertex);
    }
    parsed.vertex_entries.emplace_back(std::move(parsed_entry));
  }

  parsed.edge_entries.reserve(subgraph.edge_entries_size());
  for (const auto& edge_entry : subgraph.edge_entries()) {
    if (edge_entry.src_label_id() < 0 || edge_entry.dst_label_id() < 0 ||
        edge_entry.edge_label_id() < 0) {
      LOG(ERROR) << "Edge label IDs must be non-negative.";
      return false;
    }

    ParsedSubgraphEdgeEntry parsed_edge;
    parsed_edge.triplet = execution::LabelTriplet(edge_entry.src_label_id(),
                                                  edge_entry.dst_label_id(),
                                                  edge_entry.edge_label_id());
    if (edge_entry.has_predicate()) {
      parsed_edge.predicate = execution::parse_expression(
          edge_entry.predicate(), ctx_meta, execution::VarType::kEdge);
    }
    parsed.edge_entries.emplace_back(std::move(parsed_edge));
  }

  return true;
}

bool check_simple_graph_subgraph(const ParsedSubgraph& parsed,
                                 const std::string& algo_name) {
  if (parsed.vertex_entries.size() != 1) {
    LOG(ERROR) << algo_name
               << " currently only supports subgraphs "
                  "with exactly one vertex label.";
    return false;
  }
  if (parsed.edge_entries.size() != 1) {
    LOG(ERROR) << algo_name
               << " currently only supports subgraphs "
                  "with exactly one edge label.";
    return false;
  }

  const auto& triplet = parsed.edge_entries[0].triplet;
  if (triplet.src_label != parsed.vertex_entries[0].label ||
      triplet.dst_label != parsed.vertex_entries[0].label) {
    LOG(ERROR) << "Source and destination vertex labels of the edge must "
                  "match the vertex label in "
               << algo_name << ".";
    return false;
  }
  return true;
}

bool try_parse_source_vertex(const StorageReadInterface& graph,
                             label_t vertex_label,
                             const std::string& source_str, vid_t& out) {
  auto pk_type =
      std::get<0>(graph.schema().get_vertex_primary_key(vertex_label)[0]);

  execution::Value oid;
  try {
    switch (pk_type.id()) {
    case DataTypeId::kInt32:
      oid = execution::Value::INT32(std::stoi(source_str));
      break;
    case DataTypeId::kInt64:
      oid = execution::Value::INT64(std::atoll(source_str.c_str()));
      break;
    case DataTypeId::kUInt32:
      oid = execution::Value::UINT32(std::stoul(source_str));
      break;
    case DataTypeId::kUInt64:
      oid = execution::Value::UINT64(std::stoull(source_str));
      break;
    case DataTypeId::kVarchar:
      oid = execution::Value::CreateValue(source_str);
      break;
    default:
      LOG(ERROR) << "Unsupported primary key type for source vertex lookup.";
      return false;
    }
  } catch (const std::exception& e) {
    LOG(ERROR) << "Invalid source vertex id '" << source_str
               << "': " << e.what();
    return false;
  }

  if (!graph.GetVertexIndex(vertex_label, oid, out)) {
    LOG(ERROR) << "Source vertex not found: " << source_str;
    return false;
  }
  return true;
}

}  // namespace gds
}  // namespace neug
