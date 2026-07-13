/** Copyright 2020 Alibaba Group Holding Limited.
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

#include "neug/storages/graph/graph_stats.h"

#include <rapidjson/document.h>

#include "neug/storages/graph/schema.h"
#include "neug/utils/exception/exception.h"

namespace neug {

namespace {
uint64_t atLeastOne(uint64_t cardinality) {
  return cardinality == 0 ? 1 : cardinality;
}
}  // namespace

#ifdef NEUG_BUILD_TEST
void GraphStats::LoadFromJson(const Schema& schema,
                              const std::string& stats_json) {
  table_cardinalities_.clear();
  if (stats_json.empty()) {
    return;
  }

  rapidjson::Document document;
  document.Parse(stats_json.c_str());
  if (document.HasParseError() || !document.IsObject()) {
    THROW_RUNTIME_ERROR("Failed to parse statistics JSON");
  }

  if (document.HasMember("vertex_type_statistics") &&
      document["vertex_type_statistics"].IsArray()) {
    for (const auto& vertex_stat :
         document["vertex_type_statistics"].GetArray()) {
      if (!vertex_stat.IsObject() || !vertex_stat.HasMember("type_name") ||
          !vertex_stat["type_name"].IsString() ||
          !vertex_stat.HasMember("count") || !vertex_stat["count"].IsUint64()) {
        continue;
      }
      try {
        auto label_id =
            schema.get_vertex_label_id(vertex_stat["type_name"].GetString());
        auto vertex_schema = schema.get_vertex_schema(label_id);
        if (vertex_schema) {
          table_cardinalities_[vertex_schema->get_entry_id()] =
              vertex_stat["count"].GetUint64();
        }
      } catch (const std::exception&) { continue; }
    }
  }

  if (document.HasMember("edge_type_statistics") &&
      document["edge_type_statistics"].IsArray()) {
    for (const auto& edge_stat : document["edge_type_statistics"].GetArray()) {
      if (!edge_stat.IsObject() || !edge_stat.HasMember("type_name") ||
          !edge_stat["type_name"].IsString() ||
          !edge_stat.HasMember("vertex_type_pair_statistics") ||
          !edge_stat["vertex_type_pair_statistics"].IsArray()) {
        continue;
      }
      for (const auto& pair_stat :
           edge_stat["vertex_type_pair_statistics"].GetArray()) {
        if (!pair_stat.IsObject() || !pair_stat.HasMember("source_vertex") ||
            !pair_stat["source_vertex"].IsString() ||
            !pair_stat.HasMember("destination_vertex") ||
            !pair_stat["destination_vertex"].IsString() ||
            !pair_stat.HasMember("count") || !pair_stat["count"].IsUint64()) {
          continue;
        }
        try {
          auto src_label = schema.get_vertex_label_id(
              pair_stat["source_vertex"].GetString());
          auto dst_label = schema.get_vertex_label_id(
              pair_stat["destination_vertex"].GetString());
          auto edge_label =
              schema.get_edge_label_id(edge_stat["type_name"].GetString());
          if (!schema.is_edge_triplet_valid(src_label, dst_label, edge_label)) {
            continue;
          }
          auto edge_schema =
              schema.get_edge_schema(src_label, dst_label, edge_label);
          if (edge_schema) {
            table_cardinalities_[edge_schema->get_entry_id()] =
                pair_stat["count"].GetUint64();
          }
        } catch (const std::exception&) { continue; }
      }
    }
  }
}
#endif

uint64_t GraphStats::getTable(uint64_t tableID) const {
#ifdef NEUG_BUILD_TEST
  if (auto it = table_cardinalities_.find(tableID);
      it != table_cardinalities_.end()) {
    return atLeastOne(it->second);
  }
#endif
  if (graph_ == nullptr) {
    return 1;
  }
  if (graph_->schema().is_vertex_label_valid(tableID)) {
    return getTable(tableID, SchemaEntryType::NODE);
  }
  return getTable(tableID, SchemaEntryType::REL);
}

uint64_t GraphStats::getTable(uint64_t tableID,
                              SchemaEntryType tableType) const {
#ifdef NEUG_BUILD_TEST
  if (auto it = table_cardinalities_.find(tableID);
      it != table_cardinalities_.end()) {
    return atLeastOne(it->second);
  }
#endif
  if (graph_ == nullptr) {
    return 1;
  }
  if (tableType == SchemaEntryType::NODE) {
    if (!graph_->schema().is_vertex_label_valid(tableID)) {
      return 1;
    }
    return atLeastOne(graph_->VertexNum(tableID, MAX_TIMESTAMP));
  }

  for (auto edge_label : graph_->schema().get_edge_label_ids()) {
    for (auto src_label : graph_->schema().get_vertex_label_ids()) {
      for (auto dst_label : graph_->schema().get_vertex_label_ids()) {
        if (!graph_->schema().is_edge_triplet_valid(src_label, dst_label,
                                                    edge_label)) {
          continue;
        }
        auto edgeSchema =
            graph_->schema().get_edge_schema(src_label, dst_label, edge_label);
        if (edgeSchema && edgeSchema->get_entry_id() == tableID) {
          return atLeastOne(graph_->EdgeNum(src_label, edge_label, dst_label));
        }
      }
    }
  }
  return 1;
}

uint64_t GraphStats::getTable(SchemaEntry* tableEntry) const {
  if (tableEntry == nullptr) {
    return 1;
  }
#ifdef NEUG_BUILD_TEST
  if (auto it = table_cardinalities_.find(tableEntry->get_entry_id());
      it != table_cardinalities_.end()) {
    return atLeastOne(it->second);
  }
#endif
  if (graph_ == nullptr) {
    return 1;
  }
  if (tableEntry->get_entry_type() == SchemaEntryType::NODE) {
    return getTable(tableEntry->get_entry_id(), SchemaEntryType::NODE);
  }

  auto* edgeSchema = dynamic_cast<EdgeSchema*>(tableEntry);
  if (edgeSchema == nullptr) {
    THROW_EXCEPTION_WITH_FILE_LINE("Expected EdgeSchema for REL statistics");
  }
  if (!graph_->schema().is_edge_triplet_valid(edgeSchema->getSrcTableID(),
                                              edgeSchema->getDstTableID(),
                                              edgeSchema->getLabelId())) {
    return 1;
  }
  return atLeastOne(graph_->EdgeNum(edgeSchema->getSrcTableID(),
                                    edgeSchema->getLabelId(),
                                    edgeSchema->getDstTableID()));
}

}  // namespace neug
