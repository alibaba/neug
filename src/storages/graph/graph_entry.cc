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

#include "neug/storages/graph/graph_entry.h"

#include <algorithm>
#include <unordered_set>

#include <yaml-cpp/yaml.h>

#include "neug/utils/serialization/in_archive.h"
#include "neug/utils/serialization/out_archive.h"

namespace neug {

result<YAML::Node> ProjectedGraphEntry::ToYaml() const {
  YAML::Node node(YAML::NodeType::Map);
  auto vertices = vertexInfos;
  auto edges = edgeInfos;
  std::sort(vertices.begin(), vertices.end(),
            [](const auto& lhs, const auto& rhs) {
              return lhs.labelName < rhs.labelName;
            });
  std::sort(edges.begin(), edges.end(), [](const auto& lhs, const auto& rhs) {
    return std::tie(lhs.srcLabelName, lhs.edgeLabelName, lhs.dstLabelName) <
           std::tie(rhs.srcLabelName, rhs.edgeLabelName, rhs.dstLabelName);
  });
  for (const auto& info : vertices) {
    YAML::Node item;
    item["label"] = info.labelName;
    item["predicate"] = info.predicate;
    node["vertices"].push_back(item);
  }
  for (const auto& info : edges) {
    YAML::Node item;
    item["src"] = info.srcLabelName;
    item["label"] = info.edgeLabelName;
    item["dst"] = info.dstLabelName;
    item["predicate"] = info.predicate;
    node["edges"].push_back(item);
  }
  return node;
}

result<ProjectedGraphEntry> ProjectedGraphEntry::FromYaml(
    const YAML::Node& node) {
  if (!node.IsMap()) {
    RETURN_ERROR(Status(StatusCode::ERR_INVALID_SCHEMA,
                        "projected graph entry must be a map"));
  }
  ProjectedGraphEntry entry;
  try {
    if (node["vertices"] && !node["vertices"].IsSequence()) {
      RETURN_ERROR(Status(StatusCode::ERR_INVALID_SCHEMA,
                          "projected graph vertices must be a sequence"));
    }
    for (const auto& item : node["vertices"]) {
      entry.vertexInfos.push_back(
          {item["label"].as<std::string>(),
           item["predicate"] ? item["predicate"].as<std::string>() : ""});
    }
    if (node["edges"] && !node["edges"].IsSequence()) {
      RETURN_ERROR(Status(StatusCode::ERR_INVALID_SCHEMA,
                          "projected graph edges must be a sequence"));
    }
    for (const auto& item : node["edges"]) {
      entry.edgeInfos.push_back(
          {item["src"].as<std::string>(), item["label"].as<std::string>(),
           item["dst"].as<std::string>(),
           item["predicate"] ? item["predicate"].as<std::string>() : ""});
    }
  } catch (const YAML::Exception& e) {
    RETURN_ERROR(
        Status(StatusCode::ERR_INVALID_SCHEMA,
               std::string("invalid projected graph entry: ") + e.what()));
  }
  GraphEntrySet validator;
  auto status = validator.AddEntry("entry", entry);
  if (!status.ok()) {
    RETURN_ERROR(status);
  }
  return entry;
}

bool GraphEntrySet::HasEntry(const std::string& name) const {
  return name_to_entry_.contains(name);
}

result<const ProjectedGraphEntry*> GraphEntrySet::GetEntry(
    const std::string& name) const {
  auto it = name_to_entry_.find(name);
  if (it == name_to_entry_.end()) {
    RETURN_STATUS_ERROR(StatusCode::ERR_INVALID_ARGUMENT,
                        "Projected graph '" + name + "' does not exist.");
  }
  return &it->second;
}

Status GraphEntrySet::AddEntry(const std::string& name,
                               const ProjectedGraphEntry& entry) {
  if (name.empty() || name.find('.') != std::string::npos) {
    return Status(
        StatusCode::ERR_INVALID_ARGUMENT,
        "Projected graph name must be a non-empty identifier without '.'.");
  }
  std::unordered_set<std::string> vertices;
  for (const auto& vertex : entry.vertexInfos) {
    if (vertex.labelName.empty() || !vertices.insert(vertex.labelName).second) {
      return Status(StatusCode::ERR_INVALID_ARGUMENT,
                    "Projected graph '" + name +
                        "' contains an empty or duplicate vertex label.");
    }
  }
  std::unordered_set<std::string> edges;
  for (const auto& edge : entry.edgeInfos) {
    if (!vertices.contains(edge.srcLabelName) ||
        !vertices.contains(edge.dstLabelName)) {
      return Status(StatusCode::ERR_INVALID_ARGUMENT,
                    "Projected graph '" + name +
                        "' contains an edge whose endpoint is not projected.");
    }
    auto key = edge.srcLabelName + "\x1f" + edge.edgeLabelName + "\x1f" +
               edge.dstLabelName;
    if (edge.edgeLabelName.empty() || !edges.insert(std::move(key)).second) {
      return Status(StatusCode::ERR_INVALID_ARGUMENT,
                    "Projected graph '" + name +
                        "' contains an empty or duplicate edge triplet.");
    }
  }
  if (!name_to_entry_.emplace(name, entry).second) {
    return Status(StatusCode::ERR_INVALID_ARGUMENT,
                  "Projected graph '" + name + "' already exists.");
  }
  return Status::OK();
}

Status GraphEntrySet::DropEntry(const std::string& name) {
  if (name_to_entry_.erase(name) == 0) {
    return Status(StatusCode::ERR_INVALID_ARGUMENT,
                  "Projected graph '" + name + "' does not exist.");
  }
  return Status::OK();
}

result<YAML::Node> GraphEntrySet::ToYaml() const {
  YAML::Node graphs(YAML::NodeType::Map);
  std::vector<std::string> names;
  names.reserve(name_to_entry_.size());
  for (const auto& [name, _] : name_to_entry_) {
    names.push_back(name);
  }
  std::sort(names.begin(), names.end());
  for (const auto& name : names) {
    auto entry = name_to_entry_.at(name);
    std::sort(entry.vertexInfos.begin(), entry.vertexInfos.end(),
              [](const auto& lhs, const auto& rhs) {
                return lhs.labelName < rhs.labelName;
              });
    std::sort(entry.edgeInfos.begin(), entry.edgeInfos.end(),
              [](const auto& lhs, const auto& rhs) {
                return std::tie(lhs.srcLabelName, lhs.edgeLabelName,
                                lhs.dstLabelName) < std::tie(rhs.srcLabelName,
                                                             rhs.edgeLabelName,
                                                             rhs.dstLabelName);
              });
    YAML::Node vertices(YAML::NodeType::Sequence);
    for (const auto& info : entry.vertexInfos) {
      YAML::Node item;
      item["label"] = info.labelName;
      item["predicate"] = info.predicate;
      vertices.push_back(item);
    }
    YAML::Node edges(YAML::NodeType::Sequence);
    for (const auto& info : entry.edgeInfos) {
      YAML::Node item;
      item["src"] = info.srcLabelName;
      item["label"] = info.edgeLabelName;
      item["dst"] = info.dstLabelName;
      item["predicate"] = info.predicate;
      edges.push_back(item);
    }
    graphs[name]["vertices"] = vertices;
    graphs[name]["edges"] = edges;
  }
  return graphs;
}

result<GraphEntrySet> GraphEntrySet::FromYaml(const YAML::Node& node) {
  GraphEntrySet result;
  if (!node || node.IsNull()) {
    return result;
  }
  if (!node.IsMap()) {
    RETURN_ERROR(Status(StatusCode::ERR_INVALID_SCHEMA,
                        "projected_graphs must be a map"));
  }
  try {
    for (const auto& graph : node) {
      const auto name = graph.first.as<std::string>();
      if (name.empty() || name.find('.') != std::string::npos) {
        RETURN_ERROR(Status(StatusCode::ERR_INVALID_SCHEMA,
                            "invalid projected graph name"));
      }
      const auto value = graph.second;
      if (!value.IsMap()) {
        RETURN_ERROR(Status(StatusCode::ERR_INVALID_SCHEMA,
                            "projected graph entry must be a map"));
      }
      ProjectedGraphEntry entry;
      std::unordered_set<std::string> vertices;
      if (value["vertices"]) {
        if (!value["vertices"].IsSequence()) {
          RETURN_ERROR(Status(StatusCode::ERR_INVALID_SCHEMA,
                              "projected graph vertices must be a sequence"));
        }
        for (const auto& item : value["vertices"]) {
          if (!item.IsMap() || !item["label"] || !item["label"].IsScalar() ||
              (item["predicate"] && !item["predicate"].IsScalar())) {
            RETURN_ERROR(Status(StatusCode::ERR_INVALID_SCHEMA,
                                "invalid projected vertex entry"));
          }
          auto label = item["label"].as<std::string>();
          if (label.empty() || !vertices.insert(label).second) {
            RETURN_ERROR(Status(StatusCode::ERR_INVALID_SCHEMA,
                                "duplicate projected vertex: " + label));
          }
          entry.vertexInfos.push_back(
              {std::move(label),
               item["predicate"] ? item["predicate"].as<std::string>() : ""});
        }
      }
      std::unordered_set<std::string> edges;
      if (value["edges"]) {
        if (!value["edges"].IsSequence()) {
          RETURN_ERROR(Status(StatusCode::ERR_INVALID_SCHEMA,
                              "projected graph edges must be a sequence"));
        }
        for (const auto& item : value["edges"]) {
          if (!item.IsMap() || !item["src"] || !item["src"].IsScalar() ||
              !item["label"] || !item["label"].IsScalar() || !item["dst"] ||
              !item["dst"].IsScalar() ||
              (item["predicate"] && !item["predicate"].IsScalar())) {
            RETURN_ERROR(Status(StatusCode::ERR_INVALID_SCHEMA,
                                "invalid projected edge entry"));
          }
          auto src = item["src"].as<std::string>();
          auto label = item["label"].as<std::string>();
          auto dst = item["dst"].as<std::string>();
          if (src.empty() || label.empty() || dst.empty() ||
              !vertices.contains(src) || !vertices.contains(dst)) {
            RETURN_ERROR(
                Status(StatusCode::ERR_INVALID_SCHEMA,
                       "projected edge has an empty or unprojected endpoint"));
          }
          auto key = src + "\x1f" + label + "\x1f" + dst;
          if (!edges.insert(key).second) {
            RETURN_ERROR(Status(StatusCode::ERR_INVALID_SCHEMA,
                                "duplicate projected edge triplet"));
          }
          entry.edgeInfos.push_back(
              {std::move(src), std::move(label), std::move(dst),
               item["predicate"] ? item["predicate"].as<std::string>() : ""});
        }
      }
      if (!result.name_to_entry_.emplace(name, std::move(entry)).second) {
        RETURN_ERROR(Status(StatusCode::ERR_INVALID_SCHEMA,
                            "duplicate projected graph: " + name));
      }
    }
  } catch (const YAML::Exception& e) {
    RETURN_ERROR(Status(StatusCode::ERR_INVALID_SCHEMA,
                        std::string("invalid projected_graphs: ") + e.what()));
  }
  return result;
}

InArchive& operator<<(InArchive& archive, const VertexEntryInfo& info) {
  return archive << info.labelName << info.predicate;
}
OutArchive& operator>>(OutArchive& archive, VertexEntryInfo& info) {
  return archive >> info.labelName >> info.predicate;
}
InArchive& operator<<(InArchive& archive, const EdgeEntryInfo& info) {
  return archive << info.srcLabelName << info.edgeLabelName << info.dstLabelName
                 << info.predicate;
}
OutArchive& operator>>(OutArchive& archive, EdgeEntryInfo& info) {
  return archive >> info.srcLabelName >> info.edgeLabelName >>
         info.dstLabelName >> info.predicate;
}
InArchive& operator<<(InArchive& archive, const ProjectedGraphEntry& entry) {
  return archive << entry.vertexInfos << entry.edgeInfos;
}
OutArchive& operator>>(OutArchive& archive, ProjectedGraphEntry& entry) {
  return archive >> entry.vertexInfos >> entry.edgeInfos;
}

}  // namespace neug
