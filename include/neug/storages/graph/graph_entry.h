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
#pragma once

#include <string>
#include <unordered_map>
#include <vector>

#include "neug/utils/result.h"

namespace YAML {
class Node;
}

namespace neug {

class InArchive;
class OutArchive;

struct VertexEntryInfo {
  std::string labelName;
  std::string predicate;
  bool operator==(const VertexEntryInfo&) const = default;
};

struct EdgeEntryInfo {
  std::string srcLabelName;
  std::string edgeLabelName;
  std::string dstLabelName;
  std::string predicate;
  bool operator==(const EdgeEntryInfo&) const = default;
};

struct ProjectedGraphEntry {
  std::vector<VertexEntryInfo> vertexInfos;
  std::vector<EdgeEntryInfo> edgeInfos;
  bool operator==(const ProjectedGraphEntry&) const = default;

  result<YAML::Node> ToYaml() const;
  static result<ProjectedGraphEntry> FromYaml(const YAML::Node& node);
};

class GraphEntrySet {
 public:
  bool HasEntry(const std::string& name) const;
  result<const ProjectedGraphEntry*> GetEntry(const std::string& name) const;
  Status AddEntry(const std::string& name, const ProjectedGraphEntry& entry);
  Status DropEntry(const std::string& name);

  const std::unordered_map<std::string, ProjectedGraphEntry>& Entries() const {
    return name_to_entry_;
  }
  void Clear() { name_to_entry_.clear(); }

  result<YAML::Node> ToYaml() const;
  static result<GraphEntrySet> FromYaml(const YAML::Node& node);

  bool operator==(const GraphEntrySet&) const = default;

 private:
  std::unordered_map<std::string, ProjectedGraphEntry> name_to_entry_;
};

InArchive& operator<<(InArchive& archive, const VertexEntryInfo& info);
OutArchive& operator>>(OutArchive& archive, VertexEntryInfo& info);
InArchive& operator<<(InArchive& archive, const EdgeEntryInfo& info);
OutArchive& operator>>(OutArchive& archive, EdgeEntryInfo& info);
InArchive& operator<<(InArchive& archive, const ProjectedGraphEntry& entry);
OutArchive& operator>>(OutArchive& archive, ProjectedGraphEntry& entry);

}  // namespace neug
