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

#pragma once

#include <memory>
#include <string>
#include <vector>

#include "neug/execution/expression/expr.h"
#include "neug/storages/graph/graph_interface.h"

namespace neug {
namespace gds {

struct ParsedSubgraphEntry {
  label_t label = 0;
  std::unique_ptr<execution::ExprBase> predicate;
};

struct ParsedSubgraphEdgeEntry {
  execution::LabelTriplet triplet;
  std::unique_ptr<execution::ExprBase> predicate;
};

struct ParsedSubgraph {
  std::vector<ParsedSubgraphEntry> vertex_entries;
  std::vector<ParsedSubgraphEdgeEntry> edge_entries;
};

bool parse_subgraph_entries(const ::physical::Subgraph& subgraph,
                            const execution::ContextMeta& ctx_meta,
                            ParsedSubgraph& parsed);

bool check_simple_graph_subgraph(const ParsedSubgraph& parsed,
                                 const std::string& algo_name);

// Resolve a source vertex given its primary-key value (as a string) to its
// internal vertex id. Throws if the primary-key type is unsupported or the
// vertex does not exist.
vid_t parse_source_vertex(const StorageReadInterface& graph,
                          label_t vertex_label, const std::string& source_str);

}  // namespace gds
}  // namespace neug
