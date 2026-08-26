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

#include <cstddef>
#include <cstdint>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "neug/utils/property/types.h"

namespace neug {

class Schema;

struct VertexTableDetachState {
  bool indexer_detached{false};
  bool vertex_timestamp_detached{false};
  std::vector<bool> columns_detached;
};

struct EdgeTableDetachState {
  bool out_csr_detached{false};
  bool in_csr_detached{false};
  std::vector<bool> columns_detached;
  // Per-vertex adjlist detachment tracking (sparse, lazily populated).
  // Ensures each adjlist is only detached once per transaction.
  std::unordered_set<vid_t> out_adjlists_detached;
  std::unordered_set<vid_t> in_adjlists_detached;
};

/// Records storage granules explicitly detached by mutation operations.
/// The state only deduplicates physical detach work within one workspace; it
/// does not determine whether the transaction has logical changes to commit.
struct CowDetachState {
  std::vector<VertexTableDetachState> vertex_tables;
  std::unordered_map<uint32_t, EdgeTableDetachState> edge_tables;
  // record index detach state according to unique index name
  std::unordered_map<std::string, bool> index_detached;

  static CowDetachState FromSchema(const Schema& schema);
};

}  // namespace neug
