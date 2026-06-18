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
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "neug/utils/property/types.h"

namespace neug {

class Schema;

struct VertexTableCowState {
  bool indexer_detached{false};
  bool vertex_timestamp_detached{false};
  std::vector<bool> columns_detached;
};

struct EdgeTableCowState {
  bool out_csr_detached{false};
  bool in_csr_detached{false};
  std::vector<bool> columns_detached;
  // Per-vertex adjlist detachment tracking (sparse, lazily populated).
  // Ensures each adjlist is only detached once per transaction.
  std::unordered_set<vid_t> out_adjlists_detached;
  std::unordered_set<vid_t> in_adjlists_detached;
};

/// A structure isomorphic to PropertyGraph's module organization.
/// Each bool tracks whether the corresponding module has been detached
/// in the current transaction's COW copy.
struct PropertyGraphCowState {
  std::vector<VertexTableCowState> vertex_tables;
  std::unordered_map<uint32_t, EdgeTableCowState> edge_tables;

  static PropertyGraphCowState FromSchema(const Schema& schema);
};

}  // namespace neug
