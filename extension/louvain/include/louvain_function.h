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

#include "neug/compiler/function/function.h"
#include "neug/compiler/function/neug_call_function.h"

namespace neug {
namespace function {

/// @brief Louvain community detection function (whole-graph).
///
/// Automatically iterates over all vertex labels and all edge labels
/// in the graph. No need to specify vertex/edge label names.
///
/// Usage in Cypher:
///   CALL louvain()
///   CALL louvain(1.0)            -- resolution
///   CALL louvain(1.0, true)      -- resolution + directed
///   CALL louvain(1.0, true, 1e-7) -- resolution + directed + threshold
///   CALL louvain(1.0, true, 1e-7, "name,age") -- + extra properties
///
/// Parameters (all optional):
///   1. resolution  (DOUBLE, default=1.0)  - resolution parameter (gamma)
///   2. directed    (BOOL,   default=false) - treat graph as directed
///   3. threshold   (DOUBLE, default=1e-7)  - modularity gain threshold
///   4. prop_names  (STRING, default="")    - comma-separated property names
///                                             to include in the output
///
/// Output columns (without prop_names):
///   - node_id    (INT64) - internal global id ((label_id << 56) | vertex_id)
///   - community  (INT64) - community id (0-based)
///
/// Output columns (with prop_names):
///   - node_id    (INT64) - internal global id ((label_id << 56) | vertex_id)
///   - community  (INT64) - community id (0-based)
///   - properties (STRING) - JSON map of requested property values
struct LouvainFunction {
  static constexpr const char* name = "LOUVAIN";

  static function_set getFunctionSet();
};

}  // namespace function
}  // namespace neug
