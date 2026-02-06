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

#include "neug/execution/expression/predicates.h"

#include "neug/utils/result.h"

namespace neug {
class StorageReadInterface;

namespace execution {

struct EdgeExpandParams;

class Intersect {
 public:
  static neug::result<neug::execution::Context> Binary_Intersect(
      const StorageReadInterface& graph, const ParamsMap& params,
      neug::execution::Context&& ctx, EdgeAndNbrPredicate&& left_pred,
      EdgeAndNbrPredicate&& right_pred, const EdgeExpandParams& eep0,
      const EdgeExpandParams& eep1, int alias);

  static neug::result<neug::execution::Context> Binary_Intersect_With_Edge(
      const StorageReadInterface& graph, const ParamsMap& params,
      neug::execution::Context&& ctx, EdgeAndNbrPredicate&& left_pred,
      EdgeAndNbrPredicate&& right_pred, const EdgeExpandParams& eep0,
      const EdgeExpandParams& eep1, int vertex_alias,
      const std::vector<int>& edge_alias);

  static neug::result<neug::execution::Context> Multiple_Intersect(
      const StorageReadInterface& graph, const ParamsMap& params,
      neug::execution::Context&& ctx, std::vector<EdgeAndNbrPredicate>&& preds,
      const std::vector<EdgeExpandParams>& eeps, int vertex_alias);
};

}  // namespace execution

}  // namespace neug
