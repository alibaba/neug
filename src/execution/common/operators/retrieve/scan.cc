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

#include "neug/execution/common/operators/retrieve/scan.h"

#include "neug/execution/expression/special_predicates.h"
#include "neug/utils/result.h"

namespace neug {
namespace execution {

neug::result<ContextChunk> Scan::find_vertex_with_oid(
    ContextChunk&& chunk, const IStorageInterface& graph, label_t label,
    const Value& oid, int32_t alias) {
  MSVertexColumnBuilder builder(label);
  vid_t vid;
  if (graph.GetVertexIndex(label, oid, vid)) {
    builder.push_back_opt(vid);
  }
  chunk.set(alias, builder.finish());
  return chunk;
}

struct ScanVertexSPOp {
  template <typename PRED_T>
  static neug::result<ContextChunk> eval_with_predicate(
      const PRED_T& pred, const IStorageInterface& graph, ContextChunk&& chunk,
      const ScanParams& params) {
    return Scan::scan_vertex<PRED_T>(std::move(chunk), graph, params, pred);
  }
};

neug::result<ContextChunk> Scan::scan_vertex_with_special_vertex_predicate(
    ContextChunk&& chunk, const IStorageInterface& graph,
    const ScanParams& params, const SpecialPredicateConfig& config,
    const ParamsMap& query_params) {
  std::set<label_t> expected_labels;
  for (auto label : params.tables) {
    expected_labels.insert(label);
  }
  return dispatch_vertex_predicate<ScanVertexSPOp>(graph, expected_labels,
                                                   config, query_params, graph,
                                                   std::move(chunk), params);
}

}  // namespace execution

}  // namespace neug
