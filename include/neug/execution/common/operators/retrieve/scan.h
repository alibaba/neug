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

#include "neug/execution/common/columns/vertex_columns.h"
#include "neug/execution/common/context_chunk.h"
#include "neug/execution/common/params_map.h"
#include "neug/execution/common/types/value.h"
#include "neug/execution/expression/special_predicates.h"
#include "neug/execution/utils/params.h"
#include "neug/storages/graph/graph_interface.h"
#include "neug/utils/result.h"

namespace neug {

namespace execution {

class Scan {
 public:
  template <typename PRED_T>
  static neug::result<ContextChunk> scan_vertex(ContextChunk&& chunk,
                                                const IStorageInterface& gi,
                                                const ScanParams& params,
                                                const PRED_T& predicate) {
    const auto& graph = dynamic_cast<const StorageReadInterface&>(gi);
    MSVertexColumnBuilder builder(params.tables[0]);
    for (auto label : params.tables) {
      auto vertices = graph.GetVertexSet(label);
      builder.start_label(label);
      for (auto vid : vertices) {
        if (predicate(label, vid)) {
          builder.push_back_opt(vid);
        }
      }
    }
    chunk.set(params.alias, builder.finish());
    return chunk;
  }

  static neug::result<ContextChunk> scan_vertex_with_special_vertex_predicate(
      ContextChunk&& chunk, const IStorageInterface& graph,
      const ScanParams& params, const SpecialPredicateConfig& config,
      const ParamsMap& query_params);

  template <typename PRED_T>
  static neug::result<ContextChunk> filter_oids(
      ContextChunk&& chunk, const IStorageInterface& graph,
      const ScanParams& params, const PRED_T& predicate,
      const std::vector<Value>& oids) {
    if (params.tables.size() == 1) {
      label_t label = params.tables[0];
      MSVertexColumnBuilder builder(label);
      for (auto oid : oids) {
        vid_t vid;
        if (graph.GetVertexIndex(label, oid, vid)) {
          if (predicate(label, vid)) {
            builder.push_back_opt(vid);
          }
        }
      }
      chunk.set(params.alias, builder.finish());
    } else if (params.tables.size() > 1) {
      // TODO(luoxiaojian): use MSVertexColumnBuilder
      std::vector<std::pair<label_t, vid_t>> vids;

      for (auto label : params.tables) {
        for (auto oid : oids) {
          vid_t vid;
          if (graph.GetVertexIndex(label, oid, vid)) {
            if (predicate(label, vid)) {
              vids.emplace_back(label, vid);
            }
          }
        }
      }
      if (vids.size() == 1) {
        MSVertexColumnBuilder builder(vids[0].first);
        builder.push_back_opt(vids[0].second);
        chunk.set(params.alias, builder.finish());
      } else {
        MLVertexColumnBuilder builder;
        for (auto& pair : vids) {
          builder.push_back_vertex({pair.first, pair.second});
        }
        chunk.set(params.alias, builder.finish());
      }
    }
    return chunk;
  }

  static neug::result<ContextChunk> find_vertex_with_oid(
      ContextChunk&& chunk, const IStorageInterface& graph, label_t label,
      const Value& pk, int32_t alias);
};

}  // namespace execution

}  // namespace neug
