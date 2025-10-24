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

#ifndef INCLUDE_NEUG_EXECUTION_COMMON_OPERATORS_RETRIEVE_SCAN_H_
#define INCLUDE_NEUG_EXECUTION_COMMON_OPERATORS_RETRIEVE_SCAN_H_

#include <stdint.h>

#include <compare>
#include <map>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "neug/execution/common/columns/vertex_columns.h"
#include "neug/execution/common/context.h"
#include "neug/execution/common/graph_interface.h"
#include "neug/execution/utils/params.h"
#include "neug/execution/utils/special_predicates.h"
#include "neug/utils/property/types.h"
#include "neug/utils/result.h"

namespace gs {

namespace runtime {
class SPVertexPredicate;

class Scan {
 public:
  template <typename PRED_T>
  static gs::result<Context> scan_vertex(Context&& ctx,
                                         const GraphReadInterface& graph,
                                         const ScanParams& params,
                                         const PRED_T& predicate) {
    MSVertexColumnBuilder builder(params.tables[0]);
    for (auto label : params.tables) {
      auto vertices = graph.GetVertexSet(label);
      builder.start_label(label);
      for (auto vid : vertices) {
        if (predicate(label, vid, 0)) {
          builder.push_back_opt(vid);
        }
      }
    }
    ctx.set(params.alias, builder.finish());
    return ctx;
  }

  template <typename PRED_T>
  static gs::result<Context> scan_vertex_with_limit(
      Context&& ctx, const GraphReadInterface& graph, const ScanParams& params,
      const PRED_T& predicate) {
    int32_t cur_limit = params.limit;
    if (params.tables.size() == 1) {
      label_t label = params.tables[0];
      MSVertexColumnBuilder builder(label);
      auto vertices = graph.GetVertexSet(label);
      for (auto vid : vertices) {
        if (cur_limit <= 0) {
          break;
        }
        if (predicate(label, vid, 0)) {
          builder.push_back_opt(vid);
          cur_limit--;
        }
      }
      ctx.set(params.alias, builder.finish());
    } else if (params.tables.size() > 1) {
      MSVertexColumnBuilder builder(params.tables[0]);
      for (auto label : params.tables) {
        if (cur_limit <= 0) {
          break;
        }
        auto vertices = graph.GetVertexSet(label);
        builder.start_label(label);
        for (auto vid : vertices) {
          if (cur_limit <= 0) {
            break;
          }
          if (predicate(label, vid, 0)) {
            builder.push_back_opt(vid);
            cur_limit--;
          }
        }
      }
      ctx.set(params.alias, builder.finish());
    }
    return ctx;
  }

  static gs::result<Context> scan_vertex_with_special_vertex_predicate(
      Context&& ctx, const GraphReadInterface& graph, const ScanParams& params,
      const SpecialVertexPredicateConfig& config,
      const std::map<std::string, std::string>& query_params);

  template <typename PRED_T>
  static gs::result<Context> filter_gids(Context&& ctx,
                                         const GraphReadInterface& graph,
                                         const ScanParams& params,
                                         const PRED_T& predicate,
                                         const std::vector<int64_t>& gids) {
    int32_t cur_limit = params.limit;
    if (params.tables.empty()) {
      MLVertexColumnBuilder builder;
      ctx.set(params.alias, builder.finish());
    } else {
      MSVertexColumnBuilder builder(params.tables[0]);

      for (auto label : params.tables) {
        if (cur_limit <= 0) {
          break;
        }
        builder.start_label(label);
        for (auto gid : gids) {
          if (cur_limit <= 0) {
            break;
          }
          vid_t vid = GlobalId::get_vid(gid);
          if (GlobalId::get_label_id(gid) == label &&
              predicate(label, vid, 0)) {
            builder.push_back_opt(vid);
            cur_limit--;
          }
        }
      }
      ctx.set(params.alias, builder.finish());
    }
    return ctx;
  }

  static gs::result<Context> filter_gids_with_special_vertex_predicate(
      Context&& ctx, const GraphReadInterface& graph, const ScanParams& params,
      const SpecialVertexPredicateConfig& config,
      const std::vector<int64_t>& oids);

  template <typename PRED_T>
  static gs::result<Context> filter_oids(Context&& ctx,
                                         const GraphReadInterface& graph,
                                         const ScanParams& params,
                                         const PRED_T& predicate,
                                         const std::vector<Prop>& oids) {
    auto limit = params.limit;
    if (params.tables.size() == 1) {
      label_t label = params.tables[0];
      MSVertexColumnBuilder builder(label);
      for (auto oid : oids) {
        if (limit <= 0) {
          break;
        }
        vid_t vid;
        if (graph.GetVertexIndex(label, oid, vid)) {
          if (predicate(label, vid, 0)) {
            builder.push_back_opt(vid);
            --limit;
          }
        }
      }
      ctx.set(params.alias, builder.finish());
    } else if (params.tables.size() > 1) {
      // TODO(luoxiaojian): use MSVertexColumnBuilder
      std::vector<std::pair<label_t, vid_t>> vids;

      for (auto label : params.tables) {
        if (limit <= 0) {
          break;
        }
        for (auto oid : oids) {
          if (limit <= 0) {
            break;
          }
          vid_t vid;
          if (graph.GetVertexIndex(label, oid, vid)) {
            if (predicate(label, vid, 0)) {
              vids.emplace_back(label, vid);
              --limit;
            }
          }
        }
      }
      if (vids.size() == 1) {
        MSVertexColumnBuilder builder(vids[0].first);
        builder.push_back_opt(vids[0].second);
        ctx.set(params.alias, builder.finish());
      } else {
        MLVertexColumnBuilder builder;
        for (auto& pair : vids) {
          builder.push_back_vertex({pair.first, pair.second});
        }
        ctx.set(params.alias, builder.finish());
      }
    }
    return ctx;
  }

  static gs::result<Context> filter_oids_with_special_vertex_predicate(
      Context&& ctx, const GraphReadInterface& graph, const ScanParams& params,
      const SpecialVertexPredicateConfig& config,
      const std::vector<Prop>& oids);

  static gs::result<Context> find_vertex_with_oid(
      Context&& ctx, const GraphReadInterface& graph, label_t label,
      const Prop& pk, int32_t alias);

  static gs::result<Context> find_vertex_with_gid(
      Context&& ctx, const GraphReadInterface& graph, label_t label, int64_t pk,
      int32_t alias);
};

}  // namespace runtime

}  // namespace gs

#endif  // INCLUDE_NEUG_EXECUTION_COMMON_OPERATORS_RETRIEVE_SCAN_H_
