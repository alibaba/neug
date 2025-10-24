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

#include <glog/logging.h>
#include <ostream>
#include <string_view>

#include "neug/execution/utils/special_predicates.h"
#include "neug/utils/result.h"

namespace gs {
namespace runtime {

gs::result<Context> Scan::find_vertex_with_oid(Context&& ctx,
                                               const GraphReadInterface& graph,
                                               label_t label, const Prop& oid,
                                               int32_t alias) {
  MSVertexColumnBuilder builder(label);
  vid_t vid;
  if (graph.GetVertexIndex(label, oid, vid)) {
    builder.push_back_opt(vid);
  }
  ctx.set(alias, builder.finish());
  return ctx;
}

gs::result<Context> Scan::find_vertex_with_gid(Context&& ctx,
                                               const GraphReadInterface& graph,
                                               label_t label, int64_t gid,
                                               int32_t alias) {
  MSVertexColumnBuilder builder(label);
  if (GlobalId::get_label_id(gid) == label &&
      graph.IsValidVertex(label, GlobalId::get_vid(gid))) {
    builder.push_back_opt(GlobalId::get_vid(gid));
  } else {
    LOG(ERROR) << "Invalid label id: "
               << static_cast<int>(GlobalId::get_label_id(gid));
  }
  ctx.set(alias, builder.finish());
  return ctx;
}

struct ScanVertexSPOp {
  template <typename PRED_T>
  static gs::result<Context> eval_with_predicate(
      const PRED_T& pred, const GraphReadInterface& graph, Context&& ctx,
      const ScanParams& params) {
    return Scan::scan_vertex<PRED_T>(std::move(ctx), graph, params, pred);
  }
};

gs::result<Context> Scan::scan_vertex_with_special_vertex_predicate(
    Context&& ctx, const GraphReadInterface& graph, const ScanParams& params,
    const SpecialVertexPredicateConfig& config,
    const std::map<std::string, std::string>& query_params) {
  std::set<label_t> expected_labels;
  for (auto label : params.tables) {
    expected_labels.insert(label);
  }
  return dispatch_vertex_predicate<ScanVertexSPOp>(graph, expected_labels,
                                                   config, query_params, graph,
                                                   std::move(ctx), params);
}

struct FilterGidSPOp {
  template <typename PRED_T>
  static gs::result<Context> eval_with_predicate(
      const PRED_T& pred, const GraphReadInterface& graph, Context&& ctx,
      const ScanParams& params, const std::vector<int64_t>& gids) {
    return Scan::filter_gids<PRED_T>(std::move(ctx), graph, params, pred, gids);
  }
};

gs::result<Context> Scan::filter_gids_with_special_vertex_predicate(
    Context&& ctx, const GraphReadInterface& graph, const ScanParams& params,
    const SpecialVertexPredicateConfig& config,
    const std::vector<int64_t>& gids) {
  std::set<label_t> expected_labels;
  for (auto label : params.tables) {
    expected_labels.insert(label);
  }
  return dispatch_vertex_predicate<FilterGidSPOp>(
      graph, expected_labels, config, {}, graph, std::move(ctx), params, gids);
}

struct FilterOidsSPOp {
  template <typename PRED_T>
  static gs::result<Context> eval_with_predicate(
      const PRED_T& pred, const GraphReadInterface& graph, Context&& ctx,
      const ScanParams& params, const std::vector<Prop>& oids) {
    return Scan::filter_oids<PRED_T>(std::move(ctx), graph, params, pred, oids);
  }
};

gs::result<Context> Scan::filter_oids_with_special_vertex_predicate(
    Context&& ctx, const GraphReadInterface& graph, const ScanParams& params,
    const SpecialVertexPredicateConfig& config, const std::vector<Prop>& oids) {
  std::set<label_t> expected_labels;
  for (auto label : params.tables) {
    expected_labels.insert(label);
  }
  return dispatch_vertex_predicate<FilterOidsSPOp>(
      graph, expected_labels, config, {}, graph, std::move(ctx), params, oids);
}

}  // namespace runtime

}  // namespace gs
