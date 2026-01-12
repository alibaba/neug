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

#include "neug/execution/common/operators/retrieve/intersect.h"

#include <glog/logging.h>
#include <parallel_hashmap/phmap_base.h>
#include <stddef.h>

#include <limits>
#include <memory>
#include <ostream>
#include <utility>

#include "neug/execution/common/columns/edge_columns.h"
#include "neug/execution/common/columns/i_context_column.h"
#include "neug/execution/common/columns/value_columns.h"
#include "neug/execution/common/columns/vertex_columns.h"
#include "neug/execution/common/context.h"
#include "neug/execution/utils/params.h"
#include "neug/utils/result.h"
#include "parallel_hashmap/phmap.h"

namespace gs {

namespace runtime {

void get_labels(
    const EdgeExpandParams& eep, const StorageReadInterface& graph,
    std::vector<std::vector<std::pair<LabelTriplet, std::vector<DataTypeId>>>>&
        labels) {
  std::vector<std::pair<LabelTriplet, std::vector<DataTypeId>>> labels_i;
  for (const auto& label_triplet : eep.labels) {
    std::vector<DataTypeId> props;
    props = graph.schema().get_edge_properties(label_triplet.src_label,
                                               label_triplet.dst_label,
                                               label_triplet.edge_label);
    if (props.empty()) {
      labels_i.emplace_back(label_triplet, std::vector{DataTypeId::EMPTY});
    } else {
      labels_i.emplace_back(label_triplet, props);
    }
  }
  labels.push_back(std::move(labels_i));
}

gs::result<gs::runtime::Context> Intersect::Multiple_Intersect(
    const StorageReadInterface& graph,
    const std::map<std::string, std::string>& params,
    gs::runtime::Context&& ctx,
    const std::vector<std::function<bool(label_t, vid_t)>>& preds,
    const std::vector<std::function<bool(label_t, vid_t, label_t, vid_t,
                                         label_t, Direction, const void*)>>&
        e_preds,
    const std::vector<EdgeExpandParams>& eeps, int vertex_alias) {
  std::vector<IVertexColumn*> vertex_cols;
  for (const auto& eep : eeps) {
    auto col = ctx.get(eep.v_tag);
    vertex_cols.push_back(dynamic_cast<IVertexColumn*>(col.get()));
  }
  size_t row_num = ctx.row_num();
  // TODO(luoxiaojian): opt with MLVertexColumnBuilderOpt
  MLVertexColumnBuilder builder;
  std::vector<std::vector<std::pair<LabelTriplet, std::vector<DataTypeId>>>>
      labels;
  labels.reserve(eeps.size());

  for (size_t i = 0; i < eeps.size(); ++i) {
    get_labels(eeps[i], graph, labels);
    std::vector<LabelTriplet> label_triplets;
    for (const auto& p : labels.back()) {
      label_triplets.push_back(p.first);
    }
  }

  std::vector<size_t> offsets;

  for (size_t i = 0; i < row_num; ++i) {
    phmap::flat_hash_map<VertexRecord, size_t, VertexRecordHash> vertex_set;
    auto v = vertex_cols[0]->get_vertex(i);
    if (eeps[0].dir == Direction::kOut || eeps[0].dir == Direction::kBoth) {
      for (const auto& label_triplet : eeps[0].labels) {
        if (label_triplet.src_label != v.label_) {
          continue;
        }
        auto oview = graph.GetGenericOutgoingGraphView(
            v.label_, label_triplet.dst_label, label_triplet.edge_label);
        auto oes = oview.get_edges(v.vid_);
        for (auto iter = oes.begin(); iter != oes.end(); ++iter) {
          vid_t vid = iter.get_vertex();
          if (e_preds[0](v.label_, v.vid_, label_triplet.dst_label, vid,
                         label_triplet.edge_label, Direction::kOut,
                         iter.get_data_ptr())) {
            if (preds[0](label_triplet.dst_label, vid)) {
              VertexRecord v_record{label_triplet.dst_label, vid};
              if (vertex_set.find(v_record) == vertex_set.end()) {
                vertex_set.emplace(v_record, 0);
              }
              vertex_set[v_record]++;
            }
          }
        }
      }
    }
    if (eeps[0].dir == Direction::kIn || eeps[0].dir == Direction::kBoth) {
      for (const auto& label_triplet : eeps[0].labels) {
        if (label_triplet.dst_label != v.label_) {
          continue;
        }
        auto iview = graph.GetGenericIncomingGraphView(
            v.label_, label_triplet.src_label, label_triplet.edge_label);
        auto ies = iview.get_edges(v.vid_);
        for (auto iter = ies.begin(); iter != ies.end(); ++iter) {
          vid_t vid = iter.get_vertex();
          if (e_preds[0](v.label_, v.vid_, label_triplet.src_label, vid,
                         label_triplet.edge_label, Direction::kIn,
                         iter.get_data_ptr())) {
            if (preds[0](label_triplet.src_label, vid)) {
              VertexRecord v_record{label_triplet.src_label, vid};
              if (vertex_set.find(v_record) == vertex_set.end()) {
                vertex_set.emplace(v_record, 0);
              }
              vertex_set[v_record]++;
            }
          }
        }
      }
    }

    for (size_t j = 1; j < eeps.size(); ++j) {
      phmap::flat_hash_map<VertexRecord, size_t, VertexRecordHash> tmp_set;
      v = vertex_cols[j]->get_vertex(i);
      if (eeps[j].dir == Direction::kOut || eeps[j].dir == Direction::kBoth) {
        for (const auto& label_triplet : eeps[j].labels) {
          if (label_triplet.src_label != v.label_) {
            continue;
          }
          auto oview = graph.GetGenericOutgoingGraphView(
              v.label_, label_triplet.dst_label, label_triplet.edge_label);
          auto oes = oview.get_edges(v.vid_);
          for (auto iter = oes.begin(); iter != oes.end(); ++iter) {
            vid_t vid = iter.get_vertex();
            if (e_preds[j](v.label_, v.vid_, label_triplet.dst_label, vid,
                           label_triplet.edge_label, Direction::kOut,
                           iter.get_data_ptr())) {
              VertexRecord v_record{label_triplet.dst_label, vid};
              if (vertex_set.find(v_record) != vertex_set.end() &&
                  preds[j](v_record.label_, v_record.vid_)) {
                if (tmp_set.find(v_record) == tmp_set.end()) {
                  tmp_set.emplace(v_record, 0);
                }
                tmp_set[v_record] += vertex_set[v_record];
              }
            }
          }
        }
      }
      if (eeps[j].dir == Direction::kIn || eeps[j].dir == Direction::kBoth) {
        for (const auto& label_triplet : eeps[j].labels) {
          if (label_triplet.dst_label != v.label_) {
            continue;
          }
          auto iview = graph.GetGenericIncomingGraphView(
              v.label_, label_triplet.src_label, label_triplet.edge_label);
          auto ies = iview.get_edges(v.vid_);
          for (auto iter = ies.begin(); iter != ies.end(); ++iter) {
            vid_t vid = iter.get_vertex();
            if (e_preds[j](v.label_, v.vid_, label_triplet.src_label, vid,
                           label_triplet.edge_label, Direction::kIn,
                           iter.get_data_ptr())) {
              VertexRecord v_record{label_triplet.src_label, vid};
              if (vertex_set.find(v_record) != vertex_set.end() &&
                  preds[j](v_record.label_, v_record.vid_)) {
                if (tmp_set.find(v_record) == tmp_set.end()) {
                  tmp_set.emplace(v_record, 0);
                }
                tmp_set[v_record] += vertex_set[v_record];
              }
            }
          }
        }
      }
      std::swap(vertex_set, tmp_set);
      if (vertex_set.empty()) {
        break;  // No intersection found, skip to next row
      }
    }
    if (!vertex_set.empty()) {
      for (const auto& [v_record, cnt] : vertex_set) {
        for (size_t k = 0; k < cnt; ++k) {
          builder.push_back_opt(v_record);
          offsets.emplace_back(i);
        }
      }
    }
  }
  ctx.reshuffle(offsets);
  auto col = builder.finish();
  ctx.set(vertex_alias, std::move(col));
  return std::move(ctx);
}

gs::result<gs::runtime::Context> Intersect::Binary_Intersect_With_Edge(
    const StorageReadInterface& graph,
    const std::map<std::string, std::string>& params,
    gs::runtime::Context&& ctx,
    const std::function<bool(label_t, vid_t)>& left_pred,
    const std::function<bool(label_t, vid_t)>& right_pred,
    const std::function<bool(label_t, vid_t, label_t, vid_t, label_t, Direction,
                             const void*)>& left_e_pred,
    const std::function<bool(label_t, vid_t, label_t, vid_t, label_t, Direction,
                             const void*)>& right_e_pred,
    const EdgeExpandParams& eep0, const EdgeExpandParams& eep1,
    int vertex_alias, const std::vector<int>& edge_alias) {
  const auto& vertex_col0 =
      dynamic_cast<const IVertexColumn*>(ctx.get(eep0.v_tag).get());
  const auto& vertex_col1 =
      dynamic_cast<const IVertexColumn*>(ctx.get(eep1.v_tag).get());

  CHECK(!eep0.is_optional && !eep1.is_optional)
      << "Intersect operator does not support optional edge expand.";
  size_t row_num = ctx.row_num();

  // TODO(luoxiaojian): use MLVertexColumnBuilderOpt
  MLVertexColumnBuilder builder;
  std::vector<size_t> offsets;

  std::vector<std::vector<std::pair<LabelTriplet, std::vector<DataTypeId>>>>
      labels;
  std::vector<BDMLEdgeColumnBuilder> edge_builders;
  {
    get_labels(eep0, graph, labels);
    std::vector<LabelTriplet> label_triplets;
    for (const auto& p : labels.back()) {
      label_triplets.push_back(p.first);
    }
    edge_builders.emplace_back(label_triplets);
  }
  {
    get_labels(eep1, graph, labels);
    std::vector<LabelTriplet> label_triplets;
    for (const auto& p : labels.back()) {
      label_triplets.push_back(p.first);
    }
    edge_builders.emplace_back(label_triplets);
  }

  for (size_t i = 0; i < row_num; ++i) {
    using value_t =
        std::tuple<LabelTriplet, vid_t, vid_t, const void*, Direction>;

    std::vector<value_t> aux_values;
    phmap::flat_hash_map<VertexRecord, std::vector<size_t>, VertexRecordHash>
        vertex_set;

    auto v0 = vertex_col0->get_vertex(i);
    if (eep0.dir == Direction::kOut || eep0.dir == Direction::kBoth) {
      for (const auto& label_triplet : eep0.labels) {
        if (label_triplet.src_label != v0.label_) {
          continue;
        }
        auto oview = graph.GetGenericOutgoingGraphView(
            v0.label_, label_triplet.dst_label, label_triplet.edge_label);
        auto oes = oview.get_edges(v0.vid_);
        for (auto iter = oes.begin(); iter != oes.end(); ++iter) {
          vid_t vid = iter.get_vertex();
          if (left_e_pred(v0.label_, v0.vid_, label_triplet.dst_label, vid,
                          label_triplet.edge_label, Direction::kOut,
                          iter.get_data_ptr()) &&
              left_pred(label_triplet.dst_label, vid)) {
            auto rcd = VertexRecord{label_triplet.dst_label, vid};
            aux_values.emplace_back(label_triplet, v0.vid_, vid,
                                    iter.get_data_ptr(), Direction::kOut);
            size_t idx = aux_values.size() - 1;

            vertex_set[rcd].emplace_back(idx);
          }
        }
      }
    }
    if (eep0.dir == Direction::kIn || eep0.dir == Direction::kBoth) {
      for (const auto& label_triplet : eep0.labels) {
        if (label_triplet.dst_label != v0.label_) {
          continue;
        }
        auto iview = graph.GetGenericIncomingGraphView(
            v0.label_, label_triplet.src_label, label_triplet.edge_label);
        auto ies = iview.get_edges(v0.vid_);
        for (auto iter = ies.begin(); iter != ies.end(); ++iter) {
          vid_t vid = iter.get_vertex();
          if (left_e_pred(v0.label_, v0.vid_, label_triplet.src_label, vid,
                          label_triplet.edge_label, Direction::kIn,
                          iter.get_data_ptr()) &&
              left_pred(label_triplet.src_label, vid)) {
            auto rcd = VertexRecord{label_triplet.src_label, vid};
            aux_values.emplace_back(label_triplet, vid, v0.vid_,
                                    iter.get_data_ptr(), Direction::kIn);
            size_t idx = aux_values.size() - 1;
            vertex_set[rcd].emplace_back(idx);
          }
        }
      }
    }
    auto v1 = vertex_col1->get_vertex(i);

    if (eep1.dir == Direction::kOut || eep1.dir == Direction::kBoth) {
      for (const auto& label_triplet : eep1.labels) {
        if (label_triplet.src_label != v1.label_) {
          continue;
        }
        auto oview = graph.GetGenericOutgoingGraphView(
            v1.label_, label_triplet.dst_label, label_triplet.edge_label);
        auto oes = oview.get_edges(v1.vid_);
        for (auto iter = oes.begin(); iter != oes.end(); ++iter) {
          vid_t vid = iter.get_vertex();
          if (right_e_pred(v1.label_, v1.vid_, label_triplet.dst_label, vid,
                           label_triplet.edge_label, Direction::kOut,
                           iter.get_data_ptr()) &&
              right_pred(label_triplet.dst_label, vid)) {
            auto rcd = VertexRecord{label_triplet.dst_label, vid};
            if (vertex_set.find(rcd) != vertex_set.end()) {
              const auto& values = vertex_set[rcd];
              for (const auto& value : values) {
                builder.push_back_opt(rcd);
                if (edge_alias[0] != -1) {
                  std::apply(
                      [&](const auto&... args) {
                        edge_builders[0].push_back_opt(args...);
                      },
                      aux_values[value]);
                }
                if (edge_alias[1] != -1) {
                  edge_builders[1].push_back_opt(label_triplet, v1.vid_, vid,
                                                 iter.get_data_ptr(),
                                                 Direction::kOut);
                }
                offsets.emplace_back(i);
              }
            }
          }
        }
      }
    }
    if (eep1.dir == Direction::kIn || eep1.dir == Direction::kBoth) {
      for (const auto& label_triplet : eep1.labels) {
        if (label_triplet.dst_label != v1.label_) {
          continue;
        }
        auto iview = graph.GetGenericIncomingGraphView(
            v1.label_, label_triplet.src_label, label_triplet.edge_label);
        auto ies = iview.get_edges(v1.vid_);
        for (auto iter = ies.begin(); iter != ies.end(); ++iter) {
          vid_t vid = iter.get_vertex();
          VertexRecord v_record{label_triplet.src_label, vid};
          if (right_e_pred(v1.label_, v1.vid_, label_triplet.src_label, vid,
                           label_triplet.edge_label, Direction::kIn,
                           iter.get_data_ptr()) &&
              vertex_set.find(v_record) != vertex_set.end() &&
              right_pred(label_triplet.src_label, vid)) {
            const auto& values = vertex_set[v_record];
            for (const auto& value : values) {
              builder.push_back_opt(v_record);
              if (edge_alias[0] != -1) {
                std::apply(
                    [&](const auto&... args) {
                      edge_builders[0].push_back_opt(args...);
                    },
                    aux_values[value]);
              }
              if (edge_alias[1] != -1) {
                edge_builders[1].push_back_opt(label_triplet, vid, v1.vid_,
                                               iter.get_data_ptr(),
                                               Direction::kIn);
              }
              offsets.emplace_back(i);
            }
          }
        }
      }
    }
  }
  ctx.reshuffle(offsets);
  auto col = builder.finish();
  ctx.set(vertex_alias, std::move(col));

  if (edge_alias[0] != -1) {
    auto left_e_col = edge_builders[0].finish();
    ctx.set(edge_alias[0], std::move(left_e_col));
  }
  if (edge_alias[1] != -1) {
    auto right_e_col = edge_builders[1].finish();
    ctx.set(edge_alias[1], std::move(right_e_col));
  }

  return std::move(ctx);
}
}  // namespace runtime

}  // namespace gs
