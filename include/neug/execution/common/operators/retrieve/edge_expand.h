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

#include "neug/execution/common/context.h"
#include "neug/execution/common/operators/retrieve/edge_expand_impl.h"
#include "neug/execution/common/params_map.h"
#include "neug/execution/common/types/graph_types.h"
#include "neug/execution/expression/special_predicates.h"
#include "neug/execution/utils/params.h"
#include "neug/utils/result.h"

namespace neug {
namespace execution {

class EdgeExpand {
 public:
  static neug::result<Context> expand_degree(const StorageReadInterface& graph,
                                             Context&& ctx,
                                             const EdgeExpandParams& params);
  template <typename PRED_T>
  static neug::result<Context> expand_edge(const StorageReadInterface& graph,
                                           Context&& ctx,
                                           const EdgeExpandParams& params,
                                           const PRED_T& pred) {
    auto input_vertex_list =
        std::dynamic_pointer_cast<IVertexColumn>(ctx.get(params.v_tag));
    auto vertex_column_type = input_vertex_list->vertex_column_type();
    if (!params.is_optional && input_vertex_list->is_optional()) {
      ctx = remove_null_from_ctx(std::move(ctx), params.v_tag);
      input_vertex_list =
          std::dynamic_pointer_cast<IVertexColumn>(ctx.get(params.v_tag));
    }
    if (params.is_optional) {
      if (vertex_column_type == VertexColumnType::kSingle) {
        const SLVertexColumn& sl_col =
            *dynamic_cast<const SLVertexColumn*>(input_vertex_list.get());
        auto pair = expand_edge_impl<PRED_T, true>(graph, sl_col, params.labels,
                                                   params.dir, pred);
        ctx.set_with_reshuffle(params.alias, pair.first, pair.second);
        return ctx;
      } else if (vertex_column_type == VertexColumnType::kMultiple) {
        const MLVertexColumn& ml_col =
            *dynamic_cast<const MLVertexColumn*>(input_vertex_list.get());
        auto pair = expand_edge_impl<PRED_T, true>(graph, ml_col, params.labels,
                                                   params.dir, pred);
        ctx.set_with_reshuffle(params.alias, pair.first, pair.second);
        return ctx;
      } else {
        CHECK(vertex_column_type == VertexColumnType::kMultiSegment);
        const MSVertexColumn& ms_col =
            *dynamic_cast<const MSVertexColumn*>(input_vertex_list.get());
        auto pair = expand_edge_impl<PRED_T, true>(graph, ms_col, params.labels,
                                                   params.dir, pred);
        ctx.set_with_reshuffle(params.alias, pair.first, pair.second);
        return ctx;
      }
    } else {
      if (vertex_column_type == VertexColumnType::kSingle) {
        const SLVertexColumn& sl_col =
            *dynamic_cast<const SLVertexColumn*>(input_vertex_list.get());
        auto pair = expand_edge_impl<PRED_T, false>(
            graph, sl_col, params.labels, params.dir, pred);
        ctx.set_with_reshuffle(params.alias, pair.first, pair.second);
        return ctx;
      } else if (vertex_column_type == VertexColumnType::kMultiple) {
        const MLVertexColumn& ml_col =
            *dynamic_cast<const MLVertexColumn*>(input_vertex_list.get());
        auto pair = expand_edge_impl<PRED_T, false>(
            graph, ml_col, params.labels, params.dir, pred);
        ctx.set_with_reshuffle(params.alias, pair.first, pair.second);
        return ctx;
      } else {
        CHECK(vertex_column_type == VertexColumnType::kMultiSegment);
        const MSVertexColumn& ms_col =
            *dynamic_cast<const MSVertexColumn*>(input_vertex_list.get());
        auto pair = expand_edge_impl<PRED_T, false>(
            graph, ms_col, params.labels, params.dir, pred);
        ctx.set_with_reshuffle(params.alias, pair.first, pair.second);
        return ctx;
      }
    }
  }

  static neug::result<Context> expand_edge_with_special_edge_predicate(
      const StorageReadInterface& graph, Context&& ctx,
      const EdgeExpandParams& params, const SpecialPredicateConfig& config,
      const Value& target_val_str);

  template <typename PRED_T>
  static neug::result<Context> expand_vertex(const StorageReadInterface& graph,
                                             Context&& ctx,
                                             const EdgeExpandParams& params,
                                             const PRED_T& pred) {
    std::shared_ptr<IVertexColumn> input_vertex_list =
        std::dynamic_pointer_cast<IVertexColumn>(ctx.get(params.v_tag));
    if (!params.is_optional && input_vertex_list->is_optional()) {
      ctx = remove_null_from_ctx(std::move(ctx), params.v_tag);
      input_vertex_list =
          std::dynamic_pointer_cast<IVertexColumn>(ctx.get(params.v_tag));
    }

    if (params.is_optional) {
      if (input_vertex_list->vertex_column_type() ==
          VertexColumnType::kSingle) {
        auto casted_input_vertex_list =
            std::dynamic_pointer_cast<SLVertexColumn>(input_vertex_list);
        auto pair = expand_vertex_impl<PRED_T, true>(
            graph, *casted_input_vertex_list, params.labels, params.dir, pred);
        ctx.set_with_reshuffle(params.alias, pair.first, pair.second);
        return ctx;
      } else if (input_vertex_list->vertex_column_type() ==
                 VertexColumnType::kMultiple) {
        auto casted_input_vertex_list =
            std::dynamic_pointer_cast<MLVertexColumn>(input_vertex_list);
        auto pair = expand_vertex_impl<PRED_T, true>(
            graph, *casted_input_vertex_list, params.labels, params.dir, pred);
        ctx.set_with_reshuffle(params.alias, pair.first, pair.second);
        return ctx;
      } else if (input_vertex_list->vertex_column_type() ==
                 VertexColumnType::kMultiSegment) {
        auto casted_input_vertex_list =
            std::dynamic_pointer_cast<MSVertexColumn>(input_vertex_list);
        auto pair = expand_vertex_impl<PRED_T, true>(
            graph, *casted_input_vertex_list, params.labels, params.dir, pred);
        ctx.set_with_reshuffle(params.alias, pair.first, pair.second);
        return ctx;
      } else {
        LOG(ERROR) << "not support vertex column type "
                   << static_cast<int>(input_vertex_list->vertex_column_type());
        RETURN_UNSUPPORTED_ERROR("not support vertex column type " +
                                 std::to_string(static_cast<int>(
                                     input_vertex_list->vertex_column_type())));
      }
    } else {
      VertexColumnType input_vertex_list_type =
          input_vertex_list->vertex_column_type();
      if (input_vertex_list_type == VertexColumnType::kSingle) {
        auto casted_input_vertex_list =
            std::dynamic_pointer_cast<SLVertexColumn>(input_vertex_list);
        auto pair = expand_vertex_impl<PRED_T, false>(
            graph, *casted_input_vertex_list, params.labels, params.dir, pred);
        ctx.set_with_reshuffle(params.alias, pair.first, pair.second);
        return ctx;
      } else if (input_vertex_list_type == VertexColumnType::kMultiple) {
        auto casted_input_vertex_list =
            std::dynamic_pointer_cast<MLVertexColumn>(input_vertex_list);
        auto pair = expand_vertex_impl<PRED_T, false>(
            graph, *casted_input_vertex_list, params.labels, params.dir, pred);
        ctx.set_with_reshuffle(params.alias, pair.first, pair.second);
        return ctx;
      } else if (input_vertex_list_type == VertexColumnType::kMultiSegment) {
        auto casted_input_vertex_list =
            std::dynamic_pointer_cast<MSVertexColumn>(input_vertex_list);
        auto pair = expand_vertex_impl<PRED_T, false>(
            graph, *casted_input_vertex_list, params.labels, params.dir, pred);
        ctx.set_with_reshuffle(params.alias, pair.first, pair.second);
        return ctx;
      } else {
        LOG(ERROR) << "not support vertex column type "
                   << static_cast<int>(input_vertex_list_type);
        RETURN_UNSUPPORTED_ERROR(
            "not support vertex column type " +
            std::to_string(static_cast<int>(input_vertex_list_type)));
      }
    }
  }

  static neug::result<Context> expand_vertex_ep_cmp(
      const StorageReadInterface& graph, Context&& ctx,
      const EdgeExpandParams& params, const Value& ep_val, SPPredicateType tp);

  static neug::result<Context> expand_vertex_with_special_vertex_predicate(
      const StorageReadInterface& graph, Context&& ctx,
      const EdgeExpandParams& params, const SpecialPredicateConfig& config,
      const ParamsMap& query_params);

  template <typename T1>
  static neug::result<Context> tc(
      const StorageReadInterface& graph, Context&& ctx,
      const std::array<std::tuple<label_t, label_t, label_t, Direction>, 3>&
          labels,
      int input_tag, int alias1, int alias2, bool LT, const Value& val) {
    std::shared_ptr<IVertexColumn> input_vertex_list =
        std::dynamic_pointer_cast<IVertexColumn>(ctx.get(input_tag));
    if (input_vertex_list->vertex_column_type() != VertexColumnType::kSingle) {
      RETURN_UNSUPPORTED_ERROR(
          "Unsupported input for triangle counting, only single vertex column");
    }
    auto casted_input_vertex_list =
        std::dynamic_pointer_cast<SLVertexColumn>(input_vertex_list);
    label_t input_label = casted_input_vertex_list->label();
    auto dir0 = std::get<3>(labels[0]);
    auto dir1 = std::get<3>(labels[1]);
    auto dir2 = std::get<3>(labels[2]);
    auto d0_nbr_label = std::get<1>(labels[0]);
    auto d0_e_label = std::get<2>(labels[0]);
    auto csr0 = (dir0 == Direction::kOut)
                    ? graph.GetGenericOutgoingGraphView(
                          input_label, d0_nbr_label, d0_e_label)
                    : graph.GetGenericIncomingGraphView(
                          input_label, d0_nbr_label, d0_e_label);
    auto ed_accessor0 = graph.GetEdgeDataAccessor(
        dir0 == Direction::kOut ? input_label : d0_nbr_label,
        dir0 == Direction::kOut ? d0_nbr_label : input_label, d0_e_label, 0);

    auto d1_nbr_label = std::get<1>(labels[1]);
    auto d1_e_label = std::get<2>(labels[1]);
    auto csr1 = (dir1 == Direction::kOut)
                    ? graph.GetGenericOutgoingGraphView(
                          input_label, d1_nbr_label, d1_e_label)
                    : graph.GetGenericIncomingGraphView(
                          input_label, d1_nbr_label, d1_e_label);
    auto d2_nbr_label = std::get<1>(labels[2]);
    auto d2_e_label = std::get<2>(labels[2]);
    auto csr2 = (dir2 == Direction::kOut)
                    ? graph.GetGenericOutgoingGraphView(
                          d1_nbr_label, d2_nbr_label, d2_e_label)
                    : graph.GetGenericIncomingGraphView(
                          d1_nbr_label, d2_nbr_label, d2_e_label);

    T1 param = val.template GetValue<T1>();

    MSVertexColumnBuilder builder1(d1_nbr_label);
    MSVertexColumnBuilder builder2(d2_nbr_label);
    std::vector<size_t> offsets;

    static thread_local StorageReadInterface::vertex_array_t<bool> d0_set;
    static thread_local std::vector<vid_t> d0_vec;

    d0_set.Init(graph.GetVertexSet(d0_nbr_label), false);

    size_t idx = 0;
    if (csr0.type() == CsrViewType::kMultipleMutable &&
        ed_accessor0.is_bundled()) {
      auto typed_csr0 =
          csr0.template get_typed_view<T1, CsrViewType::kMultipleMutable>();
      if (LT) {
        for (auto v : casted_input_vertex_list->vertices()) {
          typed_csr0.foreach_nbr_lt(v, param, [&](vid_t u, const T1& data) {
            d0_set[u] = true;
            d0_vec.push_back(u);
          });
          auto es1 = csr1.get_edges(v);
          for (auto it1 = es1.begin(); it1 != es1.end(); ++it1) {
            auto nbr1 = it1.get_vertex();
            auto es2 = csr2.get_edges(nbr1);
            for (auto it2 = es2.begin(); it2 != es2.end(); ++it2) {
              auto nbr2 = it2.get_vertex();
              if (d0_set[nbr2]) {
                builder1.push_back_opt(nbr1);
                builder2.push_back_opt(nbr2);
                offsets.push_back(idx);
              }
            }
          }
          for (auto u : d0_vec) {
            d0_set[u] = false;
          }
          d0_vec.clear();
          ++idx;
        }
      } else {
        for (auto v : casted_input_vertex_list->vertices()) {
          typed_csr0.foreach_nbr_gt(v, param, [&](vid_t u, const T1& data) {
            d0_set[u] = true;
            d0_vec.push_back(u);
          });
          auto es1 = csr1.get_edges(v);
          for (auto it1 = es1.begin(); it1 != es1.end(); ++it1) {
            auto nbr1 = it1.get_vertex();
            auto es2 = csr2.get_edges(nbr1);
            for (auto it2 = es2.begin(); it2 != es2.end(); ++it2) {
              auto nbr2 = it2.get_vertex();
              if (d0_set[nbr2]) {
                builder1.push_back_opt(nbr1);
                builder2.push_back_opt(nbr2);
                offsets.push_back(idx);
              }
            }
          }
          for (auto u : d0_vec) {
            d0_set[u] = false;
          }
          d0_vec.clear();
          ++idx;
        }
      }
    } else {
      if (LT) {
        for (auto v : casted_input_vertex_list->vertices()) {
          auto es0 = csr0.get_edges(v);
          for (auto it0 = es0.begin(); it0 != es0.end(); ++it0) {
            auto ed0 = ed_accessor0.get_typed_data<T1>(it0);
            if (ed0 < param) {
              auto nbr0 = it0.get_vertex();
              d0_set[nbr0] = true;
              d0_vec.push_back(nbr0);
            }
          }
          auto es1 = csr1.get_edges(v);
          for (auto it1 = es1.begin(); it1 != es1.end(); ++it1) {
            auto nbr1 = it1.get_vertex();
            auto es2 = csr2.get_edges(nbr1);
            for (auto it2 = es2.begin(); it2 != es2.end(); ++it2) {
              auto nbr2 = it2.get_vertex();
              if (d0_set[nbr2]) {
                builder1.push_back_opt(nbr1);
                builder2.push_back_opt(nbr2);
                offsets.push_back(idx);
              }
            }
          }
          for (auto u : d0_vec) {
            d0_set[u] = false;
          }
          d0_vec.clear();
          ++idx;
        }
      } else {
        for (auto v : casted_input_vertex_list->vertices()) {
          auto es0 = csr0.get_edges(v);
          for (auto it0 = es0.begin(); it0 != es0.end(); ++it0) {
            auto ed0 = ed_accessor0.get_typed_data<T1>(it0);
            if (param < ed0) {
              auto nbr0 = it0.get_vertex();
              d0_set[nbr0] = true;
              d0_vec.push_back(nbr0);
            }
          }
          auto es1 = csr1.get_edges(v);
          for (auto it1 = es1.begin(); it1 != es1.end(); ++it1) {
            auto nbr1 = it1.get_vertex();
            auto es2 = csr2.get_edges(nbr1);
            for (auto it2 = es2.begin(); it2 != es2.end(); ++it2) {
              auto nbr2 = it2.get_vertex();
              if (d0_set[nbr2]) {
                builder1.push_back_opt(nbr1);
                builder2.push_back_opt(nbr2);
                offsets.push_back(idx);
              }
            }
          }
        }
      }
    }

    std::shared_ptr<IContextColumn> col1 = builder1.finish();
    std::shared_ptr<IContextColumn> col2 = builder2.finish();
    ctx.set_with_reshuffle(alias1, col1, offsets);
    ctx.set(alias2, col2);
    return ctx;
  }

  static Context remove_null_from_ctx(Context&& ctx, int tag_id);
};

}  // namespace execution
}  // namespace neug
