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

#include "neug/execution/common/operators/retrieve/edge_expand.h"

#include "neug/execution/common/columns/value_columns.h"
#include "neug/execution/common/operators/retrieve/edge_expand_impl.h"
#include "neug/execution/expression/predicates.h"
#include "neug/execution/utils/opr_timer.h"
#include "neug/storages/graph/graph_interface.h"

namespace neug {

namespace execution {

Context EdgeExpand::remove_null_from_ctx(Context&& ctx, int tag_id) {
  std::shared_ptr<IVertexColumn> vertex_col =
      std::dynamic_pointer_cast<IVertexColumn>(ctx.get(tag_id));
  std::vector<size_t> selected_offsets;
  size_t num = vertex_col->size();
  for (size_t k = 0; k < num; ++k) {
    if (vertex_col->has_value(k)) {
      selected_offsets.push_back(k);
    }
  }
  ctx.reshuffle(selected_offsets);
  return ctx;
}

neug::result<Context> EdgeExpand::expand_degree(
    const StorageReadInterface& graph, Context&& ctx,
    const EdgeExpandParams& params) {
  auto vertex_col =
      dynamic_cast<const IVertexColumn*>(ctx.get(params.v_tag).get());

  std::unordered_map<label_t, std::vector<GenericView>> mps;
  const auto& vertex_labels = vertex_col->get_labels_set();
  for (auto label : params.labels) {
    if (params.dir == Direction::kOut || params.dir == Direction::kBoth) {
      if (vertex_labels.find(label.src_label) != vertex_labels.end()) {
        mps[label.src_label].emplace_back(graph.GetGenericOutgoingGraphView(
            label.src_label, label.dst_label, label.edge_label));
      }
    }
    if (params.dir == Direction::kIn || params.dir == Direction::kBoth) {
      if (vertex_labels.find(label.dst_label) != vertex_labels.end()) {
        mps[label.dst_label].emplace_back(graph.GetGenericIncomingGraphView(
            label.dst_label, label.src_label, label.edge_label));
      }
    }
  }
  ValueColumnBuilder<int64_t> builder;
  std::vector<size_t> shuffle_offset;
  if (mps.empty()) {
    ctx.set_with_reshuffle(params.alias, builder.finish(), shuffle_offset);
    return ctx;
  }
  foreach_vertex(*vertex_col, [&](size_t index, label_t label, vid_t v) {
    int64_t degree = 0;
    if (v == graph.kInvalidVid) {
      return;
    }
    if (mps.count(label)) {
      for (auto& view : mps.at(label)) {
        auto es = view.get_edges(v);
        for (auto it = es.begin(); it != es.end(); ++it) {
          ++degree;
        }
      }
    }

    if ((!params.is_optional) && degree == 0) {
      return;
    }
    builder.push_back_opt(degree);
    shuffle_offset.push_back(index);
  });
  ctx.set_with_reshuffle(params.alias, builder.finish(), shuffle_offset);
  return ctx;
}

neug::result<Context> EdgeExpand::expand_count(
    const StorageReadInterface& graph, Context&& ctx,
    const EdgeExpandParams& params) {
  auto vertex_col =
      dynamic_cast<const IVertexColumn*>(ctx.get(params.v_tag).get());

  std::unordered_map<label_t, std::vector<GenericView>> mps;
  const auto& vertex_labels = vertex_col->get_labels_set();
  for (auto label : params.labels) {
    if (params.dir == Direction::kOut || params.dir == Direction::kBoth) {
      if (vertex_labels.find(label.src_label) != vertex_labels.end()) {
        mps[label.src_label].emplace_back(graph.GetGenericOutgoingGraphView(
            label.src_label, label.dst_label, label.edge_label));
      }
    }
    if (params.dir == Direction::kIn || params.dir == Direction::kBoth) {
      if (vertex_labels.find(label.dst_label) != vertex_labels.end()) {
        mps[label.dst_label].emplace_back(graph.GetGenericIncomingGraphView(
            label.dst_label, label.src_label, label.edge_label));
      }
    }
  }
  ValueColumnBuilder<int64_t> builder;
  Context ret;
  int64_t degree = 0;
  if (mps.empty()) {
    builder.push_back_opt(degree);
    ret.set(params.alias, builder.finish());
    return ret;
  }
  foreach_vertex(*vertex_col, [&](size_t index, label_t label, vid_t v) {
    if (v == graph.kInvalidVid) {
      return;
    }
    if (mps.count(label)) {
      for (auto& view : mps.at(label)) {
        auto es = view.get_edges(v);
        for (auto it = es.begin(); it != es.end(); ++it) {
          ++degree;
        }
      }
    }
  });

  builder.push_back_opt(degree);
  ret.set(params.alias, builder.finish());
  return ret;
}

template <typename CMP_T>
static neug::result<Context> expand_edge_with_special_edge_predicate_impl1(
    const StorageReadInterface& graph, Context&& ctx,
    const EdgeExpandParams& params, const SpecialPredicateConfig& config,
    const CMP_T& cmp_value) {
  auto input_col =
      std::dynamic_pointer_cast<IVertexColumn>(ctx.get(params.v_tag));
  if (input_col->is_optional() || params.is_optional) {
    LOG(ERROR) << "not support optional edge expand with predicate";
    RETURN_UNSUPPORTED_ERROR("not support optional edge expand");
  }
  auto input_vertex_labels_set = input_col->get_labels_set();
  std::vector<LabelTriplet> expected_labels;
  for (const auto& triplet : params.labels) {
    if ((params.dir == Direction::kOut || params.dir == Direction::kBoth) &&
        input_vertex_labels_set.count(triplet.src_label)) {
      expected_labels.emplace_back(triplet);
    } else if ((params.dir == Direction::kIn ||
                params.dir == Direction::kBoth) &&
               input_vertex_labels_set.count(triplet.dst_label)) {
      expected_labels.emplace_back(triplet);
    }
  }
  {
    std::sort(expected_labels.begin(), expected_labels.end());
    expected_labels.erase(
        std::unique(expected_labels.begin(), expected_labels.end()),
        expected_labels.end());
  }
  if (expected_labels.empty()) {
    MLVertexColumnBuilder builder;
    auto col = builder.finish();
    ctx.set_with_reshuffle(params.alias, col, {});
    return ctx;
  } else if (expected_labels.size() == 1) {
    SLEdgePropertyGetter<typename CMP_T::data_t> accessor(
        graph, expected_labels, config.property_name);
    EdgePropertyCmpPredicate<typename CMP_T::data_t,
                             SLEdgePropertyGetter<typename CMP_T::data_t>,
                             CMP_T>
        pred(accessor, cmp_value);
    return EdgeExpand::expand_edge(graph, std::move(ctx), params, pred);
  } else {
    MLEdgePropertyGetter<typename CMP_T::data_t> accessor(
        graph, expected_labels, config.property_name);
    EdgePropertyCmpPredicate<typename CMP_T::data_t,
                             MLEdgePropertyGetter<typename CMP_T::data_t>,
                             CMP_T>
        pred(accessor, cmp_value);
    return EdgeExpand::expand_edge(graph, std::move(ctx), params, pred);
  }
}

template <typename T>
static neug::result<Context> expand_edge_with_special_edge_predicate_impl0(
    const StorageReadInterface& graph, Context&& ctx,
    const EdgeExpandParams& params, const SpecialPredicateConfig& config,
    const Value& target_val) {
  T target = [&target_val]() -> T {
    if constexpr (std::is_same_v<T, std::string_view>) {
      return StringValue::Get(target_val);
    } else {
      return target_val.template GetValue<T>();
    }
  }();
  if (config.ptype == SPPredicateType::kPropertyGT) {
    GTCmp<T> target_cmp(target);
    return expand_edge_with_special_edge_predicate_impl1(
        graph, std::move(ctx), params, config, target_cmp);
  } else if (config.ptype == SPPredicateType::kPropertyLT) {
    LTCmp<T> target_cmp(target);
    return expand_edge_with_special_edge_predicate_impl1(
        graph, std::move(ctx), params, config, target_cmp);
  } else if (config.ptype == SPPredicateType::kPropertyEQ) {
    EQCmp<T> target_cmp(target);
    return expand_edge_with_special_edge_predicate_impl1(
        graph, std::move(ctx), params, config, target_cmp);
  } else if (config.ptype == SPPredicateType::kPropertyNE) {
    NECmp<T> target_cmp(target);
    return expand_edge_with_special_edge_predicate_impl1(
        graph, std::move(ctx), params, config, target_cmp);
  } else if (config.ptype == SPPredicateType::kPropertyLE) {
    LECmp<T> target_cmp(target);
    return expand_edge_with_special_edge_predicate_impl1(
        graph, std::move(ctx), params, config, target_cmp);
  } else {
    CHECK(config.ptype == SPPredicateType::kPropertyGE);
    GECmp<T> target_cmp(target);
    return expand_edge_with_special_edge_predicate_impl1(
        graph, std::move(ctx), params, config, target_cmp);
  }
}

neug::result<Context> EdgeExpand::expand_edge_with_special_edge_predicate(
    const StorageReadInterface& graph, Context&& ctx,
    const EdgeExpandParams& params, const SpecialPredicateConfig& config,
    const Value& target_val) {
  if (config.param_type == DataTypeId::kInt32) {
    return expand_edge_with_special_edge_predicate_impl0<int>(
        graph, std::move(ctx), params, config, target_val);
  } else if (config.param_type == DataTypeId::kInt64) {
    return expand_edge_with_special_edge_predicate_impl0<int64_t>(
        graph, std::move(ctx), params, config, target_val);
  } else if (config.param_type == DataTypeId::kTimestampMs) {
    return expand_edge_with_special_edge_predicate_impl0<DateTime>(
        graph, std::move(ctx), params, config, target_val);
  } else if (config.param_type == DataTypeId::kVarchar) {
    return expand_edge_with_special_edge_predicate_impl0<std::string_view>(
        graph, std::move(ctx), params, config, target_val);
  } else {
    LOG(ERROR) << "not support edge property type "
               << DataType(config.param_type).ToString();
    RETURN_UNSUPPORTED_ERROR("not support edge property type " +
                             DataType(config.param_type).ToString());
  }
}

template <typename T>
void expand_vertex_ep_cmp_impl(const StorageReadInterface& graph,
                               const SLVertexColumn& input_column,
                               MSVertexColumnBuilder& builder,
                               std::vector<size_t>& offsets,
                               label_t input_label, label_t nbr_label,
                               label_t edge_label, Direction dir,
                               const Value& cmp_value, SPPredicateType tp) {
  T cmp_val = [&cmp_value]() -> T {
    if constexpr (std::is_same_v<T, std::string_view>) {
      return StringValue::Get(cmp_value);
    } else {
      return cmp_value.template GetValue<T>();
    }
  }();

  auto view = (dir == Direction::kOut)
                  ? graph.GetGenericOutgoingGraphView(input_label, nbr_label,
                                                      edge_label)
                  : graph.GetGenericIncomingGraphView(input_label, nbr_label,
                                                      edge_label);
  auto& vertices = input_column.vertices();
  size_t vertex_num = vertices.size();
  auto ed_accessor = graph.GetEdgeDataAccessor(
      dir == Direction::kOut ? input_label : nbr_label,
      dir == Direction::kOut ? nbr_label : input_label, edge_label, 0);
  if (view.type() == CsrViewType::kMultipleMutable &&
      ed_accessor.is_bundled()) {
    auto typed_view =
        view.template get_typed_view<T, CsrViewType::kMultipleMutable>();
    if (tp == SPPredicateType::kPropertyGT) {
      for (size_t idx = 0; idx < vertex_num; ++idx) {
        vid_t v = vertices[idx];
        typed_view.foreach_nbr_gt(v, cmp_val, [&](vid_t nbr, const T& ed) {
          builder.push_back_opt(nbr);
          offsets.push_back(idx);
        });
      }
    } else {
      CHECK(tp == SPPredicateType::kPropertyLT);
      for (size_t idx = 0; idx < vertex_num; ++idx) {
        vid_t v = vertices[idx];
        typed_view.foreach_nbr_lt(v, cmp_val, [&](vid_t nbr, const T& ed) {
          builder.push_back_opt(nbr);
          offsets.push_back(idx);
        });
      }
    }
  } else {
    if (tp == SPPredicateType::kPropertyGT) {
      for (size_t idx = 0; idx < vertex_num; ++idx) {
        vid_t v = vertices[idx];
        auto es = view.get_edges(v);
        for (auto it = es.begin(); it != es.end(); ++it) {
          auto nbr = it.get_vertex();
          auto ed = ed_accessor.get_typed_data<T>(it);
          if (cmp_val < ed) {
            builder.push_back_opt(nbr);
            offsets.push_back(idx);
          }
        }
      }
    } else {
      CHECK(tp == SPPredicateType::kPropertyLT);
      for (size_t idx = 0; idx < vertex_num; ++idx) {
        vid_t v = vertices[idx];
        auto es = view.get_edges(v);
        for (auto it = es.begin(); it != es.end(); ++it) {
          auto nbr = it.get_vertex();
          auto ed = ed_accessor.get_typed_data<T>(it);
          if (ed < cmp_val) {
            builder.push_back_opt(nbr);
            offsets.push_back(idx);
          }
        }
      }
    }
  }
}

neug::result<Context> EdgeExpand::expand_vertex_ep_cmp(
    const StorageReadInterface& graph, Context&& ctx,
    const EdgeExpandParams& params, const Value& ep_val, SPPredicateType tp) {
  if (params.is_optional) {
    LOG(ERROR) << "not support optional edge expand";
    RETURN_UNSUPPORTED_ERROR("not support optional edge expand");
  }
  std::shared_ptr<IVertexColumn> input_vertex_list =
      std::dynamic_pointer_cast<IVertexColumn>(ctx.get(params.v_tag));
  VertexColumnType input_vertex_list_type =
      input_vertex_list->vertex_column_type();

  if (input_vertex_list_type == VertexColumnType::kSingle) {
    auto casted_input_vertex_list =
        std::dynamic_pointer_cast<SLVertexColumn>(input_vertex_list);
    label_t input_label = casted_input_vertex_list->label();
    std::vector<std::tuple<label_t, label_t, Direction>> label_dirs;
    for (auto& triplet : params.labels) {
      if (!graph.schema().exist(triplet.src_label, triplet.dst_label,
                                triplet.edge_label)) {
        continue;
      }
      if (triplet.src_label == input_label &&
          ((params.dir == Direction::kOut) ||
           (params.dir == Direction::kBoth))) {
        label_dirs.emplace_back(triplet.dst_label, triplet.edge_label,
                                Direction::kOut);
      }
      if (triplet.dst_label == input_label &&
          ((params.dir == Direction::kIn) ||
           (params.dir == Direction::kBoth))) {
        label_dirs.emplace_back(triplet.src_label, triplet.edge_label,
                                Direction::kIn);
      }
    }

    if (label_dirs.empty()) {
      MLVertexColumnBuilder builder;
      auto col = builder.finish();
      ctx.set_with_reshuffle(params.alias, col, {});
      return ctx;
    }
    {
      std::sort(label_dirs.begin(), label_dirs.end());
      label_dirs.erase(std::unique(label_dirs.begin(), label_dirs.end()),
                       label_dirs.end());
    }
    std::vector<DataTypeId> ed_types;
    for (auto& label_dir : label_dirs) {
      Direction dir = std::get<2>(label_dir);
      label_t nbr_label = std::get<0>(label_dir);
      label_t edge_label = std::get<1>(label_dir);

      auto properties = graph.schema().get_edge_properties(
          dir == Direction::kOut ? input_label : nbr_label,
          dir == Direction::kOut ? nbr_label : input_label, edge_label);

      if (properties.size() != 1) {
        LOG(ERROR) << "not support edge type";
        RETURN_UNSUPPORTED_ERROR("not support edge type");
      }
      auto pt = properties[0].id();
      if (pt != DataTypeId::kTimestampMs && pt != DataTypeId::kInt64 &&
          pt != DataTypeId::kInt32) {
        LOG(ERROR) << "not support edge type";
        RETURN_UNSUPPORTED_ERROR("not support edge type");
      }
      ed_types.push_back(pt);
    }
    MSVertexColumnBuilder builder(std::get<0>(label_dirs[0]));
    std::vector<size_t> offsets;
    size_t ld_idx = 0;
    for (auto& label_dir : label_dirs) {
      label_t nbr_label = std::get<0>(label_dir);
      label_t edge_label = std::get<1>(label_dir);
      Direction dir = std::get<2>(label_dir);
      DataTypeId pt = ed_types[ld_idx++];
      builder.start_label(nbr_label);
      if (pt == DataTypeId::kTimestampMs) {
        expand_vertex_ep_cmp_impl<DateTime>(
            graph, *casted_input_vertex_list, builder, offsets, input_label,
            nbr_label, edge_label, dir, ep_val, tp);
      } else if (pt == DataTypeId::kInt64) {
        expand_vertex_ep_cmp_impl<int64_t>(
            graph, *casted_input_vertex_list, builder, offsets, input_label,
            nbr_label, edge_label, dir, ep_val, tp);
      } else {
        CHECK(pt == DataTypeId::kInt32);
        expand_vertex_ep_cmp_impl<int32_t>(
            graph, *casted_input_vertex_list, builder, offsets, input_label,
            nbr_label, edge_label, dir, ep_val, tp);
      }
    }
    std::shared_ptr<IContextColumn> col = builder.finish();
    ctx.set_with_reshuffle(params.alias, col, offsets);
    return ctx;
  } else {
    LOG(ERROR) << "unexpected to reach here...";
    RETURN_UNSUPPORTED_ERROR("unexpected to reach here...");
  }
}

struct ExpandVertexSPOp {
  template <typename PRED_T>
  static neug::result<Context> eval_with_predicate(
      const PRED_T& pred, const StorageReadInterface& graph, Context&& ctx,
      const EdgeExpandParams& params) {
    return EdgeExpand::expand_vertex<EdgeNbrPredicate<PRED_T>>(
        graph, std::move(ctx), params, EdgeNbrPredicate(pred));
  }
};

neug::result<Context> EdgeExpand::expand_vertex_with_special_vertex_predicate(
    const StorageReadInterface& graph, Context&& ctx,
    const EdgeExpandParams& params, const SpecialPredicateConfig& config,
    const ParamsMap& query_params) {
  auto input_col =
      std::dynamic_pointer_cast<IVertexColumn>(ctx.get(params.v_tag));
  auto input_vertex_labels_set = input_col->get_labels_set();
  std::set<label_t> expected_labels;
  for (const auto& triplet : params.labels) {
    if ((params.dir == Direction::kOut || params.dir == Direction::kBoth) &&
        input_vertex_labels_set.count(triplet.src_label)) {
      expected_labels.insert(triplet.dst_label);
    } else if ((params.dir == Direction::kIn ||
                params.dir == Direction::kBoth) &&
               input_vertex_labels_set.count(triplet.dst_label)) {
      expected_labels.insert(triplet.src_label);
    }
  }
  return dispatch_vertex_predicate<ExpandVertexSPOp>(
      graph, expected_labels, config, query_params, graph, std::move(ctx),
      params);
}

}  // namespace execution

}  // namespace neug
