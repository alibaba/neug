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
#ifndef INCLUDE_NEUG_EXECUTION_COMMON_OPERATORS_RETRIEVE_GET_V_H_
#define INCLUDE_NEUG_EXECUTION_COMMON_OPERATORS_RETRIEVE_GET_V_H_

#include <limits>
#include <memory>
#include <set>
#include <utility>
#include <vector>

#include "neug/execution/common/columns/edge_columns.h"
#include "neug/execution/common/columns/path_columns.h"
#include "neug/execution/common/columns/vertex_columns.h"
#include "neug/execution/common/context.h"
#include "neug/execution/utils/params.h"
#include "neug/execution/utils/predicates.h"
#include "neug/utils/result.h"

namespace gs {
namespace runtime {

inline std::vector<label_t> extract_labels(
    const std::vector<LabelTriplet>& labels, const std::vector<label_t>& tables,
    VOpt opt) {
  std::vector<label_t> output_labels;
  for (const auto& label : labels) {
    if (opt == VOpt::kStart) {
      if (std::find(tables.begin(), tables.end(), label.src_label) !=
              tables.end() ||
          tables.empty()) {
        output_labels.push_back(label.src_label);
      }
    } else if (opt == VOpt::kEnd) {
      if (std::find(tables.begin(), tables.end(), label.dst_label) !=
              tables.end() ||
          tables.empty()) {
        output_labels.push_back(label.dst_label);
      }
    } else {
      LOG(ERROR) << "not support" << static_cast<int>(opt);
    }
  }
  return output_labels;
}
class GetV {
 public:
  template <typename PRED_T, bool is_optional = false>
  static std::pair<std::shared_ptr<IContextColumn>, std::vector<size_t>>
  get_vertex_from_edges_impl(const IEdgeColumn& input_edge_list,
                             const GetVParams& params, const PRED_T& pred) {
    auto labels = input_edge_list.get_labels();
    auto input_dir = input_edge_list.dir();
    auto v_opt = params.opt;

    std::vector<std::pair<LabelTriplet, Direction>> containing;
    if (input_dir == Direction::kBoth) {
      for (auto& label : labels) {
        containing.emplace_back(label, Direction::kOut);
        containing.emplace_back(label, Direction::kIn);
      }
    } else {
      for (auto& label : labels) {
        containing.emplace_back(label, input_dir);
      }
    }

    if (v_opt == VOpt::kOther) {
      if (input_dir == Direction::kOut) {
        v_opt = VOpt::kEnd;
      } else if (input_dir == Direction::kIn) {
        v_opt = VOpt::kStart;
      }
    }

    std::set<label_t> keep_set;
    for (auto table : params.tables) {
      keep_set.insert(table);
    }

    std::vector<std::pair<LabelTriplet, Direction>> to_filter;
    std::set<label_t> output_labels;
    if (v_opt == VOpt::kOther) {
      CHECK(input_dir == Direction::kBoth);
      for (auto& pair : containing) {
        if (pair.second == Direction::kOut) {
          if (keep_set.find(pair.first.dst_label) == keep_set.end()) {
            if constexpr (!is_optional) {
              to_filter.push_back(pair);
            }
          } else {
            output_labels.insert(pair.first.dst_label);
          }
        } else {
          if (keep_set.find(pair.first.src_label) == keep_set.end()) {
            if constexpr (!is_optional) {
              to_filter.push_back(pair);
            }
          } else {
            output_labels.insert(pair.first.src_label);
          }
        }
      }
      if (output_labels.size() == 0) {
        MLVertexColumnBuilder builder;
        return {builder.finish(), {}};
      } else if (output_labels.size() == 1) {
        MSVertexColumnBuilder builder(*output_labels.begin());
        std::vector<size_t> shuffle_offset;
        foreach_edge(
            input_edge_list,
            [&](size_t index, const LabelTriplet& label, Direction dir,
                vid_t src, vid_t dst, const void* data_ptr) {
              if constexpr (is_optional) {
                if (src == std::numeric_limits<vid_t>::max() ||
                    dst == std::numeric_limits<vid_t>::max()) {
                  builder.push_back_null();
                  shuffle_offset.push_back(index);
                  return;
                }
              }
              if (dir == Direction::kOut) {
                if (pred(label.dst_label, dst, index)) {
                  builder.push_back_opt(dst);
                  shuffle_offset.push_back(index);
                }
              } else {
                if (pred(label.src_label, src, index)) {
                  builder.push_back_opt(src);
                  shuffle_offset.push_back(index);
                }
              }
            },
            to_filter);
        return {builder.finish(), std::move(shuffle_offset)};
      } else {
        MLVertexColumnBuilderOpt builder(output_labels);
        std::vector<size_t> shuffle_offset;
        foreach_edge(
            input_edge_list,
            [&](size_t index, const LabelTriplet& label, Direction dir,
                vid_t src, vid_t dst, const void* data_ptr) {
              if constexpr (is_optional) {
                if (src == std::numeric_limits<vid_t>::max() ||
                    dst == std::numeric_limits<vid_t>::max()) {
                  builder.push_back_null();
                  shuffle_offset.push_back(index);
                  return;
                }
              }
              if (dir == Direction::kOut) {
                if (pred(label.dst_label, dst, index)) {
                  builder.push_back_opt({label.dst_label, dst});
                  shuffle_offset.push_back(index);
                }
              } else {
                if (pred(label.src_label, src, index)) {
                  builder.push_back_opt({label.src_label, src});
                  shuffle_offset.push_back(index);
                }
              }
            },
            to_filter);
        return {builder.finish(), std::move(shuffle_offset)};
      }
    } else if (v_opt == VOpt::kStart) {
      for (auto& pair : containing) {
        if (keep_set.find(pair.first.src_label) == keep_set.end()) {
          if constexpr (!is_optional) {
            to_filter.push_back(pair);
          }
        } else {
          output_labels.insert(pair.first.src_label);
        }
      }
      if (output_labels.size() == 0) {
        MLVertexColumnBuilder builder;
        return {builder.finish(), {}};
      } else if (output_labels.size() == 1) {
        MSVertexColumnBuilder builder(*output_labels.begin());
        std::vector<size_t> shuffle_offset;
        foreach_edge(
            input_edge_list,
            [&](size_t index, const LabelTriplet& label, Direction dir,
                vid_t src, vid_t dst, const void* data_ptr) {
              if constexpr (is_optional) {
                if (src == std::numeric_limits<vid_t>::max() ||
                    dst == std::numeric_limits<vid_t>::max()) {
                  builder.push_back_null();
                  shuffle_offset.push_back(index);
                  return;
                }
              }
              if (pred(label.src_label, src, index)) {
                builder.push_back_opt(src);
                shuffle_offset.push_back(index);
              }
            },
            to_filter);
        return {builder.finish(), std::move(shuffle_offset)};
      } else {
        MLVertexColumnBuilderOpt builder(output_labels);
        std::vector<size_t> shuffle_offset;
        foreach_edge(
            input_edge_list,
            [&](size_t index, const LabelTriplet& label, Direction dir,
                vid_t src, vid_t dst, const void* data_ptr) {
              if constexpr (is_optional) {
                if (src == std::numeric_limits<vid_t>::max() ||
                    dst == std::numeric_limits<vid_t>::max()) {
                  builder.push_back_null();
                  shuffle_offset.push_back(index);
                  return;
                }
              }
              if (pred(label.src_label, src, index)) {
                builder.push_back_opt({label.src_label, src});
                shuffle_offset.push_back(index);
              }
            },
            to_filter);
        return {builder.finish(), std::move(shuffle_offset)};
      }
    } else if (v_opt == VOpt::kEnd) {
      for (auto& pair : containing) {
        if (keep_set.find(pair.first.dst_label) == keep_set.end()) {
          if constexpr (!is_optional) {
            to_filter.push_back(pair);
          }
        } else {
          output_labels.insert(pair.first.dst_label);
        }
      }
      if (output_labels.size() == 0) {
        MLVertexColumnBuilder builder;
        return {builder.finish(), {}};
      } else if (output_labels.size() == 1) {
        MSVertexColumnBuilder builder(*output_labels.begin());
        std::vector<size_t> shuffle_offset;
        foreach_edge(
            input_edge_list,
            [&](size_t index, const LabelTriplet& label, Direction dir,
                vid_t src, vid_t dst, const void* data_ptr) {
              if constexpr (is_optional) {
                if (src == std::numeric_limits<vid_t>::max() ||
                    dst == std::numeric_limits<vid_t>::max()) {
                  builder.push_back_null();
                  shuffle_offset.push_back(index);
                  return;
                }
              }
              if (pred(label.dst_label, dst, index)) {
                builder.push_back_opt(dst);
                shuffle_offset.push_back(index);
              }
            },
            to_filter);
        return {builder.finish(), std::move(shuffle_offset)};
      } else {
        MLVertexColumnBuilderOpt builder(output_labels);
        std::vector<size_t> shuffle_offset;
        foreach_edge(
            input_edge_list,
            [&](size_t index, const LabelTriplet& label, Direction dir,
                vid_t src, vid_t dst, const void* data_ptr) {
              if constexpr (is_optional) {
                if (src == std::numeric_limits<vid_t>::max() ||
                    dst == std::numeric_limits<vid_t>::max()) {
                  builder.push_back_null();
                  shuffle_offset.push_back(index);
                  return;
                }
              }
              if (pred(label.dst_label, dst, index)) {
                builder.push_back_opt({label.dst_label, dst});
                shuffle_offset.push_back(index);
              }
            },
            to_filter);
        return {builder.finish(), std::move(shuffle_offset)};
      }
    } else {
      LOG(ERROR) << "not support GetV opt " << static_cast<int>(v_opt);
      return {nullptr, {}};
    }
  }

  template <typename PRED_T>
  static gs::result<Context> _get_vertex_from_path(
      const StorageReadInterface& graph, Context&& ctx,
      const GetVParams& params, const PRED_T& pred) {
    std::vector<bool> required_label(graph.schema().vertex_label_num(), false);
    std::vector<size_t> shuffle_offset;
    auto col = ctx.get(params.tag);
    std::set<label_t> required_label_set;
    for (auto label : params.tables) {
      required_label[label] = true;
      required_label_set.insert(label);
    }
    auto& input_path_list = *std::dynamic_pointer_cast<GeneralPathColumn>(col);

    MLVertexColumnBuilderOpt builder(required_label_set);
    input_path_list.foreach_path([&](size_t index, const Path& path) {
      auto [label, vid] = path.get_end();

      if (required_label[label] && pred(label, vid, index)) {
        builder.push_back_vertex({label, vid});
        shuffle_offset.push_back(index);
      }
    });
    ctx.set_with_reshuffle(params.alias, builder.finish(), shuffle_offset);
    return ctx;
  }

  template <typename PRED_T>
  static gs::result<Context> get_vertex_from_edges(
      const StorageReadInterface& graph, Context&& ctx,
      const GetVParams& params, const PRED_T& pred) {
    std::vector<size_t> shuffle_offset;
    auto col = ctx.get(params.tag);
    if (col->column_type() == ContextColumnType::kPath) {
      return _get_vertex_from_path(graph, std::move(ctx), params, pred);
    }
    auto column = std::dynamic_pointer_cast<IEdgeColumn>(ctx.get(params.tag));
    if (!column) {
      LOG(ERROR) << "Unsupported column type: "
                 << static_cast<int>(col->column_type());
      RETURN_UNSUPPORTED_ERROR(
          "Unsupported column type: " +
          std::to_string(static_cast<int>(col->column_type())));
    }

    if (column->is_optional()) {
      auto pair =
          get_vertex_from_edges_impl<PRED_T, true>(*column, params, pred);
      ctx.set_with_reshuffle(params.alias, pair.first, pair.second);
      return ctx;
    } else {
      auto pair =
          get_vertex_from_edges_impl<PRED_T, false>(*column, params, pred);
      ctx.set_with_reshuffle(params.alias, pair.first, pair.second);
      return ctx;
    }
  }

  template <typename PRED_T>
  static gs::result<Context> get_vertex_from_vertices(
      const StorageReadInterface& graph, Context&& ctx,
      const GetVParams& params, const PRED_T& pred) {
    std::shared_ptr<IVertexColumn> input_vertex_list_ptr =
        std::dynamic_pointer_cast<IVertexColumn>(ctx.get(params.tag));
    const IVertexColumn& input_vertex_list = *input_vertex_list_ptr;

    std::vector<size_t> offset;
    if (params.tag == params.alias) {
      foreach_vertex(input_vertex_list,
                     [&](size_t idx, label_t label, vid_t v) {
                       if (pred(label, v, idx)) {
                         offset.push_back(idx);
                       }
                     });
      ctx.reshuffle(offset);
    } else {
      const std::set<label_t>& label_set = input_vertex_list.get_labels_set();
      if (label_set.size() == 1) {
        MSVertexColumnBuilder builder(*label_set.begin());
        foreach_vertex(input_vertex_list,
                       [&](size_t idx, label_t label, vid_t v) {
                         if (pred(label, v, idx)) {
                           builder.push_back_opt(v);
                           offset.push_back(idx);
                         }
                       });
        ctx.set_with_reshuffle(params.alias, builder.finish(), offset);

      } else {
        MLVertexColumnBuilderOpt builder(input_vertex_list.get_labels_set());
        foreach_vertex(input_vertex_list,
                       [&](size_t idx, label_t label, vid_t v) {
                         if (pred(label, v, idx)) {
                           builder.push_back_vertex({label, v});
                           offset.push_back(idx);
                         }
                       });
        ctx.set_with_reshuffle(params.alias, builder.finish(), offset);
      }
    }
    return ctx;
  }
};

}  // namespace runtime

}  // namespace gs

#endif  // INCLUDE_NEUG_EXECUTION_COMMON_OPERATORS_RETRIEVE_GET_V_H_
