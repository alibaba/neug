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
#ifndef EXECUTION_COMMON_OPERATORS_RETRIEVE_GET_V_H_
#define EXECUTION_COMMON_OPERATORS_RETRIEVE_GET_V_H_

#include "neug/execution/common/columns/edge_columns.h"
#include "neug/execution/common/columns/path_columns.h"
#include "neug/execution/common/columns/vertex_columns.h"
#include "neug/execution/common/context.h"
#include "neug/execution/utils/params.h"
#include "neug/execution/utils/predicates.h"
#include "neug/utils/leaf_utils.h"

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
  template <typename GraphInterface, typename PRED_T>
  static gs::result<Context> get_vertex_from_edges_optional_impl(
      const GraphInterface& graph, Context&& ctx, const GetVParams& params,
      const PRED_T& pred) {
    auto column = std::dynamic_pointer_cast<IEdgeColumn>(ctx.get(params.tag));
    if (column == nullptr) {
      LOG(ERROR) << "column is nullptr";
      RETURN_INVALID_ARGUMENT_ERROR("column is nullptr");
    }

    std::vector<size_t> shuffle_offset;
    if (column->edge_column_type() == EdgeColumnType::kBDSL) {
      MSVertexColumnBuilder builder(column->get_labels()[0].src_label);
      auto& input_edge_list =
          *std::dynamic_pointer_cast<OptionalBDSLEdgeColumn>(column);
      input_edge_list.foreach_edge([&](size_t index, const LabelTriplet& label,
                                       vid_t src, vid_t dst,
                                       const EdgeData& edata, Direction dir) {
        if (!input_edge_list.has_value(index)) {
          if (pred(label.src_label, src, index)) {
            builder.push_back_null();
            shuffle_offset.push_back(index);
          }
        } else {
          if (dir == Direction::kOut) {
            if (label.dst_label == params.tables[0]) {
              if (pred(label.dst_label, dst, index)) {
                builder.push_back_opt(dst);
                shuffle_offset.push_back(index);
              }
            }
          } else {
            if (label.src_label == params.tables[0]) {
              if (pred(label.src_label, src, index)) {
                builder.push_back_opt(src);
                shuffle_offset.push_back(index);
              }
            }
          }
        }
      });
      ctx.set_with_reshuffle(params.alias, builder.finish(), shuffle_offset);
      return ctx;
    } else if (column->edge_column_type() == EdgeColumnType::kSDSL) {
      label_t output_vertex_label{0};
      if (params.opt == VOpt::kEnd) {
        output_vertex_label = column->get_labels()[0].dst_label;
      } else {
        output_vertex_label = column->get_labels()[0].src_label;
      }
      MSVertexColumnBuilder builder(output_vertex_label);
      auto& input_edge_list =
          *std::dynamic_pointer_cast<OptionalSDSLEdgeColumn>(column);
      if (params.opt == VOpt::kEnd) {
        input_edge_list.foreach_edge(
            [&](size_t index, const LabelTriplet& label, vid_t src, vid_t dst,
                const EdgeData& edata, Direction dir) {
              if (!input_edge_list.has_value(index)) {
                if (pred(label.src_label, src, index)) {
                  builder.push_back_null();
                  shuffle_offset.push_back(index);
                }
              } else {
                if (label.dst_label == params.tables[0]) {
                  if (pred(label.dst_label, dst, index)) {
                    builder.push_back_opt(dst);
                    shuffle_offset.push_back(index);
                  }
                }
              }
            });
      }
      ctx.set_with_reshuffle(params.alias, builder.finish(), shuffle_offset);
      return ctx;
    }
    LOG(ERROR) << "Unsupported edge column type: "
               << static_cast<int>(column->edge_column_type());
    RETURN_UNSUPPORTED_ERROR(
        "Unsupported edge column type: " +
        std::to_string(static_cast<int>(column->edge_column_type())));
  }

  template <typename GraphInterface, typename PRED_T>
  static gs::result<Context> _get_vertex_from_path(const GraphInterface& graph,
                                                   Context&& ctx,
                                                   const GetVParams& params,
                                                   const PRED_T& pred) {
    std::vector<bool> required_label(graph.schema().vertex_label_num(), false);
    std::vector<size_t> shuffle_offset;
    auto col = ctx.get(params.tag);
    for (auto label : params.tables) {
      required_label[label] = true;
    }
    auto& input_path_list = *std::dynamic_pointer_cast<GeneralPathColumn>(col);

    MLVertexColumnBuilder builder;
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

  template <typename GraphInterface, typename PRED_T>
  static gs::result<Context> get_vertex_from_edges(const GraphInterface& graph,
                                                   Context&& ctx,
                                                   const GetVParams& params,
                                                   const PRED_T& pred) {
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
      return get_vertex_from_edges_optional_impl(graph, std::move(ctx), params,
                                                 pred);
    }

    if (column->edge_column_type() == EdgeColumnType::kSDSL) {
      auto& input_edge_list =
          *std::dynamic_pointer_cast<SDSLEdgeColumn>(column);
      label_t output_vertex_label{0};
      auto edge_label = input_edge_list.get_labels()[0];

      VOpt opt = params.opt;
      if (params.opt == VOpt::kOther) {
        if (input_edge_list.dir() == Direction::kOut) {
          opt = VOpt::kEnd;
        } else {
          opt = VOpt::kStart;
        }
      }
      if (opt == VOpt::kStart) {
        output_vertex_label = edge_label.src_label;
      } else if (opt == VOpt::kEnd) {
        output_vertex_label = edge_label.dst_label;
      } else {
        LOG(ERROR) << "not support GetV opt " << static_cast<int>(opt);
        RETURN_UNSUPPORTED_ERROR("not support GetV opt " +
                                 std::to_string(static_cast<int>(opt)));
      }
      // params tables size may be 0
      if (params.tables.size() == 1) {
        if (output_vertex_label != params.tables[0]) {
          LOG(ERROR) << "output_vertex_label != params.tables[0]"
                     << static_cast<int>(output_vertex_label) << " "
                     << static_cast<int>(params.tables[0]);
          RETURN_INVALID_ARGUMENT_ERROR(
              "output_vertex_label != params.tables[0]");
        }
      }

      MSVertexColumnBuilder builder(output_vertex_label);
      if (opt == VOpt::kStart) {
        input_edge_list.foreach_edge(
            [&](size_t index, const LabelTriplet& label, vid_t src, vid_t dst,
                const EdgeData& edata, Direction dir) {
              if (pred(label.src_label, src, index)) {
                builder.push_back_opt(src);
                shuffle_offset.push_back(index);
              }
            });
      } else if (opt == VOpt::kEnd) {
        input_edge_list.foreach_edge(
            [&](size_t index, const LabelTriplet& label, vid_t src, vid_t dst,
                const EdgeData& edata, Direction dir) {
              if (pred(label.dst_label, dst, index)) {
                builder.push_back_opt(dst);
                shuffle_offset.push_back(index);
              }
            });
      }
      ctx.set_with_reshuffle(params.alias, builder.finish(), shuffle_offset);
      return ctx;
    } else if (column->edge_column_type() == EdgeColumnType::kSDML) {
      auto& input_edge_list =
          *std::dynamic_pointer_cast<SDMLEdgeColumn>(column);
      VOpt opt = params.opt;
      if (params.opt == VOpt::kOther) {
        if (input_edge_list.dir() == Direction::kOut) {
          opt = VOpt::kEnd;
        } else {
          opt = VOpt::kStart;
        }
      }

      auto labels =
          extract_labels(input_edge_list.get_labels(), params.tables, opt);
      if (labels.size() == 0) {
        MLVertexColumnBuilder builder;
        ctx.set_with_reshuffle(params.alias, builder.finish(), {});
        return ctx;
      }
      if (labels.size() > 1) {
        MLVertexColumnBuilder builder;
        if (opt == VOpt::kStart) {
          input_edge_list.foreach_edge(
              [&](size_t index, const LabelTriplet& label, vid_t src, vid_t dst,
                  const EdgeData& edata, Direction dir) {
                if (std::find(labels.begin(), labels.end(), label.src_label) !=
                    labels.end()) {
                  if (pred(label.src_label, src, index)) {
                    builder.push_back_vertex({label.src_label, src});
                    shuffle_offset.push_back(index);
                  }
                }
              });
        } else if (opt == VOpt::kEnd) {
          input_edge_list.foreach_edge(
              [&](size_t index, const LabelTriplet& label, vid_t src, vid_t dst,
                  const EdgeData& edata, Direction dir) {
                if (std::find(labels.begin(), labels.end(), label.dst_label) !=
                    labels.end()) {
                  if (pred(label.dst_label, dst, index)) {
                    builder.push_back_vertex({label.dst_label, dst});
                    shuffle_offset.push_back(index);
                  }
                }
              });
        }
        ctx.set_with_reshuffle(params.alias, builder.finish(), shuffle_offset);
        return ctx;
      }
    } else if (column->edge_column_type() == EdgeColumnType::kBDSL) {
      auto& input_edge_list =
          *std::dynamic_pointer_cast<BDSLEdgeColumn>(column);
      if (params.tables.size() == 0) {
        auto type = input_edge_list.get_labels()[0];
        if (type.src_label != type.dst_label) {
          MLVertexColumnBuilder builder;
          if (params.opt != VOpt::kOther) {
            LOG(ERROR) << "not support GetV opt "
                       << static_cast<int>(params.opt);
            RETURN_UNSUPPORTED_ERROR(
                "not support GetV opt " +
                std::to_string(static_cast<int>(params.opt)));
          }
          input_edge_list.foreach_edge(
              [&](size_t index, const LabelTriplet& label, vid_t src, vid_t dst,
                  const EdgeData& edata, Direction dir) {
                if (dir == Direction::kOut) {
                  if (pred(label.dst_label, dst, index)) {
                    builder.push_back_vertex({label.dst_label, dst});
                    shuffle_offset.push_back(index);
                  }
                } else {
                  if (pred(label.src_label, src, index)) {
                    builder.push_back_vertex({label.src_label, src});
                    shuffle_offset.push_back(index);
                  }
                }
              });
          ctx.set_with_reshuffle(params.alias, builder.finish(),
                                 shuffle_offset);
          return ctx;
        } else {
          MSVertexColumnBuilder builder(type.src_label);
          input_edge_list.foreach_edge(
              [&](size_t index, const LabelTriplet& label, vid_t src, vid_t dst,
                  const EdgeData& edata, Direction dir) {
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
              });
          ctx.set_with_reshuffle(params.alias, builder.finish(),
                                 shuffle_offset);
          return ctx;
        }
      } else {
        std::vector<label_t> labels;
        auto type = input_edge_list.get_labels()[0];
        for (auto& label : params.tables) {
          if (label == type.src_label || label == type.dst_label) {
            labels.push_back(label);
          }
        }
        if (labels.size() == 1) {
          MSVertexColumnBuilder builder(labels[0]);
          input_edge_list.foreach_edge(
              [&](size_t index, const LabelTriplet& label, vid_t src, vid_t dst,
                  const EdgeData& edata, Direction dir) {
                if (dir == Direction::kOut) {
                  if (label.dst_label == labels[0]) {
                    if (pred(label.dst_label, dst, index)) {
                      builder.push_back_opt(dst);
                      shuffle_offset.push_back(index);
                    }
                  }
                } else {
                  if (label.src_label == labels[0]) {
                    if (pred(label.src_label, src, index)) {
                      builder.push_back_opt(src);
                      shuffle_offset.push_back(index);
                    }
                  }
                }
              });
          ctx.set_with_reshuffle(params.alias, builder.finish(),
                                 shuffle_offset);
          return ctx;
        } else {
          MLVertexColumnBuilder builder;
          input_edge_list.foreach_edge(
              [&](size_t index, const LabelTriplet& label, vid_t src, vid_t dst,
                  const EdgeData& edata, Direction dir) {
                if (dir == Direction::kOut) {
                  if (std::find(labels.begin(), labels.end(),
                                label.dst_label) != labels.end()) {
                    if (pred(label.dst_label, dst, index)) {
                      builder.push_back_vertex({label.dst_label, dst});
                      shuffle_offset.push_back(index);
                    }
                  }
                } else {
                  if (std::find(labels.begin(), labels.end(),
                                label.src_label) != labels.end()) {
                    if (pred(label.src_label, src, index)) {
                      builder.push_back_vertex({label.src_label, src});
                      shuffle_offset.push_back(index);
                    }
                  }
                }
              });
          ctx.set_with_reshuffle(params.alias, builder.finish(),
                                 shuffle_offset);
          return ctx;
        }
      }
    } else if (column->edge_column_type() == EdgeColumnType::kBDML) {
      auto& input_edge_list =
          *std::dynamic_pointer_cast<BDMLEdgeColumn>(column);
      if (params.tables.size() == 0) {
        MLVertexColumnBuilder builder;
        if (params.opt != VOpt::kOther) {
          LOG(ERROR) << "not support GetV opt " << static_cast<int>(params.opt);
          RETURN_UNSUPPORTED_ERROR(
              "not support GetV opt " +
              std::to_string(static_cast<int>(params.opt)));
        }
        input_edge_list.foreach_edge(
            [&](size_t index, const LabelTriplet& label, vid_t src, vid_t dst,
                const EdgeData& edata, Direction dir) {
              if (dir == Direction::kOut) {
                if (pred(label.dst_label, dst, index)) {
                  builder.push_back_vertex({label.dst_label, dst});
                  shuffle_offset.push_back(index);
                }
                builder.push_back_vertex({label.dst_label, dst});
              } else {
                if (pred(label.src_label, src, index)) {
                  builder.push_back_vertex({label.src_label, src});
                  shuffle_offset.push_back(index);
                }
              }
            });

        ctx.set_with_reshuffle(params.alias, builder.finish(), shuffle_offset);
        return ctx;
      } else {
        if (params.tables.size() == 1) {
          auto vlabel = params.tables[0];
          MSVertexColumnBuilder builder(vlabel);
          input_edge_list.foreach_edge(
              [&](size_t index, const LabelTriplet& label, vid_t src, vid_t dst,
                  const EdgeData& edata, Direction dir) {
                if (dir == Direction::kOut) {
                  if (label.dst_label == vlabel) {
                    if (pred(label.dst_label, dst, index)) {
                      builder.push_back_opt(dst);
                      shuffle_offset.push_back(index);
                    }
                  }
                } else {
                  if (label.src_label == vlabel) {
                    if (pred(label.src_label, src, index)) {
                      builder.push_back_opt(src);
                      shuffle_offset.push_back(index);
                    }
                  }
                }
              });
          ctx.set_with_reshuffle(params.alias, builder.finish(),
                                 shuffle_offset);

        } else {
          std::vector<bool> labels(graph.schema().vertex_label_num(), false);
          for (auto& label : params.tables) {
            labels[label] = true;
          }
          MLVertexColumnBuilder builder;
          input_edge_list.foreach_edge(
              [&](size_t index, const LabelTriplet& label, vid_t src, vid_t dst,
                  const EdgeData& edata, Direction dir) {
                if (dir == Direction::kOut) {
                  if (labels[label.dst_label]) {
                    if (pred(label.dst_label, dst, index)) {
                      builder.push_back_vertex({label.dst_label, dst});
                      shuffle_offset.push_back(index);
                    }
                  }
                } else {
                  if (labels[label.src_label]) {
                    if (pred(label.src_label, src, index)) {
                      builder.push_back_vertex({label.src_label, src});
                      shuffle_offset.push_back(index);
                    }
                  }
                }
              });
          ctx.set_with_reshuffle(params.alias, builder.finish(),
                                 shuffle_offset);
        }
        return ctx;
      }
    }

    LOG(ERROR) << "Unsupported edge column type: "
               << static_cast<int>(column->edge_column_type());
    RETURN_UNSUPPORTED_ERROR(
        "Unsupported edge column type: " +
        std::to_string(static_cast<int>(column->edge_column_type())));
  }

  template <typename PRED_T>
  static gs::result<Context> get_vertex_from_vertices(
      const GraphReadInterface& graph, Context&& ctx, const GetVParams& params,
      const PRED_T& pred) {
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
        MLVertexColumnBuilder builder;
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

struct GeneralVertexPredicateWrapper {
  GeneralVertexPredicateWrapper(const GeneralVertexPredicate& pred)
      : pred_(pred) {}

  inline bool operator()(label_t label, vid_t v, size_t path_idx) const {
    return pred_(label, v, path_idx, arena_);
  }
  mutable Arena arena_;

  const GeneralVertexPredicate& pred_;
};

}  // namespace runtime

}  // namespace gs

#endif  // EXECUTION_COMMON_OPERATORS_RETRIEVE_GET_V_H_