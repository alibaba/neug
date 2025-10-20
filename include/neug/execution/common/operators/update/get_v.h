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

#ifndef INCLUDE_NEUG_EXECUTION_COMMON_OPERATORS_UPDATE_GET_V_H_
#define INCLUDE_NEUG_EXECUTION_COMMON_OPERATORS_UPDATE_GET_V_H_

#include <vector>

#include "neug/execution/common/columns/edge_columns.h"
#include "neug/execution/common/context.h"
#include "neug/execution/utils/params.h"
#include "neug/utils/result.h"

namespace gs {
namespace runtime {
class UGetV {
 public:
  template <typename PRED_T>
  static gs::result<Context> get_vertex_from_vertices(
      const GraphUpdateInterface& graph, Context&& ctx,
      const GetVParams& params, const PRED_T& pred) {
    auto col = ctx.get(params.tag);
    std::vector<size_t> shuffle_offsets;
    if (col->column_type() != ContextColumnType::kVertex) {
      LOG(ERROR) << "current only support vertex column" << col->column_info();
      RETURN_INVALID_ARGUMENT_ERROR("current only support vertex column");
    }
    const auto input_vertex_list =
        dynamic_cast<const IVertexColumn*>(col.get());
    MLVertexColumnBuilderOpt builder(input_vertex_list->get_labels_set());
    foreach_vertex(*input_vertex_list,
                   [&](size_t index, label_t label, vid_t v) {
                     if (pred(label, v, index)) {
                       builder.push_back_vertex(VertexRecord{label, v});
                       shuffle_offsets.push_back(index);
                     }
                   });
    ctx.set_with_reshuffle(params.alias, builder.finish(), shuffle_offsets);
    return ctx;
  }
};

}  // namespace runtime
}  // namespace gs

#endif  // INCLUDE_NEUG_EXECUTION_COMMON_OPERATORS_UPDATE_GET_V_H_
