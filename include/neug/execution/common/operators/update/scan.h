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

#ifndef EXECUTION_COMMON_OPERATORS_UPDATE_SCAN_H_
#define EXECUTION_COMMON_OPERATORS_UPDATE_SCAN_H_
#include "neug/execution/common/columns/vertex_columns.h"
#include "neug/execution/common/context.h"
#include "neug/execution/utils/params.h"
#include "neug/utils/leaf_utils.h"

namespace gs {
namespace runtime {
class UScan {
 public:
  template <typename PRED_T>
  static gs::result<Context> scan(const GraphUpdateInterface& graph,
                                  Context&& ctx, const ScanParams& params,
                                  const PRED_T& pred) {
    MLVertexColumnBuilder builder;
    for (auto& label : params.tables) {
      auto vit = graph.GetVertexIterator(label);
      for (; vit.IsValid(); vit.Next()) {
        if (pred(label, vit.GetIndex())) {
          builder.push_back_vertex(VertexRecord{label, vit.GetIndex()});
        }
      }
    }
    ctx.set(params.alias, builder.finish());
    return ctx;
  }
};

}  // namespace runtime
}  // namespace gs
#endif  // EXECUTION_COMMON_OPERATORS_UPDATE_SCAN_H_