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

#ifndef INCLUDE_NEUG_EXECUTION_COMMON_OPERATORS_UPDATE_SCAN_H_
#define INCLUDE_NEUG_EXECUTION_COMMON_OPERATORS_UPDATE_SCAN_H_

#include "neug/execution/common/columns/vertex_columns.h"
#include "neug/execution/common/context.h"
#include "neug/execution/utils/params.h"
#include "neug/utils/result.h"

namespace gs {
namespace runtime {
class UScan {
 public:
  template <typename PRED_T>
  static gs::result<Context> scan(const StorageUpdateInterface& graph,
                                  Context&& ctx, const ScanParams& params,
                                  const PRED_T& pred) {
    MSVertexColumnBuilder builder(params.tables[0]);
    for (auto& label : params.tables) {
      builder.start_label(label);
      auto vertices = graph.GetVertexSet(label);
      for (auto vit : vertices) {
        if (pred(label, vit)) {
          builder.push_back_opt(vit);
        }
      }
    }
    ctx.set(params.alias, builder.finish());
    return ctx;
  }
};

}  // namespace runtime
}  // namespace gs

#endif  // INCLUDE_NEUG_EXECUTION_COMMON_OPERATORS_UPDATE_SCAN_H_
