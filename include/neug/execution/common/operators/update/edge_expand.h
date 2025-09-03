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

#ifndef RUNTIME_COMMON_OPERATORS_UPDATE_EDGE_H_
#define RUNTIME_COMMON_OPERATORS_UPDATE_EDGE_H_

#include "neug/execution/common/columns/edge_columns.h"
#include "neug/execution/common/columns/vertex_columns.h"
#include "neug/execution/common/context.h"
#include "neug/execution/common/graph_interface.h"
#include "neug/execution/utils/params.h"
#include "neug/utils/leaf_utils.h"

namespace gs {
namespace runtime {
class Context;
struct EdgeExpandParams;

class UEdgeExpand {
 public:
  static gs::result<Context> edge_expand_v_without_pred(
      const GraphUpdateInterface& graph, Context&& ctx,
      const EdgeExpandParams& params);

  static gs::result<Context> edge_expand_e_without_pred(
      const GraphUpdateInterface& graph, Context&& ctx,
      const EdgeExpandParams& params);
};
}  // namespace runtime
}  // namespace gs
#endif  // RUNTIME_COMMON_OPERATORS_UPDATE_EDGE_H_