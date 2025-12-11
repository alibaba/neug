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

#include <stddef.h>
#include <map>
#include <memory>
#include <string>

#include "neug/execution/utils/expr_impl.h"
#include "neug/execution/utils/var.h"
#include "neug/storages/graph/graph_interface.h"
#include "neug/utils/property/types.h"
#include "neug/utils/runtime/rt_any.h"

namespace common {
class Expression;
}  // namespace common

namespace gs {

namespace runtime {
class Context;
struct LabelTriplet;

class Expr {
 public:
  Expr(const StorageReadInterface* graph, const Context& ctx,
       const std::map<std::string, std::string>& params,
       const ::common::Expression& expr, VarType var_type) {
    expr_ = parse_expression(graph, ctx, params, expr, var_type);
    if (!expr_) {
      THROW_RUNTIME_ERROR(
          "Fail to create GeneralEdgePredicate: expr is null: " +
          expr.DebugString());
    }
  }

  RTAny eval_path(size_t idx, Arena&) const;
  RTAny eval_vertex(label_t label, vid_t v, size_t idx, Arena&) const;
  RTAny eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                  const void* data_ptr, size_t idx, Arena&) const;

  RTAnyType type() const;

  // for container such as list, set etc.
  RTAnyType elem_type() const { return expr_->elem_type(); }

  bool is_optional() const { return expr_->is_optional(); }

 private:
  std::unique_ptr<ExprBase> expr_;
};

}  // namespace runtime

}  // namespace gs
