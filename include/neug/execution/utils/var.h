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
#include <memory>
#include <string>

#include "neug/execution/common/accessors.h"
#include "neug/execution/common/context.h"
#include "neug/generated/proto/plan/common.pb.h"
#include "neug/generated/proto/plan/expr.pb.h"
#include "neug/storages/graph/graph_interface.h"
#include "neug/utils/property/types.h"
#include "neug/utils/runtime/rt_any.h"

namespace common {
class Variable;
}  // namespace common

namespace gs {

namespace runtime {
class Context;
class IContextColumnBuilder;
struct LabelTriplet;

enum class VarType {
  kVertexVar,
  kEdgeVar,
  kPathVar,
};

class VarGetterBase {
 public:
  virtual ~VarGetterBase() = default;
  virtual RTAny eval_path(size_t idx) const = 0;
  virtual RTAny eval_vertex(label_t label, vid_t v, size_t idx) const = 0;
  virtual RTAny eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                          const void* data_ptr, size_t idx) const = 0;
  virtual std::string name() const = 0;
};

class Var {
 public:
  static bool graph_related_var(const common::Variable& pb);

  Var(const StorageReadInterface* graph, const Context& ctx,
      const common::Variable& pb, VarType var_type);
  ~Var();

  RTAny get(size_t path_idx) const;
  RTAny get_vertex(label_t label, vid_t v) const;
  RTAny get_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                 const void* data_ptr) const;

  const DataType& type() const;
  bool is_optional() const { return getter_->is_optional(); }

 private:
  std::shared_ptr<IAccessor> getter_;
  DataType type_;
};

}  // namespace runtime

}  // namespace gs
