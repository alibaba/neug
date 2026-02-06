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
#include <memory>
#include "neug/common/types.h"
#include "neug/execution/common/context.h"
#include "neug/execution/common/params_map.h"
#include "neug/execution/common/types/value.h"
#include "neug/storages/graph/graph_interface.h"

namespace neug {
namespace execution {

class BindedExprBase;

enum class VarType {
  kVertex,
  kEdge,
  kRecord,
};

class VertexExprBase;
class EdgeExprBase;
class RecordExprBase;
class ExprBase {
 public:
  virtual ~ExprBase() = default;
  virtual const DataType& type() const = 0;
  virtual std::unique_ptr<BindedExprBase> bind(
      const IStorageInterface* storage, const ParamsMap& params) const = 0;
  virtual std::string name() const { return "unnamed_expr"; }
};

class BindedExprBase {
 public:
  virtual ~BindedExprBase() = default;
  virtual const DataType& type() const = 0;

  template <typename TARGET>
  TARGET& Cast() {
    if constexpr (std::is_same_v<TARGET, VertexExprBase>) {
      assert(vertex_ptr_ != nullptr);
      return *vertex_ptr_;
    } else if constexpr (std::is_same_v<TARGET, EdgeExprBase>) {
      assert(edge_ptr_ != nullptr);
      return *edge_ptr_;
    } else if constexpr (std::is_same_v<TARGET, RecordExprBase>) {
      assert(record_ptr_ != nullptr);
      return *record_ptr_;
    } else {
      static_assert(sizeof(TARGET) == 0, "Unsupported cast type");
    }
  }

  template <typename TARGET>
  const TARGET& Cast() const {
    if constexpr (std::is_same_v<TARGET, VertexExprBase>) {
      assert(vertex_ptr_ != nullptr);
      return *vertex_ptr_;
    } else if constexpr (std::is_same_v<TARGET, EdgeExprBase>) {
      assert(edge_ptr_ != nullptr);
      return *edge_ptr_;
    } else if constexpr (std::is_same_v<TARGET, RecordExprBase>) {
      assert(record_ptr_ != nullptr);
      return *record_ptr_;
    } else {
      static_assert(sizeof(TARGET) == 0, "Unsupported cast type");
    }
  }

 protected:
  VertexExprBase* vertex_ptr_;
  EdgeExprBase* edge_ptr_;
  RecordExprBase* record_ptr_;
};

class VertexExprBase : public virtual BindedExprBase {
 public:
  VertexExprBase() { vertex_ptr_ = this; }
  virtual ~VertexExprBase() = default;
  virtual Value eval_vertex(label_t v_label, vid_t v_id) const = 0;
};

class EdgeExprBase : public virtual BindedExprBase {
 public:
  EdgeExprBase() { edge_ptr_ = this; }
  virtual ~EdgeExprBase() = default;
  virtual Value eval_edge(const LabelTriplet&, vid_t src, vid_t dst,
                          const void*) const = 0;
};

class RecordExprBase : public virtual BindedExprBase {
 public:
  RecordExprBase() { record_ptr_ = this; }
  virtual ~RecordExprBase() = default;
  virtual Value eval_record(const Context& ctx, size_t idx) const = 0;
};

std::unique_ptr<ExprBase> parse_expression(const ::common::Expression& expr,
                                           const ContextMeta& ctx_meta,
                                           VarType var_type);

}  // namespace execution
}  // namespace neug