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

#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "neug/execution/common/context.h"
#include "neug/execution/utils/opr_timer.h"
#include "neug/generated/proto/plan/physical.pb.h"
#include "neug/storages/graph/graph_interface.h"
#include "neug/utils/result.h"

namespace gs {

namespace runtime {

class IOperator {
 public:
  virtual ~IOperator() = default;

  virtual std::string get_operator_name() const = 0;

  virtual gs::result<Context> Eval(
      IStorageInterface& graph,
      const std::map<std::string, std::string>& params, Context&& ctx,
      OprTimer* timer) = 0;
};

using OpBuildResultT = std::pair<std::unique_ptr<IOperator>, ContextMeta>;

class IOperatorBuilder {
 public:
  virtual ~IOperatorBuilder() = default;
  virtual gs::result<OpBuildResultT> Build(const gs::Schema& schema,
                                           const ContextMeta& ctx_meta,
                                           const physical::PhysicalPlan& plan,
                                           int op_idx) = 0;
  virtual int stepping(int i) { return i + GetOpKinds().size(); }

  virtual std::vector<physical::PhysicalOpr_Operator::OpKindCase> GetOpKinds()
      const = 0;
};

class IAdminOperatorBuilder {
 public:
  virtual ~IAdminOperatorBuilder() = default;
  virtual int stepping(int i) { return i + 1; }

  virtual gs::result<OpBuildResultT> Build(const Schema& schema,
                                           const ContextMeta& ctx_meta,
                                           const physical::AdminPlan& plan,
                                           int op_id) = 0;
  virtual physical::AdminPlan_Operator::KindCase GetOpKind() const = 0;
};

class ISchemaOperatorBuilder {
 public:
  virtual ~ISchemaOperatorBuilder() = default;
  virtual int stepping(int i) { return i + 1; }

  virtual gs::result<OpBuildResultT> Build(const Schema& schema,
                                           const ContextMeta& ctx_meta,
                                           const physical::DDLPlan& plan,
                                           int op_id) = 0;
  virtual physical::DDLPlan::PlanCase GetOpKind() const = 0;
};

}  // namespace runtime

}  // namespace gs
