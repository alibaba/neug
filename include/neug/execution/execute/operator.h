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

#ifndef INCLUDE_NEUG_EXECUTION_EXECUTE_OPERATOR_H_
#define INCLUDE_NEUG_EXECUTION_EXECUTE_OPERATOR_H_

#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "neug/execution/common/context.h"
#include "neug/execution/common/graph_interface.h"
#include "neug/execution/utils/opr_timer.h"
#include "neug/utils/result.h"
#ifdef USE_SYSTEM_PROTOBUF
#include "neug/generated/proto/plan/physical.pb.h"
#else
#include "neug/utils/proto/plan/physical.pb.h"
#endif

namespace gs {

namespace runtime {
class IUpdateOperator {
 public:
  virtual ~IUpdateOperator() = default;

  virtual std::string get_operator_name() const = 0;

  virtual gs::result<Context> Eval(
      GraphUpdateInterface& graph,
      const std::map<std::string, std::string>& params, Context&& ctx,
      OprTimer* timer) = 0;
};
class IReadOperator {
 public:
  virtual ~IReadOperator() = default;

  virtual std::string get_operator_name() const = 0;

  virtual gs::result<Context> Eval(
      const GraphReadInterface& graph,
      const std::map<std::string, std::string>& params, Context&& ctx,
      OprTimer* timer) = 0;
};

class IAdminOperator {
 public:
  virtual ~IAdminOperator() = default;

  virtual std::string get_operator_name() const = 0;

  virtual gs::result<Context> Eval(
      GraphUpdateInterface& graph,
      const std::map<std::string, std::string>& params, Context&& ctx,
      OprTimer* timer) = 0;
};

using ReadOpBuildResultT =
    std::pair<std::unique_ptr<IReadOperator>, ContextMeta>;

class IReadOperatorBuilder {
 public:
  virtual ~IReadOperatorBuilder() = default;
  virtual gs::result<ReadOpBuildResultT> Build(
      const gs::Schema& schema, const ContextMeta& ctx_meta,
      const physical::PhysicalPlan& plan, int op_idx) = 0;
  virtual int stepping(int i) { return i + GetOpKinds().size(); }

  virtual std::vector<physical::PhysicalOpr_Operator::OpKindCase> GetOpKinds()
      const = 0;
};

class IInsertOperator {
 public:
  virtual ~IInsertOperator() = default;

  virtual std::string get_operator_name() const = 0;

  virtual gs::result<WriteContext> Eval(
      GraphInsertInterface& graph,
      const std::map<std::string, std::string>& params, WriteContext&& ctx,
      OprTimer* timer) = 0;

  virtual gs::result<WriteContext> Eval(
      GraphUpdateInterface& graph,
      const std::map<std::string, std::string>& params, WriteContext&& ctx,
      OprTimer* timer) = 0;
};

class IInsertOperatorBuilder {
 public:
  virtual ~IInsertOperatorBuilder() = default;
  virtual int stepping(int i) { return i + 1; }

  virtual std::unique_ptr<IInsertOperator> Build(
      const Schema& schema, const physical::PhysicalPlan& plan, int op_id) = 0;
  virtual physical::PhysicalOpr_Operator::OpKindCase GetOpKind() const = 0;
};

class IUpdateOperatorBuilder {
 public:
  virtual ~IUpdateOperatorBuilder() = default;
  virtual int stepping(int i) { return i + GetOpKinds().size(); }

  virtual std::unique_ptr<IUpdateOperator> Build(
      const Schema& schema, const physical::PhysicalPlan& plan, int op_id) = 0;
  virtual std::vector<physical::PhysicalOpr_Operator::OpKindCase> GetOpKinds()
      const = 0;
};

class IAdminOperatorBuilder {
 public:
  virtual ~IAdminOperatorBuilder() = default;
  virtual int stepping(int i) { return i + 1; }

  virtual std::unique_ptr<IAdminOperator> Build(const Schema& schema,
                                                const physical::AdminPlan& plan,
                                                int op_id) = 0;
  virtual physical::AdminPlan_Operator::KindCase GetOpKind() const = 0;
};

}  // namespace runtime

}  // namespace gs

#endif  // INCLUDE_NEUG_EXECUTION_EXECUTE_OPERATOR_H_
