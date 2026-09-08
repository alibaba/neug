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

#include <string>
#include <vector>

#include "neug/execution/common/params_map.h"
#include "neug/execution/common/stream.h"
#include "neug/execution/utils/opr_timer.h"
#include "neug/generated/proto/plan/physical.pb.h"
#include "neug/storages/graph/graph_interface.h"
#include "neug/utils/result.h"

namespace neug {

namespace execution {

class IOperator {
 public:
  virtual ~IOperator() = default;

  virtual std::string get_operator_name() const = 0;

  virtual neug::result<Stream<DataChunk>> Eval(IStorageInterface& graph,
                                               const ParamsMap& params,
                                               Stream<DataChunk>&& input,
                                               OprTimer* timer) = 0;

  virtual void build_explain_children(OprTimer* parent_timer,
                                      const ParamsMap& params,
                                      IStorageInterface& graph) {}
};

// Pull one batch at a time for row-local transformations. The callback owns
// execution state; no mutable cursor is stored on a reusable operator.
// Pipeline::Execute keeps operator/storage references alive until consumption.
template <typename Transform>
Stream<DataChunk> transform_stream(Stream<DataChunk>&& input,
                                   Transform transform) {
  auto tags = input.tag_ids;
  auto upstream = std::make_shared<Stream<DataChunk>>(std::move(input));
  auto pending = std::make_shared<Stream<DataChunk>>();
  return Stream<DataChunk>(
      [transform = std::move(transform), upstream, pending,
       tags]() mutable -> Stream<DataChunk>::NextResult {
        while (true) {
          auto output = pending->Next();
          if (!output || *output) {
            return output;
          }
          auto next = upstream->Next();
          if (!next || !*next) {
            return next;
          }
          Context ctx;
          ctx.tag_ids = tags;
          ctx.append_chunk(std::move((**next).chunk), std::move((**next).head));
          auto result = transform(std::move(ctx));
          if (!result) {
            return tl::unexpected(result.error());
          }
          *pending = stream_from_context(std::move(*result));
        }
      },
      tags);
}

using OpBuildResultT = std::pair<std::unique_ptr<IOperator>, ContextMeta>;

class IOperatorBuilder {
 public:
  virtual ~IOperatorBuilder() = default;
  virtual neug::result<OpBuildResultT> Build(const neug::Schema& schema,
                                             const ContextMeta& ctx_meta,
                                             const physical::PhysicalPlan& plan,
                                             int op_idx) = 0;
  virtual int stepping(int i) { return i + GetOpKinds().size(); }

  virtual std::vector<physical::PhysicalOpr_Operator::OpKindCase> GetOpKinds()
      const = 0;
};

}  // namespace execution

}  // namespace neug
