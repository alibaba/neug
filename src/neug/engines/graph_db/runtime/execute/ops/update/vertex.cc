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

#include "neug/engines/graph_db/runtime/execute/ops/update/vertex.h"

#include <glog/logging.h>
#include <google/protobuf/wrappers.pb.h>
#include <stddef.h>
#include <boost/leaf.hpp>
#include <map>
#include <ostream>
#include <string>
#include <utility>
#include <vector>

#include "neug/engines/graph_db/runtime/common/context.h"
#include "neug/engines/graph_db/runtime/common/graph_interface.h"
#include "neug/engines/graph_db/runtime/common/operators/retrieve/get_v.h"
#include "neug/engines/graph_db/runtime/common/operators/update/get_v.h"
#include "neug/engines/graph_db/runtime/common/rt_any.h"
#include "neug/engines/graph_db/runtime/common/types.h"
#include "neug/engines/graph_db/runtime/utils/expr_impl.h"
#include "neug/engines/graph_db/runtime/utils/params.h"
#include "neug/engines/graph_db/runtime/utils/predicates.h"
#include "neug/engines/graph_db/runtime/utils/utils.h"
#include "neug/engines/graph_db/runtime/utils/var.h"
#include "neug/storages/rt_mutable_graph/types.h"
#include "neug/utils/leaf_utils.h"
#ifdef USE_SYSTEM_PROTOBUF
#include "neug/generated/proto/plan/algebra.pb.h"
#include "neug/generated/proto/plan/expr.pb.h"
#else
#include "neug/utils/proto/plan/algebra.pb.h"
#include "neug/utils/proto/plan/expr.pb.h"
#endif

namespace gs {
class Schema;

namespace runtime {
class OprTimer;

namespace ops {

class UGetVFromEdgeWithPredOpr : public IUpdateOperator {
 public:
  UGetVFromEdgeWithPredOpr(const GetVParams& params, const physical::GetV& opr)
      : v_params_(params), opr_(opr) {}
  ~UGetVFromEdgeWithPredOpr() = default;

  std::string get_operator_name() const override {
    return "UGetVFromEdgeWithPredOpr";
  }

  bl::result<Context> Eval(GraphUpdateInterface& graph,
                           const std::map<std::string, std::string>& params,
                           Context&& ctx, OprTimer& timer) override {
    LOG(INFO) << opr_.DebugString();
    if (opr_.params().has_predicate()) {
      GeneralVertexPredicate pred(graph, ctx, params,
                                  opr_.params().predicate());
      GeneralVertexPredicateWrapper vpred(pred);
      return GetV::get_vertex_from_edges(graph, std::move(ctx), v_params_,
                                         vpred);
    } else {
      return GetV::get_vertex_from_edges(graph, std::move(ctx), v_params_,
                                         DummyVertexPredicate());
    }
  }

 private:
  GetVParams v_params_;
  physical::GetV opr_;
};

class UGetVFromVerticesWithPredOpr : public IUpdateOperator {
 public:
  UGetVFromVerticesWithPredOpr(const GetVParams& params,
                               const common::Expression& expr)
      : params_(params), expr_(expr) {}
  ~UGetVFromVerticesWithPredOpr() = default;

  std::string get_operator_name() const override {
    return "UGetVFromVerticesWithPredOpr";
  }

  bl::result<Context> Eval(GraphUpdateInterface& graph,
                           const std::map<std::string, std::string>& params,
                           Context&& ctx, OprTimer& timer) override {
    auto expr = parse_expression<GraphUpdateInterface>(
        graph, ctx, params, expr_, VarType::kPathVar);
    if (expr->is_optional()) {
      LOG(ERROR) << "GetV does not support optional expression now";
      RETURN_INVALID_ARGUMENT_ERROR(
          "GetV does not support optional expression now");
    }
    Arena arena;
    return UGetV::get_vertex_from_vertices(
        graph, std::move(ctx), params_,
        [&](size_t idx, label_t label, vid_t vid) {
          return expr->eval_vertex(label, vid, idx, arena).as_bool();
        });
  }

 private:
  GetVParams params_;
  common::Expression expr_;
};
std::unique_ptr<IUpdateOperator> UVertexOprBuilder::Build(
    const Schema& schema, const physical::PhysicalPlan& plan, int op_idx) {
  const auto& vertex = plan.query_plan().plan(op_idx).opr().vertex();
  int alias = vertex.has_alias() ? vertex.alias().value() : -1;
  int tag = vertex.has_tag() ? vertex.tag().value() : -1;
  GetVParams params;
  VOpt opt = parse_opt(vertex.opt());
  params.opt = opt;
  params.tag = tag;
  params.alias = alias;
  params.tables = parse_tables(vertex.params());
  if (vertex.params().has_predicate()) {
    if (opt == VOpt::kEnd || opt == VOpt::kStart || opt == VOpt::kOther) {
      return std::make_unique<UGetVFromEdgeWithPredOpr>(params, vertex);
    } else if (opt == VOpt::kItself) {
      return std::make_unique<UGetVFromVerticesWithPredOpr>(
          params, vertex.params().predicate());
    }
    return std::make_unique<UGetVFromEdgeWithPredOpr>(params, vertex);
  }
  if (opt == VOpt::kEnd || opt == VOpt::kStart || opt == VOpt::kOther) {
    return std::make_unique<UGetVFromEdgeWithPredOpr>(params, vertex);
  }
  LOG(ERROR) << "GetV does not support opt " << static_cast<int>(opt);
  return nullptr;
}
}  // namespace ops
}  // namespace runtime
}  // namespace gs