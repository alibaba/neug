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

#include "neug/execution/execute/plan_parser.h"
#include "neug/generated/proto/plan/common.pb.h"

#include <glog/logging.h>
#include <stddef.h>
#include <exception>
#include <ostream>
#include <string>

#include "neug/execution/common/context.h"

#include "neug/execution/execute/ops/admin/checkpoint.h"
#include "neug/execution/execute/ops/admin/extension.h"

#include "neug/execution/execute/ops/batch/batch_delete_edge.h"
#include "neug/execution/execute/ops/batch/batch_delete_vertex.h"
#include "neug/execution/execute/ops/batch/batch_insert_edge.h"
#include "neug/execution/execute/ops/batch/batch_insert_vertex.h"
#include "neug/execution/execute/ops/batch/batch_update_edge.h"
#include "neug/execution/execute/ops/batch/batch_update_vertex.h"
#include "neug/execution/execute/ops/batch/data_export.h"
#include "neug/execution/execute/ops/batch/data_source.h"

#include "neug/execution/execute/ops/retrieve/dedup.h"
#include "neug/execution/execute/ops/retrieve/edge.h"
#include "neug/execution/execute/ops/retrieve/group_by.h"
#include "neug/execution/execute/ops/retrieve/intersect.h"
#include "neug/execution/execute/ops/retrieve/join.h"
#include "neug/execution/execute/ops/retrieve/limit.h"
#include "neug/execution/execute/ops/retrieve/order_by.h"
#include "neug/execution/execute/ops/retrieve/path.h"
#include "neug/execution/execute/ops/retrieve/procedure_call.h"
#include "neug/execution/execute/ops/retrieve/project.h"
#include "neug/execution/execute/ops/retrieve/scan.h"
#include "neug/execution/execute/ops/retrieve/select.h"
#include "neug/execution/execute/ops/retrieve/sink.h"
#include "neug/execution/execute/ops/retrieve/unfold.h"
#include "neug/execution/execute/ops/retrieve/union.h"
#include "neug/execution/execute/ops/retrieve/vertex.h"

#include "neug/execution/execute/ops/insert/create_edge.h"
#include "neug/execution/execute/ops/insert/create_vertex.h"

#include "neug/execution/execute/ops/ddl/add_edge_property.h"
#include "neug/execution/execute/ops/ddl/add_vertex_property.h"
#include "neug/execution/execute/ops/ddl/create_edge_type.h"
#include "neug/execution/execute/ops/ddl/create_vertex_type.h"
#include "neug/execution/execute/ops/ddl/drop_edge_property.h"
#include "neug/execution/execute/ops/ddl/drop_edge_type.h"
#include "neug/execution/execute/ops/ddl/drop_vertex_property.h"
#include "neug/execution/execute/ops/ddl/drop_vertex_type.h"
#include "neug/execution/execute/ops/ddl/rename_edge_property.h"
#include "neug/execution/execute/ops/ddl/rename_vertex_property.h"

#include "neug/execution/execute/pipeline.h"
#include "neug/utils/result.h"

namespace neug {
class Schema;

namespace execution {

void PlanParser::init() {
  register_operator_builder(std::make_unique<ops::ScanOprBuilder>());

  register_operator_builder(std::make_unique<ops::DummySourceOprBuilder>());

  register_operator_builder(std::make_unique<ops::TCOprBuilder>());
  register_operator_builder(std::make_unique<ops::EdgeExpandGetVOprBuilder>());
  register_operator_builder(std::make_unique<ops::EdgeExpandOprBuilder>());

  register_operator_builder(std::make_unique<ops::VertexOprBuilder>());

  register_operator_builder(std::make_unique<ops::ProjectOrderByOprBuilder>());
  register_operator_builder(std::make_unique<ops::ProjectOprBuilder>());

  register_operator_builder(std::make_unique<ops::OrderByOprBuilder>());

  register_operator_builder(std::make_unique<ops::GroupByOprBuilder>());

  register_operator_builder(std::make_unique<ops::DedupOprBuilder>());

  register_operator_builder(std::make_unique<ops::SelectOprBuilder>());

  register_operator_builder(std::make_unique<ops::SPOrderByLimitOprBuilder>());
  register_operator_builder(std::make_unique<ops::SPOprBuilder>());
  register_operator_builder(std::make_unique<ops::PathExpandVOprBuilder>());
  register_operator_builder(std::make_unique<ops::PathExpandOprBuilder>());

  register_operator_builder(std::make_unique<ops::PrimaryKeyJoinOprBuilder>());
  register_operator_builder(std::make_unique<ops::JoinOprBuilder>());

  register_operator_builder(std::make_unique<ops::IntersectOprBuilder>());

  register_operator_builder(std::make_unique<ops::LimitOprBuilder>());

  register_operator_builder(std::make_unique<ops::UnfoldOprBuilder>());

  register_operator_builder(std::make_unique<ops::UnionOprBuilder>());

  register_operator_builder(std::make_unique<ops::SinkOprBuilder>());

  register_operator_builder(std::make_unique<ops::CreateVertexOprBuilder>());
  register_operator_builder(std::make_unique<ops::CreateEdgeOprBuilder>());

  register_operator_builder(std::make_unique<ops::DataExportOprBuilder>());

  register_operator_builder(std::make_unique<ops::DataSourceOprBuilder>());
  register_operator_builder(
      std::make_unique<ops::BatchInsertVertexOprBuilder>());
  register_operator_builder(std::make_unique<ops::BatchInsertEdgeOprBuilder>());
  register_operator_builder(
      std::make_unique<ops::BatchDeleteVertexOprBuilder>());
  register_operator_builder(std::make_unique<ops::BatchDeleteEdgeOprBuilder>());
  register_operator_builder(std::make_unique<ops::UpdateVertexOprBuilder>());
  register_operator_builder(std::make_unique<ops::UpdateEdgeOprBuilder>());
  // TODO: Review which pipeline should procedureCall be put.
  register_operator_builder(std::make_unique<ops::ProcedureCallOprBuilder>());

  // ---------------------- DDL Operators ----------------------
  register_operator_builder(
      std::make_unique<ops::CreateVertexTypeOprBuilder>());
  register_operator_builder(std::make_unique<ops::CreateEdgeTypeOprBuilder>());
  register_operator_builder(
      std::make_unique<ops::AddVertexPropertySchemaOprBuilder>());
  register_operator_builder(
      std::make_unique<ops::AddEdgePropertySchemaOprBuilder>());
  register_operator_builder(
      std::make_unique<ops::DropVertexPropertySchemaOprBuilder>());
  register_operator_builder(
      std::make_unique<ops::DropEdgePropertySchemaOprBuilder>());
  register_operator_builder(
      std::make_unique<ops::RenameVertexPropertyOprBuilder>());
  register_operator_builder(
      std::make_unique<ops::RenameEdgePropertyOprBuilder>());
  register_operator_builder(std::make_unique<ops::DropVertexTypeOprBuilder>());
  register_operator_builder(std::make_unique<ops::DropEdgeTypeOprBuilder>());

  // ---------------------- Admin Operators ----------------------
  register_operator_builder(std::make_unique<ops::CheckpointOprBuilder>());
  register_operator_builder(
      std::make_unique<ops::ExtensionInstallOprBuilder>());
  register_operator_builder(std::make_unique<ops::ExtensionLoadOprBuilder>());
  register_operator_builder(
      std::make_unique<ops::ExtensionUninstallOprBuilder>());
}

PlanParser& PlanParser::get() {
  static PlanParser parser;
  return parser;
}

void PlanParser::register_operator_builder(
    std::unique_ptr<IOperatorBuilder>&& builder) {
  auto ops = builder->GetOpKinds();
  op_builders_[*ops.begin()].emplace_back(ops, std::move(builder));
}

#if 1
static std::string get_opr_name(
    physical::PhysicalOpr_Operator::OpKindCase op_kind) {
  switch (op_kind) {
  case physical::PhysicalOpr_Operator::OpKindCase::kScan: {
    return "scan";
  }
  case physical::PhysicalOpr_Operator::OpKindCase::kEdge: {
    return "edge_expand";
  }
  case physical::PhysicalOpr_Operator::OpKindCase::kVertex: {
    return "get_v";
  }
  case physical::PhysicalOpr_Operator::OpKindCase::kOrderBy: {
    return "order_by";
  }
  case physical::PhysicalOpr_Operator::OpKindCase::kProject: {
    return "project";
  }
  case physical::PhysicalOpr_Operator::OpKindCase::kSink: {
    return "sink";
  }
  case physical::PhysicalOpr_Operator::OpKindCase::kDedup: {
    return "dedup";
  }
  case physical::PhysicalOpr_Operator::OpKindCase::kGroupBy: {
    return "group_by";
  }
  case physical::PhysicalOpr_Operator::OpKindCase::kSelect: {
    return "select";
  }
  case physical::PhysicalOpr_Operator::OpKindCase::kPath: {
    return "path";
  }
  case physical::PhysicalOpr_Operator::OpKindCase::kJoin: {
    return "join";
  }
  case physical::PhysicalOpr_Operator::OpKindCase::kRoot: {
    return "root";
  }
  case physical::PhysicalOpr_Operator::OpKindCase::kIntersect: {
    return "intersect";
  }
  case physical::PhysicalOpr_Operator::OpKindCase::kUnion: {
    return "union";
  }
  case physical::PhysicalOpr_Operator::OpKindCase::kUnfold: {
    return "unfold";
  }
  case physical::PhysicalOpr_Operator::OpKindCase::kSource: {
    return "DataSource";
  }
  case physical::PhysicalOpr_Operator::OpKindCase::kLoadVertex: {
    return "load_vertex";
  }
  case physical::PhysicalOpr_Operator::OpKindCase::kLoadEdge: {
    return "load_edge";
  }
  case physical::PhysicalOpr_Operator::OpKindCase::kCreateVertex: {
    return "create_vertex";
  }
  case physical::PhysicalOpr_Operator::OpKindCase::kCreateEdge: {
    return "create_edge";
  }
  case physical::PhysicalOpr_Operator::OpKindCase::kSetVertex: {
    return "set_vertex";
  }
  case physical::PhysicalOpr_Operator::OpKindCase::kSetEdge: {
    return "set_edge";
  }
  case physical::PhysicalOpr_Operator::OpKindCase::kDeleteVertex: {
    return "delete_vertex";
  }
  case physical::PhysicalOpr_Operator::OpKindCase::kDeleteEdge: {
    return "delete_edge";
  }
  case physical::PhysicalOpr_Operator::OpKindCase::kProcedureCall: {
    return "procedure_call";
  }
  case physical::PhysicalOpr_Operator::OpKindCase::kCheckpoint: {
    return "checkpoint";
  }
  case physical::PhysicalOpr_Operator::OpKindCase::kExtInstall: {
    return "extension_install";
  }
  case physical::PhysicalOpr_Operator::OpKindCase::kExtLoad: {
    return "extension_load";
  }
  case physical::PhysicalOpr_Operator::OpKindCase::kExtUninstall: {
    return "extension_uninstall";
  }
  default:
    return "unknown";
  }
}

#endif

neug::result<std::pair<Pipeline, ContextMeta>>
PlanParser::parse_execute_pipeline_with_meta(
    const neug::Schema& schema, const ContextMeta& ctx_meta,
    const physical::PhysicalPlan& plan) {
  int opr_num = plan.plan_size();
  std::vector<std::unique_ptr<IOperator>> operators;
  ContextMeta cur_ctx_meta = ctx_meta;
  for (int i = 0; i < opr_num;) {
    physical::PhysicalOpr_Operator::OpKindCase cur_op_kind =
        plan.plan(i).opr().op_kind_case();
    if (cur_op_kind == physical::PhysicalOpr_Operator::OpKindCase::kSink) {
      // break;
    }

    auto& builders = op_builders_[cur_op_kind];
    int old_i = i;
    neug::Status status = neug::Status::OK();
    for (auto& pair : builders) {
      auto pattern = pair.first;
      auto& builder = pair.second;
      if (pattern.size() > static_cast<size_t>(opr_num - i)) {
        continue;
      }
      bool match = true;
      for (size_t j = 1; j < pattern.size(); ++j) {
        if (plan.plan(i + j).opr().op_kind_case() != pattern[j]) {
          match = false;
        }
      }
      if (match) {
        TRY_HANDLE_ALL_WITH_EXCEPTION(
            neug::result<OpBuildResultT>,
            [&]() { return builder->Build(schema, cur_ctx_meta, plan, i); },
            [&](const auto& _status) {
              status = neug::Status(
                  neug::StatusCode::ERR_INTERNAL_ERROR,
                  "Failed to build operator at index " + std::to_string(i) +
                      ", op_kind: " + get_opr_name(cur_op_kind) +
                      ", error: " + _status.ToString());
            },
            [&](neug::result<OpBuildResultT>&& res_pair_status) {
              if (res_pair_status.value().first) {
                operators.emplace_back(
                    std::move(res_pair_status.value().first));
                cur_ctx_meta = res_pair_status.value().second;
                i = builder->stepping(i);
                // Reset status to OK after a successful match.
                status = neug::Status::OK();
              } else {
                status = neug::Status(
                    neug::StatusCode::ERR_INTERNAL_ERROR,
                    "Failed to build operator at index " + std::to_string(i) +
                        ", op_kind: " + get_opr_name(cur_op_kind) +
                        ", error: No operator returned");
              }
            });
        if (status.ok()) {
          break;
        }
      }
    }
    if (i == old_i) {
      std::stringstream ss;
      ss << "[Pipeline Parse Failed] " << get_opr_name(cur_op_kind)
         << " failed to parse plan at index " << i << " "
         << plan.plan(i).DebugString() << ": "
         << ", last match error: " << status.ToString();
      auto err = neug::Status(neug::StatusCode::ERR_INTERNAL_ERROR, ss.str());
      LOG(ERROR) << err.ToString();
      RETURN_ERROR(err);
    }
  }
  return std::make_pair(Pipeline(std::move(operators)), cur_ctx_meta);
}

neug::result<std::pair<Pipeline, ContextMeta>>
PlanParser::parse_execute_pipeline(const neug::Schema& schema,
                                   const ContextMeta& ctx_meta,
                                   const physical::PhysicalPlan& plan) {
  auto ret = parse_execute_pipeline_with_meta(schema, ctx_meta, plan);
  if (!ret) {
    RETURN_ERROR(ret.error());
  }
  return ret;
}

static void expression_parse(const ::common::Expression& expr,
                             ParamsMetaMap& params_type) {
  for (int i = 0; i < expr.operators_size(); ++i) {
    const auto& opr = expr.operators(i);
    if (opr.has_param()) {
      const auto& param = opr.param();
      if (params_type.find(param.name()) != params_type.end()) {
        continue;
      }
      params_type[param.name()] = parse_from_ir_data_type(param.data_type());
    }
    if (opr.has_case_()) {
      const auto& case_expr = opr.case_();
      for (int j = 0; j < case_expr.when_then_expressions_size(); ++j) {
        const auto& when_clause = case_expr.when_then_expressions(j);
        expression_parse(when_clause.when_expression(), params_type);
        expression_parse(when_clause.then_result_expression(), params_type);
      }
      if (case_expr.has_else_result_expression()) {
        expression_parse(case_expr.else_result_expression(), params_type);
      }
    }
  }
}

static void parse_params_type_impl(const physical::PhysicalPlan& plan,
                                   ParamsMetaMap& params_type) {
  int opr_num = plan.plan_size();
  for (int i = 0; i < opr_num; ++i) {
    const auto& cur_op_kind = plan.plan(i).opr().op_kind_case();
    switch (cur_op_kind) {
    case physical::PhysicalOpr_Operator::OpKindCase::kScan: {
      const auto& scan_opr = plan.plan(i).opr().scan();
      if (scan_opr.has_params() && scan_opr.params().has_predicate()) {
        expression_parse(scan_opr.params().predicate(), params_type);
      }
      if (scan_opr.has_idx_predicate()) {
        const auto& predicate = scan_opr.idx_predicate();
        const auto& triplet = predicate.or_predicates(0).predicates(0);
        if (triplet.value_case() ==
            algebra::IndexPredicate_Triplet::ValueCase::kParam) {
          const auto& param = triplet.param();
          if (params_type.find(param.name()) == params_type.end()) {
            params_type[param.name()] =
                parse_from_ir_data_type(param.data_type());
          }
        }
      }
      break;
    }
    case physical::PhysicalOpr_Operator::OpKindCase::kEdge: {
      const auto& edge_opr = plan.plan(i).opr().edge();
      if (edge_opr.has_params() && edge_opr.params().has_predicate()) {
        expression_parse(edge_opr.params().predicate(), params_type);
      }
      break;
    }
    case physical::PhysicalOpr_Operator::OpKindCase::kProject: {
      const auto& project_opr = plan.plan(i).opr().project();
      int expr_num = project_opr.mappings_size();
      for (int j = 0; j < expr_num; ++j) {
        expression_parse(project_opr.mappings(j).expr(), params_type);
      }
      break;
    }
    case physical::PhysicalOpr_Operator::OpKindCase::kVertex: {
      const auto& vertex_opr = plan.plan(i).opr().vertex();
      if (vertex_opr.has_params() && vertex_opr.params().has_predicate()) {
        expression_parse(vertex_opr.params().predicate(), params_type);
      }
      break;
    }
    case physical::PhysicalOpr_Operator::OpKindCase::kSelect: {
      const auto& select_opr = plan.plan(i).opr().select();
      expression_parse(select_opr.predicate(), params_type);
      break;
    }
    case physical::PhysicalOpr_Operator::OpKindCase::kPath: {
      const auto& path_expand_opr = plan.plan(i).opr().path();
      if (path_expand_opr.base().edge_expand().has_params() &&
          path_expand_opr.base().edge_expand().params().has_predicate()) {
        expression_parse(
            path_expand_opr.base().edge_expand().params().predicate(),
            params_type);
      }
      break;
    }
    case physical::PhysicalOpr_Operator::OpKindCase::kJoin: {
      const auto& join_opr = plan.plan(i).opr().join();
      parse_params_type_impl(join_opr.left_plan(), params_type);
      parse_params_type_impl(join_opr.right_plan(), params_type);
      break;
    }
    case physical::PhysicalOpr_Operator::OpKindCase::kUnion: {
      const auto& union_opr = plan.plan(i).opr().union_();
      for (int j = 0; j < union_opr.sub_plans_size(); ++j) {
        parse_params_type_impl(union_opr.sub_plans(j), params_type);
      }
      break;
    }

    case physical::PhysicalOpr_Operator::OpKindCase::kIntersect: {
      const auto& intersect_opr = plan.plan(i).opr().intersect();
      for (int j = 0; j < intersect_opr.sub_plans_size(); ++j) {
        parse_params_type_impl(intersect_opr.sub_plans(j), params_type);
      }
      break;
    }

    case physical::PhysicalOpr_Operator::OpKindCase::kCreateEdge: {
      const auto& create_edge_opr = plan.plan(i).opr().create_edge();
      for (const auto& entry : create_edge_opr.entries()) {
        for (const auto& prop : entry.property_mappings()) {
          expression_parse(prop.data(), params_type);
        }
      }
      break;
    }

    case physical::PhysicalOpr_Operator::OpKindCase::kCreateVertex: {
      const auto& create_vertex_opr = plan.plan(i).opr().create_vertex();
      for (const auto& entry : create_vertex_opr.entries()) {
        for (const auto& prop : entry.property_mappings()) {
          expression_parse(prop.data(), params_type);
        }
      }
      break;
    }

    case physical::PhysicalOpr_Operator::OpKindCase::kUnfold: {
      const auto& unfold_opr = plan.plan(i).opr().unfold();
      expression_parse(unfold_opr.input_expr(), params_type);
      break;
    }
    default: {
      break;
    }
    }
  }
}
ParamsMetaMap PlanParser::parse_params_type(
    const physical::PhysicalPlan& plan) {
  ParamsMetaMap params_type;
  parse_params_type_impl(plan, params_type);
  return params_type;
}

}  // namespace execution

}  // namespace neug
