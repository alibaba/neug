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
#include "neug/execution/execute/ops/update/edge.h"
#include "neug/execution/execute/ops/update/group_by.h"
#include "neug/execution/execute/ops/update/join.h"
#include "neug/execution/execute/ops/update/path.h"
#include "neug/execution/execute/ops/update/project.h"
#include "neug/execution/execute/ops/update/scan.h"
#include "neug/execution/execute/ops/update/select.h"
#include "neug/execution/execute/ops/update/set.h"
#include "neug/execution/execute/ops/update/sink.h"
#include "neug/execution/execute/ops/update/vertex.h"
#include "neug/execution/execute/pipeline.h"
#include "neug/utils/result.h"

namespace gs {
class Schema;

namespace runtime {

void PlanParser::init() {
  register_read_operator_builder(std::make_unique<ops::ScanOprBuilder>());

  register_read_operator_builder(
      std::make_unique<ops::DummySourceOprBuilder>());

  register_read_operator_builder(std::make_unique<ops::TCOprBuilder>());
  register_read_operator_builder(
      std::make_unique<ops::EdgeExpandGetVOprBuilder>());
  register_read_operator_builder(std::make_unique<ops::EdgeExpandOprBuilder>());

  register_read_operator_builder(std::make_unique<ops::VertexOprBuilder>());

  register_read_operator_builder(
      std::make_unique<ops::ProjectOrderByOprBuilder>());
  register_read_operator_builder(std::make_unique<ops::ProjectOprBuilder>());

  register_read_operator_builder(std::make_unique<ops::OrderByOprBuilder>());

  register_read_operator_builder(std::make_unique<ops::GroupByOprBuilder>());

  register_read_operator_builder(std::make_unique<ops::DedupOprBuilder>());

  register_read_operator_builder(std::make_unique<ops::SelectOprBuilder>());

  register_read_operator_builder(
      std::make_unique<ops::SPOrderByLimitOprBuilder>());
  register_read_operator_builder(std::make_unique<ops::SPOprBuilder>());
  register_read_operator_builder(
      std::make_unique<ops::PathExpandVOprBuilder>());
  register_read_operator_builder(std::make_unique<ops::PathExpandOprBuilder>());

  register_read_operator_builder(std::make_unique<ops::JoinOprBuilder>());

  register_read_operator_builder(std::make_unique<ops::IntersectOprBuilder>());

  register_read_operator_builder(std::make_unique<ops::LimitOprBuilder>());

  register_read_operator_builder(std::make_unique<ops::UnfoldOprBuilder>());

  register_read_operator_builder(std::make_unique<ops::UnionOprBuilder>());

  register_read_operator_builder(std::make_unique<ops::SinkOprBuilder>());
  register_read_operator_builder(std::make_unique<ops::DataExportOprBuilder>());

  // ------------- Update operators -------------
  register_update_operator_builder(
      std::make_unique<ops::UEdgeExpandOprBuilder>());
  register_update_operator_builder(std::make_unique<ops::UScanOprBuilder>());
  register_update_operator_builder(std::make_unique<ops::USetOprBuilder>());
  register_update_operator_builder(std::make_unique<ops::UVertexOprBuilder>());
  register_update_operator_builder(std::make_unique<ops::USinkOprBuilder>());
  register_update_operator_builder(std::make_unique<ops::UProjectOprBuilder>());
  register_update_operator_builder(std::make_unique<ops::USelectOprBuilder>());
  register_update_operator_builder(std::make_unique<ops::UJoinOprBuilder>());
  register_update_operator_builder(std::make_unique<ops::UGroupByOprBuilder>());
  register_update_operator_builder(
      std::make_unique<ops::UPathExpandVOprBuilder>());
  register_update_operator_builder(
      std::make_unique<ops::DataSourceOprBuilder>());
  register_update_operator_builder(
      std::make_unique<ops::BatchInsertVertexOprBuilder>());
  register_update_operator_builder(
      std::make_unique<ops::BatchInsertEdgeOprBuilder>());
  register_update_operator_builder(
      std::make_unique<ops::BatchDeleteVertexOprBuilder>());
  register_update_operator_builder(
      std::make_unique<ops::BatchDeleteEdgeOprBuilder>());
  register_update_operator_builder(
      std::make_unique<ops::InsertVertexOprBuilder>());
  register_update_operator_builder(
      std::make_unique<ops::InsertEdgeOprBuilder>());
  register_update_operator_builder(
      std::make_unique<ops::UpdateVertexOprBuilder>());
  register_update_operator_builder(
      std::make_unique<ops::UpdateEdgeOprBuilder>());
  // TODO: Review which pipeline should procedureCall be put.
  register_update_operator_builder(
      std::make_unique<ops::ProcedureCallOprBuilder>());

  // ---------------------- Admin Operators ----------------------
  register_admin_operator_builder(
      std::make_unique<ops::CheckpointOprBuilder>());
  register_admin_operator_builder(
      std::make_unique<ops::ExtensionInstallOprBuilder>());
  register_admin_operator_builder(
      std::make_unique<ops::ExtensionLoadOprBuilder>());
  register_admin_operator_builder(
      std::make_unique<ops::ExtensionUninstallOprBuilder>());
}

PlanParser& PlanParser::get() {
  static PlanParser parser;
  return parser;
}

void PlanParser::register_read_operator_builder(
    std::unique_ptr<IOperatorBuilder>&& builder) {
  auto ops = builder->GetOpKinds();
  read_op_builders_[*ops.begin()].emplace_back(ops, std::move(builder));
}

void PlanParser::register_write_operator_builder(
    std::unique_ptr<IOperatorBuilder>&& builder) {
  auto op = builder->GetOpKinds();
  write_op_builders_[*op.begin()] = std::move(builder);
}

void PlanParser::register_update_operator_builder(
    std::unique_ptr<IOperatorBuilder>&& builder) {
  auto ops = builder->GetOpKinds();
  update_op_builders_[*ops.begin()].emplace_back(ops, std::move(builder));
}

void PlanParser::register_admin_operator_builder(
    std::unique_ptr<IAdminOperatorBuilder>&& builder) {
  auto op = builder->GetOpKind();
  admin_op_builders_[op] = std::move(builder);
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
  default:
    return "unknown";
  }
}

static std::string get_opr_name(
    physical::AdminPlan_Operator::KindCase op_kind) {
  switch (op_kind) {
  case physical::AdminPlan_Operator::KindCase::kCheckpoint: {
    return "checkpoint";
  }
  case physical::AdminPlan_Operator::KindCase::kExtInstall: {
    return "extension_install";
  }
  case physical::AdminPlan_Operator::KindCase::kExtLoad: {
    return "extension_load";
  }
  case physical::AdminPlan_Operator::KindCase::kExtUninstall: {
    return "extension_uninstall";
  }
  default:
    return "unknown";
  }
}

#endif

gs::result<std::pair<ReadPipeline, ContextMeta>>
PlanParser::parse_read_pipeline_with_meta(const gs::Schema& schema,
                                          const ContextMeta& ctx_meta,
                                          const physical::PhysicalPlan& plan) {
  int opr_num = plan.query_plan().plan_size();
  std::vector<std::unique_ptr<IOperator>> operators;
  ContextMeta cur_ctx_meta = ctx_meta;
  for (int i = 0; i < opr_num;) {
    physical::PhysicalOpr_Operator::OpKindCase cur_op_kind =
        plan.query_plan().plan(i).opr().op_kind_case();
    if (cur_op_kind == physical::PhysicalOpr_Operator::OpKindCase::kSink) {
      // break;
    }
    if (cur_op_kind == physical::PhysicalOpr_Operator::OpKindCase::kRoot) {
      if (i + 1 < opr_num &&
          plan.query_plan().plan(i + 1).opr().op_kind_case() ==
              physical::PhysicalOpr_Operator::OpKindCase::kProject) {
      } else {
        // skip root only
        i++;
        continue;
      }
    }
    auto& builders = read_op_builders_[cur_op_kind];
    int old_i = i;
    gs::Status status = gs::Status::OK();
    for (auto& pair : builders) {
      auto pattern = pair.first;
      auto& builder = pair.second;
      if (pattern.size() > static_cast<size_t>(opr_num - i)) {
        continue;
      }
      bool match = true;
      for (size_t j = 1; j < pattern.size(); ++j) {
        if (plan.query_plan().plan(i + j).opr().op_kind_case() != pattern[j]) {
          match = false;
        }
      }
      if (match) {
        TRY_HANDLE_ALL_WITH_EXCEPTION(
            gs::result<OpBuildResultT>,
            [&]() { return builder->Build(schema, cur_ctx_meta, plan, i); },
            [&](const auto& _status) {
              status = gs::Status(
                  gs::StatusCode::ERR_INTERNAL_ERROR,
                  "Failed to build operator at index " + std::to_string(i) +
                      ", op_kind: " + get_opr_name(cur_op_kind) +
                      ", error: " + _status.ToString());
            },
            [&](gs::result<OpBuildResultT>&& res_pair_status) {
              if (res_pair_status.value().first) {
                operators.emplace_back(
                    std::move(res_pair_status.value().first));
                cur_ctx_meta = res_pair_status.value().second;
                i = builder->stepping(i);
                // Reset status to OK after a successful match.
                status = gs::Status::OK();
              } else {
                status = gs::Status(
                    gs::StatusCode::ERR_INTERNAL_ERROR,
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
      ss << "[ReadPipeline Parse Failed] " << get_opr_name(cur_op_kind)
         << " failed to parse plan at index " << i << " "
         << plan.query_plan().plan(i).DebugString() << ": "
         << ", last match error: " << status.ToString();
      auto err = gs::Status(gs::StatusCode::ERR_INTERNAL_ERROR, ss.str());
      LOG(ERROR) << err.ToString();
      RETURN_ERROR(err);
    }
  }
  return std::make_pair(ReadPipeline(std::move(operators)), cur_ctx_meta);
}

gs::result<ReadPipeline> PlanParser::parse_read_pipeline(
    const gs::Schema& schema, const ContextMeta& ctx_meta,
    const physical::PhysicalPlan& plan) {
  auto ret = parse_read_pipeline_with_meta(schema, ctx_meta, plan);
  if (!ret) {
    RETURN_ERROR(ret.error());
  }
  return std::move(ret.value().first);
}

gs::result<InsertPipeline> PlanParser::parse_write_pipeline(
    const gs::Schema& schema, const physical::PhysicalPlan& plan) {
  std::vector<std::unique_ptr<IOperator>> operators;
  ContextMeta cur_ctx_meta;
  for (int i = 0; i < plan.query_plan().plan_size(); ++i) {
    auto op_kind = plan.query_plan().plan(i).opr().op_kind_case();
    if (write_op_builders_.find(op_kind) == write_op_builders_.end()) {
      std::stringstream ss;
      ss << "[Write Pipeline Parse Failed] " << get_opr_name(op_kind)
         << " failed to parse plan at index " << i;
      RETURN_ERROR(gs::Status(gs::StatusCode::ERR_INTERNAL_ERROR, ss.str()));
    }
    auto op =
        write_op_builders_.at(op_kind)->Build(schema, cur_ctx_meta, plan, i);
    if (!op) {
      std::stringstream ss;
      ss << "[Write Pipeline Parse Failed] " << get_opr_name(op_kind)
         << " failed to parse plan at index " << i;
      auto err = gs::Status(gs::StatusCode::ERR_INTERNAL_ERROR, ss.str());
      LOG(ERROR) << err.ToString();
      RETURN_ERROR(err);
    }
    operators.emplace_back(std::move(op.value().first));
  }
  return InsertPipeline(std::move(operators));
}

gs::result<UpdatePipeline> PlanParser::parse_update_pipeline(
    const gs::Schema& schema, const physical::PhysicalPlan& plan) {
  Status status = Status::OK();
  ContextMeta cur_ctx_meta;
  std::vector<std::unique_ptr<IOperator>> operators;
  int opr_num = plan.query_plan().plan_size();
  for (int i = 0; i < opr_num;) {
    auto op_kind = plan.query_plan().plan(i).opr().op_kind_case();

    auto& builders = update_op_builders_[op_kind];
    int old_i = i;
    gs::Status status = gs::Status::OK();
    for (auto& pair : builders) {
      auto pattern = pair.first;
      auto& builder = pair.second;
      if (pattern.size() > static_cast<size_t>(opr_num - i)) {
        continue;
      }
      bool match = true;
      for (size_t j = 1; j < pattern.size(); ++j) {
        if (plan.query_plan().plan(i + j).opr().op_kind_case() != pattern[j]) {
          match = false;
        }
      }
      if (match) {
        TRY_HANDLE_ALL_WITH_EXCEPTION(
            gs::result<OpBuildResultT>,
            [&]() {
              return gs::result<OpBuildResultT>(
                  builder->Build(schema, cur_ctx_meta, plan, i));
            },
            [&](const auto& _status) { status = _status; },
            [&](gs::result<OpBuildResultT>&& res) {
              if (res.value().first) {
                operators.emplace_back(std::move(res.value().first));
              } else {
                status = gs::Status(gs::StatusCode::ERR_INTERNAL_ERROR,
                                    "Failed to build operator at index " +
                                        std::to_string(i) +
                                        ", op_kind: " + get_opr_name(op_kind) +
                                        ", error: No operator returned");
              }
            });
        if (status.ok()) {
          i = builder->stepping(i);
          // Reset status to OK after a successful match.
          status = gs::Status::OK();
          break;
        }
      }
    }

    if (i == old_i) {
      std::stringstream ss;
      ss << "[Update Pipeline Parse Failed] " << get_opr_name(op_kind)
         << ", op_kind " << op_kind << " failed to Build plan at index " << i
         << ", error message: " << status.ToString();
      auto err = gs::Status(status.error_code(), ss.str());
      LOG(ERROR) << err.ToString();
      RETURN_ERROR(err);
    }
  }
  return UpdatePipeline(std::move(operators));
}

gs::result<AdminPipeline> PlanParser::parse_admin_pipeline(
    const gs::Schema& schema, const physical::AdminPlan& admin_plan) {
  std::vector<std::unique_ptr<IOperator>> operators;
  ContextMeta cur_ctx_meta;
  for (int i = 0; i < admin_plan.plan_size(); ++i) {
    auto op_kind = admin_plan.plan(i).kind_case();
    if (admin_op_builders_.find(op_kind) == admin_op_builders_.end()) {
      std::stringstream ss;
      ss << "[Admin Pipeline Parse Failed] " << get_opr_name(op_kind)
         << " failed to parse admin plan at index " << i;
      RETURN_ERROR(gs::Status(gs::StatusCode::ERR_INTERNAL_ERROR, ss.str()));
    }
    auto op = admin_op_builders_.at(op_kind)->Build(schema, cur_ctx_meta,
                                                    admin_plan, i);
    if (!op.value().first) {
      std::stringstream ss;
      ss << "[Admin Pipeline Parse Failed] " << get_opr_name(op_kind)
         << " failed to parse admin plan at index " << i;
      auto err = gs::Status(gs::StatusCode::ERR_INTERNAL_ERROR, ss.str());
      LOG(ERROR) << err.ToString();
      RETURN_ERROR(err);
    }
    operators.emplace_back(std::move(op.value().first));
  }
  return AdminPipeline(std::move(operators));
}

gs::result<runtime::Context> ParseAndExecuteReadPipeline(
    const StorageReadInterface& graph, const physical::PhysicalPlan& plan,
    OprTimer* timer) {
  runtime::Context ctx;
  gs::Status status = Status::OK();
  TRY_HANDLE_ALL_WITH_EXCEPTION(
      gs::result<runtime::Context>,
      [&]() {
        return runtime::PlanParser::get()
            .parse_read_pipeline(graph.schema(), ContextMeta(), plan)
            .and_then([&](ReadPipeline&& rp) {
              return rp.Execute(const_cast<StorageReadInterface&>(graph),
                                runtime::Context(), {}, timer);
            });
      },
      [&](const auto& _status) { status = _status; },
      [&](gs::result<runtime::Context>&& res) {
        ctx = std::move(res.value());
      });
  if (!status.ok()) {
    RETURN_ERROR(status);
  }
  return std::move(ctx);
}

gs::result<runtime::Context> ParseAndExecuteUpdatePipeline(
    StorageUpdateInterface& graph, const physical::PhysicalPlan& plan,
    OprTimer* timer) {
  runtime::Context ctx;
  gs::Status status = Status::OK();
  TRY_HANDLE_ALL_WITH_EXCEPTION(
      gs::result<runtime::Context>,
      [&]() {
        return runtime::PlanParser::get()
            .parse_update_pipeline(graph.schema(), plan)
            .and_then([&](UpdatePipeline&& up) {
              return up.Execute(graph, runtime::Context(), {}, timer);
            });
      },
      [&](const auto& _status) { status = _status; },
      [&](gs::result<runtime::Context>&& res) {
        ctx = std::move(res.value());
      });
  if (!status.ok()) {
    RETURN_ERROR(status);
  }
  return std::move(ctx);
}

gs::result<runtime::Context> ParseAndExecuteAdminPipeline(
    StorageUpdateInterface& graph, const physical::AdminPlan& admin_plan,
    OprTimer* timer) {
  runtime::Context ctx;
  gs::Status status = Status::OK();
  TRY_HANDLE_ALL_WITH_EXCEPTION(
      gs::result<runtime::Context>,
      [&]() {
        return runtime::PlanParser::get()
            .parse_admin_pipeline(graph.schema(), admin_plan)
            .and_then([&](AdminPipeline&& ap) {
              return ap.Execute(graph, runtime::Context(), {}, timer);
            });
      },
      [&](const auto& _status) { status = _status; },
      [&](gs::result<runtime::Context>&& res) {
        ctx = std::move(res.value());
      });
  if (!status.ok()) {
    RETURN_ERROR(status);
  }
  return std::move(ctx);
}

}  // namespace runtime

}  // namespace gs
