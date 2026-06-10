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

#include "neug/execution/execute/ops/ddl/create_edge_type.h"
#include "neug/execution/common/types/value.h"
#include "neug/utils/pb_utils.h"

namespace neug {
namespace execution {
namespace ops {

class CreateEdgeTypeOpr : public IOperator {
 public:
  using property_def_t = std::vector<std::pair<std::string, Value>>;
  using create_edge_type_t =
      std::tuple<std::string, std::string, std::string, property_def_t,
                 EdgeStrategy, EdgeStrategy, std::optional<std::string>, bool>;
  CreateEdgeTypeOpr(const std::vector<create_edge_type_t>& create_edge_types,
                    bool ignore_conflict)
      : create_edge_types_(create_edge_types),
        ignore_conflict_(ignore_conflict) {}

  std::string get_operator_name() const override { return "CreateEdgeTypeOpr"; }

  neug::result<Context> Eval(IStorageInterface& graph, const ParamsMap& params,
                             Context&& ctx, OprTimer* timer) override {
    StorageUpdateInterface& storage =
        dynamic_cast<StorageUpdateInterface&>(graph);
    int32_t defs_size = create_edge_types_.size();
    // Track indices of edge types actually created by this operator,
    // so rollback only reverts what we created (not pre-existing types).
    std::vector<int32_t> created_indices;
    Status status;
    bool failed = false;
    for (int32_t i = 0; i < defs_size; ++i) {
      const auto& create_edge_def = create_edge_types_[i];
      CreateEdgeTypeParamBuilder config_builder;
      for (const auto& [prop_name, prop_value] : std::get<3>(create_edge_def)) {
        config_builder.AddProperty(prop_name, prop_value);
      }
      config_builder.SrcLabel(std::get<0>(create_edge_def))
          .DstLabel(std::get<1>(create_edge_def))
          .EdgeLabel(std::get<2>(create_edge_def))
          .OEEdgeStrategy(std::get<4>(create_edge_def))
          .IEEdgeStrategy(std::get<5>(create_edge_def))
          .SortKeyForNbr(std::get<6>(create_edge_def))
          .Temporary(std::get<7>(create_edge_def));
      status = storage.CreateEdgeType(config_builder.Build());
      if (!status.ok()) {
        if (ignore_conflict_ && IsSchemaConflictError(status)) {
          continue;
        }
        LOG(ERROR) << "Fail to insert edge triplet: "
                   << std::get<0>(create_edge_def) << ", "
                   << std::get<1>(create_edge_def) << ", "
                   << std::get<2>(create_edge_def)
                   << ", reason: " << status.ToString();
        failed = true;
        break;
      }
      created_indices.push_back(i);
    }
    if (failed) {
      // Rollback only the edge types we actually created.
      for (auto it = created_indices.rbegin(); it != created_indices.rend();
           ++it) {
        const auto& create_edge_def = create_edge_types_[*it];
        if (!storage
                 .DeleteEdgeType(std::get<0>(create_edge_def),
                                 std::get<1>(create_edge_def),
                                 std::get<2>(create_edge_def))
                 .ok()) {
          LOG(ERROR) << "Fail to revert created edge type in CreateEdgeSchema "
                        "request";
        }
      }
      RETURN_ERROR(status);
    }
    return neug::result<Context>(std::move(ctx));
  }

 private:
  std::vector<create_edge_type_t> create_edge_types_;
  bool ignore_conflict_;
};

static void parse_options(const physical::CreateEdgeSchema& create_edges,
                          std::optional<std::string>& sort_key_for_nbr) {
  if (create_edges.options_size() > 0) {
    for (const auto& option : create_edges.options()) {
      if (option.first == "SORT_KEY_FOR_NBR") {
        sort_key_for_nbr = option.second;
        break;
      } else {
        LOG(WARNING) << "Unknown option: " << option.first
                     << " in CreateEdgeSchema, it will be ignored";
      }
    }
  }
  if (sort_key_for_nbr.has_value()) {
    bool valid_sort_key = false;
    for (const auto& t : create_edges.properties()) {
      if (t.name() == sort_key_for_nbr.value()) {
        valid_sort_key = true;
        break;
      }
    }
    if (!valid_sort_key) {
      LOG(ERROR) << "Invalid sort key: " << sort_key_for_nbr.value()
                 << " for CreateEdgeTypeOpr, it should be one of the "
                    "property names";
      THROW_INVALID_ARGUMENT_EXCEPTION(
          "Invalid sort key: " + sort_key_for_nbr.value() +
          " for CreateEdgeTypeOpr, it should be one of "
          "the property names");
    }
  }
}

neug::result<OpBuildResultT> CreateEdgeTypeOprBuilder::Build(
    const Schema& schema, const ContextMeta& ctx_meta,
    const physical::PhysicalPlan& plan, int op_id) {
  const auto& create_edges = plan.plan(op_id).opr().create_edge_schema();
  auto tuple_res = property_defs_to_value(create_edges.properties());
  std::optional<std::string> sortkey_for_nbr = std::nullopt;
  parse_options(create_edges, sortkey_for_nbr);
  if (!tuple_res) {
    RETURN_ERROR(tuple_res.error());
  }
  if (create_edges.primary_key_size() != 0) {
    LOG(ERROR) << "Primary key is not supported for edge type creation";
    RETURN_ERROR(Status(StatusCode::ERR_INVALID_ARGUMENT,
                        "Primary key is not supported for edge type creation"));
  }
  bool ignore_conflict =
      !conflict_action_to_bool(create_edges.conflict_action());
  bool is_temporary = create_edges.temporary();
  using create_edge_value_t = typename CreateEdgeTypeOpr::create_edge_type_t;
  std::vector<create_edge_value_t> create_edge_defs;
  for (int32_t i = 0; i < create_edges.type_info_size(); ++i) {
    const auto& create_edge = create_edges.type_info(i);
    auto multiplicity = create_edges.type_info(i).multiplicity();
    auto edge_type_name = create_edge.edge_type().type_name().name();
    auto src_vertex_type_name = create_edge.edge_type().src_type_name().name();
    auto dst_vertex_type_name = create_edge.edge_type().dst_type_name().name();
    EdgeStrategy oe_stragety, ie_stragety;
    if (!multiplicity_to_storage_strategy(multiplicity, oe_stragety,
                                          ie_stragety)) {
      LOG(ERROR) << "Invalid edge multiplicity: " << multiplicity;
      RETURN_ERROR(Status(
          StatusCode::ERR_INVALID_ARGUMENT,
          "Invalid edge multiplicity: " +
              physical::CreateEdgeSchema_Multiplicity_Name(multiplicity)));
    }
    create_edge_defs.emplace_back(src_vertex_type_name, dst_vertex_type_name,
                                  edge_type_name, tuple_res.value(),
                                  oe_stragety, ie_stragety, sortkey_for_nbr,
                                  is_temporary);
  }

  return std::make_pair(
      std::make_unique<CreateEdgeTypeOpr>(create_edge_defs, ignore_conflict),
      ctx_meta);
}

}  // namespace ops
}  // namespace execution
}  // namespace neug
