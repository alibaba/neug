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

#include "src/main/connection.h"

#include "src/proto_generated_gie/results.pb.h"
#include "src/utils/pb_utils.h"
#include "src/utils/yaml_utils.h"

namespace gs {

physical::PhysicalPlan load_plan_from_resource(
    const std::string& resource_path) {
  physical::PhysicalPlan plan;
  // Load the plan from the resource file.
  std::ifstream plan_file(resource_path, std::ios::in | std::ios::binary);
  if (!plan_file.is_open()) {
    LOG(ERROR) << "Failed to open plan file: " << resource_path;
    return plan;
  }
  if (!plan.ParseFromIstream(&plan_file)) {
    LOG(ERROR) << "Failed to parse plan file: " << resource_path;
    return plan;
  }
  plan_file.close();
  LOG(INFO) << "Loaded plan from resource: " << resource_path
            << ", plan size: " << plan.ByteSizeLong();
  return plan;
}

Result<QueryResult> Connection::query(const std::string& query_string) {
  LOG(INFO) << "Query: " << query_string;
  if (is_closed()) {
    LOG(ERROR) << "Connection is closed, cannot execute query.";
    return Result<QueryResult>(
        Status(StatusCode::ERR_CONNECTION_BROKEN, "Connection is closed."));
  }
  auto result = query_impl(query_string);
  if (result.ok()) {
    return Result<QueryResult>(
        QueryResult::From(std::move(result.move_value())));
  } else {
    return Result<QueryResult>(result.status());
  }
}

Result<results::CollectiveResults> Connection::query_impl(
    const std::string& query_string) {
  LOG(INFO) << "Executing query: " << query_string;

  Plan plan;

  plan = planner_->compilePlan(query_string);

  if (plan.error_code != StatusCode::OK) {
    throw std::runtime_error("Failed to compile plan, error code " +
                             std::to_string(static_cast<int>(plan.error_code)) +
                             ", " + plan.full_message);
  }

  VLOG(10) << "Physical plan: " << plan.physical_plan.DebugString();
  LOG(INFO) << "Got physical plan, "
            << plan.physical_plan.query_plan().plan_size() << " operators.";

  auto result = query_processor_->execute(plan.physical_plan);
  if (result.ok()) {
    LOG(INFO) << "Query executed successfully.";
  } else {
    LOG(ERROR) << "Error in executing query: " << query_string
               << ", error code: " << result.status().error_code()
               << ", message: " << result.status().error_message();
    return result.status();
  }
  result.value().set_result_schema(plan.result_schema);
  // If the query contains operator that may mutable the graph schema or data,
  // we should update the schema and statistics for the planner.
  if (has_update_opr_in_plan(plan.physical_plan) ||
      plan.physical_plan.has_ddl_plan()) {
    VLOG(10) << "Update operation detected, updating schema and statistics.";
    auto yaml = db_.schema().to_yaml();
    if (!yaml.ok()) {
      LOG(ERROR) << "Failed to convert schema to YAML: "
                 << yaml.status().error_message();
      throw std::runtime_error("Failed to convert schema to YAML: " +
                               yaml.status().error_message());
    }
    planner_->update_meta(yaml.value(), db_.get_statistics_json());
  } else {
    LOG(INFO) << "No update operation detected, no need to update schema."
              << (int) plan.physical_plan.has_ddl_plan();
  }

  return Result(std::move(result.move_value()));
}

void _create_batch_load_vertex_plan(physical::QueryPlan* query_plan,
                                    const std::string& file_path,
                                    const std::string& vertex_type_name) {
  {
    // data source
    auto read_csv_opr = query_plan->add_plan()
                            ->mutable_opr()
                            ->mutable_source()
                            ->mutable_read_csv();
    read_csv_opr->set_file_path(file_path);
    auto csv_options = read_csv_opr->mutable_csv_options();
    csv_options->set_delimiter("|");
    csv_options->set_header(true);
    read_csv_opr->set_batch_reader(true);  // Using batch reader saves memory
  }

  {
    // BatchInsert
    // 将table里面的每个column通过propertyMapping映射到Vertex
    // Property，expression就只是一个Var
    auto batch_insert_vertex_opt =
        query_plan->add_plan()->mutable_opr()->mutable_load_vertex();
    batch_insert_vertex_opt->mutable_vertex_type()->set_name("person");
    {
      // property mappings
      auto id_mapping = batch_insert_vertex_opt->add_property_mappings();
      id_mapping->mutable_property()->mutable_key()->set_name("id");
      id_mapping->mutable_data()
          ->add_operators()
          ->mutable_var()
          ->mutable_tag()
          ->set_id(0);
      auto name_mapping = batch_insert_vertex_opt->add_property_mappings();
      name_mapping->mutable_property()->mutable_key()->set_name("name");
      name_mapping->mutable_data()
          ->add_operators()
          ->mutable_var()
          ->mutable_tag()
          ->set_id(1);
      auto age_mapping = batch_insert_vertex_opt->add_property_mappings();
      age_mapping->mutable_property()->mutable_key()->set_name("age");
      age_mapping->mutable_data()
          ->add_operators()
          ->mutable_var()
          ->mutable_tag()
          ->set_id(2);
    }
  }
}

void _create_batch_load_edge_plan(physical::QueryPlan* query_plan,
                                  const std::string& file_path,
                                  const std::string& edge_type_name,
                                  const std::string& src_type_name,
                                  const std::string& dst_type_name) {
  {
    // data source
    auto read_csv_opr = query_plan->add_plan()
                            ->mutable_opr()
                            ->mutable_source()
                            ->mutable_read_csv();
    read_csv_opr->set_file_path(file_path);
    auto csv_options = read_csv_opr->mutable_csv_options();
    csv_options->set_delimiter("|");
    csv_options->set_header(true);
  }
  {
    auto batch_insert_edge_opt =
        query_plan->add_plan()->mutable_opr()->mutable_load_edge();
    batch_insert_edge_opt->mutable_edge_type()->mutable_type_name()->set_name(
        edge_type_name);
    batch_insert_edge_opt->mutable_edge_type()
        ->mutable_src_type_name()
        ->set_name(src_type_name);
    batch_insert_edge_opt->mutable_edge_type()
        ->mutable_dst_type_name()
        ->set_name(dst_type_name);
    {
      // property mappings
      auto src_mapping = batch_insert_edge_opt->add_source_vertex_binding();
      src_mapping->mutable_property()->mutable_key()->set_name("id");
      src_mapping->mutable_data()
          ->add_operators()
          ->mutable_var()
          ->mutable_tag()
          ->set_id(0);

      auto dst_mapping =
          batch_insert_edge_opt->add_destination_vertex_binding();
      dst_mapping->mutable_property()->mutable_key()->set_name("id");
      dst_mapping->mutable_data()
          ->add_operators()
          ->mutable_var()
          ->mutable_tag()
          ->set_id(1);
      auto prop_mapping = batch_insert_edge_opt->add_property_mappings();
      prop_mapping->mutable_property()->mutable_key()->set_name("weight");
      prop_mapping->mutable_data()
          ->add_operators()
          ->mutable_var()
          ->mutable_tag()
          ->set_id(2);
    }
  }
}

}  // namespace gs
