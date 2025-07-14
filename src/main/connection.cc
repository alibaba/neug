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
  ////////////////////////////////////////////////////////////////////
  // Idealy we should compile the plan from query_string, but currently we use
  // string find to check whether we need to bulk load.
  if (query_string.find("CREATE") != std::string::npos ||
      query_string.find("create") != std::string::npos ||
      query_string.find("ALTER") != std::string::npos ||
      query_string.find("alter") != std::string::npos ||
      query_string.find("DROP") != std::string::npos ||
      query_string.find("drop") != std::string::npos) {
    auto ddl_plan = createDDLPlan(query_string);
    auto query_res = query_processor_->execute(ddl_plan.physical_plan);
    if (!query_res.ok()) {
      LOG(ERROR) << "Error in executing DDL query: " << query_string
                 << ", error code: " << query_res.status().error_code()
                 << ", message: " << query_res.status().error_message();
      return Result<results::CollectiveResults>(query_res.status());
    }
    query_res.value().set_result_schema(ddl_plan.result_schema);
    return Result(std::move(query_res.move_value()));
  }

  if (query_string.find("COPY") != std::string::npos ||
      query_string.find("copy") != std::string::npos) {
    LOG(INFO) << "Start create DML plan for " << query_string;
    auto dml_plan = createDMLPlan(query_string);
    auto query_res = query_processor_->execute(dml_plan.physical_plan);
    if (!query_res.ok()) {
      LOG(ERROR) << "Error in executing DML query: " << query_string
                 << ", error code: " << query_res.status().error_code()
                 << ", message: " << query_res.status().error_message();
      return Result<results::CollectiveResults>(query_res.status());
    }
    query_res.value().set_result_schema(dml_plan.result_schema);
    return Result(std::move(query_res.move_value()));
  }
  Plan plan;

  plan = planner_->compilePlan(
      query_string, read_yaml_file_to_string(db_.get_schema_yaml_path()),
      db_.get_statistics_json());

  if (plan.error_code != StatusCode::OK) {
    LOG(ERROR) << "Error in query: " << query_string
               << ", error code: " << plan.error_code
               << ", message: " << plan.full_message;
    return Result<results::CollectiveResults>(
        // TODO: Use the error code from the plan after plan.error_code is
        // defined as int.
        Status(StatusCode::ERR_COMPILATION, plan.full_message));
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
  return Result(std::move(result.move_value()));
}

Plan Connection::createDDLPlanWithGopt(const std::string& query_string) {
  auto plan = planner_->compilePlan(
      query_string, read_yaml_file_to_string(db_.get_schema_yaml_path()),
      db_.get_statistics_json());
  if (plan.error_code != StatusCode::OK) {
    throw std::runtime_error("Failed to compile DDL plan: " +
                             plan.full_message);
  }
  return plan;
}

Plan Connection::createDDLPlan(const std::string& query_string) {
  if (planner_->type() == "gopt") {
    return createDDLPlanWithGopt(query_string);
  }
  throw std::runtime_error(
      "Unknown planner type: " + planner_->type() +
      ", cannot create DDL plan for query: " + query_string);
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

Plan Connection::createDMLPlanWithGopt(const std::string& query_string) {
  auto graph_schema_yaml = read_yaml_file_to_string(db_.get_schema_yaml_path());
  auto graph_statistic_json = db_.get_statistics_json();
  Plan plan = planner_->compilePlan(query_string, graph_schema_yaml,
                                    graph_statistic_json);
  if (plan.error_code != StatusCode::OK) {
    throw std::runtime_error("Failed to compile load edge plan: " +
                             plan.full_message);
  }
  LOG(INFO) << "Compiled plan for query: " << query_string
            << ", plan size: " << plan.physical_plan.ByteSizeLong();
  if (plan.physical_plan.query_plan().plan(0).opr().op_kind_case() !=
      physical::PhysicalOpr::Operator::kSource) {
    LOG(FATAL) << "The first operator is not a LoadEdge operator for query: "
               << query_string << ", first operator: "
               << plan.physical_plan.query_plan().plan(0).opr().DebugString();
    return plan;
  }
  LOG(INFO) << "Successfully compiled load edge plan for query: "
            << query_string;
  return plan;
}

Plan Connection::createDMLPlan(const std::string& query_string) {
  if (planner_->type() == "gopt") {
    return createDMLPlanWithGopt(query_string);
  }
  // physical::PhysicalPlan plan;
  Plan plan;
  // Read env FLEX_DATA_DIR
  const char* env_p = std::getenv("FLEX_DATA_DIR");
  if (env_p == nullptr) {
    LOG(FATAL) << "FLEX_DATA_DIR is not set.";
    return plan;
  }
  std::string flex_data_dir(env_p);
  std::string person_csv_path = flex_data_dir + "/person.csv";
  std::string knows_csv_path = flex_data_dir + "/person_knows_person.csv";

  auto query_plan = plan.physical_plan.mutable_query_plan();
  query_plan->set_mode(physical::QueryPlan::Mode::QueryPlan_Mode_WRITE_ONLY);
  if (query_string.find("knows") != std::string::npos) {
    if (planner_->type() == "dummy") {
      _create_batch_load_edge_plan(query_plan, knows_csv_path, "knows",
                                   "person", "person");
    } else {
      LOG(FATAL) << "Unknown planner type: " << planner_->type()
                 << ", cannot create DML plan for query: " << query_string;
    }
  } else if (query_string.find("person") != std::string::npos) {
    if (planner_->type() == "dummy") {
      _create_batch_load_vertex_plan(query_plan, person_csv_path, "person");
    } else {
      LOG(FATAL) << "Unknown planner type: " << planner_->type()
                 << ", cannot create DML plan for query: " << query_string;
    }
  } else {
    LOG(FATAL) << "Unknown query: " << query_string;
  }

  LOG(INFO) << "plan: " << query_plan->DebugString();
  // TODO(zhanglei): Remove this after we have a real plan.
  return plan;
}

}  // namespace gs
