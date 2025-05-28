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
      query_string.find("alter") != std::string::npos) {
    auto ddl_plan = createDDLPlan(query_string);
    return query_processor_->execute(ddl_plan);
  }

  if (query_string.find("COPY") != std::string::npos ||
      query_string.find("copy") != std::string::npos) {
    auto dml_plan = createDMLPlan(query_string);
    return query_processor_->execute(dml_plan);
  }
  Plan plan;
  if (planner_->type() == "dummy") {
    plan.error_code = "OK";
    plan.full_message = "OK";
    plan.physical_plan = load_plan_from_resource(resource_path_ + "/nexg.pb");
  } else {
    plan = planner_->compilePlan(
        query_string, read_yaml_file_to_string(db_.get_schema_yaml_path()),
        db_.get_statistics_json());
  }
  // auto plan = planner_->compilePlan(
  //     query_string, read_yaml_file_to_string(db_.get_schema_yaml_path()),
  //     db_.get_statistics_json());

  if (plan.error_code != "OK") {
    LOG(ERROR) << "Error in query: " << query_string
               << ", error code: " << plan.error_code
               << ", message: " << plan.full_message;
    return Result<results::CollectiveResults>(
        // TODO: Use the error code from the plan after plan.error_code is
        // defined as int.
        Status(StatusCode::ERR_COMPILATION, plan.full_message));
  }
  // Dump plan to binary
  // {
  //   std::string plan_binary;
  //   if (!plan.physical_plan.SerializeToString(&plan_binary)) {
  //     LOG(ERROR) << "Error in serializing plan to binary.";
  //     return Result<results::CollectiveResults>(Status(
  //         StatusCode::ERR_COMPILATION, "Error in serializing plan."));
  //   }
  //   // Open ~/nexg.pb and write
  //   std::ofstream fs("/tmp/nexg.pb", std::ios::out | std::ios::binary);
  //   fs.write(plan_binary.data(), plan_binary.size());
  //   fs.close();
  //   LOG(INFO) << "Serialized plan to /tmp/nexg.pb, size: "
  //             << plan_binary.size();
  // }

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
  }
  return result;
}

physical::PhysicalPlan Connection::createDDLPlan(
    const std::string& query_string) {
  physical::PhysicalPlan physical_plan;
  auto plan = physical_plan.mutable_ddl_plan();
  // Currently we use a builtin plan for testing
  // TODO(zhanglei): Remove this after we have a real plan.

  if (query_string == "ALTER TABLE person ADD birthday DATE;") {
    auto alter_vertex_request = plan->mutable_add_vertex_property_schema();
    alter_vertex_request->mutable_vertex_type()->set_name("person");
    auto birthday_property = alter_vertex_request->add_properties();
    birthday_property->set_name("birthday");
    birthday_property->mutable_type()
        ->mutable_temporal()
        ->mutable_date()
        ->set_date_format(
            common::Temporal::DateFormat::Temporal_DateFormat_DF_YYYY_MM_DD);
    alter_vertex_request->set_conflict_action(
        physical::ConflictAction::ON_CONFLICT_THROW);
    return physical_plan;
  }

  if (query_string == "ALTER TABLE person ADD IF NOT EXISTS birthday DATE;") {
    auto alter_vertex_request = plan->mutable_add_vertex_property_schema();
    alter_vertex_request->mutable_vertex_type()->set_name("person");
    auto birthday_property = alter_vertex_request->add_properties();
    birthday_property->set_name("birthday");
    birthday_property->mutable_type()
        ->mutable_temporal()
        ->mutable_date()
        ->set_date_format(
            common::Temporal::DateFormat::Temporal_DateFormat_DF_YYYY_MM_DD);
    alter_vertex_request->set_conflict_action(
        physical::ConflictAction::ON_CONFLICT_DO_NOTHING);
    return physical_plan;
  }

  if (query_string == "ALTER TABLE person ADD name STRING;") {
    auto alter_vertex_request = plan->mutable_add_vertex_property_schema();
    alter_vertex_request->mutable_vertex_type()->set_name("person");
    auto name_property = alter_vertex_request->add_properties();
    name_property->set_name("name");
    name_property->mutable_type()->mutable_string()->mutable_long_text();
    alter_vertex_request->set_conflict_action(
        physical::ConflictAction::ON_CONFLICT_THROW);
    return physical_plan;
  }

  // Insert proeprty to edge schema
  if (query_string == "ALTER TABLE knows ADD registion DATE;") {
    auto alter_edge_request = plan->mutable_add_edge_property_schema();
    alter_edge_request->mutable_edge_type()->mutable_type_name()->set_name(
        "knows");
    alter_edge_request->mutable_edge_type()->mutable_src_type_name()->set_name(
        "person");
    alter_edge_request->mutable_edge_type()->mutable_dst_type_name()->set_name(
        "person");
    auto registion_property = alter_edge_request->add_properties();
    registion_property->set_name("registion");
    registion_property->mutable_type()
        ->mutable_temporal()
        ->mutable_date()
        ->set_date_format(
            common::Temporal::DateFormat::Temporal_DateFormat_DF_YYYY_MM_DD);
    alter_edge_request->set_conflict_action(
        physical::ConflictAction::ON_CONFLICT_THROW);
    return physical_plan;
  }

  // Drop an non-existing column
  if (query_string == "ALTER TABLE person DROP non_existing_column;") {
    auto alter_vertex_request = plan->mutable_drop_vertex_property_schema();
    alter_vertex_request->mutable_vertex_type()->set_name("person");
    alter_vertex_request->set_conflict_action(
        physical::ConflictAction::ON_CONFLICT_THROW);
    alter_vertex_request->add_properties("non_existing_column");
    return physical_plan;
  }

  // Drop an existing column
  if (query_string == "ALTER TABLE person DROP IF EXISTS birthday;") {
    auto alter_vertex_request = plan->mutable_drop_vertex_property_schema();
    alter_vertex_request->mutable_vertex_type()->set_name("person");
    alter_vertex_request->set_conflict_action(
        physical::ConflictAction::ON_CONFLICT_DO_NOTHING);
    alter_vertex_request->add_properties("birthday");
    return physical_plan;
  }

  // Drop a column that already been dropped
  if (query_string == "ALTER TABLE person DROP birthday;") {
    auto alter_vertex_request = plan->mutable_drop_vertex_property_schema();
    alter_vertex_request->mutable_vertex_type()->set_name("person");
    alter_vertex_request->set_conflict_action(
        physical::ConflictAction::ON_CONFLICT_THROW);
    alter_vertex_request->add_properties("birthday");
    return physical_plan;
  }

  // Rename a column
  if (query_string == "ALTER TABLE person RENAME name TO username;") {
    auto alter_vertex_request = plan->mutable_rename_vertex_property_schema();
    alter_vertex_request->mutable_vertex_type()->set_name("person");
    alter_vertex_request->set_conflict_action(
        physical::ConflictAction::ON_CONFLICT_THROW);
    auto mappings = alter_vertex_request->mutable_mappings();
    mappings->insert({"name", "username"});  // Rename 'name' to 'username'
    return physical_plan;
  }

  if (query_string.find("knows") != std::string::npos) {
    auto create_edge_request = plan->mutable_create_edge_schema();
    create_edge_request->set_multiplicity(
        physical::CreateEdgeSchema::Multiplicity::
            CreateEdgeSchema_Multiplicity_MANY_TO_MANY);
    create_edge_request->mutable_edge_type()->mutable_type_name()->set_name(
        "knows");
    create_edge_request->mutable_edge_type()->mutable_src_type_name()->set_name(
        "person");
    create_edge_request->mutable_edge_type()->mutable_dst_type_name()->set_name(
        "person");
    auto weight_prop = create_edge_request->add_properties();
    weight_prop->set_name("weight");
    weight_prop->mutable_type()->set_primitive_type(
        common::PrimitiveType::DT_DOUBLE);
    return physical_plan;
  }

  if (query_string.find("person") != std::string::npos) {
    auto create_vertex_reequest = plan->mutable_create_vertex_schema();
    create_vertex_reequest->mutable_vertex_type()->set_name("person");
    create_vertex_reequest->mutable_primary_key()->Add("id");
    auto id_property = create_vertex_reequest->add_properties();
    id_property->set_name("id");
    id_property->mutable_type()->set_primitive_type(
        common::PrimitiveType::DT_SIGNED_INT64);
    auto name_property = create_vertex_reequest->add_properties();
    name_property->set_name("name");
    name_property->mutable_type()->mutable_string()->mutable_long_text();
    auto age_property = create_vertex_reequest->add_properties();
    age_property->set_name("age");
    age_property->mutable_type()->set_primitive_type(
        common::PrimitiveType::DT_SIGNED_INT64);
    return physical_plan;
  }
  LOG(FATAL) << "Unknown query: " << query_string;
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

physical::PhysicalPlan Connection::createDMLPlan(
    const std::string& query_string) {
  physical::PhysicalPlan plan;
  // Read env FLEX_DATA_DIR
  const char* env_p = std::getenv("FLEX_DATA_DIR");
  if (env_p == nullptr) {
    LOG(FATAL) << "FLEX_DATA_DIR is not set.";
    return plan;
  }
  std::string flex_data_dir(env_p);
  std::string person_csv_path = flex_data_dir + "/person.csv";
  std::string knows_csv_path = flex_data_dir + "/person_knows_person.csv";

  auto query_plan = plan.mutable_query_plan();
  query_plan->set_mode(physical::QueryPlan::Mode::QueryPlan_Mode_WRITE_ONLY);
  if (query_string.find("knows") != std::string::npos) {
    _create_batch_load_edge_plan(query_plan, knows_csv_path, "knows", "person",
                                 "person");
  } else if (query_string.find("person") != std::string::npos) {
    _create_batch_load_vertex_plan(query_plan, person_csv_path, "person");
  } else {
    LOG(FATAL) << "Unknown query: " << query_string;
  }

  LOG(INFO) << "plan: " << plan.DebugString();
  // TODO(zhanglei): Remove this after we have a real plan.
  return plan;
}

}  // namespace gs
