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

#include <gtest/gtest.h>
#include <filesystem>
#include "neug/storages/graph/property_graph.h"
#include "neug/utils/property/types.h"
#include "neug/utils/yaml_utils.h"

namespace neug {

class PropertyGraphLogicalDeleteTest : public ::testing::Test {
 protected:
  void SetUp() override {
    test_dir_ =
        "/tmp/property_graph_logical_delete_test_" +
        std::to_string(
            std::chrono::steady_clock::now().time_since_epoch().count());
    if (std::filesystem::exists(test_dir_)) {
      std::filesystem::remove_all(test_dir_);
    }
    std::filesystem::create_directories(test_dir_);
    graph_.Open(test_dir_, MemoryLevel::kInMemory);
  }

  void TearDown() override {
    if (std::filesystem::exists(test_dir_)) {
      std::filesystem::remove_all(test_dir_);
    }
  }

  PropertyGraph graph_;
  std::string test_dir_;
};

// Test DeleteVertexType - physically removes vertex type and data
TEST_F(PropertyGraphLogicalDeleteTest, DeleteVertexType_RemovesTypeAndData) {
  // Create a vertex type with properties
  std::vector<std::tuple<DataType, std::string, Property>> properties = {
      {DataTypeId::kInt32, "age", Property::from_int32(0)},
      {DataTypeId::kVarchar, "name", Property::from_string_view("string")}};
  std::vector<std::string> pk_names = {"id"};
  properties.insert(properties.begin(),
                    {DataTypeId::kInt64, "id", Property::from_int64(0L)});

  auto status = graph_.CreateVertexType("Person", properties, pk_names);
  ASSERT_TRUE(status.ok());

  // Verify vertex type exists
  EXPECT_TRUE(graph_.schema().contains_vertex_label("Person"));
  label_t v_label = graph_.schema().get_vertex_label_id("Person");
  EXPECT_EQ(graph_.schema().get_vertex_label_name(v_label), "Person");

  // Delete physically
  status = graph_.DeleteVertexType("Person");
  ASSERT_TRUE(status.ok());

  // Verify type is physically deleted (not visible in schema)
  EXPECT_FALSE(graph_.schema().contains_vertex_label("Person"));
}

// Test corner case: Create -> Delete Physical -> Create again
TEST_F(PropertyGraphLogicalDeleteTest, CreateDeletePhysicalRecreate_Succeeds) {
  std::vector<std::tuple<DataType, std::string, Property>> properties = {
      {DataTypeId::kInt64, "id", Property::from_int64(0L)},
      {DataTypeId::kVarchar, "name", Property::from_string_view("string")}};
  std::vector<std::string> pk_names = {"id"};

  // First creation
  auto status = graph_.CreateVertexType("Person", properties, pk_names);
  ASSERT_TRUE(status.ok());
  graph_.schema().get_vertex_label_id("Person");

  // Physical delete
  status = graph_.DeleteVertexType("Person");
  ASSERT_TRUE(status.ok());
  EXPECT_FALSE(graph_.schema().contains_vertex_label("Person"));

  // Recreate should succeed
  status = graph_.CreateVertexType("Person", properties, pk_names);
  ASSERT_TRUE(status.ok());
  label_t second_label = graph_.schema().get_vertex_label_id("Person");
  EXPECT_TRUE(graph_.schema().vertex_label_valid(second_label));

  // May get same or different label ID depending on implementation
  EXPECT_TRUE(graph_.schema().contains_vertex_label("Person"));
}

TEST_F(PropertyGraphLogicalDeleteTest,
       CreateDeleteLogicalRecreate_ActsAsRevert) {
  std::vector<std::tuple<DataType, std::string, Property>> properties = {
      {DataTypeId::kInt64, "id", Property::from_int64(0L)},
      {DataTypeId::kVarchar, "name", Property::from_string_view("string")}};
  std::vector<std::string> pk_names = {"id"};

  auto status = graph_.CreateVertexType("Person", properties, pk_names);
  ASSERT_TRUE(status.ok());
  label_t v_label = graph_.schema().get_vertex_label_id("Person");
  EXPECT_EQ(graph_.schema().get_vertex_label_name(v_label), "Person");

  // Logical delete
  status = graph_.DeleteVertexType("Person", true);
  ASSERT_TRUE(status.ok());

  // Trying to create again should either:
  // 1. Fail with error_on_conflict=true
  // 2. Act as revert with error_on_conflict=false

  status = graph_.CreateVertexType("Person", properties, pk_names, false);
  ASSERT_TRUE(status.ok());
}

// Test DeleteEdgeType
TEST_F(PropertyGraphLogicalDeleteTest, DeleteEdgeTypePhysical_RemovesEdgeType) {
  // Create source and destination vertex types
  std::vector<std::tuple<DataType, std::string, Property>> v_props = {
      {DataTypeId::kInt64, "id", Property::from_int64(0L)}};
  std::vector<std::string> pk_names = {"id"};

  auto status = graph_.CreateVertexType("Person", v_props, pk_names);
  ASSERT_TRUE(status.ok());
  status = graph_.CreateVertexType("Company", v_props, pk_names);
  ASSERT_TRUE(status.ok());

  // Create edge type
  std::vector<std::tuple<DataType, std::string, Property>> e_props = {
      {DataTypeId::kInt32, "years", Property::from_int32(0)}};
  status = graph_.CreateEdgeType("Person", "Company", "WorksAt", e_props);
  ASSERT_TRUE(status.ok());

  // Verify edge exists
  EXPECT_TRUE(graph_.schema().has_edge_label("Person", "Company", "WorksAt"));

  // Delete physically
  status = graph_.DeleteEdgeType("Person", "Company", "WorksAt");
  ASSERT_TRUE(status.ok());

  // Verify edge type is removed
  EXPECT_FALSE(graph_.schema().has_edge_label("Person", "Company", "WorksAt"));
}

// Test DeleteVertexProperties
TEST_F(PropertyGraphLogicalDeleteTest,
       DeleteVertexPropertiesPhysical_RemovesProperties) {
  std::vector<std::tuple<DataType, std::string, Property>> properties = {
      {DataTypeId::kInt64, "id", Property::from_int64(0L)},
      {DataTypeId::kVarchar, "name", Property::from_string_view("string")},
      {DataTypeId::kInt32, "age", Property::from_int32(0)}};
  std::vector<std::string> pk_names = {"id"};

  auto status = graph_.CreateVertexType("Person", properties, pk_names);
  ASSERT_TRUE(status.ok());

  label_t v_label = graph_.schema().get_vertex_label_id("Person");
  EXPECT_TRUE(graph_.schema().vertex_has_property(v_label, "name"));
  EXPECT_TRUE(graph_.schema().vertex_has_property(v_label, "age"));

  // Delete property physically
  std::vector<std::string> delete_props = {"age"};
  status = graph_.DeleteVertexProperties("Person", delete_props);
  ASSERT_TRUE(status.ok());

  // Verify property is removed
  EXPECT_TRUE(graph_.schema().vertex_has_property(v_label, "name"));
  EXPECT_FALSE(graph_.schema().vertex_has_property(v_label, "age"));
}

// Test DeleteVertexPropertiesSoft
TEST_F(PropertyGraphLogicalDeleteTest,
       DeleteVertexPropertiesLogical_MarksPropertiesDeleted) {
  std::vector<std::tuple<DataType, std::string, Property>> properties = {
      {DataTypeId::kInt64, "id", Property::from_int64(0L)},
      {DataTypeId::kVarchar, "name", Property::from_string_view("string")},
      {DataTypeId::kInt32, "age", Property::from_int32(0)}};
  std::vector<std::string> pk_names = {"id"};

  auto status = graph_.CreateVertexType("Person", properties, pk_names);
  ASSERT_TRUE(status.ok());

  label_t v_label = graph_.schema().get_vertex_label_id("Person");

  // Delete property logically
  std::vector<std::string> delete_props = {"age"};
  status = graph_.DeleteVertexProperties("Person", delete_props, true);
  ASSERT_TRUE(status.ok());

  // Property should be logically hidden
  EXPECT_TRUE(graph_.schema().vertex_has_property(v_label, "name"));
  EXPECT_FALSE(graph_.schema().vertex_has_property(v_label, "age"));
}

// Test RevertDeleteVertexProperties
TEST_F(PropertyGraphLogicalDeleteTest,
       RevertDeleteVertexProperties_RestoresProperties) {
  std::vector<std::tuple<DataType, std::string, Property>> properties = {
      {DataTypeId::kInt64, "id", Property::from_int64(0L)},
      {DataTypeId::kVarchar, "name", Property::from_string_view("string")},
      {DataTypeId::kInt32, "age", Property::from_int32(0)}};
  std::vector<std::string> pk_names = {"id"};

  auto status = graph_.CreateVertexType("Person", properties, pk_names);
  ASSERT_TRUE(status.ok());

  label_t v_label = graph_.schema().get_vertex_label_id("Person");

  // Delete logically then revert
  std::vector<std::string> delete_props = {"age"};
  graph_.mutable_schema().DeleteVertexProperties("Person", delete_props, true);

  EXPECT_FALSE(graph_.schema().vertex_has_property(v_label, "age"));

  graph_.mutable_schema().RevertDeleteVertexProperties("Person", delete_props);
  // Property should be visible again
  EXPECT_TRUE(graph_.schema().vertex_has_property(v_label, "age"));
}

// Test DeleteEdgeProperties
TEST_F(PropertyGraphLogicalDeleteTest,
       DeleteEdgePropertiesPhysical_RemovesProperties) {
  std::vector<std::tuple<DataType, std::string, Property>> v_props = {
      {DataTypeId::kInt64, "id", Property::from_int64(0L)}};
  std::vector<std::string> pk_names = {"id"};

  graph_.CreateVertexType("Person", v_props, pk_names);
  graph_.CreateVertexType("Company", v_props, pk_names);

  std::vector<std::tuple<DataType, std::string, Property>> e_props = {
      {DataTypeId::kInt32, "years", Property::from_int32(0)},
      {DataTypeId::kVarchar, "position", Property::from_string_view("string")}};
  auto status = graph_.CreateEdgeType("Person", "Company", "WorksAt", e_props);
  ASSERT_TRUE(status.ok());

  label_t src_label = graph_.schema().get_vertex_label_id("Person");
  label_t dst_label = graph_.schema().get_vertex_label_id("Company");
  label_t e_label = graph_.schema().get_edge_label_id("WorksAt");

  EXPECT_TRUE(graph_.schema().edge_has_property(src_label, dst_label, e_label,
                                                "years"));
  EXPECT_TRUE(graph_.schema().edge_has_property(src_label, dst_label, e_label,
                                                "position"));

  // Delete property physically
  std::vector<std::string> delete_props = {"position"};
  status =
      graph_.DeleteEdgeProperties("Person", "Company", "WorksAt", delete_props);
  ASSERT_TRUE(status.ok());

  EXPECT_TRUE(graph_.schema().edge_has_property(src_label, dst_label, e_label,
                                                "years"));
  EXPECT_FALSE(graph_.schema().edge_has_property(src_label, dst_label, e_label,
                                                 "position"));
}

// Test DeleteEdgePropertiesSoft
TEST_F(PropertyGraphLogicalDeleteTest,
       DeleteEdgePropertiesLogical_MarksPropertiesDeleted) {
  std::vector<std::tuple<DataType, std::string, Property>> v_props = {
      {DataTypeId::kInt64, "id", Property::from_int64(0L)}};
  std::vector<std::string> pk_names = {"id"};

  graph_.CreateVertexType("Person", v_props, pk_names);
  graph_.CreateVertexType("Company", v_props, pk_names);

  std::vector<std::tuple<DataType, std::string, Property>> e_props = {
      {DataTypeId::kInt32, "years", Property::from_int32(0)},
      {DataTypeId::kVarchar, "position", Property::from_string_view("string")}};
  graph_.CreateEdgeType("Person", "Company", "WorksAt", e_props);

  label_t src_label = graph_.schema().get_vertex_label_id("Person");
  label_t dst_label = graph_.schema().get_vertex_label_id("Company");
  label_t e_label = graph_.schema().get_edge_label_id("WorksAt");

  // Delete property logically
  std::vector<std::string> delete_props = {"position"};
  graph_.mutable_schema().DeleteEdgeProperties("Person", "Company", "WorksAt",
                                               delete_props);

  EXPECT_TRUE(graph_.schema().edge_has_property(src_label, dst_label, e_label,
                                                "years"));
  EXPECT_FALSE(graph_.schema().edge_has_property(src_label, dst_label, e_label,
                                                 "position"));
}

// Test RevertDeleteEdgeProperties
TEST_F(PropertyGraphLogicalDeleteTest,
       RevertDeleteEdgeProperties_RestoresProperties) {
  std::vector<std::tuple<DataType, std::string, Property>> v_props = {
      {DataTypeId::kInt64, "id", Property::from_int64(0L)}};
  std::vector<std::string> pk_names = {"id"};

  graph_.CreateVertexType("Person", v_props, pk_names);
  graph_.CreateVertexType("Company", v_props, pk_names);

  std::vector<std::tuple<DataType, std::string, Property>> e_props = {
      {DataTypeId::kInt32, "years", Property::from_int32(0)},
      {DataTypeId::kVarchar, "position", Property::from_string_view("string")}};
  graph_.CreateEdgeType("Person", "Company", "WorksAt", e_props);

  label_t src_label = graph_.schema().get_vertex_label_id("Person");
  label_t dst_label = graph_.schema().get_vertex_label_id("Company");
  label_t e_label = graph_.schema().get_edge_label_id("WorksAt");

  // Delete logically then revert
  std::vector<std::string> delete_props = {"position"};
  graph_.mutable_schema().DeleteEdgeProperties("Person", "Company", "WorksAt",
                                               delete_props, true);
  EXPECT_FALSE(graph_.schema().edge_has_property(src_label, dst_label, e_label,
                                                 "position"));
  graph_.mutable_schema().RevertDeleteEdgeProperties("Person", "Company",
                                                     "WorksAt", delete_props);
  EXPECT_TRUE(graph_.schema().edge_has_property(src_label, dst_label, e_label,
                                                "position"));
}

// Test corner case: Multiple logical deletes and reverts
TEST_F(PropertyGraphLogicalDeleteTest,
       MultipleLogicalDeletesAndReverts_WorksCorrectly) {
  std::vector<std::tuple<DataType, std::string, Property>> properties = {
      {DataTypeId::kInt64, "id", Property::from_int64(0L)},
      {DataTypeId::kVarchar, "name", Property::from_string_view("string")},
      {DataTypeId::kInt32, "age", Property::from_int32(0)},
      {DataTypeId::kVarchar, "email", Property::from_string_view("string")}};
  std::vector<std::string> pk_names = {"id"};

  graph_.CreateVertexType("Person", properties, pk_names);
  label_t v_label = graph_.schema().get_vertex_label_id("Person");

  // Delete age logically
  graph_.mutable_schema().DeleteVertexProperties("Person", {"age"}, true);
  EXPECT_FALSE(graph_.schema().vertex_has_property(v_label, "age"));
  EXPECT_TRUE(graph_.schema().vertex_has_property(v_label, "email"));

  // Delete email logically
  graph_.mutable_schema().DeleteVertexProperties("Person", {"email"}, true);
  EXPECT_FALSE(graph_.schema().vertex_has_property(v_label, "age"));
  EXPECT_FALSE(graph_.schema().vertex_has_property(v_label, "email"));

  // Revert age
  graph_.mutable_schema().RevertDeleteVertexProperties("Person", {"age"});
  EXPECT_TRUE(graph_.schema().vertex_has_property(v_label, "age"));
  EXPECT_FALSE(graph_.schema().vertex_has_property(v_label, "email"));

  // Revert email
  graph_.mutable_schema().RevertDeleteVertexProperties("Person", {"email"});
  EXPECT_TRUE(graph_.schema().vertex_has_property(v_label, "age"));
  EXPECT_TRUE(graph_.schema().vertex_has_property(v_label, "email"));
}

// Test corner case: Cannot delete primary key property
TEST_F(PropertyGraphLogicalDeleteTest, DeletePrimaryKeyProperty_ShouldFail) {
  std::vector<std::tuple<DataType, std::string, Property>> properties = {
      {DataTypeId::kInt64, "id", Property::from_int64(0L)},
      {DataTypeId::kVarchar, "name", Property::from_string_view("string")}};
  std::vector<std::string> pk_names = {"id"};

  graph_.CreateVertexType("Person", properties, pk_names);

  EXPECT_THROW({ graph_.DeleteVertexProperties("Person", {"id"}); },
               neug::exception::Exception);
  EXPECT_THROW({ graph_.DeleteVertexProperties("Person", {"id"}); },
               neug::exception::RuntimeError);
}

// Test physical delete of properties after logical delete
TEST_F(PropertyGraphLogicalDeleteTest,
       PhysicalDeletePropertiesAfterLogicalDelete_Succeeds) {
  std::vector<std::tuple<DataType, std::string, Property>> properties = {
      {DataTypeId::kInt64, "id", Property::from_int64(0L)},
      {DataTypeId::kVarchar, "name", Property::from_string_view("string")},
      {DataTypeId::kInt32, "age", Property::from_int32(0)}};
  std::vector<std::string> pk_names = {"id"};
  graph_.CreateVertexType("Person", properties, pk_names);
  label_t v_label = graph_.schema().get_vertex_label_id("Person");
  // Logical delete property 'age'

  graph_.mutable_schema().DeleteVertexProperties("Person", {"age"}, true);
  EXPECT_FALSE(graph_.schema().vertex_has_property(v_label, "age"));
  // Physical delete should succeed
  auto status = graph_.DeleteVertexProperties("Person", {"age"});
  EXPECT_TRUE(status.ok()) << status.ToString();
  // Property 'age' should not exist
  EXPECT_FALSE(graph_.schema().vertex_has_property(v_label, "age"));
}

// Test physical delete of edge properties after logical delete
TEST_F(PropertyGraphLogicalDeleteTest,
       PhysicalDeleteEdgePropertiesAfterLogicalDelete_Succeeds) {
  std::vector<std::tuple<DataType, std::string, Property>> v_props = {
      {DataTypeId::kInt64, "id", Property::from_int64(0L)}};
  std::vector<std::string> pk_names = {"id"};
  graph_.CreateVertexType("Person", v_props, pk_names);
  graph_.CreateVertexType("Company", v_props, pk_names);
  std::vector<std::tuple<DataType, std::string, Property>> e_props = {
      {DataTypeId::kInt32, "years", Property::from_int32(0)},
      {DataTypeId::kVarchar, "position", Property::from_string_view("string")}};
  graph_.CreateEdgeType("Person", "Company", "WorksAt", e_props);
  label_t src_label = graph_.schema().get_vertex_label_id("Person");
  label_t dst_label = graph_.schema().get_vertex_label_id("Company");
  label_t e_label = graph_.schema().get_edge_label_id("WorksAt");
  // Logical delete edge property 'position'
  graph_.mutable_schema().DeleteEdgeProperties("Person", "Company", "WorksAt",
                                               {"position"}, true);
  EXPECT_FALSE(graph_.schema().edge_has_property(src_label, dst_label, e_label,
                                                 "position"));

  // Physical delete should succeed
  graph_.mutable_schema().DeleteEdgeProperties("Person", "Company", "WorksAt",
                                               {"position"});
  // Property 'position' should not exist
  EXPECT_FALSE(graph_.schema().edge_has_property(src_label, dst_label, e_label,
                                                 "position"));
}

// Test After logical deletion, the generated statistic json string does'n
// contains the Delete label and properties.
TEST_F(PropertyGraphLogicalDeleteTest,
       StatisticsAfterLogicalDelete_DoesNotContainDeletedInfo) {
  std::vector<std::tuple<DataType, std::string, Property>> v_props = {
      {DataTypeId::kInt64, "id", Property::from_int64(0L)},
      {DataTypeId::kVarchar, "Name", Property::from_string_view("string")}};
  std::vector<std::string> pk_names = {"id"};
  graph_.CreateVertexType("Person", v_props, pk_names);
  graph_.CreateVertexType("Company", v_props, pk_names);
  graph_.CreateVertexType("Location", v_props, pk_names);
  std::vector<std::tuple<DataType, std::string, Property>> e_props_workat = {
      {DataTypeId::kInt32, "years", Property::from_int32(0)},
      {DataTypeId::kVarchar, "position", Property::from_string_view("string")}};
  std::vector<std::tuple<DataType, std::string, Property>> e_props_locatedat = {
      {DataTypeId::kInt32, "since", Property::from_int32(0)},
      {DataTypeId::kVarchar, "city", Property::from_string_view("string")}};
  graph_.CreateEdgeType("Person", "Company", "WorksAt", e_props_workat);
  graph_.CreateEdgeType("Company", "Location", "LocatedAt", e_props_locatedat);
  // Logical delete vertex label
  graph_.DeleteVertexType("Person", true);
  // Logical delete edge label
  graph_.DeleteEdgeType("Person", "Company", "WorksAt", true);
  // Logical delete vertex property
  graph_.DeleteVertexProperties("Company", {"Name"});
  // Logical delete edge property
  graph_.DeleteEdgeProperties("Person", "Company", "WorksAt", {"years"});
  // Get statistics json string
  auto schema_json_ =
      neug::get_json_string_from_yaml(graph_.schema().to_yaml().value());
  EXPECT_TRUE(schema_json_);
  auto schema_json = schema_json_.value();

  LOG(INFO) << "Schema JSON: " << schema_json;
  // Check that deleted vertex label is not present
  EXPECT_EQ(schema_json.find("Person"), std::string::npos);
  // Check that deleted edge label is not present
  EXPECT_EQ(schema_json.find("WorksAt"), std::string::npos);
  // Check that deleted edge property is not present
  EXPECT_EQ(schema_json.find("years"), std::string::npos);
  EXPECT_NE(schema_json.find("Company"), std::string::npos);
  EXPECT_NE(schema_json.find("since"), std::string::npos);
  EXPECT_NE(schema_json.find("id"), std::string::npos);
}

// Test deleting primary key: should raise an error
TEST_F(PropertyGraphLogicalDeleteTest,
       DeletePrimaryKeyProperty_ShouldRaiseError) {
  std::vector<std::tuple<DataType, std::string, Property>> properties = {
      {DataTypeId::kInt64, "id", Property::from_int64(0L)},
      {DataTypeId::kVarchar, "name", Property::from_string_view("string")}};
  std::vector<std::string> pk_names = {"id"};
  auto status = graph_.CreateVertexType("Person", properties, pk_names);
  ASSERT_TRUE(status.ok());

  EXPECT_THROW({ graph_.DeleteVertexProperties("Person", {"id"}); },
               neug::exception::Exception);
}

TEST_F(PropertyGraphLogicalDeleteTest, TestStatistics) {
  // Insert vertex types and edge types to an empty schema, and check
  // whether the statistics are correct.
  std::vector<std::tuple<DataType, std::string, Property>> v_props = {
      {DataTypeId::kInt64, "id", Property::from_int64(0L)},
      {DataTypeId::kVarchar, "Name", Property::from_string_view("string")}};
  std::vector<std::string> pk_names = {"id"};
  graph_.CreateVertexType("Person", v_props, pk_names);
  graph_.CreateVertexType("Company", v_props, pk_names);
  graph_.CreateVertexType("Location", v_props, pk_names);
  std::vector<std::tuple<DataType, std::string, Property>> e_props = {
      {DataTypeId::kInt32, "years", Property::from_int32(0)},
      {DataTypeId::kVarchar, "position", Property::from_string_view("string")}};
  graph_.CreateEdgeType("Person", "Company", "WorksAt", e_props);
  graph_.CreateEdgeType("Company", "Location", "LocatedAt", e_props);

  auto stats = graph_.get_statistics_json();
  LOG(INFO) << "Statistics JSON: " << stats;
  EXPECT_TRUE(stats.find("\"vertex_type_statistics\"") != std::string::npos);
  EXPECT_TRUE(stats.find("\"edge_type_statistics\"") != std::string::npos);

  graph_.DeleteVertexType("Person", true);
  graph_.DeleteVertexProperties("Company", {"Name"});
  graph_.DeleteEdgeProperties("Person", "Company", "WorksAt", {"years"});
  // Get statistics json string
  auto after_delete_stats = graph_.get_statistics_json();
  EXPECT_EQ(after_delete_stats,
            "{\"vertex_type_statistics\":[{\"type_id\":1,\"type_name\":"
            "\"Company\",\"count\":0},{\"type_id\":2,\"type_name\":"
            "\"Location\",\"count\":0}],\"edge_type_statistics\":[{\"type_id\":"
            "1,\"type_name\":\"LocatedAt\",\"vertex_type_pair_statistics\":[{"
            "\"source_vertex\":\"Company\",\"destination_vertex\":\"Location\","
            "\"count\":0}]}],\"total_vertex_count\":0,\"total_edge_count\":0}");
}
}  // namespace neug
