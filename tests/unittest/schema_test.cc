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

#include <string>
#include <tuple>
#include <vector>

#include "neug/storages/graph/schema.h"
#include "neug/utils/property/types.h"
#include "neug/utils/yaml_utils.h"

using neug::DataType;
using neug::DataTypeId;
using neug::EdgeStrategy;
using neug::StorageStrategy;

// Small helpers to construct common inputs
std::vector<DataType> VProps(std::initializer_list<DataType> il) {
  return std::vector<DataType>(il);
}
std::vector<std::string> VNames(std::initializer_list<const char*> il) {
  std::vector<std::string> out;
  for (auto* s : il)
    out.emplace_back(s);
  return out;
}
std::vector<StorageStrategy> VStrats(size_t n, StorageStrategy s) {
  return std::vector<StorageStrategy>(n, s);
}

std::vector<std::tuple<DataType, std::string, size_t>> VPk(
    const DataType& t, const std::string& name, size_t idx) {
  return {std::make_tuple(t, name, idx)};
}

TEST(SchemaTest, AddVertexLabel_AddRenameDeleteVertexProperties_Physical) {
  neug::Schema schema;

  // 1) Add a vertex label "Person" with 2 props and a single primary key
  auto v_types = VProps({DataType::VARCHAR});
  auto v_names = VNames({"name"});
  auto v_pk = VPk(DataType::INT64, "id", /*idx in props*/ 0);
  auto v_strats = VStrats(v_types.size(), StorageStrategy::kMem);

  schema.AddVertexLabel("Person", v_types,
                        /*property_names*/ {v_names.begin(), v_names.end()},
                        v_pk,
                        /*strategies*/ {v_strats.begin(), v_strats.end()},
                        /*max_vnum*/ 1024, /*desc*/ "person vertex");

  // Check basics
  EXPECT_TRUE(schema.contains_vertex_label("Person"));
  auto vid = schema.get_vertex_label_id("Person");
  EXPECT_EQ(schema.get_vertex_label_name(vid), "Person");
  EXPECT_EQ(schema.get_vertex_description("Person"), "person vertex");
  EXPECT_EQ(schema.get_max_vnum("Person"), 1024);
  // Only non-PK properties are stored in vproperties_/vprop_names_
  ASSERT_EQ(schema.get_vertex_properties("Person").size(), 1);
  EXPECT_EQ(schema.get_vertex_properties("Person")[0], DataTypeId::kVarchar);
  ASSERT_EQ(schema.get_vertex_property_names("Person").size(), 1);
  EXPECT_EQ(schema.get_vertex_property_names("Person")[0], "name");
  EXPECT_EQ(schema.get_vertex_primary_key_name(vid), "id");
  LOG(INFO) << "1)";

  // 2) Add vertex properties
  std::vector<std::string> add_names = {"age", "score"};
  std::vector<DataType> add_types = {DataTypeId::kInt32, DataTypeId::kDouble};
  std::vector<StorageStrategy> add_strats = {StorageStrategy::kMem,
                                             StorageStrategy::kMem};
  std::vector<neug::Property> add_defaults;  // not used currently
  schema.AddVertexProperties("Person", add_names, add_types, add_strats,
                             add_defaults);

  ASSERT_EQ(schema.get_vertex_properties("Person").size(), 3);
  auto names_after_add = schema.get_vertex_property_names("Person");
  EXPECT_EQ((std::vector<std::string>{names_after_add.begin(),
                                      names_after_add.end()}),
            std::vector<std::string>({"name", "age", "score"}));

  // 3) Rename vertex properties
  std::vector<std::string> rename_from = {"score"};
  std::vector<std::string> rename_to = {"gpa"};
  schema.RenameVertexProperties("Person", rename_from, rename_to);
  auto names_after_rename = schema.get_vertex_property_names("Person");
  EXPECT_EQ((std::vector<std::string>{names_after_rename.begin(),
                                      names_after_rename.end()}),
            std::vector<std::string>({"name", "age", "gpa"}));

  // 4) Delete vertex properties (physical)
  std::vector<std::string> del_names = {"age"};
  schema.DeleteVertexProperties("Person", del_names);
  auto names_after_del = schema.get_vertex_property_names("Person");
  EXPECT_EQ((std::vector<std::string>{names_after_del.begin(),
                                      names_after_del.end()}),
            std::vector<std::string>({"name", "gpa"}));
  EXPECT_TRUE(
      schema.vertex_has_property("Person", "id"));  // primary key still exists
  EXPECT_FALSE(schema.vertex_has_property("Person", "age"));  // removed
}

TEST(SchemaTest, AddEdgeLabel_AddRenameDeleteEdgeProperties_Physical) {
  neug::Schema schema;

  // Prepare two vertex labels first
  {
    auto t = VProps({DataTypeId::kVarchar});
    auto n = VNames({"name"});
    auto pk = VPk(DataTypeId::kInt64, "id", 0);
    auto s = VStrats(t.size(), StorageStrategy::kMem);
    schema.AddVertexLabel("Person", t, {n.begin(), n.end()}, pk,
                          {s.begin(), s.end()}, 1024, "");
    schema.AddVertexLabel("Company", t, {n.begin(), n.end()}, pk,
                          {s.begin(), s.end()}, 1024, "");
  }

  // 1) Add an edge label Person -[WorksAt]-> Company
  std::vector<DataType> e_types = {DataTypeId::kInt32};
  std::vector<std::string> e_names = {"since"};
  schema.AddEdgeLabel("Person", "Company", "WorksAt", e_types, e_names, {},
                      /*oe*/ EdgeStrategy::kMultiple,
                      /*ie*/ EdgeStrategy::kSingle,
                      /*oe_mutable*/ true, /*ie_mutable*/ false,
                      /*sort_on_compaction*/ true, /*desc*/ "employment");

  // Check basics
  EXPECT_TRUE(schema.exist("Person", "Company", "WorksAt"));
  auto props = schema.get_edge_properties("Person", "Company", "WorksAt");
  ASSERT_EQ(props.size(), 1);
  EXPECT_EQ(props[0], DataTypeId::kInt32);
  auto names = schema.get_edge_property_names("Person", "Company", "WorksAt");
  ASSERT_EQ(names.size(), 1);
  EXPECT_EQ(names[0], "since");
  EXPECT_EQ(schema.get_edge_description("Person", "Company", "WorksAt"),
            "employment");
  neug::label_t src_label = schema.get_vertex_label_id("Person");
  neug::label_t dst_label = schema.get_vertex_label_id("Company");
  neug::label_t edge_label = schema.get_edge_label_id("WorksAt");
  EXPECT_EQ(schema.get_edge_description(src_label, dst_label, edge_label),
            "employment");
  EXPECT_EQ(schema.get_outgoing_edge_strategy("Person", "Company", "WorksAt"),
            EdgeStrategy::kMultiple);
  EXPECT_EQ(schema.get_incoming_edge_strategy("Person", "Company", "WorksAt"),
            EdgeStrategy::kSingle);
  EXPECT_TRUE(schema.outgoing_edge_mutable("Person", "Company", "WorksAt"));
  EXPECT_FALSE(schema.incoming_edge_mutable("Person", "Company", "WorksAt"));
  EXPECT_TRUE(schema.get_sort_on_compaction("Person", "Company", "WorksAt"));

  // 2) Add edge properties
  std::vector<std::string> add_e_names = {"role", "salary"};
  std::vector<DataType> add_e_types = {DataTypeId::kVarchar,
                                       DataTypeId::kInt64};
  std::vector<neug::Property> dummy_defaults;
  schema.AddEdgeProperties("Person", "Company", "WorksAt", add_e_names,
                           add_e_types, dummy_defaults);
  auto names_after_add =
      schema.get_edge_property_names("Person", "Company", "WorksAt");
  EXPECT_EQ((std::vector<std::string>{names_after_add.begin(),
                                      names_after_add.end()}),
            std::vector<std::string>({"since", "role", "salary"}));

  // 3) Rename an edge property
  std::vector<std::string> rename_from = {"salary"};
  std::vector<std::string> rename_to = {"income"};
  schema.RenameEdgeProperties("Person", "Company", "WorksAt", rename_from,
                              rename_to);
  auto names_after_rename =
      schema.get_edge_property_names("Person", "Company", "WorksAt");
  EXPECT_EQ((std::vector<std::string>{names_after_rename.begin(),
                                      names_after_rename.end()}),
            std::vector<std::string>({"since", "role", "income"}));

  // 4) Delete edge properties (physical)
  std::vector<std::string> del_e = {"role"};
  schema.DeleteEdgeProperties("Person", "Company", "WorksAt", del_e);
  auto names_after_del =
      schema.get_edge_property_names("Person", "Company", "WorksAt");
  EXPECT_EQ((std::vector<std::string>{names_after_del.begin(),
                                      names_after_del.end()}),
            std::vector<std::string>({"since", "income"}));
  EXPECT_TRUE(
      schema.edge_has_property("Person", "Company", "WorksAt", "since"));
  EXPECT_FALSE(
      schema.edge_has_property("Person", "Company", "WorksAt", "role"));
}

TEST(SchemaTest, DeleteVertexLabel_LogicalThenReAddActsAsRevert) {
  neug::Schema schema;
  // Add vertex
  auto t = VProps({DataTypeId::kVarchar});
  auto n = VNames({"name"});
  auto pk = VPk(DataTypeId::kInt64, "id", 0);
  auto s = VStrats(t.size(), StorageStrategy::kMem);
  schema.AddVertexLabel("City", t, {n.begin(), n.end()}, pk,
                        {s.begin(), s.end()}, 100, "");
  ASSERT_TRUE(schema.contains_vertex_label("City"));

  schema.DeleteVertexLabel("City", true);
  EXPECT_FALSE(schema.contains_vertex_label("City"));

  schema.AddVertexLabel("City", {DataTypeId::kVarchar}, {"name"},
                        VPk(DataTypeId::kVarchar, "name", 0),
                        /*strategies*/ {}, /*max_vnum*/ 100, "");
  EXPECT_TRUE(schema.contains_vertex_label("City"));
  EXPECT_EQ(schema.get_vertex_property_names("City")[0], "name");
}

TEST(SchemaTest, DeleteVertexLabel_PhysicalThenReAdd) {
  neug::Schema schema;
  auto t = VProps({DataTypeId::kVarchar});
  auto n = VNames({"name"});
  auto pk = VPk(DataTypeId::kInt64, "id", 0);
  auto s = VStrats(t.size(), StorageStrategy::kMem);
  schema.AddVertexLabel("Project", t, {n.begin(), n.end()}, pk,
                        {s.begin(), s.end()}, 100, "");
  ASSERT_TRUE(schema.contains_vertex_label("Project"));

  schema.DeleteVertexLabel("Project");
  EXPECT_FALSE(schema.contains_vertex_label("Project"));

  schema.AddVertexLabel("Project", {DataTypeId::kVarchar}, {"name"},
                        VPk(DataTypeId::kVarchar, "name", 0), {}, 100, "");
  EXPECT_TRUE(schema.contains_vertex_label("Project"));
}

TEST(SchemaTest, DeleteEdgeLabel_LogicalAndPhysicalAndReAdd) {
  neug::Schema schema;
  {
    auto t = VProps({DataTypeId::kInt64, DataTypeId::kVarchar});
    auto n = VNames({"id", "name"});
    auto pk = VPk(DataTypeId::kInt64, "id", 0);
    auto s = VStrats(t.size(), StorageStrategy::kMem);
    schema.AddVertexLabel("A", t, {n.begin(), n.end()}, pk,
                          {s.begin(), s.end()}, 100, "");
    schema.AddVertexLabel("B", t, {n.begin(), n.end()}, pk,
                          {s.begin(), s.end()}, 100, "");
  }

  schema.AddEdgeLabel("A", "B", "Link", {DataTypeId::kInt32}, {"w"}, {},
                      EdgeStrategy::kMultiple, EdgeStrategy::kMultiple, true,
                      true, false, "");

  ASSERT_TRUE(schema.exist("A", "B", "Link"));
  auto src = schema.get_vertex_label_id("A");
  auto dst = schema.get_vertex_label_id("B");
  auto el = schema.get_edge_label_id("Link");

  schema.DeleteEdgeLabel(src, dst, el, true);
  EXPECT_FALSE(schema.exist(src, dst, el));
  EXPECT_FALSE(schema.exist(src, dst, el));

  schema.AddEdgeLabel("A", "B", "Link", {DataTypeId::kInt32}, {"w"}, {},
                      EdgeStrategy::kMultiple, EdgeStrategy::kMultiple, true,
                      true, false, "");
  EXPECT_TRUE(schema.edge_triplet_valid(src, dst, el));

  schema.DeleteEdgeLabel(src, dst, el);
  EXPECT_FALSE(schema.exist(src, dst, el));

  schema.AddEdgeLabel("A", "B", "Link", {DataTypeId::kInt32}, {"w"}, {},
                      EdgeStrategy::kMultiple, EdgeStrategy::kMultiple, true,
                      true, false, "");
  EXPECT_TRUE(schema.exist(src, dst, el));

  schema.DeleteEdgeLabel("Link");
  EXPECT_FALSE(schema.contains_edge_label("Link"));
}

TEST(SchemaTest, LogicalDeleteVertexProperties_HidesProperty) {
  neug::Schema schema;

  // Person(id PK, name, age)
  auto types = VProps({DataTypeId::kVarchar, DataTypeId::kInt32});
  auto names = VNames({"name", "age"});
  auto pk = VPk(DataTypeId::kInt64, "id", 0);
  auto strats = VStrats(types.size(), StorageStrategy::kMem);
  // Only non-PK properties go into vproperties_/vprop_names_
  schema.AddVertexLabel("Person", types, {names.begin(), names.end()}, pk,
                        {strats.begin(), strats.end()}, 1024, "");

  // Pre-condition
  ASSERT_TRUE(schema.vertex_has_property("Person", "name"));
  ASSERT_TRUE(schema.vertex_has_property("Person", "age"));
  ASSERT_TRUE(schema.vertex_has_primary_key("Person", "id"));

  std::vector<std::string> del = {"age"};
  schema.DeleteVertexProperties("Person", del, true);

  EXPECT_TRUE(schema.vertex_has_property("Person", "name"));
  EXPECT_FALSE(schema.vertex_has_property("Person", "age"));
  EXPECT_TRUE(schema.vertex_has_primary_key("Person", "id"));
}

TEST(SchemaTest, LogicalDeleteEdgeProperties_HidesProperty) {
  neug::Schema schema;

  auto vt = VProps({DataTypeId::kVarchar});
  auto vn = VNames({"name"});
  auto vpk = VPk(DataTypeId::kInt64, "id", 0);
  auto vs = VStrats(vt.size(), StorageStrategy::kMem);
  schema.AddVertexLabel("A", vt, {vn.begin(), vn.end()}, vpk,
                        {vs.begin(), vs.end()}, 100, "");
  schema.AddVertexLabel("B", vt, {vn.begin(), vn.end()}, vpk,
                        {vs.begin(), vs.end()}, 100, "");

  std::vector<DataType> e_types = {DataTypeId::kInt32, DataTypeId::kVarchar};
  std::vector<std::string> e_names = {"w", "tag"};
  schema.AddEdgeLabel("A", "B", "Link", e_types, e_names, {},
                      EdgeStrategy::kMultiple, EdgeStrategy::kMultiple, true,
                      true, false, "");

  ASSERT_TRUE(schema.edge_has_property("A", "B", "Link", "w"));
  ASSERT_TRUE(schema.edge_has_property("A", "B", "Link", "tag"));

  // Logical delete edge property "tag"
  std::vector<std::string> del = {"tag"};
  schema.DeleteEdgeProperties("A", "B", "Link", del, true);

  // Expected behavior: logically-deleted edge property should be hidden
  EXPECT_TRUE(schema.edge_has_property("A", "B", "Link", "w"));
  EXPECT_FALSE(schema.edge_has_property("A", "B", "Link", "tag"));
}

TEST(SchemaTest, RevertDeleteVertexLabel_ClearsTombstone) {
  neug::Schema schema;
  auto t = VProps({DataTypeId::kVarchar});
  auto n = VNames({"name"});
  auto pk = VPk(DataTypeId::kInt64, "id", 0);
  auto s = VStrats(t.size(), StorageStrategy::kMem);
  schema.AddVertexLabel("City", t, {n.begin(), n.end()}, pk,
                        {s.begin(), s.end()}, 100, "");
  ASSERT_TRUE(schema.contains_vertex_label("City"));

  schema.DeleteVertexLabel("City", true);
  EXPECT_FALSE(schema.contains_vertex_label("City"));

  // When implemented, this should restore visibility
  schema.RevertDeleteVertexLabel("City");
  EXPECT_TRUE(schema.contains_vertex_label("City"));
}

TEST(SchemaTest, RevertDeleteEdgeLabel_ByName_ClearsTombstone) {
  neug::Schema schema;
  auto t = VProps({DataTypeId::kVarchar});
  auto n = VNames({"name"});
  auto pk = VPk(DataTypeId::kInt64, "id", 0);
  auto s = VStrats(t.size(), StorageStrategy::kMem);
  schema.AddVertexLabel("A", t, {n.begin(), n.end()}, pk, {s.begin(), s.end()},
                        100, "");
  schema.AddVertexLabel("B", t, {n.begin(), n.end()}, pk, {s.begin(), s.end()},
                        100, "");

  schema.AddEdgeLabel("A", "B", "Link", {DataTypeId::kInt32}, {"w"}, {},
                      EdgeStrategy::kMultiple, EdgeStrategy::kMultiple, true,
                      true, false, "");
  ASSERT_TRUE(schema.contains_edge_label("Link"));
  auto e_label = schema.get_edge_label_id("Link");

  schema.DeleteEdgeLabel("Link", true);
  EXPECT_FALSE(schema.contains_edge_label("Link"));

  schema.RevertDeleteEdgeLabel(e_label);
  EXPECT_TRUE(schema.contains_edge_label("Link"));
}

TEST(SchemaTest, RevertDeleteEdgeLabel_ByTriplet_ClearsTombstone) {
  neug::Schema schema;
  auto t = VProps({DataTypeId::kVarchar});
  auto n = VNames({"name"});
  auto pk = VPk(DataTypeId::kInt64, "id", 0);
  auto s = VStrats(t.size(), StorageStrategy::kMem);
  schema.AddVertexLabel("A", t, {n.begin(), n.end()}, pk, {s.begin(), s.end()},
                        100, "");
  schema.AddVertexLabel("B", t, {n.begin(), n.end()}, pk, {s.begin(), s.end()},
                        100, "");

  schema.AddEdgeLabel("A", "B", "Link", {DataTypeId::kInt32}, {"w"}, {},
                      EdgeStrategy::kMultiple, EdgeStrategy::kMultiple, true,
                      true, false, "");
  auto src = schema.get_vertex_label_id("A");
  auto dst = schema.get_vertex_label_id("B");
  auto el = schema.get_edge_label_id("Link");
  ASSERT_TRUE(schema.edge_triplet_valid(src, dst, el));

  schema.DeleteEdgeLabel(src, dst, el, true);
  EXPECT_FALSE(schema.edge_triplet_valid(src, dst, el));

  schema.RevertDeleteEdgeLabel("A", "B", "Link");
  EXPECT_TRUE(schema.edge_triplet_valid(src, dst, el));
}

TEST(SchemaDumpTest, SchemaDumpWithMultipleEdgeTriplet) {
  neug::Schema schema;

  // Add vertex label "person"
  auto person_property_types_ =
      VProps({DataTypeId::kVarchar, DataTypeId::kInt32, DataTypeId::kDouble});
  auto person_property_names_ = VNames({"name", "age", "score"});
  auto person_pk_ = VPk(DataTypeId::kInt64, "id", 0);
  auto person_strategies_ =
      VStrats(person_property_types_.size(), StorageStrategy::kMem);
  schema.AddVertexLabel("person", person_property_types_,
                        person_property_names_, person_pk_, person_strategies_,
                        4096, "person vertex");

  // Add vertex label "company"
  auto company_property_types_ =
      VProps({DataTypeId::kVarchar, DataTypeId::kInt32});
  auto company_property_names_ = VNames({"company_name", "employee_count"});
  auto company_pk_ = VPk(DataTypeId::kInt64, "id", 0);
  auto company_strategies_ =
      VStrats(company_property_types_.size(), StorageStrategy::kMem);

  schema.AddVertexLabel("company", company_property_types_,
                        company_property_names_, company_pk_,
                        company_strategies_, 2048, "company vertex");

  // Add edge label "knows"
  auto edge_property_types_ = VProps({DataTypeId::kInt64});
  auto edge_property_names_ = VNames({"since"});
  auto edge_strategies_ =
      VStrats(edge_property_types_.size(), StorageStrategy::kMem);

  schema.AddEdgeLabel("person", "person", "knows", edge_property_types_,
                      edge_property_names_, edge_strategies_,
                      EdgeStrategy::kMultiple, EdgeStrategy::kMultiple, true,
                      true, false, "knows edge");

  // Add edge label "worksAt"
  schema.AddEdgeLabel("person", "company", "knows", edge_property_types_,
                      edge_property_names_, edge_strategies_,
                      EdgeStrategy::kMultiple, EdgeStrategy::kMultiple, true,
                      true, false, "knows edge");

  auto yaml = neug::Schema::DumpToYaml(schema);
  EXPECT_TRUE(yaml);
  // to string
  auto edge_type_prop = yaml.value()["schema"]["edge_types"][0]["properties"];
  EXPECT_EQ(edge_type_prop.size(), 1);
}

class SchemaDeleteTest : public ::testing::Test {
 protected:
  void SetUp() override {
    schema_ = std::make_unique<neug::Schema>();

    // Add vertex label "person"
    person_property_types_ = {neug::DataTypeId::kVarchar,
                              neug::DataTypeId::kInt32,
                              neug::DataTypeId::kDouble};
    person_property_names_ = {"name", "age", "score"};
    person_pk_ = {std::make_tuple(neug::DataTypeId::kInt64, "id", 0)};
    person_strategies_ = {neug::StorageStrategy::kMem,
                          neug::StorageStrategy::kMem,
                          neug::StorageStrategy::kMem};

    schema_->AddVertexLabel("person", person_property_types_,
                            person_property_names_, person_pk_,
                            person_strategies_, 4096, "person vertex");

    // Add vertex label "company"
    company_property_types_ = {neug::DataTypeId::kVarchar,
                               neug::DataTypeId::kInt32};
    company_property_names_ = {"company_name", "employee_count"};
    company_pk_ = {std::make_tuple(neug::DataTypeId::kInt64, "id", 0)};
    company_strategies_ = {neug::StorageStrategy::kMem,
                           neug::StorageStrategy::kMem};

    schema_->AddVertexLabel("company", company_property_types_,
                            company_property_names_, company_pk_,
                            company_strategies_, 2048, "company vertex");

    // Add edge label "knows"
    edge_property_types_ = {neug::DataTypeId::kInt64};
    edge_property_names_ = {"since"};
    edge_strategies_ = {neug::StorageStrategy::kMem};

    schema_->AddEdgeLabel(
        "person", "person", "knows", edge_property_types_, edge_property_names_,
        edge_strategies_, neug::EdgeStrategy::kMultiple,
        neug::EdgeStrategy::kMultiple, true, true, false, "knows edge");

    // Add edge label "worksAt"
    schema_->AddEdgeLabel(
        "person", "company", "worksAt", edge_property_types_,
        edge_property_names_, edge_strategies_, neug::EdgeStrategy::kMultiple,
        neug::EdgeStrategy::kMultiple, true, true, false, "worksAt edge");
  }

  void TearDown() override { schema_.reset(); }

  std::unique_ptr<neug::Schema> schema_;

  std::vector<neug::DataType> person_property_types_;
  std::vector<std::string> person_property_names_;
  std::vector<std::tuple<neug::DataType, std::string, size_t>> person_pk_;
  std::vector<neug::StorageStrategy> person_strategies_;

  std::vector<neug::DataType> company_property_types_;
  std::vector<std::string> company_property_names_;
  std::vector<std::tuple<neug::DataType, std::string, size_t>> company_pk_;
  std::vector<neug::StorageStrategy> company_strategies_;

  std::vector<neug::DataType> edge_property_types_;
  std::vector<std::string> edge_property_names_;
  std::vector<neug::StorageStrategy> edge_strategies_;
};

// Test VertexSchema::is_property_soft_deleted
TEST_F(SchemaDeleteTest, VertexSchemaPropertySoftDelete) {
  auto person_label = schema_->get_vertex_label_id("person");
  auto vertex_schema = schema_->get_vertex_schema(person_label);

  EXPECT_FALSE(vertex_schema->is_property_soft_deleted("name"));
  EXPECT_FALSE(vertex_schema->is_property_soft_deleted("age"));
  EXPECT_FALSE(vertex_schema->is_property_soft_deleted("score"));

  std::vector<std::string> props_to_delete = {"age"};
  schema_->DeleteVertexProperties("person", props_to_delete, true);

  EXPECT_TRUE(vertex_schema->is_property_soft_deleted("age"));
  EXPECT_FALSE(vertex_schema->is_property_soft_deleted("name"));
  EXPECT_FALSE(vertex_schema->is_property_soft_deleted("score"));

  auto v_prop_names = schema_->get_vertex_property_names(person_label);
  EXPECT_EQ(v_prop_names.size(), 2);

  schema_->RevertDeleteVertexProperties("person", props_to_delete);

  EXPECT_FALSE(vertex_schema->is_property_soft_deleted("age"));

  v_prop_names = schema_->get_vertex_property_names(person_label);
  EXPECT_EQ(v_prop_names.size(), 3);
  std::vector<std::string> expected = {"name", "age", "score"};
  EXPECT_EQ(v_prop_names, expected);
}

// Test VertexSchema::get_property_index
TEST_F(SchemaDeleteTest, VertexSchemaPropertyIndex) {
  auto person_label = schema_->get_vertex_label_id("person");
  auto vertex_schema = schema_->get_vertex_schema(person_label);

  // Test getting property indices
  EXPECT_THROW(vertex_schema->get_property_index("id"),
               neug::exception::Exception);  // Primary key
  EXPECT_EQ(vertex_schema->get_property_index("name"), 0);
  EXPECT_EQ(vertex_schema->get_property_index("age"), 1);
  EXPECT_EQ(vertex_schema->get_property_index("score"), 2);
  EXPECT_EQ(vertex_schema->get_property_index("nonexistent"), -1);

  // Soft delete "age" and check index behavior
  std::vector<std::string> props_to_delete = {"age"};
  schema_->DeleteVertexProperties("person", props_to_delete, true);

  // Should return -1 for soft-deleted property
  EXPECT_EQ(vertex_schema->get_property_index("age"), -1);
  EXPECT_EQ(vertex_schema->get_property_index("name"), 0);
  EXPECT_EQ(vertex_schema->get_property_index("score"), 2);
}

// Test EdgeSchema::is_property_soft_deleted
TEST_F(SchemaDeleteTest, EdgeSchemaPropertySoftDelete) {
  auto person_label = schema_->get_vertex_label_id("person");
  auto edge_label = schema_->get_edge_label_id("knows");
  auto edge_schema =
      schema_->get_edge_schema(person_label, person_label, edge_label);

  // Initially, no properties should be soft deleted
  EXPECT_FALSE(edge_schema->is_property_soft_deleted("since"));

  // Soft delete "since" property
  std::vector<std::string> props_to_delete = {"since"};
  schema_->DeleteEdgeProperties("person", "person", "knows", props_to_delete,
                                true);

  // Check that "since" is now soft deleted
  EXPECT_TRUE(edge_schema->is_property_soft_deleted("since"));
  auto e_prop_names =
      schema_->get_edge_property_names(person_label, person_label, edge_label);
  EXPECT_EQ(e_prop_names.size(), 0);

  // Revert the deletion
  schema_->RevertDeleteEdgeProperties("person", "person", "knows",
                                      props_to_delete);

  // Check that "since" is no longer soft deleted
  EXPECT_FALSE(edge_schema->is_property_soft_deleted("since"));
  e_prop_names =
      schema_->get_edge_property_names(person_label, person_label, edge_label);
  EXPECT_EQ(e_prop_names.size(), 1);
}

// Test Schema::IsVertexLabelSoftDeleted
TEST_F(SchemaDeleteTest, VertexLabelLogicalDelete) {
  // Initially, vertex label should not be deleted
  EXPECT_FALSE(schema_->IsVertexLabelSoftDeleted("person"));
  EXPECT_FALSE(schema_->IsVertexLabelSoftDeleted("company"));

  auto person_label = schema_->get_vertex_label_id("person");
  EXPECT_FALSE(schema_->IsVertexLabelSoftDeleted(person_label));

  // Logically delete "person" vertex label
  schema_->DeleteVertexLabel("person", true);

  // Check that "person" is now logically deleted
  EXPECT_TRUE(schema_->IsVertexLabelSoftDeleted("person"));
  EXPECT_TRUE(schema_->IsVertexLabelSoftDeleted(person_label));
  EXPECT_FALSE(schema_->IsVertexLabelSoftDeleted("company"));

  // Revert the deletion
  schema_->RevertDeleteVertexLabel("person");

  // Check that "person" is no longer logically deleted
  EXPECT_FALSE(schema_->IsVertexLabelSoftDeleted("person"));
  EXPECT_FALSE(schema_->IsVertexLabelSoftDeleted(person_label));
}

// Test Schema::IsEdgeLabelSoftDeleted
TEST_F(SchemaDeleteTest, EdgeLabelLogicalDelete) {
  auto person_label = schema_->get_vertex_label_id("person");
  auto knows_label = schema_->get_edge_label_id("knows");

  // Initially, edge labels should not be deleted
  EXPECT_FALSE(schema_->IsEdgeLabelSoftDeleted("person", "person", "knows"));
  EXPECT_FALSE(
      schema_->IsEdgeLabelSoftDeleted(person_label, person_label, knows_label));

  // Logically delete "knows" edge label
  schema_->DeleteEdgeLabel("person", "person", "knows", true);

  // Check that "knows" is now logically deleted
  EXPECT_TRUE(schema_->IsEdgeLabelSoftDeleted("person", "person", "knows"));
  EXPECT_TRUE(
      schema_->IsEdgeLabelSoftDeleted(person_label, person_label, knows_label));
  EXPECT_FALSE(schema_->IsEdgeLabelSoftDeleted("person", "company", "worksAt"));

  // Revert the deletion
  schema_->RevertDeleteEdgeLabel("person", "person", "knows");

  // Check that "knows" is no longer logically deleted
  EXPECT_FALSE(schema_->IsEdgeLabelSoftDeleted("person", "person", "knows"));
  EXPECT_FALSE(
      schema_->IsEdgeLabelSoftDeleted(person_label, person_label, knows_label));
}

// Test Schema::IsVertexPropertySoftDeleted
TEST_F(SchemaDeleteTest, VertexPropertyLogicalDelete) {
  auto person_label = schema_->get_vertex_label_id("person");

  // Initially, no properties should be logically deleted
  EXPECT_FALSE(schema_->IsVertexPropertySoftDeleted("person", "name"));
  EXPECT_FALSE(schema_->IsVertexPropertySoftDeleted("person", "age"));
  EXPECT_FALSE(schema_->IsVertexPropertySoftDeleted(person_label, "score"));

  // Logically delete "age" property
  std::vector<std::string> props_to_delete = {"age"};
  schema_->DeleteVertexProperties("person", props_to_delete, true);

  // Check that "age" is now logically deleted
  EXPECT_TRUE(schema_->IsVertexPropertySoftDeleted("person", "age"));
  EXPECT_TRUE(schema_->IsVertexPropertySoftDeleted(person_label, "age"));
  EXPECT_FALSE(schema_->IsVertexPropertySoftDeleted("person", "name"));
  EXPECT_FALSE(schema_->IsVertexPropertySoftDeleted("person", "score"));

  // Revert the deletion
  schema_->RevertDeleteVertexProperties("person", props_to_delete);

  // Check that "age" is no longer logically deleted
  EXPECT_FALSE(schema_->IsVertexPropertySoftDeleted("person", "age"));
  EXPECT_FALSE(schema_->IsVertexPropertySoftDeleted(person_label, "age"));
}

// Test Schema::IsEdgePropertySoftDeleted
TEST_F(SchemaDeleteTest, EdgePropertyLogicalDelete) {
  auto person_label = schema_->get_vertex_label_id("person");
  auto knows_label = schema_->get_edge_label_id("knows");

  // Initially, no properties should be logically deleted
  EXPECT_FALSE(
      schema_->IsEdgePropertySoftDeleted("person", "person", "knows", "since"));
  EXPECT_FALSE(schema_->IsEdgePropertySoftDeleted(person_label, person_label,
                                                  knows_label, "since"));

  // Soft delete "since" property
  std::vector<std::string> props_to_delete = {"since"};
  schema_->DeleteEdgeProperties("person", "person", "knows", props_to_delete,
                                true);

  // Check that "since" is now logically deleted
  EXPECT_TRUE(
      schema_->IsEdgePropertySoftDeleted("person", "person", "knows", "since"));
  EXPECT_TRUE(schema_->IsEdgePropertySoftDeleted(person_label, person_label,
                                                 knows_label, "since"));

  // Revert the deletion
  schema_->RevertDeleteEdgeProperties("person", "person", "knows",
                                      props_to_delete);

  // Check that "since" is no longer soft deleted
  EXPECT_FALSE(
      schema_->IsEdgePropertySoftDeleted("person", "person", "knows", "since"));
  EXPECT_FALSE(schema_->IsEdgePropertySoftDeleted(person_label, person_label,
                                                  knows_label, "since"));
}

// Test multiple vertex properties deletion and revert
TEST_F(SchemaDeleteTest, MultipleVertexPropertiesDeletionAndRevert) {
  // Delete multiple properties
  std::vector<std::string> props_to_delete = {"name", "score"};
  schema_->DeleteVertexProperties("person", props_to_delete, true);

  // Verify all are deleted
  EXPECT_TRUE(schema_->IsVertexPropertySoftDeleted("person", "name"));
  EXPECT_TRUE(schema_->IsVertexPropertySoftDeleted("person", "score"));
  EXPECT_FALSE(schema_->IsVertexPropertySoftDeleted("person", "age"));

  // Revert one property
  std::vector<std::string> props_to_revert = {"name"};
  schema_->RevertDeleteVertexProperties("person", props_to_revert);

  // Verify only "name" is reverted
  EXPECT_FALSE(schema_->IsVertexPropertySoftDeleted("person", "name"));
  EXPECT_TRUE(schema_->IsVertexPropertySoftDeleted("person", "score"));

  // Revert the other property
  std::vector<std::string> props_to_revert2 = {"score"};
  schema_->RevertDeleteVertexProperties("person", props_to_revert2);

  // Verify both are reverted
  EXPECT_FALSE(schema_->IsVertexPropertySoftDeleted("person", "name"));
  EXPECT_FALSE(schema_->IsVertexPropertySoftDeleted("person", "score"));
}

// Test edge property operations with label_t overloads
TEST_F(SchemaDeleteTest, EdgePropertyOperationsWithLabelId) {
  auto person_label = schema_->get_vertex_label_id("person");
  auto knows_label = schema_->get_edge_label_id("knows");

  // Delete property using label_t
  std::vector<std::string> props_to_delete = {"since"};
  schema_->DeleteEdgeProperties("person", "person", "knows", props_to_delete,
                                true);

  // Verify using both string and label_t overloads
  EXPECT_TRUE(
      schema_->IsEdgePropertySoftDeleted("person", "person", "knows", "since"));
  EXPECT_TRUE(schema_->IsEdgePropertySoftDeleted(person_label, person_label,
                                                 knows_label, "since"));

  // Revert using label_t overload
  schema_->RevertDeleteEdgeProperties(person_label, person_label, knows_label,
                                      props_to_delete);

  // Verify property is reverted
  EXPECT_FALSE(
      schema_->IsEdgePropertySoftDeleted("person", "person", "knows", "since"));
  EXPECT_FALSE(schema_->IsEdgePropertySoftDeleted(person_label, person_label,
                                                  knows_label, "since"));
}

// Test has_property behavior with soft-deleted properties
TEST_F(SchemaDeleteTest, HasPropertyWithSoftDelete) {
  auto person_label = schema_->get_vertex_label_id("person");
  auto vertex_schema = schema_->get_vertex_schema(person_label);

  // Initially, all properties should exist
  EXPECT_TRUE(vertex_schema->has_property("name"));
  EXPECT_TRUE(vertex_schema->has_property("age"));
  EXPECT_TRUE(vertex_schema->has_property("score"));

  // Soft delete "age" property
  std::vector<std::string> props_to_delete = {"age"};
  schema_->DeleteVertexProperties("person", props_to_delete, true);

  // has_property should return false for soft-deleted property
  EXPECT_TRUE(vertex_schema->has_property("name"));
  EXPECT_FALSE(vertex_schema->has_property("age"));
  EXPECT_TRUE(vertex_schema->has_property("score"));

  // Revert deletion
  schema_->RevertDeleteVertexProperties("person", props_to_delete);

  // has_property should return true again
  EXPECT_TRUE(vertex_schema->has_property("age"));
}

// Test edge has_property behavior with soft-deleted properties
TEST_F(SchemaDeleteTest, EdgeHasPropertyWithSoftDelete) {
  auto person_label = schema_->get_vertex_label_id("person");
  auto edge_schema = schema_->get_edge_schema(
      person_label, person_label, schema_->get_edge_label_id("knows"));

  // Initially, property should exist
  EXPECT_TRUE(edge_schema->has_property("since"));

  // Soft delete "since" property
  std::vector<std::string> props_to_delete = {"since"};
  schema_->DeleteEdgeProperties("person", "person", "knows", props_to_delete,
                                true);

  // has_property should return false for soft-deleted property
  EXPECT_FALSE(edge_schema->has_property("since"));

  // Revert deletion
  schema_->RevertDeleteEdgeProperties("person", "person", "knows",
                                      props_to_delete);

  // has_property should return true again
  EXPECT_TRUE(edge_schema->has_property("since"));
}

// Test schema-level vertex_has_property with soft delete
TEST_F(SchemaDeleteTest, SchemaVertexHasPropertyWithSoftDelete) {
  // Initially, all properties should exist
  EXPECT_TRUE(schema_->vertex_has_property("person", "name"));
  EXPECT_TRUE(schema_->vertex_has_property("person", "age"));

  // Soft delete "age" property
  std::vector<std::string> props_to_delete = {"age"};
  schema_->DeleteVertexProperties("person", props_to_delete, true);

  // vertex_has_property should return false for soft-deleted property
  EXPECT_TRUE(schema_->vertex_has_property("person", "name"));
  EXPECT_FALSE(schema_->vertex_has_property("person", "age"));
}

// Test schema-level edge_has_property with soft delete
TEST_F(SchemaDeleteTest, SchemaEdgeHasPropertyWithSoftDelete) {
  // Initially, property should exist
  EXPECT_TRUE(schema_->edge_has_property("person", "person", "knows", "since"));

  // Soft delete "since" property
  std::vector<std::string> props_to_delete = {"since"};
  schema_->DeleteEdgeProperties("person", "person", "knows", props_to_delete,
                                true);

  // edge_has_property should return false for soft-deleted property
  EXPECT_FALSE(
      schema_->edge_has_property("person", "person", "knows", "since"));

  // Revert deletion
  schema_->RevertDeleteEdgeProperties("person", "person", "knows",
                                      props_to_delete);

  // edge_has_property should return true again
  EXPECT_TRUE(schema_->edge_has_property("person", "person", "knows", "since"));
}

TEST(VertexSchemaTest, TestVertexSchemaIndex) {
  neug::VertexSchema schema(
      "test",
      {
          neug::DataType::VARCHAR,  // name
          neug::DataType::DOUBLE    // score
      },
      {"name", "score"}, VPk(neug::DataType::INT64, "id", 1),
      {neug::StorageStrategy::kMem, neug::StorageStrategy::kMem,
       neug::StorageStrategy::kMem});
  // id is at index 1

  EXPECT_THROW(schema.get_property_index("id"), neug::exception::Exception);
  EXPECT_EQ(schema.get_property_index("name"), 0);
  EXPECT_EQ(schema.get_property_index("score"), 1);
  EXPECT_EQ(schema.get_property_index("nonexistent"), -1);

  EXPECT_EQ(schema.get_property_name(0), "name");
  EXPECT_EQ(schema.get_property_name(1), "score");
  EXPECT_THROW(schema.get_property_name(2), neug::exception::Exception);
}

TEST(SchemaTest, TestSchemaEqual) {
  neug::Schema schema;

  // Prepare two vertex labels first
  auto t = VProps({DataType::VARCHAR});
  auto n = VNames({"name"});
  auto pk = VPk(DataType::INT64, "id", 0);
  auto s = VStrats(t.size(), StorageStrategy::kMem);
  schema.AddVertexLabel("Person", t, {n.begin(), n.end()}, pk,
                        {s.begin(), s.end()}, 1024, "");
  schema.AddVertexLabel("Company", t, {n.begin(), n.end()}, pk,
                        {s.begin(), s.end()}, 1024, "");

  // 1) Add an edge label Person -[WorksAt]-> Company
  std::vector<DataType> e_types = {DataType::INT32};
  std::vector<std::string> e_names = {"since"};
  schema.AddEdgeLabel("Person", "Company", "WorksAt", e_types, e_names, {},
                      /*oe*/ EdgeStrategy::kMultiple,
                      /*ie*/ EdgeStrategy::kSingle,
                      /*oe_mutable*/ true, /*ie_mutable*/ false,
                      /*sort_on_compaction*/ true, /*desc*/ "employment");

  // 2) Copy schema and test equal
  neug::Schema other_schema = schema;
  EXPECT_TRUE(schema.Equals(other_schema));
}