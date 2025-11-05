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

using gs::EdgeStrategy;
using gs::PropertyType;
using gs::StorageStrategy;

namespace {

// Small helpers to construct common inputs
std::vector<PropertyType> VProps(std::initializer_list<PropertyType> il) {
  return std::vector<PropertyType>(il);
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

std::vector<std::tuple<PropertyType, std::string, size_t>> VPk(
    const PropertyType& t, const std::string& name, size_t idx) {
  return {std::make_tuple(t, name, idx)};
}

}  // namespace

TEST(SchemaTest, AddVertexLabel_AddRenameDeleteVertexProperties_Physical) {
  gs::Schema schema;

  // 1) Add a vertex label "Person" with 2 props and a single primary key
  auto v_types = VProps({PropertyType::StringView()});
  auto v_names = VNames({"name"});
  auto v_pk = VPk(PropertyType::Int64(), "id", /*idx in props*/ 0);
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
  // Only non-PK properties are stored in vproperties_/vprop_names_
  ASSERT_EQ(schema.get_vertex_properties("Person").size(), 1);
  EXPECT_EQ(schema.get_vertex_properties("Person")[0],
            PropertyType::StringView());
  ASSERT_EQ(schema.get_vertex_property_names("Person").size(), 1);
  EXPECT_EQ(schema.get_vertex_property_names("Person")[0], "name");
  EXPECT_EQ(schema.get_vertex_primary_key_name(vid), "id");
  LOG(INFO) << "1)";

  // 2) Add vertex properties
  std::vector<std::string> add_names = {"age", "score"};
  std::vector<PropertyType> add_types = {PropertyType::Int32(),
                                         PropertyType::Double()};
  std::vector<StorageStrategy> add_strats = {StorageStrategy::kMem,
                                             StorageStrategy::kMem};
  std::vector<gs::Property> add_defaults;  // not used currently
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
  gs::Schema schema;

  // Prepare two vertex labels first
  {
    auto t = VProps({PropertyType::StringView()});
    auto n = VNames({"name"});
    auto pk = VPk(PropertyType::Int64(), "id", 0);
    auto s = VStrats(t.size(), StorageStrategy::kMem);
    schema.AddVertexLabel("Person", t, {n.begin(), n.end()}, pk,
                          {s.begin(), s.end()}, 1024, "");
    schema.AddVertexLabel("Company", t, {n.begin(), n.end()}, pk,
                          {s.begin(), s.end()}, 1024, "");
  }

  // 1) Add an edge label Person -[WorksAt]-> Company
  std::vector<PropertyType> e_types = {PropertyType::Int32()};
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
  EXPECT_EQ(props[0], PropertyType::Int32());
  auto names = schema.get_edge_property_names("Person", "Company", "WorksAt");
  ASSERT_EQ(names.size(), 1);
  EXPECT_EQ(names[0], "since");
  EXPECT_EQ(schema.get_outgoing_edge_strategy("Person", "Company", "WorksAt"),
            EdgeStrategy::kMultiple);
  EXPECT_EQ(schema.get_incoming_edge_strategy("Person", "Company", "WorksAt"),
            EdgeStrategy::kSingle);
  EXPECT_TRUE(schema.outgoing_edge_mutable("Person", "Company", "WorksAt"));
  EXPECT_FALSE(schema.incoming_edge_mutable("Person", "Company", "WorksAt"));
  EXPECT_TRUE(schema.get_sort_on_compaction("Person", "Company", "WorksAt"));

  // 2) Add edge properties
  std::vector<std::string> add_e_names = {"role", "salary"};
  std::vector<PropertyType> add_e_types = {PropertyType::StringView(),
                                           PropertyType::Int64()};
  std::vector<gs::Property> dummy_defaults;
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
  gs::Schema schema;
  // Add vertex
  auto t = VProps({PropertyType::StringView()});
  auto n = VNames({"name"});
  auto pk = VPk(PropertyType::Int64(), "id", 0);
  auto s = VStrats(t.size(), StorageStrategy::kMem);
  schema.AddVertexLabel("City", t, {n.begin(), n.end()}, pk,
                        {s.begin(), s.end()}, 100, "");
  ASSERT_TRUE(schema.contains_vertex_label("City"));

  schema.DeleteVertexLabel("City", true);
  EXPECT_FALSE(schema.contains_vertex_label("City"));

  schema.AddVertexLabel("City", {PropertyType::StringView()}, {"name"},
                        VPk(PropertyType::StringView(), "name", 0),
                        /*strategies*/ {}, /*max_vnum*/ 100, "");
  EXPECT_TRUE(schema.contains_vertex_label("City"));
  EXPECT_EQ(schema.get_vertex_property_names("City")[0], "name");
}

TEST(SchemaTest, DeleteVertexLabel_PhysicalThenReAdd) {
  gs::Schema schema;
  auto t = VProps({PropertyType::StringView()});
  auto n = VNames({"name"});
  auto pk = VPk(PropertyType::Int64(), "id", 0);
  auto s = VStrats(t.size(), StorageStrategy::kMem);
  schema.AddVertexLabel("Project", t, {n.begin(), n.end()}, pk,
                        {s.begin(), s.end()}, 100, "");
  ASSERT_TRUE(schema.contains_vertex_label("Project"));

  schema.DeleteVertexLabel("Project");
  EXPECT_FALSE(schema.contains_vertex_label("Project"));

  schema.AddVertexLabel("Project", {PropertyType::StringView()}, {"name"},
                        VPk(PropertyType::StringView(), "name", 0), {}, 100,
                        "");
  EXPECT_TRUE(schema.contains_vertex_label("Project"));
}

TEST(SchemaTest, DeleteEdgeLabel_LogicalAndPhysicalAndReAdd) {
  gs::Schema schema;
  {
    auto t = VProps({PropertyType::Int64(), PropertyType::StringView()});
    auto n = VNames({"id", "name"});
    auto pk = VPk(PropertyType::Int64(), "id", 0);
    auto s = VStrats(t.size(), StorageStrategy::kMem);
    schema.AddVertexLabel("A", t, {n.begin(), n.end()}, pk,
                          {s.begin(), s.end()}, 100, "");
    schema.AddVertexLabel("B", t, {n.begin(), n.end()}, pk,
                          {s.begin(), s.end()}, 100, "");
  }

  schema.AddEdgeLabel("A", "B", "Link", {PropertyType::Int32()}, {"w"}, {},
                      EdgeStrategy::kMultiple, EdgeStrategy::kMultiple, true,
                      true, false, "");

  ASSERT_TRUE(schema.exist("A", "B", "Link"));
  auto src = schema.get_vertex_label_id("A");
  auto dst = schema.get_vertex_label_id("B");
  auto el = schema.get_edge_label_id("Link");

  schema.DeleteEdgeLabel(src, dst, el, true);
  EXPECT_FALSE(schema.exist(src, dst, el));
  EXPECT_FALSE(schema.exist(src, dst, el));

  schema.AddEdgeLabel("A", "B", "Link", {PropertyType::Int32()}, {"w"}, {},
                      EdgeStrategy::kMultiple, EdgeStrategy::kMultiple, true,
                      true, false, "");
  EXPECT_TRUE(schema.edge_triplet_valid(src, dst, el));

  schema.DeleteEdgeLabel(src, dst, el);
  EXPECT_FALSE(schema.exist(src, dst, el));

  schema.AddEdgeLabel("A", "B", "Link", {PropertyType::Int32()}, {"w"}, {},
                      EdgeStrategy::kMultiple, EdgeStrategy::kMultiple, true,
                      true, false, "");
  EXPECT_TRUE(schema.exist(src, dst, el));

  schema.DeleteEdgeLabel("Link");
  EXPECT_FALSE(schema.contains_edge_label("Link"));
}

TEST(SchemaTest, LogicalDeleteVertexProperties_HidesProperty) {
  gs::Schema schema;

  // Person(id PK, name, age)
  auto types = VProps({PropertyType::StringView(), PropertyType::Int32()});
  auto names = VNames({"name", "age"});
  auto pk = VPk(PropertyType::Int64(), "id", 0);
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
  gs::Schema schema;

  auto vt = VProps({PropertyType::StringView()});
  auto vn = VNames({"name"});
  auto vpk = VPk(PropertyType::Int64(), "id", 0);
  auto vs = VStrats(vt.size(), StorageStrategy::kMem);
  schema.AddVertexLabel("A", vt, {vn.begin(), vn.end()}, vpk,
                        {vs.begin(), vs.end()}, 100, "");
  schema.AddVertexLabel("B", vt, {vn.begin(), vn.end()}, vpk,
                        {vs.begin(), vs.end()}, 100, "");

  std::vector<PropertyType> e_types = {PropertyType::Int32(),
                                       PropertyType::StringView()};
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
  gs::Schema schema;
  auto t = VProps({PropertyType::StringView()});
  auto n = VNames({"name"});
  auto pk = VPk(PropertyType::Int64(), "id", 0);
  auto s = VStrats(t.size(), StorageStrategy::kMem);
  schema.AddVertexLabel("City", t, {n.begin(), n.end()}, pk,
                        {s.begin(), s.end()}, 100, "");
  ASSERT_TRUE(schema.contains_vertex_label("City"));
  auto city_label = schema.get_vertex_label_id("City");

  schema.DeleteVertexLabel("City", true);
  EXPECT_FALSE(schema.contains_vertex_label("City"));

  // When implemented, this should restore visibility
  schema.RevertDeleteVertexLabel(city_label);
  EXPECT_TRUE(schema.contains_vertex_label("City"));
}

TEST(SchemaTest, RevertDeleteEdgeLabel_ByName_ClearsTombstone) {
  gs::Schema schema;
  auto t = VProps({PropertyType::StringView()});
  auto n = VNames({"name"});
  auto pk = VPk(PropertyType::Int64(), "id", 0);
  auto s = VStrats(t.size(), StorageStrategy::kMem);
  schema.AddVertexLabel("A", t, {n.begin(), n.end()}, pk, {s.begin(), s.end()},
                        100, "");
  schema.AddVertexLabel("B", t, {n.begin(), n.end()}, pk, {s.begin(), s.end()},
                        100, "");

  schema.AddEdgeLabel("A", "B", "Link", {PropertyType::Int32()}, {"w"}, {},
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
  gs::Schema schema;
  auto t = VProps({PropertyType::StringView()});
  auto n = VNames({"name"});
  auto pk = VPk(PropertyType::Int64(), "id", 0);
  auto s = VStrats(t.size(), StorageStrategy::kMem);
  schema.AddVertexLabel("A", t, {n.begin(), n.end()}, pk, {s.begin(), s.end()},
                        100, "");
  schema.AddVertexLabel("B", t, {n.begin(), n.end()}, pk, {s.begin(), s.end()},
                        100, "");

  schema.AddEdgeLabel("A", "B", "Link", {PropertyType::Int32()}, {"w"}, {},
                      EdgeStrategy::kMultiple, EdgeStrategy::kMultiple, true,
                      true, false, "");
  auto src = schema.get_vertex_label_id("A");
  auto dst = schema.get_vertex_label_id("B");
  auto el = schema.get_edge_label_id("Link");
  ASSERT_TRUE(schema.edge_triplet_valid(src, dst, el));

  schema.DeleteEdgeLabel(src, dst, el, true);
  EXPECT_FALSE(schema.edge_triplet_valid(src, dst, el));

  schema.RevertDeleteEdgeLabel(src, dst, el);
  EXPECT_TRUE(schema.edge_triplet_valid(src, dst, el));
}
