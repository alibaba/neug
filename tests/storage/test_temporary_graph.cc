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
#include <fstream>
#include <sstream>

#include "neug/execution/common/types/value.h"
#include "neug/main/connection.h"
#include "neug/main/neug_db.h"
#include "neug/storages/checkpoint_manager.h"
#include "neug/storages/graph/property_graph.h"
#include "neug/storages/graph/schema.h"
#include "neug/utils/yaml_utils.h"
#include "unittest/utils.h"

using neug::CheckpointManager;
using neug::CreateEdgeTypeParamBuilder;
using neug::CreateVertexTypeParamBuilder;
using neug::DataType;
using neug::DataTypeId;
using neug::EdgeStrategy;
using neug::label_t;
using neug::MemoryLevel;
using neug::PropertyGraph;
using neug::Schema;
using neug::execution::Value;

// ============================================================================
// Part 1: Schema layer temporary marking tests
// ============================================================================

class SchemaTemporaryTest : public ::testing::Test {
 protected:
  Schema schema_;

  void SetUp() override {
    schema_.AddVertexLabel("Person", {DataTypeId::kVarchar, DataTypeId::kInt32},
                           {"name", "age"},
                           {std::make_tuple(DataTypeId::kInt64, "id", 0)}, 4096,
                           "persistent person");

    schema_.AddVertexLabel("TempUser", {DataTypeId::kVarchar}, {"username"},
                           {std::make_tuple(DataTypeId::kInt64, "uid", 0)},
                           1024, "temporary user", {}, true);
  }
};

TEST_F(SchemaTemporaryTest, VertexLabelTemporaryFlag) {
  auto person_id = schema_.get_vertex_label_id("Person");
  auto temp_id = schema_.get_vertex_label_id("TempUser");

  EXPECT_FALSE(schema_.is_vertex_label_temporary(person_id));
  EXPECT_TRUE(schema_.is_vertex_label_temporary(temp_id));
}

TEST_F(SchemaTemporaryTest, GetTemporaryVertexLabels) {
  auto temp_labels = schema_.get_temporary_vertex_labels();
  ASSERT_EQ(temp_labels.size(), 1);
  EXPECT_EQ(schema_.get_vertex_label_name(temp_labels[0]), "TempUser");
}

TEST_F(SchemaTemporaryTest, GetTemporaryVertexLabelsEmpty) {
  Schema clean_schema;
  clean_schema.AddVertexLabel("A", {DataTypeId::kVarchar}, {"name"},
                              {std::make_tuple(DataTypeId::kInt64, "id", 0)},
                              100, "");
  auto temp_labels = clean_schema.get_temporary_vertex_labels();
  EXPECT_TRUE(temp_labels.empty());
}

TEST_F(SchemaTemporaryTest, InvalidLabelReturnsFalse) {
  EXPECT_FALSE(schema_.is_vertex_label_temporary(255));
}

TEST_F(SchemaTemporaryTest, EdgeLabelTemporaryFlag) {
  schema_.AddEdgeLabel("TempUser", "Person", "TempFollows",
                       {DataTypeId::kInt64}, {"since"}, EdgeStrategy::kMultiple,
                       EdgeStrategy::kMultiple, true, true, std::nullopt,
                       "temporary edge", {}, true);

  auto src = schema_.get_vertex_label_id("TempUser");
  auto dst = schema_.get_vertex_label_id("Person");
  auto edge = schema_.get_edge_label_id("TempFollows");
  uint32_t key = schema_.generate_edge_label(src, dst, edge);

  EXPECT_TRUE(schema_.is_edge_label_temporary(key));
}

TEST_F(SchemaTemporaryTest, PersistentEdgeLabelNotTemporary) {
  schema_.AddEdgeLabel("Person", "Person", "Knows", {DataTypeId::kDouble},
                       {"weight"}, EdgeStrategy::kMultiple,
                       EdgeStrategy::kMultiple, true, true, std::nullopt,
                       "persistent edge");

  auto src = schema_.get_vertex_label_id("Person");
  auto dst = schema_.get_vertex_label_id("Person");
  auto edge = schema_.get_edge_label_id("Knows");
  uint32_t key = schema_.generate_edge_label(src, dst, edge);

  EXPECT_FALSE(schema_.is_edge_label_temporary(key));
}

TEST_F(SchemaTemporaryTest, GetTemporaryEdgeTripletKeys) {
  schema_.AddEdgeLabel("Person", "Person", "Knows", {DataTypeId::kDouble},
                       {"weight"}, EdgeStrategy::kMultiple,
                       EdgeStrategy::kMultiple, true, true, std::nullopt, "");

  schema_.AddEdgeLabel("TempUser", "Person", "TempFollows",
                       {DataTypeId::kInt64}, {"since"}, EdgeStrategy::kMultiple,
                       EdgeStrategy::kMultiple, true, true, std::nullopt, "",
                       {}, true);

  auto temp_edges = schema_.get_temporary_edge_triplet_keys();
  ASSERT_EQ(temp_edges.size(), 1);

  auto [s, d, e] = schema_.parse_edge_label(temp_edges[0]);
  EXPECT_EQ(schema_.get_vertex_label_name(s), "TempUser");
  EXPECT_EQ(schema_.get_vertex_label_name(d), "Person");
  EXPECT_EQ(schema_.get_edge_label_name(e), "TempFollows");
}

TEST_F(SchemaTemporaryTest, InvalidEdgeKeyReturnsFalse) {
  EXPECT_FALSE(schema_.is_edge_label_temporary(0xFFFFFF));
}

// ============================================================================
// Part 2: Schema serialization split tests
//   to_yaml() includes temp labels; DumpToYaml() excludes them
// ============================================================================

class SchemaSerializationTest : public ::testing::Test {
 protected:
  Schema schema_;

  void SetUp() override {
    schema_.AddVertexLabel("Person", {DataTypeId::kVarchar}, {"name"},
                           {std::make_tuple(DataTypeId::kInt64, "id", 0)}, 4096,
                           "");

    schema_.AddVertexLabel("TempUser", {DataTypeId::kVarchar}, {"username"},
                           {std::make_tuple(DataTypeId::kInt64, "uid", 0)},
                           1024, "", {}, true);

    schema_.AddEdgeLabel("Person", "Person", "Knows", {DataTypeId::kDouble},
                         {"weight"}, EdgeStrategy::kMultiple,
                         EdgeStrategy::kMultiple, true, true, std::nullopt, "");

    schema_.AddEdgeLabel("TempUser", "Person", "TempFollows",
                         {DataTypeId::kInt64}, {"since"},
                         EdgeStrategy::kMultiple, EdgeStrategy::kMultiple, true,
                         true, std::nullopt, "", {}, true);
  }

  bool yaml_has_vertex_type(const YAML::Node& yaml, const std::string& name) {
    if (!yaml["schema"]["vertex_types"])
      return false;
    for (const auto& v : yaml["schema"]["vertex_types"]) {
      if (v["type_name"].as<std::string>() == name)
        return true;
    }
    return false;
  }

  bool yaml_has_edge_type(const YAML::Node& yaml, const std::string& name) {
    if (!yaml["schema"]["edge_types"])
      return false;
    for (const auto& e : yaml["schema"]["edge_types"]) {
      if (e["type_name"].as<std::string>() == name)
        return true;
    }
    return false;
  }
};

TEST_F(SchemaSerializationTest, ToYamlIncludesTemporaryLabels) {
  auto yaml_res = schema_.to_yaml();
  ASSERT_TRUE(yaml_res);
  auto yaml = yaml_res.value();

  EXPECT_TRUE(yaml_has_vertex_type(yaml, "Person"));
  EXPECT_TRUE(yaml_has_vertex_type(yaml, "TempUser"));
  EXPECT_TRUE(yaml_has_edge_type(yaml, "Knows"));
  EXPECT_TRUE(yaml_has_edge_type(yaml, "TempFollows"));
}

TEST_F(SchemaSerializationTest, DumpToYamlExcludesTemporaryLabels) {
  // DumpToYaml is a raw serializer; caller must StripTemporary() first
  // to exclude temporary labels from the output.
  auto yaml_res = Schema::DumpToYaml(schema_.StripTemporary());
  ASSERT_TRUE(yaml_res);
  auto yaml = yaml_res.value();

  EXPECT_TRUE(yaml_has_vertex_type(yaml, "Person"));
  EXPECT_FALSE(yaml_has_vertex_type(yaml, "TempUser"));
  EXPECT_TRUE(yaml_has_edge_type(yaml, "Knows"));
  EXPECT_FALSE(yaml_has_edge_type(yaml, "TempFollows"));
}

// Verifies the binary Schema::Serialize path does not propagate the
// `temporary` flag. operator<<(InArchive, VertexSchema/EdgeSchema)
// intentionally omits the field, so a round-trip through Serialize/Deserialize
// yields a non-temporary copy of every label. This guards the spec L28
// invariant that any persistence path drops temp markers.
TEST_F(SchemaSerializationTest, SerializeStripsTemporaryFlag) {
  // Sanity: source schema has temp labels.
  ASSERT_FALSE(schema_.get_temporary_vertex_labels().empty());
  ASSERT_FALSE(schema_.get_temporary_edge_triplet_keys().empty());

  std::stringstream ss;
  schema_.Serialize(ss);

  Schema restored;
  restored.Deserialize(ss);

  // Persistent labels survive…
  EXPECT_TRUE(restored.is_vertex_label_valid("Person"));
  EXPECT_TRUE(restored.is_edge_label_valid("Knows"));

  // …and after deserialize NO label should still be marked temporary.
  EXPECT_TRUE(restored.get_temporary_vertex_labels().empty());
  EXPECT_TRUE(restored.get_temporary_edge_triplet_keys().empty());
}

// ============================================================================
// Part 3: PropertyGraph temporary data protection tests
// ============================================================================

class PropertyGraphTemporaryTest : public ::testing::Test {
 protected:
  std::string work_dir_;
  std::unique_ptr<PropertyGraph> graph_;
  CheckpointManager ws_;

  void SetUp() override {
    work_dir_ = std::string("/tmp/test_temp_graph_") +
                ::testing::UnitTest::GetInstance()->current_test_info()->name();
    if (std::filesystem::exists(work_dir_)) {
      std::filesystem::remove_all(work_dir_);
    }
    std::filesystem::create_directories(work_dir_);
    graph_ = std::make_unique<PropertyGraph>();
    ws_.Open(work_dir_);
    auto ckp = make_checkpoint(ws_);
    graph_->Open(ckp, MemoryLevel::kInMemory);
  }

  void TearDown() override {
    graph_.reset();
    if (std::filesystem::exists(work_dir_)) {
      std::filesystem::remove_all(work_dir_);
    }
  }

  void CreatePersistentPerson() {
    CreateVertexTypeParamBuilder builder;
    EXPECT_TRUE(
        graph_
            ->CreateVertexType(builder.VertexLabel("person")
                                   .AddProperty("id", Value::INT64(0))
                                   .AddProperty("name", Value::STRING(""))
                                   .AddPrimaryKeyName("id")
                                   .Build())
            .ok());
  }

  void CreateTemporaryUser() {
    CreateVertexTypeParamBuilder builder;
    EXPECT_TRUE(
        graph_
            ->CreateVertexType(builder.VertexLabel("temp_user")
                                   .AddProperty("uid", Value::INT64(0))
                                   .AddProperty("username", Value::STRING(""))
                                   .AddPrimaryKeyName("uid")
                                   .Temporary(true)
                                   .Build())
            .ok());
  }
};

TEST_F(PropertyGraphTemporaryTest, CreateTemporaryVertexType) {
  CreateTemporaryUser();

  auto label = graph_->schema().get_vertex_label_id("temp_user");
  EXPECT_TRUE(graph_->schema().is_vertex_label_valid("temp_user"));
  EXPECT_TRUE(graph_->schema().is_vertex_label_temporary(label));
}

TEST_F(PropertyGraphTemporaryTest, CreatePersistentVertexTypeNotTemporary) {
  CreatePersistentPerson();

  auto label = graph_->schema().get_vertex_label_id("person");
  EXPECT_TRUE(graph_->schema().is_vertex_label_valid("person"));
  EXPECT_FALSE(graph_->schema().is_vertex_label_temporary(label));
}

TEST_F(PropertyGraphTemporaryTest, CreateTemporaryEdgeType) {
  CreatePersistentPerson();
  CreateTemporaryUser();

  CreateEdgeTypeParamBuilder builder;
  EXPECT_TRUE(graph_
                  ->CreateEdgeType(builder.SrcLabel("temp_user")
                                       .DstLabel("person")
                                       .EdgeLabel("temp_follows")
                                       .AddProperty("since", Value::INT64(0))
                                       .Temporary(true)
                                       .Build())
                  .ok());

  auto src = graph_->schema().get_vertex_label_id("temp_user");
  auto dst = graph_->schema().get_vertex_label_id("person");
  auto edge = graph_->schema().get_edge_label_id("temp_follows");
  uint32_t key = graph_->schema().generate_edge_label(src, dst, edge);
  EXPECT_TRUE(graph_->schema().is_edge_label_temporary(key));
}

TEST_F(PropertyGraphTemporaryTest, PersistentEdgeCannotReferenceTempVertex) {
  CreatePersistentPerson();
  CreateTemporaryUser();

  CreateEdgeTypeParamBuilder builder;
  auto status = graph_->CreateEdgeType(builder.SrcLabel("temp_user")
                                           .DstLabel("person")
                                           .EdgeLabel("bad_edge")
                                           .AddProperty("w", Value::DOUBLE(0.0))
                                           .Temporary(false)
                                           .Build());
  EXPECT_FALSE(status.ok());
}

TEST_F(PropertyGraphTemporaryTest, PersistentEdgeBetweenPersistentVerticesOk) {
  CreatePersistentPerson();

  CreateEdgeTypeParamBuilder builder;
  auto status = graph_->CreateEdgeType(builder.SrcLabel("person")
                                           .DstLabel("person")
                                           .EdgeLabel("knows")
                                           .AddProperty("w", Value::DOUBLE(0.0))
                                           .Temporary(false)
                                           .Build());
  EXPECT_TRUE(status.ok());
}

TEST_F(PropertyGraphTemporaryTest, DumpSkipsTemporaryData) {
  CreatePersistentPerson();
  CreateTemporaryUser();

  // Add a vertex to the temp type
  label_t temp_label = graph_->schema().get_vertex_label_id("temp_user");
  neug::vid_t vid;
  EXPECT_TRUE(graph_
                  ->AddVertex(temp_label, Value::INT64(1),
                              {Value::STRING("alice")}, vid, 0)
                  .ok());
  EXPECT_EQ(graph_->VertexNum(temp_label), 1);

  // Add a vertex to the persistent type
  label_t person_label = graph_->schema().get_vertex_label_id("person");
  neug::vid_t vid2;
  EXPECT_TRUE(graph_
                  ->AddVertex(person_label, Value::INT64(100),
                              {Value::STRING("Bob")}, vid2, 0)
                  .ok());

  // Dump (checkpoint)
  auto ckp2 = make_checkpoint(ws_);
  graph_->Dump(ckp2);

  // Open a fresh graph from the checkpoint — temp data should not be there
  auto graph2 = std::make_unique<PropertyGraph>();
  graph2->Open(ckp2, MemoryLevel::kInMemory);

  // Persistent data survives
  EXPECT_TRUE(graph2->schema().is_vertex_label_valid("person"));
  label_t person_label2 = graph2->schema().get_vertex_label_id("person");
  EXPECT_EQ(graph2->VertexNum(person_label2), 1);

  // Temporary data is gone (not even in schema)
  auto temp_labels = graph2->schema().get_temporary_vertex_labels();
  EXPECT_TRUE(temp_labels.empty());
}

// Verifies the on-disk checkpoint manifest JSON does not leak temporary
// label names. spec L28 requires Dump()/persistence to skip temporary
// labels entirely, so the serialized manifest text MUST NOT mention
// `temp_user` even though that label was live in memory at Dump time.
TEST_F(PropertyGraphTemporaryTest, DumpManifestFileExcludesTemporary) {
  CreatePersistentPerson();
  CreateTemporaryUser();

  auto ckp2 = make_checkpoint(ws_);
  graph_->Dump(ckp2);

  std::string meta_file = ckp2->path() + "/meta";
  ASSERT_TRUE(std::filesystem::exists(meta_file)) << meta_file;

  std::ifstream ifs(meta_file);
  ASSERT_TRUE(ifs.is_open());
  std::stringstream buf;
  buf << ifs.rdbuf();
  std::string manifest_text = buf.str();

  // Persistent label name appears, temporary label name does not.
  EXPECT_NE(manifest_text.find("person"), std::string::npos);
  EXPECT_EQ(manifest_text.find("temp_user"), std::string::npos)
      << "temporary label leaked into on-disk manifest:\n"
      << manifest_text;
}

TEST_F(PropertyGraphTemporaryTest, DumpToYamlExcludesTemporary) {
  CreatePersistentPerson();
  CreateTemporaryUser();

  // DumpToYaml is a raw serializer; caller must StripTemporary() first.
  auto yaml_res = Schema::DumpToYaml(graph_->schema().StripTemporary());
  ASSERT_TRUE(yaml_res);
  auto yaml = yaml_res.value();

  bool found_person = false;
  bool found_temp = false;
  for (const auto& v : yaml["schema"]["vertex_types"]) {
    auto name = v["type_name"].as<std::string>();
    if (name == "person")
      found_person = true;
    if (name == "temp_user")
      found_temp = true;
  }
  EXPECT_TRUE(found_person);
  EXPECT_FALSE(found_temp);
}

TEST_F(PropertyGraphTemporaryTest, ToYamlIncludesTemporary) {
  CreatePersistentPerson();
  CreateTemporaryUser();

  auto yaml_res = graph_->schema().to_yaml();
  ASSERT_TRUE(yaml_res);
  auto yaml = yaml_res.value();

  bool found_person = false;
  bool found_temp = false;
  for (const auto& v : yaml["schema"]["vertex_types"]) {
    auto name = v["type_name"].as<std::string>();
    if (name == "person")
      found_person = true;
    if (name == "temp_user")
      found_temp = true;
  }
  EXPECT_TRUE(found_person);
  EXPECT_TRUE(found_temp);
}

// ============================================================================
// Part 4: Connection::Close() temporary data cleanup tests
// ============================================================================

class ConnectionTemporaryCleanupTest : public ::testing::Test {
 protected:
  std::string data_path_;
  std::unique_ptr<neug::NeugDB> db_;

  void SetUp() override {
    data_path_ =
        std::string("/tmp/test_conn_temp_") +
        ::testing::UnitTest::GetInstance()->current_test_info()->name();
    if (std::filesystem::exists(data_path_)) {
      std::filesystem::remove_all(data_path_);
    }
    std::filesystem::create_directories(data_path_);
    db_ = std::make_unique<neug::NeugDB>();
    db_->Open(data_path_);
  }

  void TearDown() override {
    db_.reset();
    if (std::filesystem::exists(data_path_)) {
      std::filesystem::remove_all(data_path_);
    }
  }
};

TEST_F(ConnectionTemporaryCleanupTest, CloseRemovesTemporaryTypes) {
  auto conn = db_->Connect();
  ASSERT_NE(conn, nullptr);

  // Create persistent type via Cypher
  EXPECT_TRUE(conn->Query(
      "CREATE NODE TABLE person(id INT64, name STRING, PRIMARY KEY(id));"));

  // Create temporary type via storage API
  CreateVertexTypeParamBuilder builder;
  {
    neug::SnapshotGuard guard(db_->graph_snapshot_store());
    auto* pg = guard.get().mutable_graph();
    EXPECT_TRUE(pg->CreateVertexType(builder.VertexLabel("temp_node")
                                         .AddProperty("id", Value::INT64(0))
                                         .AddProperty("val", Value::STRING(""))
                                         .AddPrimaryKeyName("id")
                                         .Temporary(true)
                                         .Build())
                    .ok());
  }

  // Verify temp type exists before close
  {
    neug::SnapshotGuard guard(db_->graph_snapshot_store());
    auto* pg = guard.get().mutable_graph();
    EXPECT_TRUE(pg->schema().is_vertex_label_valid("temp_node"));
    auto temp_labels = pg->schema().get_temporary_vertex_labels();
    EXPECT_EQ(temp_labels.size(), 1);
  }

  // Close connection
  conn->Close();

  // Temp type should be cleaned up
  {
    neug::SnapshotGuard guard(db_->graph_snapshot_store());
    auto* pg = guard.get().mutable_graph();
    auto temp_labels = pg->schema().get_temporary_vertex_labels();
    EXPECT_TRUE(temp_labels.empty());

    // Persistent type should remain
    EXPECT_TRUE(pg->schema().is_vertex_label_valid("person"));
  }
}

TEST_F(ConnectionTemporaryCleanupTest, CloseRemovesTemporaryEdges) {
  auto conn = db_->Connect();
  ASSERT_NE(conn, nullptr);

  // Create persistent person
  EXPECT_TRUE(conn->Query(
      "CREATE NODE TABLE person(id INT64, name STRING, PRIMARY KEY(id));"));

  // Create temp vertex + temp edge via storage API
  {
    neug::SnapshotGuard guard(db_->graph_snapshot_store());
    auto* pg = guard.get().mutable_graph();
    CreateVertexTypeParamBuilder vbuilder;
    EXPECT_TRUE(pg->CreateVertexType(vbuilder.VertexLabel("temp_src")
                                         .AddProperty("id", Value::INT64(0))
                                         .AddPrimaryKeyName("id")
                                         .Temporary(true)
                                         .Build())
                    .ok());
    CreateEdgeTypeParamBuilder ebuilder;
    EXPECT_TRUE(pg->CreateEdgeType(ebuilder.SrcLabel("temp_src")
                                       .DstLabel("person")
                                       .EdgeLabel("temp_link")
                                       .AddProperty("w", Value::DOUBLE(0.0))
                                       .Temporary(true)
                                       .Build())
                    .ok());
  }

  // Verify both exist
  {
    neug::SnapshotGuard guard(db_->graph_snapshot_store());
    auto* pg = guard.get().mutable_graph();
    EXPECT_EQ(pg->schema().get_temporary_vertex_labels().size(), 1);
    EXPECT_EQ(pg->schema().get_temporary_edge_triplet_keys().size(), 1);
  }

  conn->Close();

  // Both should be gone
  EXPECT_TRUE(db_->schema().get_temporary_vertex_labels().empty());
  EXPECT_TRUE(db_->schema().get_temporary_edge_triplet_keys().empty());

  // Persistent type survives
  EXPECT_TRUE(db_->schema().is_vertex_label_valid("person"));
}

TEST_F(ConnectionTemporaryCleanupTest, CloseWithNoTempIsHarmless) {
  auto conn = db_->Connect();
  ASSERT_NE(conn, nullptr);

  EXPECT_TRUE(conn->Query(
      "CREATE NODE TABLE person(id INT64, name STRING, PRIMARY KEY(id));"));

  conn->Close();

  EXPECT_TRUE(db_->schema().is_vertex_label_valid("person"));
}

TEST_F(ConnectionTemporaryCleanupTest, DoubleCloseIsIdempotent) {
  auto conn = db_->Connect();
  ASSERT_NE(conn, nullptr);

  CreateVertexTypeParamBuilder builder;
  {
    neug::SnapshotGuard guard(db_->graph_snapshot_store());
    auto* pg = guard.get().mutable_graph();
    EXPECT_TRUE(pg->CreateVertexType(builder.VertexLabel("temp_x")
                                         .AddProperty("id", Value::INT64(0))
                                         .AddPrimaryKeyName("id")
                                         .Temporary(true)
                                         .Build())
                    .ok());
  }

  conn->Close();
  conn->Close();  // should not crash or throw

  EXPECT_TRUE(db_->schema().get_temporary_vertex_labels().empty());
}
