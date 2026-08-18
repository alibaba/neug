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
#include <string>

#include "neug/main/neug_db.h"
#include "neug/main/query_request.h"
#include "neug/server/neug_db_service.h"
#include "neug/storages/checkpoint_manager.h"
#include "neug/storages/graph/graph_interface.h"
#include "neug/storages/graph/schema.h"
#include "neug/storages/graph/vertex_table.h"
#include "neug/storages/graph_snapshot_store.h"
#include "neug/storages/index/storage_index_manager.h"
#include "neug/storages/module/module_factory.h"
#include "test_index_common.h"

namespace {

std::string TestDir(const std::string& name) {
  const auto dir =
      std::filesystem::temp_directory_path() / ("neug_checkpoint_883_" + name);
  std::error_code ec;
  std::filesystem::remove_all(dir, ec);
  std::filesystem::create_directories(dir);
  return dir.string();
}

void WriteManifest(neug::Checkpoint& checkpoint,
                   const std::string& object_path = {}) {
  neug::CheckpointManifest manifest;
  manifest.SetSchema(neug::Schema());
  if (!object_path.empty()) {
    neug::ModuleDescriptor descriptor;
    descriptor.module_type = "test-object";
    descriptor.set_path(neug::ModuleDescriptor::kDataPath, object_path);
    manifest.SetModule("test-object", std::move(descriptor));
  }
  checkpoint.SetManifest(std::move(manifest));
}

std::shared_ptr<neug::Checkpoint> PublishCheckpoint(
    neug::CheckpointManager& manager, const std::string& object_payload = {}) {
  auto staging = manager.CreateStaging();
  std::string object_path;
  if (!object_payload.empty()) {
    auto runtime_file = staging.checkpoint()->CreateRuntimeFile();
    std::ofstream output(runtime_file.path(), std::ios::binary);
    EXPECT_TRUE(output.is_open());
    output << object_payload;
    EXPECT_TRUE(output.good());
    output.close();
    object_path =
        staging.checkpoint()->CommitRuntimeFile(std::move(runtime_file));
  }
  WriteManifest(*staging.checkpoint(), object_path);
  return staging.Publish();
}

void InsertThroughService(neug::NeugDBService& service, int64_t id) {
  auto slot = service.AcquireExecutionSlot();
  auto transaction = slot->GetInsertTransaction();
  neug::StorageTPInsertInterface storage(transaction);
  const auto label = transaction.schema().get_vertex_label_id("Item");
  neug::vid_t vid = 0;
  ASSERT_TRUE(storage.AddVertex(label, neug::Value::INT64(id), {}, vid));
  ASSERT_TRUE(transaction.Commit());
}

void CheckpointThroughService(neug::NeugDBService& service) {
  auto slot = service.AcquireExecutionSlot();
  auto result = slot->ExecuteTransactionalRequest(
      neug::RequestSerializer::SerializeRequest("CHECKPOINT;", "update", {}));
  ASSERT_TRUE(result) << result.error().ToString();
}

neug::NeugDBConfig Config(const std::string& data_dir) {
  neug::NeugDBConfig config(data_dir, 1);
  config.memory_level = neug::MemoryLevel::kInMemory;
  config.checkpoint_on_close = false;
  config.checkpoint_on_recovery = false;
  return config;
}

neug::NeugDBConfig CheckpointOnCloseConfig(const std::string& data_dir) {
  auto config = Config(data_dir);
  config.checkpoint_on_close = true;
  return config;
}

neug::NeugDBConfig SyncToFileConfig(const std::string& data_dir) {
  auto config = Config(data_dir);
  config.memory_level = neug::MemoryLevel::kSyncToFile;
  return config;
}

void RegisterExampleIndex() {
  static const bool registered = [] {
    return neug::ModuleFactory::instance().Register(
        neug::kExampleIndexType,
        [] { return std::make_unique<neug::ExampleIndex>(); });
  }();
  ASSERT_TRUE(registered);
}

void CreateExampleAgeIndex(neug::NeugDB& db) {
  neug::SnapshotGuard guard(db.graph_snapshot_store());
  auto& graph = *guard.get().mutable_graph();
  const auto label = graph.schema().get_vertex_label_id("Person");
  const auto schema = graph.schema().get_vertex_schema(label);
  const auto property_id = schema->get_property_index("age");
  auto meta = std::make_unique<neug::IndexMeta>();
  meta->name = "idx_person_age";
  meta->type = "example";
  meta->schema.label_id = label;
  meta->schema.property_name = "age";
  meta->schema.property_type = schema->property_types[property_id];
  auto* column = graph.get_vertex_table(label).GetPropertyColumnBase("age");
  auto created = graph.mutable_index_manager().CreateIndex(
      std::move(meta), std::make_unique<neug::DefaultIndexIDAccessor>(), column,
      graph.GetVertexSet(label));
  ASSERT_TRUE(created) << created.error().ToString();
}

TEST(CheckpointFormatTest, RejectsLegacyCheckpointDirectoriesWithoutMutation) {
  const auto db_dir = TestDir("legacy_rejected");
  std::filesystem::create_directories(std::filesystem::path(db_dir) /
                                      "checkpoint-7");

  neug::CheckpointManager manager;
  try {
    manager.Open(db_dir);
    FAIL() << "Expected legacy checkpoint directory to be rejected";
  } catch (const neug::exception::NotSupportedException& e) {
    EXPECT_NE(std::string(e.what()).find("does not read or migrate"),
              std::string::npos);
  }
  EXPECT_FALSE(std::filesystem::exists(std::filesystem::path(db_dir) /
                                       "checkpoint" / "CURRENT"));
  EXPECT_TRUE(
      std::filesystem::exists(std::filesystem::path(db_dir) / "checkpoint-7"));

  std::filesystem::remove_all(db_dir);
}

TEST(CheckpointFormatTest, RuntimeWorkspaceFollowsLastRuntimeResource) {
  const auto db_dir = TestDir("runtime_workspace_lifetime");
  neug::CheckpointManager manager;
  manager.Open(db_dir);

  auto checkpoint = PublishCheckpoint(manager);
  const auto runtime_dir = checkpoint->runtime_dir();
  auto container =
      checkpoint->CreateRuntimeContainer(64, neug::MemoryLevel::kSyncToFile);
  ASSERT_NE(container, nullptr);
  const auto runtime_file = container->GetPath();
  ASSERT_TRUE(std::filesystem::exists(runtime_file));

  manager.Close();
  checkpoint.reset();
  EXPECT_TRUE(std::filesystem::exists(runtime_dir));
  EXPECT_TRUE(std::filesystem::exists(runtime_file));

  container.reset();
  EXPECT_FALSE(std::filesystem::exists(runtime_dir));
  std::filesystem::remove_all(db_dir);
}

TEST(CheckpointFormatTest, PublishedOpenDoesNotRecreateMissingObjectStore) {
  const auto db_dir = TestDir("strict_published_open");
  neug::CheckpointManager manager;
  manager.Open(db_dir);
  auto checkpoint = PublishCheckpoint(manager);
  checkpoint.reset();
  manager.Close();

  const auto object_dir =
      std::filesystem::path(db_dir) / "checkpoint" / "objects";
  std::filesystem::remove_all(object_dir);
  neug::CheckpointManager reopened;
  EXPECT_THROW(reopened.Open(db_dir), std::exception);
  EXPECT_FALSE(std::filesystem::exists(object_dir));

  std::filesystem::remove_all(db_dir);
}

TEST(CheckpointFormatTest, StagingHandleOwnsTheSingleActiveStagingCheckpoint) {
  const auto db_dir = TestDir("staging_lifecycle");
  neug::CheckpointManager manager;
  manager.Open(db_dir);

  {
    auto staging = manager.CreateStaging();
    EXPECT_THROW(manager.CreateStaging(), std::exception);
  }

  auto replacement = manager.CreateStaging();
  WriteManifest(*replacement.checkpoint());
  auto published = replacement.Publish();
  ASSERT_NE(published, nullptr);
  EXPECT_EQ(published->id(), 0u);
  EXPECT_THROW(replacement.checkpoint(), std::exception);
  EXPECT_THROW(replacement.Publish(), std::exception);

  manager.Close();
  std::filesystem::remove_all(db_dir);
}

TEST(CheckpointFormatTest, PublishRejectsManifestWithoutSchema) {
  const auto db_dir = TestDir("missing_schema");
  neug::CheckpointManager manager;
  manager.Open(db_dir);

  auto staging = manager.CreateStaging();
  EXPECT_THROW(staging.Publish(), std::exception);
  EXPECT_EQ(manager.Current(), nullptr);
  staging.Discard();

  manager.Close();
  std::filesystem::remove_all(db_dir);
}

TEST(CheckpointFormatTest, RuntimeFileHandleCleansUpAndMaterializesByCopy) {
  const auto db_dir = TestDir("runtime_file_cleanup");
  neug::CheckpointManager manager;
  manager.Open(db_dir);
  auto staging = manager.CreateStaging();

  std::string runtime_path;
  {
    auto runtime_file = staging.checkpoint()->CreateRuntimeFile();
    runtime_path = runtime_file.path();
    ASSERT_TRUE(std::filesystem::exists(runtime_path));
  }
  EXPECT_FALSE(std::filesystem::exists(runtime_path));

  std::string materialized_path;
  {
    auto runtime_file = staging.checkpoint()->CreateRuntimeFile();
    runtime_path = runtime_file.path();
    {
      std::ofstream output(runtime_path, std::ios::binary);
      ASSERT_TRUE(output.is_open());
      output << "runtime-object";
    }
    materialized_path =
        staging.checkpoint()->MaterializeObject(runtime_file.path());
    EXPECT_NE(materialized_path, runtime_path);
    EXPECT_TRUE(std::filesystem::exists(materialized_path));
  }
  EXPECT_FALSE(std::filesystem::exists(runtime_path));
  EXPECT_TRUE(std::filesystem::exists(materialized_path));

  staging.Discard();
  manager.Close();
  std::filesystem::remove_all(db_dir);
}

TEST(CheckpointFormatTest, GarbageCollectionSkipsActiveStagingObjects) {
  const auto db_dir = TestDir("gc_active_staging");
  neug::CheckpointManager manager;
  manager.Open(db_dir);
  auto current = PublishCheckpoint(manager);

  auto staging = manager.CreateStaging();
  auto runtime_file = staging.checkpoint()->CreateRuntimeFile();
  {
    std::ofstream output(runtime_file.path(), std::ios::binary);
    ASSERT_TRUE(output.is_open());
    output << "staging-object";
  }
  const auto staging_object =
      staging.checkpoint()->CommitRuntimeFile(std::move(runtime_file));
  WriteManifest(*staging.checkpoint(), staging_object);

  manager.CollectGarbage();
  EXPECT_TRUE(std::filesystem::exists(staging_object));
  EXPECT_TRUE(std::filesystem::exists(current->manifest_path()));

  staging.Discard();
  manager.CollectGarbage();
  EXPECT_FALSE(std::filesystem::exists(staging_object));

  manager.Close();
  std::filesystem::remove_all(db_dir);
}

TEST(CheckpointFormatTest, FailedCurrentReplacementKeepsInMemoryCurrent) {
  const auto db_dir = TestDir("current_replace_failure");
  neug::CheckpointManager manager;
  manager.Open(db_dir);
  auto current = PublishCheckpoint(manager);

  const auto current_path =
      std::filesystem::path(db_dir) / "checkpoint" / "CURRENT";
  ASSERT_TRUE(std::filesystem::remove(current_path));
  ASSERT_TRUE(std::filesystem::create_directory(current_path));

  auto staging = manager.CreateStaging();
  WriteManifest(*staging.checkpoint());
  EXPECT_THROW(staging.Publish(), std::exception);
  EXPECT_EQ(manager.Current(), current);
  EXPECT_TRUE(std::filesystem::exists(current->manifest_path()));
  staging.Discard();

  manager.Close();
  std::filesystem::remove_all(db_dir);
}

TEST(CheckpointFormatTest, CurrentSelectsManifestAndGcRespectsCheckpointPins) {
  const auto db_dir = TestDir("manifest_gc_pins");
  neug::CheckpointManager manager;
  manager.Open(db_dir);

  auto pinned_checkpoint = PublishCheckpoint(manager, "pinned-object");
  const auto pinned_manifest = pinned_checkpoint->manifest_path();
  const auto pinned_object = pinned_checkpoint->manifest()
                                 .FindModule("test-object")
                                 ->get_path(neug::ModuleDescriptor::kDataPath)
                                 .value();
  ASSERT_TRUE(std::filesystem::exists(pinned_manifest));
  ASSERT_TRUE(std::filesystem::exists(pinned_object));

  auto current_checkpoint = PublishCheckpoint(manager);
  EXPECT_EQ(current_checkpoint->id(), 1u);
  EXPECT_EQ(
      std::ifstream(std::filesystem::path(db_dir) / "checkpoint" / "CURRENT")
          .peek(),
      '1');

  manager.CollectGarbage();
  EXPECT_TRUE(std::filesystem::exists(pinned_manifest));
  EXPECT_TRUE(std::filesystem::exists(pinned_object));
  EXPECT_TRUE(
      std::filesystem::exists(std::filesystem::path(db_dir) / "wal" / "0"));

  pinned_checkpoint.reset();
  manager.CollectGarbage();
  EXPECT_FALSE(std::filesystem::exists(pinned_manifest));
  EXPECT_FALSE(std::filesystem::exists(pinned_object));
  EXPECT_FALSE(
      std::filesystem::exists(std::filesystem::path(db_dir) / "wal" / "0"));
  EXPECT_TRUE(std::filesystem::exists(current_checkpoint->manifest_path()));

  manager.Close();
  std::filesystem::remove_all(db_dir);
}

TEST(CheckpointFormatTest, CurrentIsAuthoritativeAndDoesNotFallback) {
  const auto db_dir = TestDir("current_is_authoritative");
  neug::CheckpointManager manager;
  manager.Open(db_dir);
  auto first = PublishCheckpoint(manager, "first");
  auto second = PublishCheckpoint(manager, "second");
  const auto second_manifest = second->manifest_path();
  manager.Close();

  const auto current_path =
      std::filesystem::path(db_dir) / "checkpoint" / "CURRENT";
  {
    std::ofstream current(current_path, std::ios::trunc);
    ASSERT_TRUE(current.is_open());
    current << "99\n";
  }

  neug::CheckpointManager reopened;
  EXPECT_THROW(reopened.Open(db_dir), std::exception);
  EXPECT_TRUE(std::filesystem::exists(second_manifest));
  EXPECT_EQ(std::ifstream(current_path).peek(), '9');
  reopened.Close();

  std::filesystem::remove_all(db_dir);
}

TEST(CheckpointFormatTest,
     OpenEpochWorkspacesAreDistinctAndIndependentlyOwned) {
  const auto db_dir = TestDir("distinct_open_epoch_workspaces");
  neug::CheckpointManager first;
  neug::CheckpointManager second;
  first.Open(db_dir);
  const auto first_runtime = first.CreateStaging().checkpoint()->runtime_dir();
  second.Open(db_dir);
  const auto second_runtime =
      second.CreateStaging().checkpoint()->runtime_dir();

  EXPECT_NE(first_runtime, second_runtime);
  EXPECT_TRUE(std::filesystem::exists(first_runtime));
  EXPECT_TRUE(std::filesystem::exists(second_runtime));
  first.Close();
  EXPECT_FALSE(std::filesystem::exists(first_runtime));
  EXPECT_TRUE(std::filesystem::exists(second_runtime));
  second.Close();
  EXPECT_FALSE(std::filesystem::exists(second_runtime));

  std::filesystem::remove_all(db_dir);
}

TEST(CheckpointFormatTest, GarbageCollectionReclaimsAbandonedOpenEpochs) {
  const auto db_dir = TestDir("abandoned_open_epochs");
  const auto runtime_root = std::filesystem::path(db_dir) / "runtime";
  const auto abandoned = runtime_root / "open-abandoned";
  const auto unrelated = runtime_root / "keep-this-directory";
  std::filesystem::create_directories(abandoned / "allocator");
  std::filesystem::create_directories(unrelated);
  {
    std::ofstream payload(abandoned / "allocator" / "data");
    ASSERT_TRUE(payload.is_open());
    payload << "stale runtime data";
  }

  neug::CheckpointManager manager;
  manager.Open(db_dir);
  auto current = PublishCheckpoint(manager);
  const auto active_runtime = current->runtime_dir();
  ASSERT_TRUE(std::filesystem::exists(active_runtime));

  manager.CollectGarbage();
  EXPECT_FALSE(std::filesystem::exists(abandoned));
  EXPECT_TRUE(std::filesystem::exists(active_runtime));
  EXPECT_TRUE(std::filesystem::exists(unrelated));

  current.reset();
  manager.Close();
  EXPECT_FALSE(std::filesystem::exists(active_runtime));
  EXPECT_TRUE(std::filesystem::exists(unrelated));

  std::filesystem::remove_all(db_dir);
}

TEST(CheckpointFormatTest,
     ManualCheckpointReopensRuntimeAndReplaysOnlyNewWalEpoch) {
  const auto db_dir = TestDir("manual_wal_epoch");
  {
    neug::NeugDB db;
    ASSERT_TRUE(db.Open(Config(db_dir)));
    auto connection = db.Connect();
    auto result =
        connection->Query("CREATE NODE TABLE Item(id INT64, PRIMARY KEY(id));");
    ASSERT_TRUE(result) << result.error().ToString();
    result = connection->Query("CREATE (:Item {id: 1});");
    ASSERT_TRUE(result) << result.error().ToString();
    connection->Close();

    {
      neug::NeugDBService service(db);
      InsertThroughService(service, 2);
      const auto runtime_before = db.graph().checkpoint().runtime_dir();
      CheckpointThroughService(service);

      const auto first_checkpoint = db.graph().checkpoint_ptr();
      ASSERT_NE(first_checkpoint, nullptr);
      EXPECT_EQ(first_checkpoint->id(), 1u);
      EXPECT_EQ(first_checkpoint->runtime_dir(), runtime_before);
      EXPECT_EQ(first_checkpoint->manifest().base_timestamp(), 0u);
      EXPECT_TRUE(std::filesystem::exists(std::filesystem::path(db_dir) /
                                          "checkpoint" / "CURRENT"));
      const auto* first_keys = first_checkpoint->manifest().FindModule(
          neug::VertexTable::KeyKeys("Item"));
      ASSERT_TRUE(first_keys);
      const auto first_keys_json = first_keys->ToJsonString();

      // Manual CHECKPOINT keeps its full-checkpoint behavior: compact, publish,
      // reopen graph and allocator state, and rotate to a new WAL epoch. Clean
      // immutable objects may still be reused by the new complete manifest.
      CheckpointThroughService(service);
      const auto second_checkpoint = db.graph().checkpoint_ptr();
      ASSERT_NE(second_checkpoint, nullptr);
      EXPECT_EQ(second_checkpoint->id(), 2u);
      EXPECT_EQ(second_checkpoint->runtime_dir(), runtime_before);
      EXPECT_EQ(second_checkpoint->manifest().base_timestamp(), 0u);
      const auto* second_keys = second_checkpoint->manifest().FindModule(
          neug::VertexTable::KeyKeys("Item"));
      ASSERT_TRUE(second_keys);
      EXPECT_EQ(second_keys->ToJsonString(), first_keys_json);

      InsertThroughService(service, 3);
    }
    db.Close();
  }

  {
    neug::NeugDB db;
    ASSERT_TRUE(db.Open(Config(db_dir)));
    auto connection = db.Connect();
    auto result = connection->Query("MATCH (v:Item) RETURN v.id;");
    ASSERT_TRUE(result) << result.error().ToString();
    EXPECT_EQ(result.value().response().row_count(), 3);
    connection->Close();
    db.Close();
  }

  std::filesystem::remove_all(db_dir);
}

TEST(CheckpointFormatTest, ManualCheckpointPreservesDeletedVertexState) {
  const auto db_dir = TestDir("manual_deleted_vertex");
  {
    neug::NeugDB db;
    ASSERT_TRUE(db.Open(Config(db_dir)));
    auto connection = db.Connect();
    auto result =
        connection->Query("CREATE NODE TABLE Item(id INT64, PRIMARY KEY(id));");
    ASSERT_TRUE(result) << result.error().ToString();
    result = connection->Query("CREATE (:Item {id: 1}), (:Item {id: 2});");
    ASSERT_TRUE(result) << result.error().ToString();
    result = connection->Query("MATCH (v:Item) WHERE v.id = 1 DELETE v;");
    ASSERT_TRUE(result) << result.error().ToString();
    connection->Close();

    const auto label = db.graph().schema().get_vertex_label_id("Item");
    ASSERT_EQ(db.graph().get_vertex_table(label).LidNum(), 2u);

    {
      neug::NeugDBService service(db);
      CheckpointThroughService(service);
    }

    const auto checkpoint = db.graph().checkpoint_ptr();
    ASSERT_NE(checkpoint, nullptr);
    EXPECT_EQ(checkpoint->manifest().base_timestamp(), 0u);
    EXPECT_EQ(db.graph().get_vertex_table(label).LidNum(), 2u);
    db.Close();
  }

  {
    neug::NeugDB db;
    ASSERT_TRUE(db.Open(Config(db_dir)));
    const auto label = db.graph().schema().get_vertex_label_id("Item");
    EXPECT_EQ(db.graph().get_vertex_table(label).LidNum(), 2u);
    auto connection = db.Connect();
    auto result = connection->Query("MATCH (v:Item) RETURN v.id;");
    ASSERT_TRUE(result) << result.error().ToString();
    EXPECT_EQ(result.value().response().row_count(), 1);
    connection->Close();
    db.Close();
  }

  std::filesystem::remove_all(db_dir);
}

TEST(CheckpointFormatTest,
     ManualCheckpointRoundTripsEvolvedVertexAndEdgeDescriptors) {
  const auto db_dir = TestDir("evolved_vertex_edge_descriptors");
  {
    neug::NeugDB db;
    ASSERT_TRUE(db.Open(Config(db_dir)));
    auto connection = db.Connect();
    auto result = connection->Query(
        "CREATE NODE TABLE Person(id STRING, PRIMARY KEY(id));");
    ASSERT_TRUE(result) << result.error().ToString();
    result = connection->Query(
        "CREATE NODE TABLE Software(id STRING, PRIMARY KEY(id));");
    ASSERT_TRUE(result) << result.error().ToString();
    result = connection->Query(
        "CREATE REL TABLE Uses(FROM Person TO Software, weight DOUBLE);");
    ASSERT_TRUE(result) << result.error().ToString();
    result = connection->Query("CREATE (:Person {id: 'alice'});");
    ASSERT_TRUE(result) << result.error().ToString();
    result = connection->Query("CREATE (:Software {id: 'neug'});");
    ASSERT_TRUE(result) << result.error().ToString();
    result = connection->Query(
        "MATCH (p:Person {id: 'alice'}), (s:Software {id: 'neug'}) "
        "CREATE (p)-[:Uses {weight: 1.5}]->(s);");
    ASSERT_TRUE(result) << result.error().ToString();
    connection->Close();
    {
      neug::NeugDBService service(db);
      CheckpointThroughService(service);
    }

    connection = db.Connect();
    result = connection->Query("ALTER TABLE Person ADD score INT64;");
    ASSERT_TRUE(result) << result.error().ToString();
    result =
        connection->Query("MATCH (p:Person {id: 'alice'}) SET p.score = 42;");
    ASSERT_TRUE(result) << result.error().ToString();
    result = connection->Query("ALTER TABLE Uses ADD description STRING;");
    ASSERT_TRUE(result) << result.error().ToString();
    result = connection->Query(
        "MATCH (:Person)-[e:Uses]->(:Software) SET e.description = 'core';");
    ASSERT_TRUE(result) << result.error().ToString();
    connection->Close();
    {
      neug::NeugDBService service(db);
      CheckpointThroughService(service);
    }
    db.Close();
  }

  {
    neug::NeugDB db;
    ASSERT_TRUE(db.Open(Config(db_dir)));
    auto connection = db.Connect();
    auto result = connection->Query(
        "MATCH (p:Person)-[e:Uses]->(:Software) "
        "RETURN p.score, e.weight, e.description;");
    ASSERT_TRUE(result) << result.error().ToString();
    const auto& response = result.value().response();
    ASSERT_EQ(response.row_count(), 1);
    ASSERT_EQ(response.arrays_size(), 3);
    EXPECT_EQ(response.arrays(0).int64_array().values(0), 42);
    EXPECT_DOUBLE_EQ(response.arrays(1).double_array().values(0), 1.5);
    EXPECT_EQ(response.arrays(2).string_array().values(0), "core");
    connection->Close();
    db.Close();
  }

  std::filesystem::remove_all(db_dir);
}

TEST(CheckpointFormatTest,
     DropEdgeTableCheckpointReopenRecreateHasNoStaleData) {
  const auto db_dir = TestDir("drop_edge_recreate");
  {
    neug::NeugDB db;
    ASSERT_TRUE(db.Open(Config(db_dir)));
    auto connection = db.Connect();
    auto result = connection->Query(
        "CREATE NODE TABLE Person(id STRING, PRIMARY KEY(id));");
    ASSERT_TRUE(result) << result.error().ToString();
    result = connection->Query(
        "CREATE NODE TABLE Software(id STRING, PRIMARY KEY(id));");
    ASSERT_TRUE(result) << result.error().ToString();
    result = connection->Query(
        "CREATE REL TABLE Created(FROM Person TO Software, weight DOUBLE);");
    ASSERT_TRUE(result) << result.error().ToString();
    result = connection->Query("CREATE (:Person {id: 'alice'});");
    ASSERT_TRUE(result) << result.error().ToString();
    result = connection->Query("CREATE (:Software {id: 'neug'});");
    ASSERT_TRUE(result) << result.error().ToString();
    result = connection->Query(
        "MATCH (p:Person), (s:Software) CREATE (p)-[:Created {weight: "
        "1.0}]->(s);");
    ASSERT_TRUE(result) << result.error().ToString();
    connection->Close();
    {
      neug::NeugDBService service(db);
      CheckpointThroughService(service);
    }

    connection = db.Connect();
    result = connection->Query("DROP TABLE Created;");
    ASSERT_TRUE(result) << result.error().ToString();
    connection->Close();
    {
      neug::NeugDBService service(db);
      CheckpointThroughService(service);
    }
    db.Close();
  }

  {
    neug::NeugDB db;
    ASSERT_TRUE(db.Open(Config(db_dir)));
    auto connection = db.Connect();
    auto result = connection->Query(
        "CREATE REL TABLE Created(FROM Person TO Software, weight DOUBLE);");
    ASSERT_TRUE(result) << result.error().ToString();
    result =
        connection->Query("MATCH (:Person)-[e:Created]->(:Software) RETURN e;");
    ASSERT_TRUE(result) << result.error().ToString();
    EXPECT_EQ(result.value().response().row_count(), 0);
    result = connection->Query(
        "MATCH (p:Person), (s:Software) CREATE (p)-[:Created {weight: "
        "2.0}]->(s);");
    ASSERT_TRUE(result) << result.error().ToString();
    connection->Close();
    {
      neug::NeugDBService service(db);
      CheckpointThroughService(service);
    }
    db.Close();
  }

  {
    neug::NeugDB db;
    ASSERT_TRUE(db.Open(Config(db_dir)));
    auto connection = db.Connect();
    auto result = connection->Query(
        "MATCH (:Person)-[e:Created]->(:Software) RETURN e.weight;");
    ASSERT_TRUE(result) << result.error().ToString();
    const auto& response = result.value().response();
    ASSERT_EQ(response.row_count(), 1);
    EXPECT_DOUBLE_EQ(response.arrays(0).double_array().values(0), 2.0);
    connection->Close();
    db.Close();
  }

  std::filesystem::remove_all(db_dir);
}

TEST(CheckpointFormatTest, DropLabelDoesNotSweepSiblingWithSharedPrefix) {
  const auto db_dir = TestDir("drop_sibling_prefix");
  {
    neug::NeugDB db;
    ASSERT_TRUE(db.Open(Config(db_dir)));
    auto connection = db.Connect();
    auto result = connection->Query(
        "CREATE NODE TABLE User(id STRING, PRIMARY KEY(id));");
    ASSERT_TRUE(result) << result.error().ToString();
    result = connection->Query(
        "CREATE NODE TABLE UserAccount(id STRING, PRIMARY KEY(id));");
    ASSERT_TRUE(result) << result.error().ToString();
    result = connection->Query("CREATE (:User {id: 'u1'});");
    ASSERT_TRUE(result) << result.error().ToString();
    result = connection->Query("CREATE (:UserAccount {id: 'a1'});");
    ASSERT_TRUE(result) << result.error().ToString();
    connection->Close();
    {
      neug::NeugDBService service(db);
      CheckpointThroughService(service);
    }

    connection = db.Connect();
    result = connection->Query("DROP TABLE User;");
    ASSERT_TRUE(result) << result.error().ToString();
    connection->Close();
    {
      neug::NeugDBService service(db);
      CheckpointThroughService(service);
    }
    db.Close();
  }

  {
    neug::NeugDB db;
    ASSERT_TRUE(db.Open(Config(db_dir)));
    auto connection = db.Connect();
    auto result = connection->Query("MATCH (u:UserAccount) RETURN u.id;");
    ASSERT_TRUE(result) << result.error().ToString();
    const auto& response = result.value().response();
    ASSERT_EQ(response.row_count(), 1);
    EXPECT_EQ(response.arrays(0).string_array().values(0), "a1");
    connection->Close();
    db.Close();
  }

  std::filesystem::remove_all(db_dir);
}

TEST(CheckpointFormatTest, SyncToFileCheckpointRecoversAcrossOpenEpochs) {
  const auto db_dir = TestDir("sync_to_file_reopen");
  {
    neug::NeugDB db;
    ASSERT_TRUE(db.Open(SyncToFileConfig(db_dir)));
    auto connection = db.Connect();
    auto result =
        connection->Query("CREATE NODE TABLE Item(id INT64, PRIMARY KEY(id));");
    ASSERT_TRUE(result) << result.error().ToString();
    result = connection->Query("CREATE (:Item {id: 1}), (:Item {id: 2});");
    ASSERT_TRUE(result) << result.error().ToString();
    connection->Close();
    {
      neug::NeugDBService service(db);
      CheckpointThroughService(service);
    }
    db.Close();
  }

  {
    neug::NeugDB db;
    ASSERT_TRUE(db.Open(SyncToFileConfig(db_dir)));
    auto connection = db.Connect();
    auto result = connection->Query("MATCH (v:Item) RETURN v.id;");
    ASSERT_TRUE(result) << result.error().ToString();
    EXPECT_EQ(result.value().response().row_count(), 2);
    connection->Close();
    db.Close();
  }

  std::filesystem::remove_all(db_dir);
}

TEST(CheckpointFormatTest,
     ManualCheckpointReplacesDirtyIndexAndReusesCleanIndexObject) {
  RegisterExampleIndex();
  const auto db_dir = TestDir("manual_index_object_reuse");
  {
    neug::NeugDB db;
    ASSERT_TRUE(db.Open(Config(db_dir)));
    auto connection = db.Connect();
    auto result = connection->Query(
        "CREATE NODE TABLE Person(id INT64, age INT32, PRIMARY KEY(id));");
    ASSERT_TRUE(result) << result.error().ToString();
    result = connection->Query("CREATE (:Person {id: 1, age: 30});");
    ASSERT_TRUE(result) << result.error().ToString();
    connection->Close();

    CreateExampleAgeIndex(db);
    std::string first_index_object;
    {
      neug::NeugDBService service(db);
      CheckpointThroughService(service);
      auto checkpoint = db.graph().checkpoint_ptr();
      ASSERT_NE(checkpoint, nullptr);
      const auto* descriptor = checkpoint->manifest().FindModule(
          neug::StorageIndexManager::GetKey("idx_person_age"));
      ASSERT_TRUE(descriptor);
      const auto object = descriptor->get_path(neug::kIndexBufferPath);
      ASSERT_TRUE(object);
      first_index_object = *object;
    }

    connection = db.Connect();
    result = connection->Query("MATCH (p:Person {id: 1}) SET p.age = 31;");
    ASSERT_TRUE(result) << result.error().ToString();
    connection->Close();

    std::string second_index_object;
    {
      neug::NeugDBService service(db);
      CheckpointThroughService(service);
      auto checkpoint = db.graph().checkpoint_ptr();
      ASSERT_NE(checkpoint, nullptr);
      const auto* descriptor = checkpoint->manifest().FindModule(
          neug::StorageIndexManager::GetKey("idx_person_age"));
      ASSERT_TRUE(descriptor);
      const auto object = descriptor->get_path(neug::kIndexBufferPath);
      ASSERT_TRUE(object);
      second_index_object = *object;
      EXPECT_NE(second_index_object, first_index_object);

      CheckpointThroughService(service);
      checkpoint = db.graph().checkpoint_ptr();
      ASSERT_NE(checkpoint, nullptr);
      const auto* reused_descriptor = checkpoint->manifest().FindModule(
          neug::StorageIndexManager::GetKey("idx_person_age"));
      ASSERT_TRUE(reused_descriptor);
      EXPECT_EQ(reused_descriptor->get_path(neug::kIndexBufferPath),
                std::optional<std::string>(second_index_object));
    }
    db.Close();
  }

  {
    neug::NeugDB db;
    ASSERT_TRUE(db.Open(Config(db_dir)));
    auto* index = dynamic_cast<neug::ExampleIndex*>(
        db.graph().index_manager().GetIndexByName("idx_person_age").value());
    ASSERT_NE(index, nullptr);
    neug::ExampleIndexQueryParams query(31);
    auto matches = index->Search(query);
    ASSERT_TRUE(matches) << matches.error().ToString();
    EXPECT_EQ(matches->size(), 1u);
    db.Close();
  }

  std::filesystem::remove_all(db_dir);
}

TEST(CheckpointFormatTest, IndexCatalogChangesTriggerCheckpointOnClose) {
  RegisterExampleIndex();
  const auto db_dir = TestDir("index_catalog_checkpoint_on_close");

  {
    neug::NeugDB db;
    ASSERT_TRUE(db.Open(CheckpointOnCloseConfig(db_dir)));
    auto connection = db.Connect();
    auto result = connection->Query(
        "CREATE NODE TABLE Person(id INT64, age INT32, PRIMARY KEY(id));");
    ASSERT_TRUE(result) << result.error().ToString();
    result = connection->Query("CREATE (:Person {id: 1, age: 30});");
    ASSERT_TRUE(result) << result.error().ToString();
    connection->Close();

    {
      neug::NeugDBService service(db);
      CheckpointThroughService(service);
    }
    EXPECT_FALSE(db.graph().IsModified());

    CreateExampleAgeIndex(db);
    EXPECT_TRUE(db.graph().IsModified());
    db.Close();
  }

  {
    neug::NeugDB db;
    ASSERT_TRUE(db.Open(CheckpointOnCloseConfig(db_dir)));
    auto index = db.graph().index_manager().GetIndexByName("idx_person_age");
    ASSERT_TRUE(index) << index.error().ToString();
    EXPECT_FALSE(db.graph().IsModified());

    {
      neug::SnapshotGuard guard(db.graph_snapshot_store());
      ASSERT_TRUE(guard.get()
                      .mutable_graph()
                      ->mutable_index_manager()
                      .DropIndex("idx_person_age")
                      .ok());
    }
    EXPECT_TRUE(db.graph().IsModified());
    db.Close();
  }

  {
    neug::NeugDB db;
    ASSERT_TRUE(db.Open(Config(db_dir)));
    EXPECT_FALSE(db.graph()
                     .index_manager()
                     .GetIndexByName("idx_person_age")
                     .has_value());
    db.Close();
  }

  std::filesystem::remove_all(db_dir);
}

TEST(CheckpointFormatTest,
     DropCheckpointRecreateDoesNotResurrectStaleLabelObjects) {
  const auto db_dir = TestDir("drop_recreate");
  {
    neug::NeugDB db;
    ASSERT_TRUE(db.Open(Config(db_dir)));
    auto connection = db.Connect();
    auto result = connection->Query(
        "CREATE NODE TABLE Person(id STRING, PRIMARY KEY(id));");
    ASSERT_TRUE(result) << result.error().ToString();
    result = connection->Query("CREATE (:Person {id: 'alice'});");
    ASSERT_TRUE(result) << result.error().ToString();
    connection->Close();

    {
      neug::NeugDBService service(db);
      CheckpointThroughService(service);
    }

    connection = db.Connect();
    result = connection->Query("DROP TABLE IF EXISTS Person;");
    ASSERT_TRUE(result) << result.error().ToString();
    connection->Close();

    {
      neug::NeugDBService service(db);
      CheckpointThroughService(service);
    }
    db.Close();
  }

  {
    neug::NeugDB db;
    ASSERT_TRUE(db.Open(Config(db_dir)));
    auto connection = db.Connect();
    auto result = connection->Query(
        "CREATE NODE TABLE Person(id STRING, PRIMARY KEY(id));");
    ASSERT_TRUE(result) << result.error().ToString();
    result = connection->Query("MATCH (p:Person) RETURN p.id;");
    ASSERT_TRUE(result) << result.error().ToString();
    EXPECT_EQ(result.value().response().row_count(), 0);

    result = connection->Query("CREATE (:Person {id: 'bob'});");
    ASSERT_TRUE(result) << result.error().ToString();
    connection->Close();
    {
      neug::NeugDBService service(db);
      CheckpointThroughService(service);
    }
    db.Close();
  }

  {
    neug::NeugDB db;
    ASSERT_TRUE(db.Open(Config(db_dir)));
    auto connection = db.Connect();
    auto result = connection->Query("MATCH (p:Person) RETURN p.id;");
    ASSERT_TRUE(result) << result.error().ToString();
    const auto& response = result.value().response();
    ASSERT_EQ(response.row_count(), 1);
    ASSERT_EQ(response.arrays_size(), 1);
    EXPECT_EQ(response.arrays(0).string_array().values(0), "bob");
    connection->Close();
    db.Close();
  }

  std::filesystem::remove_all(db_dir);
}

}  // namespace
