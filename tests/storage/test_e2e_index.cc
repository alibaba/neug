/** Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <gtest/gtest.h>

#include <fcntl.h>
#include <sys/wait.h>
#include <unistd.h>

#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <memory>
#include <string>

#ifdef __APPLE__
#include <mach-o/dyld.h>
#endif

#include "neug/main/connection.h"
#include "neug/main/neug_db.h"
#include "neug/transaction/wal/wal_builder.h"
#include "neug/utils/exception/exception.h"
#include "test_index_common.h"

namespace neug {
namespace {

constexpr const char* kReopenDataDirEnv = "NEUG_TEST_E2E_INDEX_REOPEN_DIR";
constexpr const char* kPendingMutationModeEnv =
    "NEUG_TEST_E2E_PENDING_MUTATION_MODE";

std::string CurrentExecutablePath() {
#ifdef __APPLE__
  uint32_t size = 0;
  _NSGetExecutablePath(nullptr, &size);
  std::string path(size, '\0');
  if (_NSGetExecutablePath(path.data(), &size) != 0) {
    return {};
  }
  path.resize(std::char_traits<char>::length(path.c_str()));
  return std::filesystem::canonical(path).string();
#else
  std::string path(4096, '\0');
  const auto length = readlink("/proc/self/exe", path.data(), path.size() - 1);
  if (length < 0) {
    return {};
  }
  path.resize(static_cast<size_t>(length));
  return path;
#endif
}

TEST(E2EIndexReopenSubprocess, ActivatesPendingIndexAfterLoad) {
  const char* data_dir = std::getenv(kReopenDataDirEnv);
  if (data_dir == nullptr || *data_dir == '\0') {
    GTEST_SKIP() << "Only executed by the cross-process reopen test";
  }

  NeugDBConfig config;
  config.data_dir = data_dir;
  config.mode = DBMode::READ_WRITE;
  config.checkpoint_on_close = false;

  NeugDB reopened;
  ASSERT_TRUE(reopened.Open(config));
  auto connection = reopened.Connect();
  ASSERT_NE(connection, nullptr);

  auto show_before_load = connection->Query("CALL SHOW_INDEXES() RETURN *;");
  ASSERT_TRUE(show_before_load) << show_before_load.error().ToString();
  ASSERT_EQ(show_before_load->response().row_count(), 1);
  ASSERT_EQ(show_before_load->response().arrays_size(), 6);
  EXPECT_EQ(show_before_load->response().arrays(5).string_array().values(0),
            "pending");

  auto create_unrelated = connection->Query(
      "CREATE NODE TABLE Unrelated (id INT64, PRIMARY KEY(id));");
  ASSERT_TRUE(create_unrelated) << create_unrelated.error().ToString();
  auto insert_unrelated = connection->Query("CREATE (:Unrelated {id: 1});");
  ASSERT_TRUE(insert_unrelated) << insert_unrelated.error().ToString();
  auto checkpoint = connection->Query("CHECKPOINT;");
  ASSERT_TRUE(checkpoint) << checkpoint.error().ToString();

  auto show_after_checkpoint =
      connection->Query("CALL SHOW_INDEXES() RETURN *;");
  ASSERT_TRUE(show_after_checkpoint)
      << show_after_checkpoint.error().ToString();
  ASSERT_EQ(show_after_checkpoint->response().row_count(), 1);
  EXPECT_EQ(
      show_after_checkpoint->response().arrays(5).string_array().values(0),
      "pending");

  auto load = connection->Query("LOAD vec_index;");
  ASSERT_TRUE(load) << load.error().ToString();

  auto show_after_load = connection->Query("CALL SHOW_INDEXES() RETURN *;");
  ASSERT_TRUE(show_after_load) << show_after_load.error().ToString();
  ASSERT_EQ(show_after_load->response().row_count(), 1);
  EXPECT_EQ(show_after_load->response().arrays(5).string_array().values(0),
            "active");

  connection->Close();
  reopened.Close();
}

TEST(E2EIndexReopenSubprocess, DropsPendingIndexWithoutExtension) {
  const char* data_dir = std::getenv(kReopenDataDirEnv);
  if (data_dir == nullptr || *data_dir == '\0') {
    GTEST_SKIP() << "Only executed by the cross-process pending-drop test";
  }

  NeugDBConfig config;
  config.data_dir = data_dir;
  config.mode = DBMode::READ_WRITE;
  config.checkpoint_on_close = false;

  {
    NeugDB reopened;
    ASSERT_TRUE(reopened.Open(config));
    auto connection = reopened.Connect();
    ASSERT_NE(connection, nullptr);

    auto show_pending = connection->Query("CALL SHOW_INDEXES() RETURN *;");
    ASSERT_TRUE(show_pending) << show_pending.error().ToString();
    ASSERT_EQ(show_pending->response().row_count(), 1);
    EXPECT_EQ(show_pending->response().arrays(5).string_array().values(0),
              "pending");

    auto drop = connection->Query("DROP INDEX entity_embedding_hnsw;");
    ASSERT_TRUE(drop) << drop.error().ToString();
    const auto label = reopened.graph().schema().get_vertex_label_id("Entity");
    const auto& table = reopened.graph().get_vertex_table(label);
    const auto property =
        table.get_vertex_schema_ptr()->get_property_index("embedding");
    ASSERT_GE(property, 0);
    EXPECT_NE(dynamic_cast<const ArrayColumn*>(
                  table.get_table().get_column_by_id(property)),
              nullptr);
    auto checkpoint = connection->Query("CHECKPOINT;");
    ASSERT_TRUE(checkpoint) << checkpoint.error().ToString();
    connection->Close();
    reopened.Close();
  }

  {
    NeugDB reopened;
    ASSERT_TRUE(reopened.Open(config));
    auto connection = reopened.Connect();
    ASSERT_NE(connection, nullptr);
    auto show = connection->Query("CALL SHOW_INDEXES() RETURN *;");
    ASSERT_TRUE(show) << show.error().ToString();
    EXPECT_EQ(show->response().row_count(), 0);
    connection->Close();
    reopened.Close();
  }
}

TEST(E2EIndexReopenSubprocess, RejectsCheckpointWithPendingMutations) {
  const char* data_dir = std::getenv(kReopenDataDirEnv);
  const char* mode = std::getenv(kPendingMutationModeEnv);
  if (data_dir == nullptr || *data_dir == '\0' || mode == nullptr ||
      *mode == '\0') {
    GTEST_SKIP() << "Only executed by the pending-mutation reopen test";
  }

  NeugDBConfig config;
  config.data_dir = data_dir;
  config.mode = DBMode::READ_WRITE;
  config.checkpoint_on_close = true;

  NeugDB reopened;
  ASSERT_TRUE(reopened.Open(config));
  auto connection = reopened.Connect();
  ASSERT_NE(connection, nullptr);

  auto checkpoint = connection->Query("CHECKPOINT;");
  ASSERT_FALSE(checkpoint);
  EXPECT_NE(checkpoint.error().ToString().find("mutations for pending"),
            std::string::npos);

  connection->Close();
  reopened.Close();
}

TEST(E2EIndexReopenSubprocess, PreparePendingMutationWal) {
  const char* data_dir = std::getenv(kReopenDataDirEnv);
  const char* mode = std::getenv(kPendingMutationModeEnv);
  if (data_dir == nullptr || *data_dir == '\0' || mode == nullptr ||
      std::string(mode) != "prepare") {
    GTEST_SKIP() << "Only executed to prepare the pending-mutation WAL";
  }

  NeugDBConfig config;
  config.data_dir = data_dir;
  config.mode = DBMode::READ_WRITE;
  config.checkpoint_on_close = false;

  NeugDB db;
  ASSERT_TRUE(db.Open(config));
  auto connection = db.Connect();
  ASSERT_NE(connection, nullptr);
  auto load = connection->Query("LOAD vec_index;");
  ASSERT_TRUE(load) << load.error().ToString();
  auto create_table = connection->Query(
      "CREATE NODE TABLE Entity (id INT64, embedding FLOAT[2], "
      "PRIMARY KEY(id));");
  ASSERT_TRUE(create_table) << create_table.error().ToString();
  auto create_entity =
      connection->Query("CREATE (:Entity {id: 1, embedding: [1.0, 2.0]});");
  ASSERT_TRUE(create_entity) << create_entity.error().ToString();
  auto create_index = connection->Query(
      "CREATE INDEX entity_embedding_hnsw "
      "ON Entity USING HNSW (embedding);");
  ASSERT_TRUE(create_index) << create_index.error().ToString();
  auto checkpoint = connection->Query("CHECKPOINT;");
  ASSERT_TRUE(checkpoint) << checkpoint.error().ToString();

  WalBuilder wal;
  wal.LogInsertVertex(db.graph().schema().get_vertex_label_id("Entity"),
                      Value::INT64(2),
                      {Value::ARRAY(DataType::Array(DataType::FLOAT, 2),
                                    {Value::FLOAT(3.0f), Value::FLOAT(4.0f)})});
  wal.finalize(1);
  auto wal_writer =
      WalWriterFactory::CreateWalWriter(db.graph().checkpoint().wal_dir(), 0);
  wal_writer->open(db.graph().checkpoint().wal_dir());
  ASSERT_TRUE(wal_writer->append(wal.data(), wal.size()));

  _exit(0);
}

class E2EIndexTest : public ::testing::Test {
 protected:
  void SetUp() override {
    setenv("NEUG_EXTENSION_HOME_PYENV", NEUG_TEST_VEC_INDEX_EXTENSION_HOME, 1);
    workDir_ = std::string("/tmp/test_e2e_index_") +
               ::testing::UnitTest::GetInstance()->current_test_info()->name();
    std::filesystem::remove_all(workDir_);

    config_.data_dir = workDir_;
    config_.mode = DBMode::READ_WRITE;
    config_.checkpoint_on_close = false;
  }

  void TearDown() override { std::filesystem::remove_all(workDir_); }

  static void LoadVecIndex(Connection& connection) {
    auto load = connection.Query("LOAD vec_index;");
    ASSERT_TRUE(load) << load.error().ToString();
  }

  void AssertFreshProcessActivatesPendingIndex() const {
    const auto executable = CurrentExecutablePath();
    ASSERT_FALSE(executable.empty());
    const auto log_path = workDir_ + "/reopen.log";

    const pid_t pid = fork();
    ASSERT_GE(pid, 0);
    if (pid == 0) {
      const int log_fd =
          open(log_path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
      if (log_fd < 0 || dup2(log_fd, STDOUT_FILENO) < 0 ||
          dup2(log_fd, STDERR_FILENO) < 0) {
        _exit(126);
      }
      close(log_fd);
      setenv(kReopenDataDirEnv, workDir_.c_str(), 1);
      execl(executable.c_str(), executable.c_str(),
            "--gtest_filter="
            "E2EIndexReopenSubprocess.ActivatesPendingIndexAfterLoad",
            static_cast<char*>(nullptr));
      _exit(127);
    }

    int status = 0;
    ASSERT_EQ(waitpid(pid, &status, 0), pid);

    std::ifstream log_stream(log_path);
    ASSERT_TRUE(log_stream.is_open());
    const std::string log((std::istreambuf_iterator<char>(log_stream)),
                          std::istreambuf_iterator<char>());
    ASSERT_TRUE(WIFEXITED(status)) << log;
    EXPECT_EQ(WEXITSTATUS(status), 0) << log;
  }

  void AssertFreshProcessDropsPendingIndex() const {
    const auto executable = CurrentExecutablePath();
    ASSERT_FALSE(executable.empty());
    const auto log_path = workDir_ + "/drop_pending.log";

    const pid_t pid = fork();
    ASSERT_GE(pid, 0);
    if (pid == 0) {
      const int log_fd =
          open(log_path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
      if (log_fd < 0 || dup2(log_fd, STDOUT_FILENO) < 0 ||
          dup2(log_fd, STDERR_FILENO) < 0) {
        _exit(126);
      }
      close(log_fd);
      setenv(kReopenDataDirEnv, workDir_.c_str(), 1);
      execl(executable.c_str(), executable.c_str(),
            "--gtest_filter="
            "E2EIndexReopenSubprocess.DropsPendingIndexWithoutExtension",
            static_cast<char*>(nullptr));
      _exit(127);
    }

    int status = 0;
    ASSERT_EQ(waitpid(pid, &status, 0), pid);
    std::ifstream log_stream(log_path);
    ASSERT_TRUE(log_stream.is_open());
    const std::string log((std::istreambuf_iterator<char>(log_stream)),
                          std::istreambuf_iterator<char>());
    ASSERT_TRUE(WIFEXITED(status)) << log;
    EXPECT_EQ(WEXITSTATUS(status), 0) << log;
  }

  void AssertFreshProcessRejectsPendingMutationCheckpoint() const {
    const auto executable = CurrentExecutablePath();
    ASSERT_FALSE(executable.empty());
    std::filesystem::create_directories(workDir_);
    const auto prepare_log_path = workDir_ + "/prepare_pending_wal.log";
    const pid_t prepare_pid = fork();
    ASSERT_GE(prepare_pid, 0);
    if (prepare_pid == 0) {
      const int log_fd =
          open(prepare_log_path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
      if (log_fd < 0 || dup2(log_fd, STDOUT_FILENO) < 0 ||
          dup2(log_fd, STDERR_FILENO) < 0) {
        _exit(126);
      }
      close(log_fd);
      setenv(kReopenDataDirEnv, workDir_.c_str(), 1);
      setenv(kPendingMutationModeEnv, "prepare", 1);
      execl(executable.c_str(), executable.c_str(),
            "--gtest_filter="
            "E2EIndexReopenSubprocess.PreparePendingMutationWal",
            static_cast<char*>(nullptr));
      _exit(127);
    }
    int prepare_status = 0;
    ASSERT_EQ(waitpid(prepare_pid, &prepare_status, 0), prepare_pid);
    ASSERT_TRUE(WIFEXITED(prepare_status));
    ASSERT_EQ(WEXITSTATUS(prepare_status), 0);

    const auto log_path = workDir_ + "/pending_mutations.log";

    const pid_t pid = fork();
    ASSERT_GE(pid, 0);
    if (pid == 0) {
      const int log_fd =
          open(log_path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
      if (log_fd < 0 || dup2(log_fd, STDOUT_FILENO) < 0 ||
          dup2(log_fd, STDERR_FILENO) < 0) {
        _exit(126);
      }
      close(log_fd);
      setenv(kReopenDataDirEnv, workDir_.c_str(), 1);
      setenv(kPendingMutationModeEnv, "verify", 1);
      execl(executable.c_str(), executable.c_str(),
            "--gtest_filter="
            "E2EIndexReopenSubprocess.RejectsCheckpointWithPendingMutations",
            static_cast<char*>(nullptr));
      _exit(127);
    }

    int status = 0;
    ASSERT_EQ(waitpid(pid, &status, 0), pid);

    std::ifstream log_stream(log_path);
    ASSERT_TRUE(log_stream.is_open());
    const std::string log((std::istreambuf_iterator<char>(log_stream)),
                          std::istreambuf_iterator<char>());
    ASSERT_TRUE(WIFEXITED(status)) << log;
    EXPECT_EQ(WEXITSTATUS(status), 0) << log;
    EXPECT_NE(log.find("Checkpoint on close failed"), std::string::npos) << log;
    EXPECT_NE(log.find("mutations for pending"), std::string::npos) << log;
  }

  static void AssertNoIndexes(const QueryResult& result) {
    const auto& response = result.response();
    EXPECT_EQ(response.row_count(), 0);
    ASSERT_EQ(response.arrays_size(), 6);
    for (const auto& array : response.arrays()) {
      ASSERT_TRUE(array.has_string_array());
      EXPECT_EQ(array.string_array().values_size(), 0);
    }
  }

  static void AssertExpectedIndex(const QueryResult& result) {
    const auto& response = result.response();
    ASSERT_EQ(response.row_count(), 1);
    ASSERT_EQ(response.arrays_size(), 6);
    EXPECT_EQ(response.arrays(0).string_array().values(0),
              "entity_embedding_hnsw");
    EXPECT_EQ(response.arrays(1).string_array().values(0), "hnsw");
    EXPECT_EQ(response.arrays(2).string_array().values(0), "Entity");
    EXPECT_EQ(response.arrays(3).string_array().values(0), "embedding");
    EXPECT_EQ(
        response.arrays(4).string_array().values(0),
        R"({"description":"escapedtvalue","ef_construction":"200","m":"16","metric":"ip"})");
    EXPECT_EQ(response.arrays(5).string_array().values(0), "active");
  }

  std::string workDir_;
  NeugDBConfig config_;
};

TEST_F(E2EIndexTest, CreateShowDropAndPersistDropAcrossReopen) {
  {
    NeugDB db;
    ASSERT_TRUE(db.Open(config_));
    auto connection = db.Connect();
    ASSERT_NE(connection, nullptr);
    LoadVecIndex(*connection);

    auto createTable = connection->Query(
        "CREATE NODE TABLE Entity (id INT64, embedding FLOAT[2], "
        "PRIMARY KEY(id));");
    ASSERT_TRUE(createTable) << createTable.error().ToString();

    auto createEntity =
        connection->Query("CREATE (:Entity {id: 1, embedding: [1.0, 2.0]});");
    ASSERT_TRUE(createEntity) << createEntity.error().ToString();

    auto createIndex = connection->Query(R"(
      CREATE INDEX entity_embedding_hnsw
      ON Entity USING HNSW (embedding)
      WITH (
        metric = 'ip',
        m = 16,
        ef_construction = 200,
        description = 'escaped\tvalue'
      );
    )");
    ASSERT_TRUE(createIndex) << createIndex.error().ToString();

    auto showIndexes = connection->Query("CALL SHOW_INDEXES() RETURN *;");
    ASSERT_TRUE(showIndexes) << showIndexes.error().ToString();
    AssertExpectedIndex(showIndexes.value());

    auto showSelectedColumns =
        connection->Query("CALL SHOW_INDEXES() RETURN name, label;");
    ASSERT_TRUE(showSelectedColumns) << showSelectedColumns.error().ToString();
    const auto& selectedResponse = showSelectedColumns.value().response();
    ASSERT_EQ(selectedResponse.row_count(), 1);
    ASSERT_EQ(selectedResponse.arrays_size(), 2);
    EXPECT_EQ(selectedResponse.arrays(0).string_array().values(0),
              "entity_embedding_hnsw");
    EXPECT_EQ(selectedResponse.arrays(1).string_array().values(0), "Entity");

    auto showUnknownColumn =
        connection->Query("CALL SHOW_INDEXES() RETURN unknown_column;");
    EXPECT_FALSE(showUnknownColumn);

    auto createIfNotExists = connection->Query(
        "CREATE INDEX entity_embedding_hnsw IF NOT EXISTS "
        "ON Entity USING HNSW (embedding);");
    ASSERT_TRUE(createIfNotExists) << createIfNotExists.error().ToString();

    auto createExisting = connection->Query(
        "CREATE INDEX entity_embedding_hnsw "
        "ON Entity USING HNSW (embedding);");
    ASSERT_FALSE(createExisting);
    EXPECT_EQ(createExisting.error().error_code(),
              StatusCode::ERR_ILLEGAL_OPERATION);

    auto dropIndex = connection->Query("DROP INDEX entity_embedding_hnsw;");
    ASSERT_TRUE(dropIndex) << dropIndex.error().ToString();

    showIndexes = connection->Query("CALL SHOW_INDEXES() RETURN *;");
    ASSERT_TRUE(showIndexes) << showIndexes.error().ToString();
    AssertNoIndexes(showIndexes.value());

    auto dropIfExists =
        connection->Query("DROP INDEX entity_embedding_hnsw IF EXISTS;");
    ASSERT_TRUE(dropIfExists) << dropIfExists.error().ToString();

    auto dropMissing = connection->Query("DROP INDEX entity_embedding_hnsw;");
    ASSERT_FALSE(dropMissing);
    EXPECT_EQ(dropMissing.error().error_code(), StatusCode::ERR_NOT_FOUND);

    auto checkpoint = connection->Query("CHECKPOINT;");
    ASSERT_TRUE(checkpoint) << checkpoint.error().ToString();
    connection->Close();
    db.Close();
  }

  {
    NeugDB reopened;
    ASSERT_TRUE(reopened.Open(config_));
    auto connection = reopened.Connect();
    ASSERT_NE(connection, nullptr);

    auto showIndexes = connection->Query("CALL SHOW_INDEXES() RETURN *;");
    ASSERT_TRUE(showIndexes) << showIndexes.error().ToString();
    AssertNoIndexes(showIndexes.value());

    connection->Close();
    reopened.Close();
  }
}

TEST_F(E2EIndexTest, PersistCreatedIndexAcrossReopen) {
  {
    NeugDB db;
    ASSERT_TRUE(db.Open(config_));
    auto connection = db.Connect();
    ASSERT_NE(connection, nullptr);
    LoadVecIndex(*connection);

    auto createTable = connection->Query(
        "CREATE NODE TABLE Entity (id INT64, embedding FLOAT[2], "
        "PRIMARY KEY(id));");
    ASSERT_TRUE(createTable) << createTable.error().ToString();

    auto createEntity =
        connection->Query("CREATE (:Entity {id: 1, embedding: [1.0, 2.0]});");
    ASSERT_TRUE(createEntity) << createEntity.error().ToString();

    auto createIndex = connection->Query(R"(
      CREATE INDEX entity_embedding_hnsw
      ON Entity USING HNSW (embedding)
      WITH (
        metric = 'ip',
        m = 16,
        ef_construction = 200,
        description = 'escaped\tvalue'
      );
    )");
    ASSERT_TRUE(createIndex) << createIndex.error().ToString();

    auto checkpoint = connection->Query("CHECKPOINT;");
    ASSERT_TRUE(checkpoint) << checkpoint.error().ToString();
    connection->Close();
    db.Close();
  }

  AssertFreshProcessActivatesPendingIndex();
}

TEST_F(E2EIndexTest, DropPendingIndexWithoutLoadingExtension) {
  {
    NeugDB db;
    ASSERT_TRUE(db.Open(config_));
    auto connection = db.Connect();
    ASSERT_NE(connection, nullptr);
    LoadVecIndex(*connection);

    auto create_table = connection->Query(
        "CREATE NODE TABLE Entity (id INT64, embedding FLOAT[2], "
        "PRIMARY KEY(id));");
    ASSERT_TRUE(create_table) << create_table.error().ToString();
    auto create_entity =
        connection->Query("CREATE (:Entity {id: 1, embedding: [1.0, 2.0]});");
    ASSERT_TRUE(create_entity) << create_entity.error().ToString();
    auto create_index = connection->Query(
        "CREATE INDEX entity_embedding_hnsw "
        "ON Entity USING HNSW (embedding);");
    ASSERT_TRUE(create_index) << create_index.error().ToString();
    auto checkpoint = connection->Query("CHECKPOINT;");
    ASSERT_TRUE(checkpoint) << checkpoint.error().ToString();
    connection->Close();
    db.Close();
  }

  AssertFreshProcessDropsPendingIndex();
}

TEST_F(E2EIndexTest, RejectCheckpointAfterWalReplayForPendingIndex) {
  AssertFreshProcessRejectsPendingMutationCheckpoint();
}

}  // namespace
}  // namespace neug
