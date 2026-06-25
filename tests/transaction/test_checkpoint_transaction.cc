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
#include <algorithm>
#include <atomic>
#include <chrono>
#include <exception>
#include <filesystem>
#include <fstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include <rapidjson/document.h>
#include <rapidjson/istreamwrapper.h>
#include <rapidjson/ostreamwrapper.h>
#include <rapidjson/writer.h>

#include "column_assertions.h"
#include "neug/main/connection.h"
#include "neug/main/neug_db.h"
#include "neug/main/query_request.h"
#include "neug/server/neug_db_service.h"
#include "neug/transaction/checkpoint_transaction.h"
#include "neug/transaction/insert_transaction.h"
#include "neug/transaction/read_transaction.h"
#include "neug/transaction/wal/wal_manager.h"

namespace {

std::vector<std::filesystem::path> list_checkpoint_dirs(
    const std::string& db_dir) {
  std::vector<std::filesystem::path> dirs;
  for (const auto& entry : std::filesystem::directory_iterator(db_dir)) {
    auto name = entry.path().filename().string();
    if (entry.is_directory() && name.rfind("checkpoint-", 0) == 0 &&
        !name.ends_with(".next")) {
      dirs.push_back(entry.path());
    }
  }
  std::sort(dirs.begin(), dirs.end());
  return dirs;
}

std::vector<std::filesystem::path> list_staging_checkpoint_dirs(
    const std::string& db_dir) {
  std::vector<std::filesystem::path> dirs;
  for (const auto& entry : std::filesystem::directory_iterator(db_dir)) {
    auto name = entry.path().filename().string();
    if (entry.is_directory() && name.rfind("checkpoint-", 0) == 0 &&
        name.ends_with(".next")) {
      dirs.push_back(entry.path());
    }
  }
  std::sort(dirs.begin(), dirs.end());
  return dirs;
}

bool has_staging_checkpoint_dirs(const std::string& db_dir) {
  return !list_staging_checkpoint_dirs(db_dir).empty();
}

size_t count_regular_files(const std::filesystem::path& dir) {
  if (!std::filesystem::exists(dir)) {
    return 0;
  }
  size_t count = 0;
  for (const auto& entry : std::filesystem::directory_iterator(dir)) {
    if (entry.is_regular_file()) {
      ++count;
    }
  }
  return count;
}

std::string read_checkpoint_pointer(const std::string& db_path) {
  std::ifstream input(std::filesystem::path(db_path) / "CHECKPOINT");
  std::string value;
  std::getline(input, value);
  return value;
}

void write_file(const std::filesystem::path& path, std::string_view payload) {
  std::ofstream output(path, std::ios::binary | std::ios::trunc);
  ASSERT_TRUE(output.is_open()) << path;
  output.write(payload.data(), static_cast<std::streamsize>(payload.size()));
  ASSERT_TRUE(output.good()) << path;
}

void wait_until_true(const std::atomic<bool>& flag) {
  for (int i = 0; i < 1000; ++i) {
    if (flag.load(std::memory_order_acquire)) {
      return;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  FAIL() << "Timed out waiting for worker to reach expected state";
}

bool rewrite_module_paths_to_snapshot_dir(
    const std::filesystem::path& meta_path) {
  std::ifstream input(meta_path);
  EXPECT_TRUE(input.is_open()) << meta_path;
  rapidjson::IStreamWrapper input_stream(input);
  rapidjson::Document doc;
  doc.ParseStream(input_stream);
  EXPECT_FALSE(doc.HasParseError()) << meta_path;
  EXPECT_TRUE(doc.IsObject()) << meta_path;
  EXPECT_TRUE(doc.HasMember("modules")) << meta_path;
  EXPECT_TRUE(doc["modules"].IsObject()) << meta_path;
  if (doc.HasParseError() || !doc.IsObject() || !doc.HasMember("modules") ||
      !doc["modules"].IsObject()) {
    return false;
  }

  bool changed = false;
  auto& allocator = doc.GetAllocator();
  for (auto& module : doc["modules"].GetObject()) {
    if (!module.value.IsObject() || !module.value.HasMember("paths") ||
        !module.value["paths"].IsObject()) {
      continue;
    }
    for (auto& path : module.value["paths"].GetObject()) {
      if (!path.value.IsString()) {
        continue;
      }
      path.value.SetString("snapshot", allocator);
      changed = true;
    }
  }
  if (!changed) {
    return false;
  }

  std::ofstream output(meta_path, std::ios::binary | std::ios::trunc);
  EXPECT_TRUE(output.is_open()) << meta_path;
  rapidjson::OStreamWrapper output_stream(output);
  rapidjson::Writer<rapidjson::OStreamWrapper> writer(output_stream);
  doc.Accept(writer);
  return output.good();
}

}  // namespace

class CheckpointTransactionTest : public ::testing::Test {
 protected:
  void SetUp() override {
    const auto* test_info =
        ::testing::UnitTest::GetInstance()->current_test_info();
    db_dir_ =
        (std::filesystem::temp_directory_path() /
         ("test_checkpoint_transaction_" +
          std::string(test_info->test_suite_name()) + "_" + test_info->name()))
            .string();
    std::error_code ec;
    std::filesystem::remove_all(db_dir_, ec);
    std::filesystem::create_directories(db_dir_);
  }

  void TearDown() override {
    std::error_code ec;
    std::filesystem::remove_all(db_dir_, ec);
  }

  neug::NeugDBConfig MakeConfig(
      neug::MemoryLevel memory_level = neug::MemoryLevel::kInMemory) const {
    neug::NeugDBConfig config(db_dir_);
    config.max_thread_num = 2;
    config.memory_level = memory_level;
    config.checkpoint_on_close = false;
    config.enable_auto_compaction = false;
    return config;
  }

  void OpenSeededDB(neug::NeugDB& db, neug::MemoryLevel memory_level =
                                          neug::MemoryLevel::kInMemory) const {
    ASSERT_TRUE(db.Open(MakeConfig(memory_level)));
    auto conn = db.Connect();
    auto res =
        conn->Query("CREATE NODE TABLE Item(id INT64, PRIMARY KEY(id));");
    ASSERT_TRUE(res) << res.error().ToString();
    res = conn->Query("CREATE (:Item {id: 1});");
    ASSERT_TRUE(res) << res.error().ToString();
    res = conn->Query("CHECKPOINT;");
    ASSERT_TRUE(res) << res.error().ToString();
    conn->Close();
  }

  void AssertSingleCurrentCheckpoint() const {
    auto dirs = list_checkpoint_dirs(db_dir_);
    ASSERT_EQ(dirs.size(), 1u);
    auto pointer = read_checkpoint_pointer(db_dir_);
    ASSERT_FALSE(pointer.empty());
    auto pointer_path = std::filesystem::path(db_dir_) / pointer;
    EXPECT_TRUE(std::filesystem::exists(pointer_path));
    EXPECT_EQ(pointer_path.filename(), dirs[0].filename());
    EXPECT_FALSE(has_staging_checkpoint_dirs(db_dir_));
    EXPECT_FALSE(std::filesystem::exists(std::filesystem::path(db_dir_) /
                                         "CHECKPOINT.tmp"));
  }

  std::filesystem::path CurrentCheckpointDir() const {
    auto dirs = list_checkpoint_dirs(db_dir_);
    EXPECT_EQ(dirs.size(), 1u);
    if (dirs.empty()) {
      return {};
    }
    return dirs.front();
  }

  void ExpectSessionQuery(neug::NeugDBSession& session,
                          const std::string& query,
                          const std::string& access_mode) const {
    neug::execution::ParamsMap params;
    auto req =
        neug::RequestSerializer::SerializeRequest(query, access_mode, params);
    auto res = session.Eval(req);
    ASSERT_TRUE(res) << res.error().ToString();
  }

  void ExpectSessionItemCount(neug::NeugDBSession& session,
                              int64_t expected) const {
    neug::execution::ParamsMap params;
    auto req = neug::RequestSerializer::SerializeRequest(
        "MATCH (v:Item) RETURN count(v);", "read", params);
    auto res = session.Eval(req);
    ASSERT_TRUE(res) << res.error().ToString();

    neug::QueryResponse response;
    ASSERT_TRUE(response.ParseFromString(res.value()));
    ASSERT_EQ(response.row_count(), 1);
    neug::test::AssertInt64Column(response, 0, {expected});
  }

  void ExpectServiceUsableAfterFailedCheckpoint(neug::NeugDBService& svc,
                                                int64_t new_item_id) const {
    auto sess = svc.AcquireSession();
    ExpectSessionItemCount(*sess.get(), 1);
    ExpectSessionQuery(
        *sess.get(),
        "CREATE (:Item {id: " + std::to_string(new_item_id) + "});", "insert");
    ExpectSessionItemCount(*sess.get(), 2);
  }

  std::string db_dir_;
};

TEST_F(CheckpointTransactionTest,
       CommitPublishesCheckpointRotatesWalAndResetsTimestamp) {
  neug::NeugDB db;
  OpenSeededDB(db);
  AssertSingleCurrentCheckpoint();
  auto old_checkpoint_dir = list_checkpoint_dirs(db_dir_).front();

  {
    neug::NeugDBService svc(db);
    {
      auto sess = svc.AcquireSession();
      neug::execution::ParamsMap params;
      auto req =
          neug::RequestSerializer::SerializeRequest("CHECKPOINT;", "", params);
      auto res = sess->Eval(req);
      ASSERT_TRUE(res) << res.error().ToString();
    }

    AssertSingleCurrentCheckpoint();
    EXPECT_FALSE(std::filesystem::exists(old_checkpoint_dir));
    auto new_checkpoint_dir = CurrentCheckpointDir();
    auto new_wal_dir = new_checkpoint_dir / "wal";
    ASSERT_TRUE(std::filesystem::exists(new_wal_dir));
    EXPECT_EQ(count_regular_files(new_wal_dir), 0u);

    {
      auto sess = svc.AcquireSession();
      auto insert_txn = sess->GetInsertTransaction();
      EXPECT_EQ(insert_txn.timestamp(), 1u);
      EXPECT_TRUE(insert_txn.Commit());
    }
    EXPECT_EQ(count_regular_files(new_wal_dir), 0u);

    {
      auto sess = svc.AcquireSession();
      ExpectSessionQuery(*sess.get(), "CREATE (:Item {id: 2});", "insert");
    }
    EXPECT_GT(count_regular_files(new_wal_dir), 0u);
    {
      auto sess = svc.AcquireSession();
      ExpectSessionItemCount(*sess.get(), 2);
    }
    EXPECT_FALSE(std::filesystem::exists(old_checkpoint_dir));
  }

  db.Close();

  neug::NeugDB reopened;
  ASSERT_TRUE(reopened.Open(MakeConfig()));
  {
    neug::NeugDBService reopened_svc(reopened);
    auto sess = reopened_svc.AcquireSession();
    ExpectSessionItemCount(*sess.get(), 2);
  }
  reopened.Close();
}

TEST_F(CheckpointTransactionTest,
       PendingWalRotationDoesNotSwitchActiveWalBeforeInstall) {
  auto old_wal_dir = std::filesystem::path(db_dir_) / "old-wal";
  auto staging_wal_dir =
      std::filesystem::path(db_dir_) / "checkpoint-1.next" / "wal";
  neug::WalManager wal_manager(old_wal_dir.string(), 1);
  auto pending = wal_manager.PrepareRotation(staging_wal_dir.string());

  ASSERT_TRUE(pending.valid());
  ASSERT_TRUE(std::filesystem::exists(staging_wal_dir));
  EXPECT_EQ(count_regular_files(old_wal_dir), 0u);
  EXPECT_EQ(count_regular_files(staging_wal_dir), 0u);

  const std::string payload = "wal-before-publish";
  ASSERT_TRUE(wal_manager.Writer(0).append(payload.data(), payload.size()));

  EXPECT_GT(count_regular_files(old_wal_dir), 0u);
  EXPECT_EQ(count_regular_files(staging_wal_dir), 0u);
  wal_manager.AbortPreparedRotation(std::move(pending));
}

TEST_F(CheckpointTransactionTest,
       SyncToFileCheckpointKeepsServiceUsableAfterOldCheckpointGc) {
  neug::NeugDB db;
  OpenSeededDB(db, neug::MemoryLevel::kSyncToFile);
  auto old_checkpoint_dir = CurrentCheckpointDir();

  {
    neug::NeugDBService svc(db);
    {
      auto sess = svc.AcquireSession();
      ExpectSessionQuery(*sess.get(), "CHECKPOINT;", "checkpoint");
    }

    AssertSingleCurrentCheckpoint();
    EXPECT_FALSE(std::filesystem::exists(old_checkpoint_dir));
    {
      auto sess = svc.AcquireSession();
      ExpectSessionQuery(*sess.get(), "CREATE (:Item {id: 2});", "insert");
      ExpectSessionItemCount(*sess.get(), 2);
    }
    {
      auto sess = svc.AcquireSession();
      ExpectSessionQuery(*sess.get(), "CHECKPOINT;", "checkpoint");
    }
    {
      auto sess = svc.AcquireSession();
      ExpectSessionQuery(*sess.get(), "CREATE (:Item {id: 3});", "insert");
      ExpectSessionItemCount(*sess.get(), 3);
    }
  }

  db.Close();

  neug::NeugDB reopened;
  ASSERT_TRUE(reopened.Open(MakeConfig(neug::MemoryLevel::kSyncToFile)));
  {
    neug::NeugDBService reopened_svc(reopened);
    auto sess = reopened_svc.AcquireSession();
    ExpectSessionItemCount(*sess.get(), 3);
  }
  reopened.Close();
}

TEST_F(CheckpointTransactionTest, EvalUsesCheckpointAccessMode) {
  neug::NeugDB db;
  OpenSeededDB(db);

  {
    neug::NeugDBService svc(db);
    auto sess = svc.AcquireSession();
    neug::execution::ParamsMap params;

    auto req =
        neug::RequestSerializer::SerializeRequest("CHECKPOINT;", "", params);
    auto res = sess->Eval(req);
    ASSERT_TRUE(res) << res.error().ToString();
    AssertSingleCurrentCheckpoint();

    req = neug::RequestSerializer::SerializeRequest("CHECKPOINT;", "checkpoint",
                                                    params);
    res = sess->Eval(req);
    ASSERT_TRUE(res) << res.error().ToString();
    AssertSingleCurrentCheckpoint();

    req = neug::RequestSerializer::SerializeRequest("CHECKPOINT;", "update",
                                                    params);
    res = sess->Eval(req);
    ASSERT_FALSE(res);
    EXPECT_NE(res.error().error_message().find("checkpoint access mode"),
              std::string::npos);
  }

  db.Close();
}

TEST_F(CheckpointTransactionTest, DumpFailureRollsBackStagingAndReleasesLock) {
  neug::NeugDB db;
  OpenSeededDB(db);
  const auto old_pointer = read_checkpoint_pointer(db_dir_);
  neug::NeugDBService svc(db);

  auto holder = svc.AcquireSession();
  auto checkpoint_txn = holder->GetCheckpointTransaction();
  auto staging_dirs = list_staging_checkpoint_dirs(db_dir_);
  ASSERT_EQ(staging_dirs.size(), 1u);
  auto staging_snapshot_dir = staging_dirs[0] / "snapshot";
  std::filesystem::remove_all(staging_snapshot_dir);
  write_file(staging_snapshot_dir, "not-a-directory");

  bool dump_failed = false;
  try {
    auto status =
        checkpoint_txn.checkpoint_session().Dump(checkpoint_txn.graph());
    dump_failed = !status.ok();
  } catch (...) { dump_failed = true; }
  EXPECT_TRUE(dump_failed);
  checkpoint_txn.Abort();

  EXPECT_EQ(read_checkpoint_pointer(db_dir_), old_pointer);
  AssertSingleCurrentCheckpoint();
  ExpectServiceUsableAfterFailedCheckpoint(svc, 2);
  db.Close();
}

TEST_F(CheckpointTransactionTest,
       PublishOpenFailureKeepsOldRootAndCleansStaging) {
  neug::NeugDB db;
  OpenSeededDB(db);
  const auto old_pointer = read_checkpoint_pointer(db_dir_);
  neug::NeugDBService svc(db);

  auto holder = svc.AcquireSession();
  auto checkpoint_txn = holder->GetCheckpointTransaction();
  auto status =
      checkpoint_txn.checkpoint_session().Dump(checkpoint_txn.graph());
  ASSERT_TRUE(status.ok()) << status.ToString();
  auto staging_dirs = list_staging_checkpoint_dirs(db_dir_);
  ASSERT_EQ(staging_dirs.size(), 1u);
  write_file(staging_dirs[0] / "meta", "{ broken checkpoint meta");

  EXPECT_FALSE(checkpoint_txn.Commit());
  EXPECT_EQ(read_checkpoint_pointer(db_dir_), old_pointer);
  AssertSingleCurrentCheckpoint();
  ExpectServiceUsableAfterFailedCheckpoint(svc, 2);
  db.Close();
}

TEST_F(CheckpointTransactionTest,
       ReopenFailureRestoresOldRootAndAllowsRetryCheckpoint) {
  neug::NeugDB db;
  OpenSeededDB(db);
  const auto old_pointer = read_checkpoint_pointer(db_dir_);
  neug::NeugDBService svc(db);

  auto holder = svc.AcquireSession();
  auto checkpoint_txn = holder->GetCheckpointTransaction();
  auto status =
      checkpoint_txn.checkpoint_session().Dump(checkpoint_txn.graph());
  ASSERT_TRUE(status.ok()) << status.ToString();
  auto staging_dirs = list_staging_checkpoint_dirs(db_dir_);
  ASSERT_EQ(staging_dirs.size(), 1u);
  ASSERT_TRUE(rewrite_module_paths_to_snapshot_dir(staging_dirs[0] / "meta"));

  EXPECT_FALSE(checkpoint_txn.Commit());
  EXPECT_EQ(read_checkpoint_pointer(db_dir_), old_pointer);
  AssertSingleCurrentCheckpoint();
  ExpectServiceUsableAfterFailedCheckpoint(svc, 2);

  {
    auto sess = svc.AcquireSession();
    ExpectSessionQuery(*sess.get(), "CHECKPOINT;", "checkpoint");
  }
  AssertSingleCurrentCheckpoint();
  db.Close();
}

TEST_F(CheckpointTransactionTest, HeldCheckpointTransactionBlocksNewRead) {
  neug::NeugDB db;
  OpenSeededDB(db);
  neug::NeugDBService svc(db);

  auto holder = svc.AcquireSession();
  auto checkpoint_txn = holder->GetCheckpointTransaction();

  std::atomic<bool> worker_ready{false};
  std::atomic<bool> read_acquired{false};
  std::exception_ptr worker_error;
  std::thread worker([&]() {
    try {
      auto sess = svc.AcquireSession();
      worker_ready.store(true, std::memory_order_release);
      auto read_txn = sess->GetReadTransaction();
      read_acquired.store(true, std::memory_order_release);
      if (!read_txn.Commit()) {
        throw std::runtime_error("read transaction commit failed");
      }
    } catch (...) { worker_error = std::current_exception(); }
  });

  wait_until_true(worker_ready);
  std::this_thread::sleep_for(std::chrono::milliseconds(200));
  EXPECT_FALSE(read_acquired.load(std::memory_order_acquire));

  checkpoint_txn.Abort();
  worker.join();
  if (worker_error) {
    std::rethrow_exception(worker_error);
  }
  EXPECT_TRUE(read_acquired.load(std::memory_order_acquire));
  db.Close();
}

TEST_F(CheckpointTransactionTest, ActiveReadBlocksCheckpointTransaction) {
  neug::NeugDB db;
  OpenSeededDB(db);
  neug::NeugDBService svc(db);

  auto reader = svc.AcquireSession();
  auto read_txn = reader->GetReadTransaction();

  std::atomic<bool> worker_ready{false};
  std::atomic<bool> checkpoint_done{false};
  std::exception_ptr worker_error;
  std::thread worker([&]() {
    try {
      auto sess = svc.AcquireSession();
      worker_ready.store(true, std::memory_order_release);
      neug::execution::ParamsMap params;
      auto req =
          neug::RequestSerializer::SerializeRequest("CHECKPOINT;", "", params);
      auto res = sess->Eval(req);
      if (!res) {
        throw std::runtime_error(res.error().ToString());
      }
      checkpoint_done.store(true, std::memory_order_release);
    } catch (...) { worker_error = std::current_exception(); }
  });

  wait_until_true(worker_ready);
  std::this_thread::sleep_for(std::chrono::milliseconds(200));
  EXPECT_FALSE(checkpoint_done.load(std::memory_order_acquire));

  EXPECT_TRUE(read_txn.Commit());
  worker.join();
  if (worker_error) {
    std::rethrow_exception(worker_error);
  }
  EXPECT_TRUE(checkpoint_done.load(std::memory_order_acquire));
  AssertSingleCurrentCheckpoint();
  db.Close();
}
