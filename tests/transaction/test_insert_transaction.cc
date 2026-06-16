
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

#include "neug/execution/common/types/value.h"
#include "neug/neug.h"
#include "neug/server/neug_db_service.h"
#include "neug/storages/csr/csr_view_utils.h"
#include "neug/storages/graph/graph_interface.h"
#include "neug/transaction/insert_transaction.h"
#include "neug/transaction/wal/local_wal_parser.h"
#include "neug/transaction/wal/wal.h"
#include "neug/utils/exception/exception.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#include <filesystem>
#include <string>
#include <vector>

#include "glog/logging.h"
#include "gtest/gtest.h"

class InsertTransactionTest : public ::testing::Test {
 protected:
  std::string db_dir;
  neug::MemoryLevel memory_level;

  void SetUp() override {
    db_dir = "/tmp/test_insert_transaction_db";
    if (std::filesystem::exists(db_dir)) {
      std::filesystem::remove_all(db_dir);
    }
    std::filesystem::create_directories(db_dir);

    neug::NeugDB db;
    neug::NeugDBConfig config(db_dir);
    config.memory_level = neug::MemoryLevel::kInMemory;
    config.checkpoint_on_close = true;
    db.Open(db_dir);
    auto conn = db.Connect();
    EXPECT_TRUE(
        conn->Query("CREATE NODE TABLE person(id INT64, name STRING, "
                    "age INT64, PRIMARY KEY(id));"));
    EXPECT_TRUE(
        conn->Query("CREATE NODE TABLE software(id INT64, name STRING, "
                    "lang STRING, PRIMARY KEY(id));"));
    EXPECT_TRUE(conn->Query(
        "CREATE REL TABLE created(FROM person TO software, weight DOUBLE, "
        "since INT64);"));
    EXPECT_TRUE(
        conn->Query("CREATE REL TABLE knows(FROM person TO person, "
                    "closeness DOUBLE);"));
    EXPECT_TRUE(
        conn->Query("Create ( n:person {id: 1, name: 'Alice', age: 30});"));
    EXPECT_TRUE(conn->Query(
        "Create ( n:software {id: 1, name: 'GraphDB', lang: 'C++'});"));
    EXPECT_TRUE(
        conn->Query("Create ( n:person {id: 2, name: 'Bob', age: 25});"));
    EXPECT_TRUE(conn->Query(
        "Create ( n:software {id: 2, name: 'FastGraph', lang: 'Rust'});"));
    EXPECT_TRUE(
        conn->Query("MATCH (a:person {id: 1}), (b:software {id: 1}) "
                    "CREATE (a)-[:created {weight: 0.8, since: 2021}]->(b);"));
    EXPECT_TRUE(
        conn->Query("MATCH (a:person {id: 2}), (b:software {id: 2}) "
                    "CREATE (a)-[:created {weight: 0.7, since: 2020}]->(b);"));
    EXPECT_TRUE(
        conn->Query("MATCH (a:person {id: 1}), (b:person {id: 2}) "
                    "CREATE (a)-[:knows {closeness: 0.9}]->(b);"));
    db.Close();
  }

  void TearDown() override {
    if (std::filesystem::exists(db_dir)) {
      std::filesystem::remove_all(db_dir);
    }
  }

  size_t count_vertices(const neug::StorageReadInterface& gi,
                        neug::label_t label) {
    size_t vertex_count = 0;
    auto v_set = gi.GetVertexSet(label);
    v_set.foreach_vertex([&](neug::vid_t vid) { vertex_count++; });
    return vertex_count;
  }
};

TEST_F(InsertTransactionTest, InsertTransactionBasic) {
  neug::NeugDB db;
  neug::NeugDBConfig config(db_dir);
  config.memory_level = neug::MemoryLevel::kInMemory;
  db.Open(config);
  auto svc = std::make_shared<neug::NeugDBService>(db);
  {
    auto sess = svc->AcquireSession();
    auto txn = sess->GetInsertTransaction();
    EXPECT_EQ(txn.timestamp(), 1);
    EXPECT_TRUE(txn.schema().is_vertex_label_valid("person"));
  }
}

TEST_F(InsertTransactionTest, AddVertex) {
  neug::NeugDB db;
  neug::NeugDBConfig config(db_dir);
  config.memory_level = neug::MemoryLevel::kInMemory;
  db.Open(config);
  auto svc = std::make_shared<neug::NeugDBService>(db);
  {
    auto sess = svc->AcquireSession();
    auto txn = sess->GetInsertTransaction();
    neug::StorageTPInsertInterface interface(txn);
    auto person_label = interface.schema().get_vertex_label_id("person");
    neug::vid_t vid;
    EXPECT_TRUE(
        interface.AddVertex(person_label, neug::execution::Value::INT64(3),
                            {neug::execution::Value::STRING(std::string("Eve")),
                             neug::execution::Value::INT64(28)},
                            vid));
    EXPECT_TRUE(txn.Commit());
  }
  {
    auto sess = svc->AcquireSession();
    auto txn = sess->GetReadTransaction();
    neug::StorageReadInterface gi(txn.graph(), txn.timestamp());
    auto person_label = gi.schema().get_vertex_label_id("person");
    EXPECT_EQ(count_vertices(gi, person_label), 3);
  }
  db.Close();
}

TEST_F(InsertTransactionTest, AddEdge) {
  neug::NeugDB db;
  neug::NeugDBConfig config(db_dir);
  config.memory_level = neug::MemoryLevel::kInMemory;
  db.Open(config);
  auto svc = std::make_shared<neug::NeugDBService>(db);
  {
    auto sess = svc->AcquireSession();
    auto txn = sess->GetInsertTransaction();
    neug::StorageTPInsertInterface interface(txn);
    auto person_label = txn.schema().get_vertex_label_id("person");
    auto software_label = txn.schema().get_vertex_label_id("software");
    auto created_label = txn.schema().get_edge_label_id("created");
    neug::vid_t vid;
    EXPECT_TRUE(txn.GetVertexIndex(person_label,
                                   neug::execution::Value::INT64(1), vid));
    neug::vid_t vid2;
    EXPECT_TRUE(txn.GetVertexIndex(software_label,
                                   neug::execution::Value::INT64(2), vid2));
    const void* edge_prop = nullptr;
    EXPECT_TRUE(interface.AddEdge(person_label, vid, software_label, vid2,
                                  created_label,
                                  {neug::execution::Value::DOUBLE(0.9),
                                   neug::execution::Value::INT64(2022)},
                                  edge_prop));
    EXPECT_TRUE(txn.Commit());
  }
  {
    auto sess = svc->AcquireSession();
    auto txn = sess->GetReadTransaction();
    neug::StorageReadInterface gi(txn.graph(), txn.timestamp());
    auto person_label = gi.schema().get_vertex_label_id("person");
    auto software_label = gi.schema().get_vertex_label_id("software");
    auto created_label = gi.schema().get_edge_label_id("created");
    auto view = gi.GetGenericOutgoingGraphView(person_label, software_label,
                                               created_label);

    size_t edge_count = 0;
    auto vertex_set = gi.GetVertexSet(person_label);
    for (neug::vid_t vid : vertex_set) {
      auto oid = gi.GetVertexId(person_label, vid);
      if (oid.GetValue<int64_t>() == 1) {
        auto edge_iter = view.get_edges(vid);
        for (auto it = edge_iter.begin(); it != edge_iter.end(); ++it) {
          edge_count++;
        }
      }
    }
    EXPECT_EQ(edge_count, 2);
  }
  db.Close();
}

TEST_F(InsertTransactionTest, TestUnsupportedInterface) {
  neug::NeugDB db;
  neug::NeugDBConfig config(db_dir);
  config.memory_level = neug::MemoryLevel::kInMemory;
  db.Open(config);
  auto svc = std::make_shared<neug::NeugDBService>(db);

  {
    auto sess = svc->AcquireSession();
    auto txn = sess->GetInsertTransaction();
    neug::StorageTPInsertInterface interface(txn);
    std::vector<neug::vid_t> vids;
    std::vector<std::tuple<neug::vid_t, neug::vid_t>> edges;
    std::vector<std::pair<neug::vid_t, int32_t>> oe_edges, ie_edges;
    EXPECT_EQ(interface.BatchAddVertices(0, nullptr).error_code(),
              neug::StatusCode::ERR_NOT_SUPPORTED);
    EXPECT_EQ(interface.BatchAddEdges(0, 0, 0, nullptr).error_code(),
              neug::StatusCode::ERR_NOT_SUPPORTED);
  }
}

class LocalWalParserTest : public ::testing::Test {
 protected:
  std::string wal_dir_;

  void SetUp() override {
    auto* info = ::testing::UnitTest::GetInstance()->current_test_info();
    auto dir_name = std::string("neug_wal_parser_test_") +
                    std::to_string(::getpid()) + "_" + info->test_suite_name() +
                    "_" + info->name();
    wal_dir_ = (std::filesystem::temp_directory_path() / dir_name).string();
    if (std::filesystem::exists(wal_dir_)) {
      std::filesystem::remove_all(wal_dir_);
    }
    std::filesystem::create_directories(wal_dir_);
  }

  void TearDown() override {
    if (std::filesystem::exists(wal_dir_)) {
      std::filesystem::remove_all(wal_dir_);
    }
  }

  // Write a single WAL entry (header + payload) into a buffer.
  void AppendWalEntry(std::vector<char>& buf, uint32_t ts, uint8_t type,
                      const std::string& payload) {
    neug::WalHeader header;
    header.timestamp = ts;
    header.type = type;
    header.length = static_cast<int32_t>(payload.size());
    const char* hdr = reinterpret_cast<const char*>(&header);
    buf.insert(buf.end(), hdr, hdr + sizeof(neug::WalHeader));
    buf.insert(buf.end(), payload.begin(), payload.end());
  }

  // Append a terminator entry (timestamp=0) to mark end of WAL stream.
  void AppendWalTerminator(std::vector<char>& buf) {
    neug::WalHeader terminator;
    terminator.timestamp = 0;
    terminator.type = 0;
    terminator.length = 0;
    const char* hdr = reinterpret_cast<const char*>(&terminator);
    buf.insert(buf.end(), hdr, hdr + sizeof(neug::WalHeader));
  }

  // Write buffer contents to a .wal file in the WAL directory.
  void WriteWalFile(const std::string& filename, const std::vector<char>& buf) {
    auto path = wal_dir_ + "/" + filename;
    int fd = ::open(path.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0644);
    ASSERT_NE(fd, -1);
    ASSERT_EQ(static_cast<size_t>(write(fd, buf.data(), buf.size())),
              buf.size());
    ::close(fd);
  }
};

// Test: LocalWalParser can correctly parse a valid WAL file.
TEST_F(LocalWalParserTest, OpenAndParseValidWalFile) {
  std::vector<char> buf;
  std::string payload = "insert_vertex_data";
  AppendWalEntry(buf, /*ts=*/1, /*type=*/0, payload);  // insert WAL
  AppendWalTerminator(buf);
  WriteWalFile("thread_0_0.wal", buf);

  neug::LocalWalParser parser(wal_dir_);
  EXPECT_EQ(parser.last_ts(), 1);
  const auto& unit = parser.get_insert_wal(1);
  EXPECT_EQ(unit.size, payload.size());
  // The ptr should point to the payload data within the mmap region.
  EXPECT_NE(unit.ptr, nullptr);
  EXPECT_EQ(std::string(unit.ptr, unit.size), payload);
}

// Test: LocalWalParser throws IOException when ::open() on a WAL file fails
// (e.g. permission denied). This covers the fd == -1 check.
TEST_F(LocalWalParserTest, OpenUnreadableWalFileThrowsIOException) {
  auto file_path = wal_dir_ + "/unreadable.wal";
  int fd = ::open(file_path.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0644);
  ASSERT_NE(fd, -1);
  const char data[] = "some wal data";
  ASSERT_EQ(static_cast<size_t>(write(fd, data, sizeof(data))), sizeof(data));
  ::close(fd);

  ASSERT_EQ(chmod(file_path.c_str(), 0000), 0);

  try {
    neug::LocalWalParser parser(wal_dir_);
    FAIL() << "Expected IOException but none was thrown";
  } catch (const neug::exception::IOException& e) {
    EXPECT_NE(std::string(e.what()).find("Failed to open wal file"),
              std::string::npos);
  }

  chmod(file_path.c_str(), 0644);
}

// Test: LocalWalParser throws IOException when mmap() fails.
TEST_F(LocalWalParserTest, MmapFailureThrowsIOException) {
  auto file_path = wal_dir_ + "/huge.wal";
  int fd = ::open(file_path.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0644);
  ASSERT_NE(fd, -1);

  off_t huge_size =
      static_cast<off_t>(128ULL * 1024ULL * 1024ULL * 1024ULL * 1024ULL);
  ASSERT_EQ(ftruncate(fd, huge_size), 0);
  ::close(fd);

  try {
    neug::LocalWalParser parser(wal_dir_);
    FAIL() << "Expected IOException but none was thrown";
  } catch (const neug::exception::IOException& e) {
    EXPECT_NE(std::string(e.what()).find("Failed to mmap wal file"),
              std::string::npos);
  }

  fd = ::open(file_path.c_str(), O_RDWR);
  if (fd != -1) {
    ftruncate(fd, 0);
    ::close(fd);
  }
}

// Test: Opening an empty WAL directory does not throw and last_ts is 0.
TEST_F(LocalWalParserTest, OpenEmptyWalDirNoThrow) {
  neug::LocalWalParser parser(wal_dir_);
  EXPECT_EQ(parser.last_ts(), 0);
}
