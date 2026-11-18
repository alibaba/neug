
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

#include "neug/common/types/value.h"
#include "neug/neug.h"
#include "neug/server/neug_db_service.h"
#include "neug/storages/csr/csr_view_utils.h"
#include "neug/storages/graph/graph_interface.h"
#include "neug/transaction/mvcc_insert_transaction.h"
#include "neug/transaction/wal/local_wal_parser.h"
#include "neug/transaction/wal/local_wal_writer.h"
#include "neug/transaction/wal/wal.h"
#include "neug/utils/exception/exception.h"
#include "neug/utils/property/array_column.h"
#include "neug/utils/property/vec_column.h"

#include <fcntl.h>
#include <sys/stat.h>
#ifndef _WIN32
#include <sys/resource.h>
#include <unistd.h>
#else
#include <io.h>
#include <process.h>
#define getpid _getpid
#endif
#include <cstring>
#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

#include "glog/logging.h"
#include "gtest/gtest.h"

namespace {

// MSVC's POSIX-compat I/O functions carry an underscore prefix. Object-like
// macros (#define open _open) would also rewrite LocalWalWriter::open() and
// LocalWalWriter::close() member calls, so route raw fd I/O through these
// platform wrappers instead.
#ifdef _WIN32
int OsOpen(const char* path, int flags, int mode) {
  return _open(path, flags, mode);
}

int OsClose(int fd) { return _close(fd); }

int OsWrite(int fd, const void* buf, size_t count) {
  return _write(fd, buf, static_cast<unsigned int>(count));
}
#else
int OsOpen(const char* path, int flags, int mode) {
  return ::open(path, flags, mode);
}

int OsClose(int fd) { return ::close(fd); }

ssize_t OsWrite(int fd, const void* buf, size_t count) {
  return ::write(fd, buf, count);
}
#endif

}  // namespace

class MvccInsertTransactionTest : public ::testing::Test {
 protected:
  std::string db_dir;
  neug::MemoryLevel memory_level;

  void SetUp() override {
    db_dir = "/tmp/test_mvcc_insert_transaction_db";
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

TEST_F(MvccInsertTransactionTest, MvccInsertTransactionBasic) {
  neug::NeugDB db;
  neug::NeugDBConfig config(db_dir);
  config.memory_level = neug::MemoryLevel::kInMemory;
  db.Open(config);
  auto svc = std::make_shared<neug::NeugDBService>(db);
  {
    auto slot = svc->AcquireExecutionSlot();
    auto txn = slot->BeginMvccInsertTransaction();
    EXPECT_EQ(txn.timestamp(), 1);
    EXPECT_TRUE(txn.schema().is_vertex_label_valid("person"));
  }
}

TEST_F(MvccInsertTransactionTest, AddVertex) {
  neug::NeugDB db;
  neug::NeugDBConfig config(db_dir);
  config.memory_level = neug::MemoryLevel::kInMemory;
  db.Open(config);
  auto svc = std::make_shared<neug::NeugDBService>(db);
  {
    auto slot = svc->AcquireExecutionSlot();
    auto txn = slot->BeginMvccInsertTransaction();
    neug::StorageTPInsertInterface interface(txn);
    auto person_label = interface.schema().get_vertex_label_id("person");
    neug::vid_t vid;
    EXPECT_TRUE(interface.AddVertex(
        person_label, neug::Value::INT64(3),
        {neug::Value::STRING(std::string("Eve")), neug::Value::INT64(28)},
        vid));
    EXPECT_TRUE(txn.Commit());
  }
  {
    auto slot = svc->AcquireExecutionSlot();
    auto txn = slot->BeginSnapshotReadTransaction();
    neug::StorageReadInterface gi(txn.view(), txn.timestamp());
    auto person_label = gi.schema().get_vertex_label_id("person");
    EXPECT_EQ(count_vertices(gi, person_label), 3);
  }
  svc.reset();
  db.Close();
}

TEST_F(MvccInsertTransactionTest, AddEdge) {
  neug::NeugDB db;
  neug::NeugDBConfig config(db_dir);
  config.memory_level = neug::MemoryLevel::kInMemory;
  db.Open(config);
  auto svc = std::make_shared<neug::NeugDBService>(db);
  {
    auto slot = svc->AcquireExecutionSlot();
    auto txn = slot->BeginMvccInsertTransaction();
    neug::StorageTPInsertInterface interface(txn);
    auto person_label = txn.schema().get_vertex_label_id("person");
    auto software_label = txn.schema().get_vertex_label_id("software");
    auto created_label = txn.schema().get_edge_label_id("created");
    neug::vid_t vid;
    EXPECT_TRUE(txn.GetVertexIndex(person_label, neug::Value::INT64(1), vid));
    neug::vid_t vid2;
    EXPECT_TRUE(
        txn.GetVertexIndex(software_label, neug::Value::INT64(2), vid2));
    const void* edge_prop = nullptr;
    EXPECT_TRUE(interface.AddEdge(
        person_label, vid, software_label, vid2, created_label,
        {neug::Value::DOUBLE(0.9), neug::Value::INT64(2022)}, edge_prop));
    EXPECT_TRUE(txn.Commit());
  }
  {
    auto slot = svc->AcquireExecutionSlot();
    auto txn = slot->BeginSnapshotReadTransaction();
    neug::StorageReadInterface gi(txn.view(), txn.timestamp());
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
  svc.reset();
  db.Close();
}

TEST_F(MvccInsertTransactionTest, TestUnsupportedInterface) {
  neug::NeugDB db;
  neug::NeugDBConfig config(db_dir);
  config.memory_level = neug::MemoryLevel::kInMemory;
  db.Open(config);
  auto svc = std::make_shared<neug::NeugDBService>(db);

  {
    auto slot = svc->AcquireExecutionSlot();
    auto txn = slot->BeginMvccInsertTransaction();
    neug::StorageTPInsertInterface interface(txn);
    std::vector<neug::vid_t> vids;
    std::vector<std::tuple<neug::vid_t, neug::vid_t>> edges;
    std::vector<std::pair<neug::vid_t, int32_t>> oe_edges, ie_edges;
    EXPECT_EQ(interface.BatchAddVertices(0, nullptr).error().error_code(),
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
    int fd = OsOpen(path.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0644);
    ASSERT_NE(fd, -1);
    auto bytes_written = OsWrite(fd, buf.data(), buf.size());
    ASSERT_NE(bytes_written, -1);
    ASSERT_EQ(static_cast<size_t>(bytes_written), buf.size());
    OsClose(fd);
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

TEST_F(LocalWalParserTest, WriterOpenCloseWithoutAppendCreatesNoFile) {
  neug::LocalWalWriter writer(wal_dir_, 0);
  static constexpr char kUnused = '\0';
  for (size_t i = 0; i < 5; ++i) {
    writer.open(wal_dir_);
    EXPECT_TRUE(writer.append(&kUnused, 0));
    writer.close();
  }
  EXPECT_TRUE(std::filesystem::is_empty(wal_dir_));
}

TEST_F(LocalWalParserTest, WriterPersistsTerminatorAfterEachAppend) {
  neug::LocalWalWriter writer(wal_dir_, 0);
  writer.open(wal_dir_);

  std::vector<char> first_record;
  AppendWalEntry(first_record, 1, 0, "first");
  ASSERT_TRUE(writer.append(first_record.data(), first_record.size()));

  const auto wal_path = std::filesystem::directory_iterator(wal_dir_)->path();
  const auto expect_terminator_at = [&](size_t offset) {
    std::ifstream wal(wal_path, std::ios::binary);
    ASSERT_TRUE(wal);
    wal.seekg(static_cast<std::streamoff>(offset));
    neug::WalHeader terminator;
    wal.read(reinterpret_cast<char*>(&terminator), sizeof(terminator));
    ASSERT_TRUE(wal);
    EXPECT_EQ(terminator.timestamp, 0u);
    EXPECT_EQ(terminator.type, 0u);
    EXPECT_EQ(terminator.length, 0);
  };
  expect_terminator_at(first_record.size());

  std::vector<char> second_record;
  AppendWalEntry(second_record, 2, 0, "second");
  ASSERT_TRUE(writer.append(second_record.data(), second_record.size()));
  expect_terminator_at(first_record.size() + second_record.size());
  writer.close();

  neug::LocalWalParser parser(wal_dir_);
  EXPECT_EQ(parser.last_ts(), 2u);
}

TEST_F(LocalWalParserTest, WriterCreationFailureCanBeRetried) {
  const auto invalid_wal_dir =
      (std::filesystem::path(wal_dir_) / "not_a_directory").string();
  const int fd =
      OsOpen(invalid_wal_dir.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0644);
  ASSERT_NE(fd, -1);
  ASSERT_EQ(OsClose(fd), 0);

  neug::LocalWalWriter writer(invalid_wal_dir, 0);
  writer.open(invalid_wal_dir);
  std::vector<char> record;
  const std::string payload = "retry_after_creation_failure";
  AppendWalEntry(record, 1, 0, payload);
  EXPECT_FALSE(writer.append(record.data(), record.size()));

  ASSERT_TRUE(std::filesystem::remove(invalid_wal_dir));
  ASSERT_TRUE(std::filesystem::create_directory(invalid_wal_dir));
  ASSERT_TRUE(writer.append(record.data(), record.size()));
  EXPECT_NO_THROW(writer.close());

  neug::LocalWalParser parser(invalid_wal_dir);
  const auto& unit = parser.get_insert_wal(1);
  EXPECT_EQ(std::string(unit.ptr, unit.size), payload);
}

TEST_F(LocalWalParserTest, WriterDoesNotCreateMissingDirectory) {
  const auto missing_wal_dir = wal_dir_ + "/missing/wal";
  neug::LocalWalWriter writer(missing_wal_dir, 0);
  writer.open(missing_wal_dir);
  EXPECT_TRUE(writer.append(nullptr, 0));
  const char record = '\0';
  EXPECT_FALSE(writer.append(&record, 1));
  EXPECT_FALSE(std::filesystem::exists(wal_dir_ + "/missing"));
}

#ifndef _WIN32
TEST_F(LocalWalParserTest, WriterRetriesDirectorySyncBeforeWriting) {
  if (::geteuid() == 0) {
    GTEST_SKIP() << "Running as root; directory permissions are not enforced";
  }
  struct RestorePermissions {
    const std::string& path;
    ~RestorePermissions() { ::chmod(path.c_str(), 0700); }
  } restore{wal_dir_};

  // Allow creating a WAL file but deny opening its directory for fsync().
  ASSERT_EQ(::chmod(wal_dir_.c_str(), 0300), 0);
  const int probe_fd = ::open(wal_dir_.c_str(), O_RDONLY);
  if (probe_fd != -1) {
    ::close(probe_fd);
    GTEST_SKIP() << "Directory permissions are not enforced on this filesystem";
  }

  neug::LocalWalWriter writer(wal_dir_, 0);
  writer.open(wal_dir_);
  std::vector<char> record;
  const std::string payload = "retry_after_directory_sync_failure";
  AppendWalEntry(record, 1, 0, payload);

  for (int attempt = 0; attempt < 2; ++attempt) {
    EXPECT_FALSE(writer.append(record.data(), record.size()));
  }

  ASSERT_EQ(::chmod(wal_dir_.c_str(), 0700), 0);
  for (const auto& file : std::filesystem::directory_iterator(wal_dir_)) {
    EXPECT_EQ(file.file_size(), 0u);
  }
  ASSERT_TRUE(writer.append(record.data(), record.size()));
  writer.close();
  neug::LocalWalParser parser(wal_dir_);
  const auto& unit = parser.get_insert_wal(1);
  EXPECT_EQ(std::string(unit.ptr, unit.size), payload);
}

// Test: LocalWalParser throws IOException when ::open() on a WAL file fails
// (e.g. permission denied). This covers the fd == -1 check.
// Note: running as root or on permission-ignoring filesystems bypasses
// chmod(0000), so we pre-flight the open() and skip when it still succeeds.
TEST_F(LocalWalParserTest, OpenUnreadableWalFileThrowsIOException) {
  // Root (euid 0) bypasses file-permission checks; skip early.
  if (::geteuid() == 0) {
    GTEST_SKIP() << "Running as root; chmod(0000) does not block open()";
  }

  auto file_path = wal_dir_ + "/unreadable.wal";
  int fd = ::open(file_path.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0644);
  ASSERT_NE(fd, -1);
  const char data[] = "some wal data";
  auto bytes_written = ::write(fd, data, sizeof(data));
  ASSERT_NE(bytes_written, -1);
  ASSERT_EQ(static_cast<size_t>(bytes_written), sizeof(data));
  ::close(fd);

  ASSERT_EQ(chmod(file_path.c_str(), 0000), 0);

  // Pre-flight: verify the OS actually denies O_RDONLY access.
  // Some CI filesystems or capability sets may still allow the open.
  int probe_fd = ::open(file_path.c_str(), O_RDONLY);
  if (probe_fd != -1) {
    ::close(probe_fd);
    chmod(file_path.c_str(), 0644);
    GTEST_SKIP() << "chmod(0000) did not block open() on this platform";
  }

  try {
    neug::LocalWalParser parser(wal_dir_);
    FAIL() << "Expected IOException but none was thrown";
  } catch (const neug::exception::IOException& e) {
    EXPECT_NE(std::string(e.what()).find("Failed to open wal file"),
              std::string::npos);
  }

  chmod(file_path.c_str(), 0644);
}
#endif

#ifndef _WIN32
// Test: LocalWalParser throws IOException when mmap() fails.
// Uses RLIMIT_AS to deterministically exhaust the virtual-address space
// so mmap() reliably returns MAP_FAILED, avoiding the flakiness of the
// sparse-file approach (many kernels accept such mappings via overcommit
// and only fault on first access).
TEST_F(LocalWalParserTest, MmapFailureThrowsIOException) {
  // Create a small but non-empty WAL file so mmap() is genuinely attempted.
  auto file_path = wal_dir_ + "/mmap_fail_test.wal";
  {
    int fd = ::open(file_path.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0644);
    ASSERT_NE(fd, -1);
    const char data[] = "placeholder wal content";
    auto bytes_written = ::write(fd, data, sizeof(data));
    ASSERT_NE(bytes_written, -1);
    ASSERT_EQ(static_cast<size_t>(bytes_written), sizeof(data));
    ::close(fd);
  }

  // Clamp the virtual-address-space limit to 1 byte so any subsequent
  // mmap() call is guaranteed to fail with ENOMEM / MAP_FAILED.
  struct rlimit old_rlimit;
  ASSERT_EQ(::getrlimit(RLIMIT_AS, &old_rlimit), 0);
  const struct rlimit tight_rlimit = {1, old_rlimit.rlim_max};
  if (::setrlimit(RLIMIT_AS, &tight_rlimit) != 0) {
    GTEST_SKIP() << "Cannot lower RLIMIT_AS on this platform; skipping test";
  }

  bool threw = false;
  bool msg_ok = false;
  try {
    neug::LocalWalParser parser(wal_dir_);
  } catch (const neug::exception::IOException& e) {
    threw = true;
    // Use strstr (no heap allocation) while RLIMIT_AS is still clamped.
    msg_ok = (std::strstr(e.what(), "Failed to mmap wal file") != nullptr);
  }

  // Restore RLIMIT_AS before any GTEST assertions that may heap-allocate.
  ::setrlimit(RLIMIT_AS, &old_rlimit);

  EXPECT_TRUE(threw) << "Expected IOException but none was thrown";
  EXPECT_TRUE(msg_ok)
      << "Exception message did not contain 'Failed to mmap wal file'";
}
#endif

// Test: Opening an empty WAL directory does not throw and last_ts is 0.
TEST_F(LocalWalParserTest, OpenEmptyWalDirNoThrow) {
  neug::LocalWalParser parser(wal_dir_);
  EXPECT_EQ(parser.last_ts(), 0);
}

// A deterministic overlap: neither reservation has applied any rows yet.
TEST_F(MvccInsertTransactionTest, CapacityReservationsPreventOverbooking) {
  neug::NeugDB db;
  db.Open(db_dir);
  auto& graph = const_cast<neug::PropertyGraph&>(db.graph());
  auto label = graph.schema().get_vertex_label_id("person");
  auto& table = graph.get_vertex_table(label);
  ASSERT_GT(table.Capacity(), table.Size() + 2);
  neug::InsertCapacityRequest large, small, growth;
  for (size_t i = 0; i < table.Capacity() - table.Size() - 1; ++i) {
    large.AddVertex("person", neug::Value::INT64(i + 100),
                    {neug::Value::STRING(""), neug::Value::INT64(0)});
  }
  for (size_t i = 0; i < 2; ++i) {
    small.AddVertex("person", neug::Value::INT64(i + 10000),
                    {neug::Value::STRING(""), neug::Value::INT64(0)});
  }
  neug::InsertCapacityReservations reservations;
  EXPECT_TRUE(reservations.TryReserve(graph, large, growth));
  EXPECT_FALSE(reservations.TryReserve(graph, small, growth));
  reservations.Release(large);
  EXPECT_TRUE(reservations.TryReserve(graph, small, growth));
  reservations.Release(small);
  EXPECT_TRUE(reservations.TryReserve(graph, large, growth));
  reservations.Release(large);
  db.Close();
}

// Capacity rejection happens before any table is resized or reservation stored.
TEST_F(MvccInsertTransactionTest, RejectVectorGrowthBeforeMutation) {
  neug::NeugDB db;
  db.Open(db_dir);
  auto conn = db.Connect();
  ASSERT_TRUE(conn->Query(
      "CREATE NODE TABLE vector_item(id INT64 PRIMARY KEY, v FLOAT[2]);"));
  auto& graph = const_cast<neug::PropertyGraph&>(db.graph());
  auto& table =
      graph.get_vertex_table(graph.schema().get_vertex_label_id("vector_item"));
  auto* array =
      dynamic_cast<neug::ArrayColumn*>(table.get_table().get_column_by_id(0));
  ASSERT_NE(array, nullptr);
  const auto type = array->array_type();
  auto value =
      neug::Value::ARRAY(type, {neug::Value::FLOAT(1), neug::Value::FLOAT(2)});
  auto accessor = std::make_unique<neug::DefaultIndexIDAccessor>();
  accessor->Open(graph.checkpoint(), neug::ModuleDescriptor{},
                 neug::MemoryLevel::kInMemory);
  auto vector = std::make_unique<neug::VecColumn>(
      array->shared_buffer<float>(), std::move(accessor), type, array->size(),
      value, graph.checkpoint(), neug::MemoryLevel::kInMemory);
  const auto mapping_capacity = vector->size();
  const auto payload_capacity = vector->buffer_size();
  table.SetColumn(0, std::move(vector));
  const auto capacity = table.Capacity();
  neug::InsertCapacityRequest request, growth;
  for (size_t i = 0; i <= std::max(capacity, mapping_capacity); ++i)
    request.AddVertex("vector_item", neug::Value::INT64(i), {value});
  neug::InsertCapacityReservations reservations;
  EXPECT_THROW(reservations.TryReserve(graph, request, growth), std::exception);
  EXPECT_THROW(request.Grow(graph), std::exception);
  EXPECT_EQ(table.Capacity(), capacity);
  EXPECT_EQ(table.Size(), 0);
  auto* vec =
      dynamic_cast<neug::VecColumn*>(table.get_table().get_column_by_id(0));
  ASSERT_NE(vec, nullptr);
  EXPECT_EQ(vec->size(), mapping_capacity);
  EXPECT_EQ(vec->buffer_size(), payload_capacity);
  EXPECT_EQ(vec->get_offset_accessor()->GetNextIndexID(), 0);
  neug::InsertCapacityRequest ordinary;
  ordinary.AddVertex(
      "person", neug::Value::INT64(100),
      {neug::Value::STRING("after rejection"), neug::Value::INT64(0)});
  EXPECT_TRUE(reservations.TryReserve(graph, ordinary, growth));
  reservations.Release(ordinary);
  conn.reset();
  db.Close();
}

TEST_F(MvccInsertTransactionTest,
       AtomicGrowthPreservesSourceOnEveryAllocationFailure) {
  neug::NeugDB db;
  db.Open(db_dir);
  auto& graph = const_cast<neug::PropertyGraph&>(db.graph());
  const auto label = graph.schema().get_vertex_label_id("person");
  auto& table = graph.get_vertex_table(label);
  const auto capacity = table.Capacity();
  const auto size = table.Size();
  const auto alice = table.get_indexer().get_index(neug::Value::INT64(1));
  const auto original_name = table.get_table()
                                 .get_column_by_id(0)
                                 ->get_any(alice)
                                 .GetValue<std::string>();
  neug::InsertCapacityRequest request;
  for (size_t i = 0; i <= capacity; ++i)
    request.AddVertex(
        "person", neug::Value::INT64(100 + i),
        {neug::Value::STRING(std::string(200, 'x')), neug::Value::INT64(i)});
  bool succeeded = false;
  int failures = 0;
  for (int allocation = 0; allocation < 64; ++allocation) {
    auto candidate = graph.Clone();
    neug::SetResizeFailureCountdownForTest(allocation);
    try {
      request.Grow(*candidate);
      succeeded = true;
    } catch (const std::bad_alloc&) { ++failures; } catch (...) {
      neug::SetResizeFailureCountdownForTest(-1);
      throw;
    }
    neug::SetResizeFailureCountdownForTest(-1);
    EXPECT_EQ(table.Capacity(), capacity);
    EXPECT_EQ(table.Size(), size);
    EXPECT_EQ(table.get_indexer().get_index(neug::Value::INT64(1)), alice);
    EXPECT_EQ(table.get_table()
                  .get_column_by_id(0)
                  ->get_any(alice)
                  .GetValue<std::string>(),
              original_name);
    if (succeeded) {
      EXPECT_TRUE(request.Fits(*candidate));
      break;
    }
  }
  EXPECT_TRUE(succeeded);
  EXPECT_GE(failures,
            5);  // Includes columns, primary hash buckets and CSR directories.
  db.Close();
}

TEST_F(MvccInsertTransactionTest, NestedCapacityWithoutVertexGrowth) {
  neug::NeugDB db;
  db.Open(db_dir);
  auto conn = db.Connect();
  ASSERT_TRUE(conn->Query(
      "CREATE NODE TABLE nested_item(id INT64 PRIMARY KEY, values STRING[]);"));
  auto& graph = const_cast<neug::PropertyGraph&>(db.graph());
  const auto label = graph.schema().get_vertex_label_id("nested_item");
  const auto capacity = graph.get_vertex_table(label).Capacity();
  ASSERT_GT(capacity, 2);
  std::vector<neug::Value> strings(200,
                                   neug::Value::STRING(std::string(200, 'x')));
  auto value = neug::Value::LIST(neug::DataType(neug::DataTypeId::kVarchar),
                                 std::move(strings));
  neug::InsertCapacityRequest request;
  request.AddVertex("nested_item", neug::Value::INT64(1), {value});
  EXPECT_FALSE(request.Fits(graph));
  bool succeeded = false;
  int failures = 0;
  for (int allocation = 0; allocation < 16; ++allocation) {
    auto candidate = graph.Clone();
    neug::SetResizeFailureCountdownForTest(allocation);
    try {
      request.Grow(*candidate);
      succeeded = true;
    } catch (const std::bad_alloc&) { ++failures; } catch (...) {
      neug::SetResizeFailureCountdownForTest(-1);
      throw;
    }
    neug::SetResizeFailureCountdownForTest(-1);
    EXPECT_FALSE(request.Fits(graph));
    EXPECT_EQ(graph.get_vertex_table(label).Capacity(), capacity);
    if (succeeded) {
      EXPECT_TRUE(request.Fits(*candidate));
      EXPECT_EQ(candidate->get_vertex_table(label).Capacity(), capacity);
      // Once every nested buffer fits, growth must perform no allocations.
      neug::SetResizeFailureCountdownForTest(0);
      EXPECT_NO_THROW(request.Grow(*candidate));
      neug::SetResizeFailureCountdownForTest(-1);
      // Each reservation fits alone; their string payloads exceed the shared
      // capacity when combined. Releasing restores the complete nested budget.
      neug::InsertCapacityReservations reservations;
      neug::InsertCapacityRequest growth;
      EXPECT_TRUE(reservations.TryReserve(*candidate, request, growth));
      EXPECT_FALSE(reservations.TryReserve(*candidate, request, growth));
      reservations.Release(request);
      EXPECT_TRUE(reservations.TryReserve(*candidate, request, growth));
      reservations.Release(request);
      break;
    }
  }
  EXPECT_TRUE(succeeded);
  EXPECT_GE(failures, 2);
  conn.reset();
  db.Close();
}

TEST_F(MvccInsertTransactionTest, StringPayloadGrowthIsGeometric) {
  neug::NeugDB db;
  db.Open(db_dir);
  auto& graph = const_cast<neug::PropertyGraph&>(db.graph());
  auto& table =
      graph.get_vertex_table(graph.schema().get_vertex_label_id("person"));
  auto* column =
      dynamic_cast<neug::StringColumn*>(table.get_table().get_column_by_id(0));
  ASSERT_NE(column, nullptr);
  const auto rows = column->size();
  const auto vertex_capacity = table.Capacity();
  column->resize(rows, size_t{65536});
  const std::string payload(200, 'x');
  neug::InsertCapacityRequest request;
  request.AddVertex("person", neug::Value::INT64(100),
                    {neug::Value::STRING(payload), neug::Value::INT64(0)});
  for (int iteration = 0; iteration < 5; ++iteration) {
    while (column->available_space() >= payload.size())
      column->set_value(0, payload);
    const auto old_capacity = column->data_capacity();
    const auto required =
        old_capacity - column->available_space() + payload.size();
    EXPECT_FALSE(request.Fits(graph));
    request.Grow(graph);
    EXPECT_TRUE(request.Fits(graph));
    EXPECT_EQ(column->data_capacity(),
              std::max(size_t{4096}, required + required / 4));
    EXPECT_EQ(column->size(), rows);
    EXPECT_EQ(table.Capacity(), vertex_capacity);
    // A fitting request must not allocate again.
    neug::SetResizeFailureCountdownForTest(0);
    EXPECT_NO_THROW(request.Grow(graph));
    neug::SetResizeFailureCountdownForTest(-1);
  }
  db.Close();
}

TEST_F(MvccInsertTransactionTest, RowGrowthUsesCompleteBatchRequirement) {
  neug::NeugDB db;
  db.Open(db_dir);
  auto& graph = const_cast<neug::PropertyGraph&>(db.graph());
  const auto label = graph.schema().get_vertex_label_id("person");
  const auto& original = graph.get_vertex_table(label);
  for (const auto required :
       {original.Capacity() + 1, original.Capacity() * 3}) {
    neug::InsertCapacityRequest request;
    for (size_t i = original.Size(); i < required; ++i) {
      request.AddVertex("person", neug::Value::INT64(i + 100),
                        {neug::Value::STRING(""), neug::Value::INT64(0)});
    }
    auto candidate = graph.Clone();
    request.Grow(*candidate);
    EXPECT_TRUE(request.Fits(*candidate));
    EXPECT_EQ(candidate->get_vertex_table(label).Capacity(),
              std::max(size_t{4096}, required + required / 4));
  }
  db.Close();
}

TEST_F(MvccInsertTransactionTest, CapacityRetryLimitLeavesSlotUsable) {
  neug::NeugDB db;
  db.Open(db_dir);
  auto svc = std::make_shared<neug::NeugDBService>(db);
  auto slot = svc->AcquireExecutionSlot();
  const auto label = db.graph().schema().get_vertex_label_id("person");
  const auto before = db.graph().get_vertex_table(label).Size();
  struct ResetInjection {
    ~ResetInjection() { neug::SetInsertReservationFailuresForTest(0); }
  } reset;
  neug::SetInsertReservationFailuresForTest(2);
  const std::string query =
      R"json({"query":"CREATE (:person {id: 100, name: ')json" +
      std::string(1 << 20, 'x') +
      R"json(', age: 20})","access_mode":"insert","parameters":{}})json";
  auto failed = slot->ExecuteTransactionalRequest(query);
  ASSERT_FALSE(failed);
  EXPECT_EQ(failed.error().error_message(),
            "Insert capacity retry limit exceeded");
  EXPECT_EQ(db.graph().get_vertex_table(label).Size(), before);
  neug::SetInsertReservationFailuresForTest(0);
  auto success = slot->ExecuteTransactionalRequest(query);
  ASSERT_TRUE(success) << success.error().error_message();
  EXPECT_EQ(db.graph().get_vertex_table(label).Size(), before + 1);
  EXPECT_TRUE(slot->ExecuteTransactionalRequest(
      R"json({"query":"MATCH (n:person) RETURN count(n)","access_mode":"read","parameters":{}})json"));
}

TEST_F(MvccInsertTransactionTest, EdgeCapacityGrowthCoversRowsAndPayload) {
  neug::NeugDB db;
  db.Open(db_dir);
  auto conn = db.Connect();
  ASSERT_TRUE(
      conn->Query("CREATE REL TABLE payload(FROM person TO person, seq "
                  "INT64, note STRING)"));
  auto& graph = const_cast<neug::PropertyGraph&>(db.graph());
  auto label = graph.schema().get_vertex_label_id("person");
  auto edge = graph.schema().get_edge_label_id("payload");
  auto& table = graph.get_edge_table(label, label, edge);
  ASSERT_FALSE(table.meta()->is_bundled());
  neug::InsertCapacityRequest rows;
  for (size_t i = 0; i <= table.Capacity(); ++i)
    rows.AddEdge("person", "person", "payload",
                 {neug::Value::INT64(i), neug::Value::STRING("note")});
  ASSERT_FALSE(rows.Fits(graph));
  auto candidate = graph.Clone();
  rows.Grow(*candidate);
  EXPECT_TRUE(rows.Fits(*candidate));
  EXPECT_FALSE(rows.Fits(graph));
  auto& grown = candidate->get_edge_table(label, label, edge);
  const auto capacity = grown.Capacity();
  neug::InsertCapacityRequest payload;
  payload.AddEdge(
      "person", "person", "payload",
      {neug::Value::INT64(0), neug::Value::STRING(std::string(1 << 20, 'x'))});
  ASSERT_FALSE(payload.Fits(*candidate));
  payload.Grow(*candidate);
  EXPECT_TRUE(payload.Fits(*candidate));
  EXPECT_EQ(grown.Capacity(), capacity);
}

class InsertRetryFailureTest : public MvccInsertTransactionTest,
                               public ::testing::WithParamInterface<int> {};

TEST_P(InsertRetryFailureTest, FailureLeavesSlotUsable) {
  neug::NeugDB db;
  db.Open(db_dir);
  auto svc = std::make_shared<neug::NeugDBService>(db);
  auto slot = svc->AcquireExecutionSlot();
  auto ddl_slot = svc->AcquireExecutionSlot();
  const auto label = db.graph().schema().get_vertex_label_id("person");
  const auto before = db.graph().get_vertex_table(label).Size();
  struct ResetInjection {
    ~ResetInjection() {
      neug::SetInsertRetryHookForTest({});
      neug::SetInsertReservationFailuresForTest(0);
    }
  } reset;
  bool schema_changed = false;
  const int scenario = GetParam();
  neug::SetInsertReservationFailuresForTest(1);
  neug::SetInsertRetryHookForTest(
      [&](bool second_attempt) {
        if (scenario == 2 || second_attempt != (scenario == 1))
          return;
        auto changed = ddl_slot->ExecuteTransactionalRequest(
            R"json({"query":"CREATE NODE TABLE AddedDuringRetry(id INT64, PRIMARY KEY(id))","access_mode":"schema","parameters":{}})json");
        EXPECT_TRUE(changed) << changed.error().error_message();
        schema_changed = bool(changed);
      },
      scenario == 2);
  const std::string query =
      R"json({"query":"CREATE (:person {id: 100, name: 'retry', age: 20})","access_mode":"insert","parameters":{}})json";
  auto failed = slot->ExecuteTransactionalRequest(query);
  ASSERT_FALSE(failed);
  EXPECT_EQ(failed.error().error_message(),
            scenario == 2 ? "Capacity maintenance is unavailable"
                          : "Schema changed during insert retry");
  EXPECT_EQ(schema_changed, scenario != 2);
  EXPECT_EQ(db.graph().get_vertex_table(label).Size(), before);
  neug::SetInsertRetryHookForTest({});
  neug::SetInsertReservationFailuresForTest(0);
  auto success = slot->ExecuteTransactionalRequest(query);
  ASSERT_TRUE(success) << success.error().error_message();
  EXPECT_EQ(db.graph().get_vertex_table(label).Size(), before + 1);
  EXPECT_TRUE(slot->ExecuteTransactionalRequest(
      R"json({"query":"MATCH (n:person) RETURN count(n)","access_mode":"read","parameters":{}})json"));
}

INSTANTIATE_TEST_SUITE_P(InsertCapacity, InsertRetryFailureTest,
                         ::testing::Values(0, 1, 2),
                         [](const ::testing::TestParamInfo<int>& info) {
                           return info.param == 0
                                      ? "SchemaChangedBeforeMaintenance"
                                      : info.param == 1
                                            ? "SchemaChangedBeforeRetry"
                                            : "MaintenanceUnavailable";
                         });
