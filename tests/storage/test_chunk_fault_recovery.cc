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

// Durable fault-injection recovery tests for the chunked-column checkpoint
// format. Each case publishes a real on-disk checkpoint, corrupts or removes a
// single durable artifact (payload object, chunk-directory object, manifest,
// CURRENT selector, or WAL epoch), then reopens through a *fresh*
// CheckpointManager so recovery is exercised from disk rather than from a live
// in-memory mapping.
//
// Scope boundary: this file covers the checkpoint/chunk durable-format layer
// with a self-contained CheckpointManager + ChunkedColumn harness (no HTTP
// server). In-memory bit-flip detection on a live mapping is covered by
// test_chunked_column.cc; record-level WAL replay after a crash is a DB/
// transaction-layer concern covered by test_recovery.cc.

#include "neug/utils/property/chunked_column.h"

#include <gtest/gtest.h>

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <memory>
#include <string>
#include <vector>

#include "neug/storages/checkpoint_manager.h"
#include "neug/storages/checkpoint_manifest.h"
#include "neug/storages/chunk/chunk_types.h"
#include "neug/storages/container/file_header.h"
#include "neug/storages/graph/schema.h"
#include "neug/utils/exception/exception.h"

namespace neug {
namespace {

// Small rows_per_chunk keeps chunk/page boundaries exercised without large
// allocations; production uses >= 256 KiB payloads.
constexpr size_t kRowsPerChunk = 4;
constexpr size_t kRows = 12;  // 3 physical pages of 4 rows.

int64_t ExpectedValue(size_t row) {
  return static_cast<int64_t>(row * 5 + 1);
}

std::vector<char> ReadBytes(const std::filesystem::path& path) {
  std::ifstream in(path, std::ios::binary);
  return std::vector<char>((std::istreambuf_iterator<char>(in)),
                           std::istreambuf_iterator<char>());
}

void WriteBytes(const std::filesystem::path& path,
                const std::vector<char>& data) {
  std::ofstream out(path, std::ios::binary | std::ios::trunc);
  out.write(data.data(), static_cast<std::streamsize>(data.size()));
}

void WriteText(const std::filesystem::path& path, const std::string& text) {
  std::ofstream out(path, std::ios::trunc);
  out << text;
}

// Flip every bit of one byte at an absolute file offset. The container header
// MD5 is never verified on read (see MMapContainer::Open), so corrupting the
// payload region is what surfaces through the chunk CRC checks.
void CorruptByte(const std::filesystem::path& path, size_t offset) {
  auto bytes = ReadBytes(path);
  if (offset >= bytes.size()) {
    return;
  }
  bytes[offset] =
      static_cast<char>(static_cast<unsigned char>(bytes[offset]) ^ 0xFF);
  WriteBytes(path, bytes);
}

class ChunkFaultRecoveryTest : public ::testing::Test {
 protected:
  void SetUp() override {
    temp_dir_ =
        std::filesystem::temp_directory_path() /
        ("chunk_fault_recovery_" +
         std::to_string(
             std::chrono::steady_clock::now().time_since_epoch().count()));
    std::filesystem::remove_all(temp_dir_);
    std::filesystem::create_directories(temp_dir_);
    db_dir_ = temp_dir_.string();

    // Publish one durable checkpoint holding a chunked column, then release
    // every in-memory handle so the reopen below is genuinely from disk.
    CheckpointManager mgr;
    mgr.Open(db_dir_);
    auto staging = mgr.CreateStaging();
    auto ckp = staging.checkpoint();
    CheckpointManifest meta;
    meta.SetSchema(Schema());
    ChunkedColumn<int64_t> col(kRowsPerChunk);
    col.Open(*ckp, ModuleDescriptor{}, MemoryLevel::kInMemory);
    col.resize(kRows);
    for (size_t i = 0; i < kRows; ++i) {
      col.set_value(i, ExpectedValue(i));
    }
    col.Dump(*ckp, meta, "col");
    ckp->SetManifest(std::move(meta));
    auto published = staging.Publish();

    manifest_id_ = published->id();
    const auto* desc = published->manifest().FindModule("col");
    ASSERT_NE(desc, nullptr);
    auto dir_path = desc->get_path(kChunkDirPath);
    ASSERT_TRUE(dir_path.has_value());
    chunk_dir_path_ = *dir_path;
    ASSERT_TRUE(std::filesystem::exists(chunk_dir_path_));
    mgr.Close();
  }

  void TearDown() override { std::filesystem::remove_all(temp_dir_); }

  std::filesystem::path CheckpointDir() const { return temp_dir_ / "checkpoint"; }
  std::filesystem::path CurrentPath() const { return CheckpointDir() / "CURRENT"; }
  std::filesystem::path ObjectsDir() const { return CheckpointDir() / "objects"; }
  std::filesystem::path ManifestPath(uint64_t id) const {
    return CheckpointDir() / "manifests" / (std::to_string(id) + ".manifest");
  }
  std::filesystem::path WalDir(uint64_t id) const {
    return temp_dir_ / "wal" / std::to_string(id);
  }

  // Reopen the published database with a brand-new manager (read-only intent:
  // create_if_missing=false so a lost CURRENT is not silently re-created).
  std::unique_ptr<CheckpointManager> Reopen() const {
    auto mgr = std::make_unique<CheckpointManager>();
    mgr->Open(db_dir_, /*create_if_missing=*/false);
    return mgr;
  }

  // Decode the persisted chunk directory straight from its object file. The
  // blob is the object payload, i.e. everything after the container header.
  ChunkDirectory ReadDirectory() const {
    auto bytes = ReadBytes(chunk_dir_path_);
    return DecodeChunkDirectory(bytes.data() + sizeof(FileHeader),
                                bytes.size() - sizeof(FileHeader));
  }

  std::string db_dir_;
  std::filesystem::path temp_dir_;
  std::string chunk_dir_path_;
  uint64_t manifest_id_ = 0;
};

// Control: with no injected fault, a fresh reopen reads every value back. This
// makes the corruption failures below attributable to the injected fault rather
// than to a broken harness.
TEST_F(ChunkFaultRecoveryTest, ReopenWithoutCorruptionReadsBackValues) {
  auto mgr = Reopen();
  auto ckp = mgr->Current();
  ASSERT_NE(ckp, nullptr);
  const auto* desc = ckp->manifest().FindModule("col");
  ASSERT_NE(desc, nullptr);
  ChunkedColumn<int64_t> col(kRowsPerChunk);
  col.Open(*ckp, *desc, MemoryLevel::kInMemory);
  ASSERT_EQ(col.size(), kRows);
  for (size_t i = 0; i < kRows; ++i) {
    EXPECT_EQ(col.get_view(i), ExpectedValue(i));
  }
}

// --- CURRENT selector faults ---------------------------------------------

TEST_F(ChunkFaultRecoveryTest, CorruptCurrentSelectorRejectsOpen) {
  WriteText(CurrentPath(), "not-a-number\n");
  EXPECT_THROW(Reopen(), exception::CheckpointException);
}

TEST_F(ChunkFaultRecoveryTest, CurrentPointingToMissingManifestRejectsOpen) {
  WriteText(CurrentPath(), "999999\n");
  EXPECT_THROW(Reopen(), exception::CheckpointException);
}

// A lost selector must degrade to an empty (pre-first-publication) open rather
// than crash or fabricate a checkpoint.
TEST_F(ChunkFaultRecoveryTest, MissingCurrentYieldsEmptyOpen) {
  std::filesystem::remove(CurrentPath());
  auto mgr = Reopen();
  EXPECT_EQ(mgr->Current(), nullptr);
}

// --- Manifest faults ------------------------------------------------------

TEST_F(ChunkFaultRecoveryTest, CorruptManifestRejectsOpen) {
  WriteBytes(ManifestPath(manifest_id_),
             std::vector<char>{'{', ' ', 'b', 'a', 'd'});
  EXPECT_THROW(Reopen(), exception::StorageException);
}

// --- WAL epoch fault ------------------------------------------------------

TEST_F(ChunkFaultRecoveryTest, MissingWalEpochRejectsOpen) {
  std::filesystem::remove_all(WalDir(manifest_id_));
  EXPECT_THROW(Reopen(), exception::CheckpointException);
}

// --- Chunk-directory object faults ---------------------------------------

// The directory object is a declared module path, so it is resolved eagerly
// during CheckpointManager::Open; its absence fails the whole open.
TEST_F(ChunkFaultRecoveryTest, DeletedDirectoryObjectRejectsManagerOpen) {
  std::filesystem::remove(chunk_dir_path_);
  EXPECT_THROW(Reopen(), exception::CheckpointException);
}

// Corrupting the directory's bytes passes the eager existence check but fails
// the versioned directory's CRC trailer when the column decodes it lazily.
TEST_F(ChunkFaultRecoveryTest, CorruptDirectoryObjectDetectedOnColumnOpen) {
  const auto dir_bytes = ReadBytes(chunk_dir_path_);
  const size_t target = sizeof(FileHeader) + 8;  // inside the CRC-covered body
  ASSERT_LT(target, dir_bytes.size());
  CorruptByte(chunk_dir_path_, target);

  auto mgr = Reopen();
  auto ckp = mgr->Current();
  ASSERT_NE(ckp, nullptr);
  const auto* desc = ckp->manifest().FindModule("col");
  ASSERT_NE(desc, nullptr);
  ChunkedColumn<int64_t> col(kRowsPerChunk);
  EXPECT_THROW(col.Open(*ckp, *desc, MemoryLevel::kInMemory),
               exception::CheckpointException);
}

// --- Payload object faults ------------------------------------------------

// Payload objects are referenced only inside the directory blob, not in the
// module descriptor, so manager open stays clean and the failure surfaces when
// the column materializes its segments.
TEST_F(ChunkFaultRecoveryTest, DeletedPayloadObjectRejectsColumnOpen) {
  const auto dir = ReadDirectory();
  ASSERT_FALSE(dir.object_ids.empty());
  const auto payload_path = ObjectsDir() / dir.object_ids[0];
  ASSERT_TRUE(std::filesystem::exists(payload_path));
  std::filesystem::remove(payload_path);

  auto mgr = Reopen();
  auto ckp = mgr->Current();
  ASSERT_NE(ckp, nullptr);
  const auto* desc = ckp->manifest().FindModule("col");
  ASSERT_NE(desc, nullptr);
  ChunkedColumn<int64_t> col(kRowsPerChunk);
  EXPECT_THROW(col.Open(*ckp, *desc, MemoryLevel::kInMemory),
               exception::CheckpointException);
}

// Durable bit-rot in one physical page is caught by that page's CRC on first
// access after a full close/reopen cycle, while an intact page still reads.
TEST_F(ChunkFaultRecoveryTest, CorruptPayloadObjectDetectedOnReadAfterReopen) {
  const auto dir = ReadDirectory();
  ASSERT_GE(dir.pages.size(), 2u);
  const auto& slice = dir.pages[1].prefix;
  const auto payload_path = ObjectsDir() / dir.object_ids[slice.object_id];
  const size_t target = sizeof(FileHeader) + slice.offset;
  ASSERT_LT(target, std::filesystem::file_size(payload_path));
  CorruptByte(payload_path, target);

  auto mgr = Reopen();
  auto ckp = mgr->Current();
  ASSERT_NE(ckp, nullptr);
  const auto* desc = ckp->manifest().FindModule("col");
  ASSERT_NE(desc, nullptr);
  ChunkedColumn<int64_t> col(kRowsPerChunk);
  // Open does not verify payloads; the checksum is checked lazily on access.
  ASSERT_NO_THROW(col.Open(*ckp, *desc, MemoryLevel::kInMemory));
  // Page 0 is intact and still readable.
  EXPECT_EQ(col.get_view(0), ExpectedValue(0));
  // Page 1 carries the corrupted byte, so its first access throws.
  EXPECT_THROW(col.get_view(kRowsPerChunk), exception::CheckpointException);
}

}  // namespace
}  // namespace neug
