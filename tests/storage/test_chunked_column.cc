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

#include "neug/utils/property/chunked_column.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <filesystem>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "neug/storages/checkpoint_manager.h"
#include "neug/storages/checkpoint_manifest.h"
#include "neug/storages/graph/property_graph.h"
#include "neug/storages/graph/schema.h"
#include "neug/storages/module/module_broker.h"
#include "neug/storages/module/module_factory.h"
#include "neug/transaction/cow_graph_workspace.h"
#include "neug/utils/property/table.h"

namespace neug {
namespace {

// Uses a small rows_per_chunk (4) so chunk boundaries are exercised without
// large allocations; production uses >= 256 KiB payloads.
constexpr size_t kRowsPerChunk = 4;

class ChunkedColumnTest : public ::testing::Test {
 protected:
  void SetUp() override {
    temp_dir_ =
        std::filesystem::temp_directory_path() /
        ("chunked_column_test_" +
         std::to_string(
             std::chrono::steady_clock::now().time_since_epoch().count()));
    std::filesystem::remove_all(temp_dir_);
    std::filesystem::create_directories(temp_dir_);
    mgr_.Open(temp_dir_.string());
    auto staging = mgr_.CreateStaging();
    ckp_ = staging.checkpoint();
    CheckpointManifest meta;
    meta.SetSchema(Schema());
    ckp_->SetManifest(std::move(meta));
    staging.Discard();
  }

  void TearDown() override { std::filesystem::remove_all(temp_dir_); }

  // Count regular files under the checkpoint dir (objects + manifest + etc.).
  size_t CountObjectFiles() const {
    size_t count = 0;
    for (const auto& entry :
         std::filesystem::recursive_directory_iterator(temp_dir_)) {
      if (entry.is_regular_file()) {
        ++count;
      }
    }
    return count;
  }

  CheckpointManager mgr_;
  std::shared_ptr<Checkpoint> ckp_;
  std::filesystem::path temp_dir_;
};

TEST_F(ChunkedColumnTest, FrozenPrefixAlsoIsolatesOriginalOwner) {
  ChunkedColumn<int64_t> col(4);
  col.Open(*ckp_, ModuleDescriptor{}, MemoryLevel::kInMemory);
  col.resize(4);
  col.set_value(0, 11);
  auto module = col.Clone();
  auto& clone = dynamic_cast<ChunkedColumn<int64_t>&>(*module);
  clone.PrepareForInsert(1);
  col.set_value(0, 22);
  EXPECT_EQ(clone.get_view(0), 11);
  clone.set_any(1, Value::INT64(33), false);
  EXPECT_EQ(clone.get_view(1), 33);
}

TEST_F(ChunkedColumnTest, LegacyTypedReferenceAcceptsChunkedProperties) {
  ChunkedColumn<int64_t> col(4);
  col.Open(*ckp_, ModuleDescriptor{}, MemoryLevel::kInMemory);
  col.resize(8);
  col.set_value(5, 42);
  auto ref = CreateRefColumn(col);
  auto typed = std::dynamic_pointer_cast<TypedRefColumn<int64_t>>(ref);
  ASSERT_NE(typed, nullptr);
  EXPECT_EQ(typed->get_view(5), 42);
  EXPECT_EQ(typed->get_any(5).GetValue<int64_t>(), 42);
  EXPECT_EQ(typed->type(), DataTypeId::kInt64);
}

TEST_F(ChunkedColumnTest, ResizeSetGetAcrossChunkBoundary) {
  ChunkedColumn<int64_t> col(kRowsPerChunk);
  col.Open(*ckp_, ModuleDescriptor{}, MemoryLevel::kInMemory);
  col.resize(10);  // spans 3 chunks: 4 + 4 + 2
  EXPECT_EQ(col.size(), 10u);
  EXPECT_EQ(col.rows_per_chunk(), kRowsPerChunk);

  for (size_t i = 0; i < 10; ++i) {
    col.set_value(i, static_cast<int64_t>(i * 100));
  }
  for (size_t i = 0; i < 10; ++i) {
    EXPECT_EQ(col.get_view(i), static_cast<int64_t>(i * 100));
  }
  // Rows straddling chunk boundaries.
  EXPECT_EQ(col.get_view(3), 300);  // last slot of chunk 0
  EXPECT_EQ(col.get_view(4), 400);  // first slot of chunk 1
  EXPECT_EQ(col.get_view(8), 800);  // first slot of chunk 2

  // get_any round-trips through Value.
  EXPECT_EQ(col.get_any(5).GetValue<int64_t>(), 500);
}

TEST_F(ChunkedColumnTest, CloneSharesUntilWriteThenIsolatesPerChunk) {
  ChunkedColumn<int64_t> original(kRowsPerChunk);
  original.Open(*ckp_, ModuleDescriptor{}, MemoryLevel::kInMemory);
  original.resize(8);  // 2 chunks
  for (size_t i = 0; i < 8; ++i) {
    original.set_value(i, static_cast<int64_t>(i));
  }

  auto cow_module = original.Clone();
  auto* cow = dynamic_cast<ChunkedColumn<int64_t>*>(cow_module.get());
  ASSERT_NE(cow, nullptr);

  // Before any write, the clone reads the shared data.
  EXPECT_EQ(cow->get_view(1), 1);

  // Write only chunk 0 (row 1) in the clone; chunk 1 stays shared.
  cow->set_value(1, 999);
  EXPECT_EQ(cow->get_view(1), 999);
  EXPECT_EQ(original.get_view(1), 1);  // original chunk 0 unaffected
  EXPECT_EQ(cow->get_view(5), 5);      // clone chunk 1 still shared
  EXPECT_EQ(original.get_view(5), 5);

  // Mutating the original's chunk 0 does not disturb the clone's forked chunk.
  original.set_value(1, 111);
  EXPECT_EQ(original.get_view(1), 111);
  EXPECT_EQ(cow->get_view(1), 999);
}

TEST_F(ChunkedColumnTest, DetachDefersChunkCopyUntilWrite) {
  ChunkedColumn<int64_t> original(kRowsPerChunk);
  original.Open(*ckp_, ModuleDescriptor{}, MemoryLevel::kInMemory);
  original.resize(8);
  for (size_t i = 0; i < 8; ++i) {
    original.set_value(i, static_cast<int64_t>(i));
  }

  auto cow_module = original.Clone();
  auto* cow = dynamic_cast<ChunkedColumn<int64_t>*>(cow_module.get());
  ASSERT_NE(cow, nullptr);
  cow->Detach(*ckp_, MemoryLevel::kInMemory);

  // Detach only rebinds the COW allocation context. The target chunk is copied
  // on its first write, while untouched chunks stay shared.
  cow->set_value(5, 555);  // chunk 1
  EXPECT_EQ(cow->get_view(5), 555);
  EXPECT_EQ(original.get_view(5), 5);
  cow->set_value(0, 111);  // chunk 0
  EXPECT_EQ(cow->get_view(0), 111);
  EXPECT_EQ(original.get_view(0), 0);
}

TEST_F(ChunkedColumnTest, DetachDoesNotMakeCleanChunksDirty) {
  ChunkedColumn<int64_t> original(kRowsPerChunk);
  original.Open(*ckp_, ModuleDescriptor{}, MemoryLevel::kInMemory);
  original.resize(8);
  for (size_t i = 0; i < 8; ++i) {
    original.set_value(i, static_cast<int64_t>(i));
  }
  CheckpointManifest original_meta;
  original.Dump(*ckp_, original_meta, "original");
  ckp_->FinalizeObjectWriter(original_meta);

  auto cow_module = original.Clone();
  auto* cow = dynamic_cast<ChunkedColumn<int64_t>*>(cow_module.get());
  ASSERT_NE(cow, nullptr);
  cow->Detach(*ckp_, MemoryLevel::kInMemory);

  const size_t before = CountObjectFiles();
  CheckpointManifest clean_meta;
  cow->Dump(*ckp_, clean_meta, "clean_clone");
  ckp_->FinalizeObjectWriter(clean_meta);
  const size_t after = CountObjectFiles();

  // A clean clone needs only its directory; copying every chunk would add a
  // second payload object here.
  EXPECT_EQ(after - before, 1u);
}

TEST_F(ChunkedColumnTest, ComputeRowsPerChunkMeetsPayloadFloor) {
  // int64 (8 bytes): 256 KiB / 8 = 32768 rows.
  EXPECT_EQ(ChunkedColumn<int64_t>::ComputeRowsPerChunk(sizeof(int64_t)),
            ChunkedColumn<int64_t>::kMinChunkPayloadBytes / sizeof(int64_t));
  // A power of two.
  const size_t rows = ChunkedColumn<int64_t>::ComputeRowsPerChunk(8);
  EXPECT_EQ(rows & (rows - 1), 0u);
}

TEST_F(ChunkedColumnTest, DumpOpenRoundtripPreservesValues) {
  ChunkedColumn<int64_t> col(kRowsPerChunk);
  col.Open(*ckp_, ModuleDescriptor{}, MemoryLevel::kInMemory);
  col.resize(10);  // 3 chunks
  for (size_t i = 0; i < 10; ++i) {
    col.set_value(i, static_cast<int64_t>(i * 7));
  }

  CheckpointManifest meta;
  col.Dump(*ckp_, meta, "chunked_col");
  ckp_->FinalizeObjectWriter(meta);

  // Reopen from the serialized chunk directory (crc32c verified on load).
  ChunkedColumn<int64_t> reopened(kRowsPerChunk);
  reopened.Open(*ckp_, *meta.FindModule("chunked_col"), MemoryLevel::kInMemory);
  EXPECT_EQ(reopened.size(), 10u);
  EXPECT_EQ(reopened.rows_per_chunk(), kRowsPerChunk);
  for (size_t i = 0; i < 10; ++i) {
    EXPECT_EQ(reopened.get_view(i), static_cast<int64_t>(i * 7));
  }
}

TEST_F(ChunkedColumnTest, ShrinkDropsUnreachableChunksBeforeDump) {
  ChunkedColumn<int64_t> col(kRowsPerChunk);
  col.Open(*ckp_, ModuleDescriptor{}, MemoryLevel::kInMemory);
  col.resize(8);
  for (size_t i = 0; i < 8; ++i) {
    col.set_value(i, static_cast<int64_t>(i));
  }
  col.resize(4);

  CheckpointManifest meta;
  col.Dump(*ckp_, meta, "shrunk");
  ckp_->FinalizeObjectWriter(meta);

  ChunkedColumn<int64_t> reopened(kRowsPerChunk);
  reopened.Open(*ckp_, *meta.FindModule("shrunk"), MemoryLevel::kInMemory);
  ASSERT_EQ(reopened.size(), 4u);
  for (size_t i = 0; i < 4; ++i) {
    EXPECT_EQ(reopened.get_view(i), static_cast<int64_t>(i));
  }
}

TEST_F(ChunkedColumnTest, CreateRefColumnReadsChunkedColumn) {
  ChunkedColumn<int64_t> col(kRowsPerChunk);
  col.Open(*ckp_, ModuleDescriptor{}, MemoryLevel::kInMemory);
  col.resize(6);
  for (size_t i = 0; i < 6; ++i) {
    col.set_value(i, static_cast<int64_t>(i * 3));
  }
  // CreateRefColumn must recognize ChunkedColumn (not bad_cast to TypedColumn)
  // and read through chunk-localized access.
  auto ref = CreateRefColumn(col);
  ASSERT_NE(ref, nullptr);
  EXPECT_EQ(ref->type(), DataTypeId::kInt64);
  for (size_t i = 0; i < 6; ++i) {
    EXPECT_EQ(ref->get_any(i).GetValue<int64_t>(), static_cast<int64_t>(i * 3));
  }
}

TEST(CreatePropertyColumnTest, FixedLengthUsesChunkedColumn) {
  // Fixed-length property columns use the chunked layout (PR-E E2).
  auto col = CreatePropertyColumn(DataType(DataTypeId::kInt64));
  ASSERT_NE(col, nullptr);
  EXPECT_NE(dynamic_cast<ChunkedColumn<int64_t>*>(col.get()), nullptr);
  // Varchar falls back to a non-chunked column.
  auto str = CreatePropertyColumn(DataType(DataTypeId::kVarchar));
  ASSERT_NE(str, nullptr);
  EXPECT_EQ(dynamic_cast<ChunkedColumn<std::string_view>*>(str.get()), nullptr);
}

TEST_F(ChunkedColumnTest, IncrementalDumpRewritesOnlyDirtyChunk) {
  // Build a 3-chunk column (12 rows / 4 per chunk) and dump it.
  ChunkedColumn<int64_t> col(kRowsPerChunk);
  col.Open(*ckp_, ModuleDescriptor{}, MemoryLevel::kInMemory);
  col.resize(12);
  for (size_t i = 0; i < 12; ++i) {
    col.set_value(i, static_cast<int64_t>(i));
  }
  CheckpointManifest meta1;
  col.Dump(*ckp_, meta1, "col");
  ckp_->FinalizeObjectWriter(meta1);

  // Reopen and modify only chunk 1 (rows 4..7); chunks 0 and 2 stay clean.
  ChunkedColumn<int64_t> col2(kRowsPerChunk);
  col2.Open(*ckp_, *meta1.FindModule("col"), MemoryLevel::kInMemory);
  for (size_t i = 4; i < 8; ++i) {
    col2.set_value(i, static_cast<int64_t>(i + 1000));
  }

  // Incremental dump must re-commit only the dirty chunk plus its directory,
  // reusing the two clean chunks' objects (chunk-granular reuse).
  const size_t before = CountObjectFiles();
  CheckpointManifest meta2;
  col2.Dump(*ckp_, meta2, "col");
  ckp_->FinalizeObjectWriter(meta2);
  const size_t after = CountObjectFiles();
  EXPECT_LE(after - before, 2u)
      << "chunk-granular reuse failed: too many objects re-committed";

  // Reopen and verify: chunk 1 modified, chunks 0/2 unchanged.
  ChunkedColumn<int64_t> col3(kRowsPerChunk);
  col3.Open(*ckp_, *meta2.FindModule("col"), MemoryLevel::kInMemory);
  for (size_t i = 0; i < 12; ++i) {
    const int64_t expected = (i >= 4 && i < 8) ? static_cast<int64_t>(i + 1000)
                                               : static_cast<int64_t>(i);
    EXPECT_EQ(col3.get_view(i), expected);
  }
}

TEST(ChunkDirTest, ExtractChunkDirObjectIdsParsesAndRejectsCorruption) {
  // Build a directory blob in the ChunkedColumn::Dump format: header +
  // object-ID table + per-chunk ObjectSlice section.
  std::vector<char> blob;
  auto append_u64 = [&](uint64_t v) {
    for (int i = 0; i < 8; ++i) {
      blob.push_back(static_cast<char>((v >> (8 * i)) & 0xFF));
    }
  };
  auto append_u32 = [&](uint32_t v) {
    for (int i = 0; i < 4; ++i) {
      blob.push_back(static_cast<char>((v >> (8 * i)) & 0xFF));
    }
  };
  const std::string p0 = "aaa";
  const std::string p1 = "bbb";
  append_u64(4);  // rows_per_chunk
  append_u64(2);  // chunk_count
  append_u64(8);  // row_count
  append_u64(2);  // object_count
  append_u32(static_cast<uint32_t>(p0.size()));
  blob.insert(blob.end(), p0.begin(), p0.end());
  append_u32(static_cast<uint32_t>(p1.size()));
  const size_t p1_bytes_at = blob.size();
  blob.insert(blob.end(), p1.begin(), p1.end());
  // Per-chunk ObjectSlice section (obj_index, offset, length, crc32c).
  append_u32(0);
  append_u64(0);
  append_u32(32);
  append_u32(111);
  append_u32(1);
  append_u64(0);
  append_u32(32);
  append_u32(222);

  auto object_ids = ExtractChunkDirObjectIds(blob.data(), blob.size());
  ASSERT_EQ(object_ids.size(), 2u);
  EXPECT_EQ(object_ids[0], p0);
  EXPECT_EQ(object_ids[1], p1);

  // GC must reject rather than partially parse a corrupt directory; otherwise
  // it could reclaim a live object missing from the partial retain set.
  EXPECT_THROW(
      ExtractChunkDirObjectIds(blob.data(), p1_bytes_at + p1.size() - 1),
      exception::CheckpointException);
}

TEST_F(ChunkedColumnTest, OpenRejectsTruncatedDirectory) {
  auto dir = ckp_->CreateRuntimeContainer(1, MemoryLevel::kInMemory);
  static_cast<char*>(dir->GetData())[0] = '\0';
  ModuleDescriptor desc;
  desc.module_type = ChunkedColumn<int64_t>::type_name();
  desc.set_path(kChunkDirPath, ckp_->Commit(*dir));

  ChunkedColumn<int64_t> reopened(kRowsPerChunk);
  EXPECT_THROW(reopened.Open(*ckp_, desc, MemoryLevel::kInMemory),
               exception::CheckpointException);
}

TEST_F(ChunkedColumnTest, OpenConvertsLegacyWholeChunkDirectoryWithTail) {
  // Legacy directories (no version header) reference whole-chunk slices whose
  // last chunk may be partial. Conversion must load the tail by its actual
  // row count and pad the tail page to a full page, keeping the invariant
  // that every page covers exactly rows_per_page_ slots.
  const std::vector<int64_t> values = {10, 11, 12, 13, 14, 15};
  const size_t payload = values.size() * sizeof(int64_t);
  auto obj = ckp_->CreateRuntimeContainer(payload, MemoryLevel::kInMemory);
  std::memcpy(obj->GetData(), values.data(), payload);
  const std::string obj_id =
      std::filesystem::path(ckp_->Commit(*obj)).filename().string();

  // Hand-encode the legacy blob: rows_per_chunk, chunk_count, row_count,
  // object table, then one ObjectSlice per chunk with its real payload CRC.
  std::vector<char> blob;
  auto append_u64 = [&](uint64_t v) {
    for (int i = 0; i < 8; ++i) {
      blob.push_back(static_cast<char>((v >> (8 * i)) & 0xFF));
    }
  };
  auto append_u32 = [&](uint32_t v) {
    for (int i = 0; i < 4; ++i) {
      blob.push_back(static_cast<char>((v >> (8 * i)) & 0xFF));
    }
  };
  append_u64(kRowsPerChunk);
  append_u64(2);  // chunk_count
  append_u64(6);  // row_count
  append_u64(1);  // object_count
  append_u32(static_cast<uint32_t>(obj_id.size()));
  blob.insert(blob.end(), obj_id.begin(), obj_id.end());
  // Chunk 0 holds a full 4 rows; chunk 1 is a 2-row tail.
  append_u32(0);
  append_u64(0);
  append_u32(4 * sizeof(int64_t));
  append_u32(Crc32c(values.data(), 4 * sizeof(int64_t)));
  append_u32(0);
  append_u64(4 * sizeof(int64_t));
  append_u32(2 * sizeof(int64_t));
  append_u32(Crc32c(values.data() + 4, 2 * sizeof(int64_t)));
  // Legacy blobs have no overall CRC trailer (versioned ones do).

  auto dir = ckp_->CreateRuntimeContainer(blob.size(), MemoryLevel::kInMemory);
  std::memcpy(dir->GetData(), blob.data(), blob.size());
  ModuleDescriptor desc;
  desc.module_type = ChunkedColumn<int64_t>::type_name();
  desc.set_path(kChunkDirPath, ckp_->Commit(*dir));

  ChunkedColumn<int64_t> col(kRowsPerChunk);
  col.Open(*ckp_, desc, MemoryLevel::kInMemory);
  ASSERT_EQ(col.size(), 6u);
  for (size_t i = 0; i < 6; ++i) {
    EXPECT_EQ(col.get_view(i), values[i]);
  }

  // The converted column must round-trip through the versioned format.
  CheckpointManifest meta;
  col.Dump(*ckp_, meta, "col");
  ckp_->FinalizeObjectWriter(meta);
  ChunkedColumn<int64_t> reopened(kRowsPerChunk);
  reopened.Open(*ckp_, *meta.FindModule("col"), MemoryLevel::kInMemory);
  ASSERT_EQ(reopened.size(), 6u);
  for (size_t i = 0; i < 6; ++i) {
    EXPECT_EQ(reopened.get_view(i), values[i]);
  }
}

TEST_F(ChunkedColumnTest, GarbageCollectionRetainsChunkObjects) {
  // Publish a checkpoint holding a chunked column. Its chunk objects are
  // referenced only inside the directory blob, so GC must parse the directory
  // to retain them; otherwise reopening after GC would fail.
  std::shared_ptr<Checkpoint> published;
  {
    auto staging = mgr_.CreateStaging();
    auto ckp = staging.checkpoint();
    CheckpointManifest meta;
    meta.SetSchema(Schema());
    ChunkedColumn<int64_t> col(kRowsPerChunk);
    col.Open(*ckp, ModuleDescriptor{}, MemoryLevel::kInMemory);
    col.resize(12);
    for (size_t i = 0; i < 12; ++i) {
      col.set_value(i, static_cast<int64_t>(i * 5));
    }
    col.Dump(*ckp, meta, "col");
    ckp->SetManifest(std::move(meta));
    published = staging.Publish();
  }

  mgr_.CollectGarbage();

  // Reopen from the published checkpoint; fails if GC reclaimed chunk objects.
  const auto* desc = published->manifest().FindModule("col");
  ASSERT_NE(desc, nullptr);
  ChunkedColumn<int64_t> reopened(kRowsPerChunk);
  reopened.Open(*published, *desc, MemoryLevel::kInMemory);
  ASSERT_EQ(reopened.size(), 12u);
  for (size_t i = 0; i < 12; ++i) {
    EXPECT_EQ(reopened.get_view(i), static_cast<int64_t>(i * 5));
  }
}

TEST_F(ChunkedColumnTest, DumpPacksMultipleChunksIntoOneObject) {
  // A multi-chunk column dumps its (small) chunks into a single packed object
  // plus the directory object, not one object per chunk.
  ChunkedColumn<int64_t> col(kRowsPerChunk);
  col.Open(*ckp_, ModuleDescriptor{}, MemoryLevel::kInMemory);
  col.resize(12);  // 3 chunks of 4 rows
  for (size_t i = 0; i < 12; ++i) {
    col.set_value(i, static_cast<int64_t>(i));
  }
  const size_t before = CountObjectFiles();
  CheckpointManifest meta;
  col.Dump(*ckp_, meta, "col");
  ckp_->FinalizeObjectWriter(meta);
  const size_t after = CountObjectFiles();
  // 3 chunks pack into 1 object + 1 directory object = 2 (per-chunk would be
  // 4).
  EXPECT_EQ(after - before, 2u);
  // Reopen and verify all values survive the packed slice layout.
  ChunkedColumn<int64_t> reopened(kRowsPerChunk);
  reopened.Open(*ckp_, *meta.FindModule("col"), MemoryLevel::kInMemory);
  ASSERT_EQ(reopened.size(), 12u);
  for (size_t i = 0; i < 12; ++i) {
    EXPECT_EQ(reopened.get_view(i), static_cast<int64_t>(i));
  }
}

TEST_F(ChunkedColumnTest, DumpPacksDirtyChunksAcrossColumns) {
  ChunkedColumn<int64_t> left(kRowsPerChunk);
  ChunkedColumn<int64_t> right(kRowsPerChunk);
  left.Open(*ckp_, ModuleDescriptor{}, MemoryLevel::kInMemory);
  right.Open(*ckp_, ModuleDescriptor{}, MemoryLevel::kInMemory);
  left.resize(4);
  right.resize(4);
  left.set_value(0, 11);
  right.set_value(0, 22);

  const size_t before = CountObjectFiles();
  CheckpointManifest meta;
  left.Dump(*ckp_, meta, "left");
  right.Dump(*ckp_, meta, "right");
  ckp_->FinalizeObjectWriter(meta);
  const size_t after = CountObjectFiles();

  // Both dirty chunks share one payload object; each column still owns a
  // separate directory, for three new objects total.
  EXPECT_EQ(after - before, 3u);
}

TEST_F(ChunkedColumnTest, PublishedCheckpointReopensAfterDirectoryMove) {
  auto staging = mgr_.CreateStaging();
  auto checkpoint = staging.checkpoint();
  CheckpointManifest meta;
  meta.SetSchema(Schema());
  ChunkedColumn<int64_t> col(kRowsPerChunk);
  col.Open(*checkpoint, ModuleDescriptor{}, MemoryLevel::kInMemory);
  col.resize(4);
  col.set_value(0, 1234);
  col.Dump(*checkpoint, meta, "col");
  checkpoint->SetManifest(std::move(meta));
  auto published = staging.Publish();

  const auto moved_dir =
      temp_dir_.parent_path() / (temp_dir_.filename().string() + "_moved");
  std::filesystem::remove_all(moved_dir);
  std::filesystem::copy(temp_dir_, moved_dir,
                        std::filesystem::copy_options::recursive);
  published.reset();
  mgr_.Close();
  ckp_.reset();
  std::filesystem::remove_all(temp_dir_);

  CheckpointManager reopened_manager;
  reopened_manager.Open(moved_dir.string(), false);
  auto reopened_checkpoint = reopened_manager.Current();
  ASSERT_NE(reopened_checkpoint, nullptr);
  const auto* desc = reopened_checkpoint->manifest().FindModule("col");
  ASSERT_NE(desc, nullptr);
  ChunkedColumn<int64_t> reopened(kRowsPerChunk);
  reopened.Open(*reopened_checkpoint, *desc, MemoryLevel::kInMemory);
  EXPECT_EQ(reopened.get_view(0), 1234);

  reopened_manager.Close();
  std::filesystem::remove_all(moved_dir);
}

TEST_F(ChunkedColumnTest, FromLegacyCopiesTypedColumnValues) {
  // A legacy TypedColumn is converted into a fresh, writable ChunkedColumn.
  TypedColumn<int64_t> legacy;
  legacy.Open(*ckp_, ModuleDescriptor{}, MemoryLevel::kInMemory);
  legacy.resize(10);
  for (size_t i = 0; i < 10; ++i) {
    legacy.set_value(i, static_cast<int64_t>(i * 11));
  }
  auto chunked =
      ChunkedColumn<int64_t>::FromLegacy(*ckp_, MemoryLevel::kInMemory, legacy);
  ASSERT_NE(chunked, nullptr);
  ASSERT_EQ(chunked->size(), 10u);
  for (size_t i = 0; i < 10; ++i) {
    EXPECT_EQ(chunked->get_view(i), static_cast<int64_t>(i * 11));
  }
  // The converted column is writable and persists like any ChunkedColumn.
  chunked->set_value(3, 999);
  EXPECT_EQ(chunked->get_view(3), 999);
  CheckpointManifest meta;
  chunked->Dump(*ckp_, meta, "conv");
  ckp_->FinalizeObjectWriter(meta);
  ChunkedColumn<int64_t> reopened(kRowsPerChunk);
  reopened.Open(*ckp_, *meta.FindModule("conv"), MemoryLevel::kInMemory);
  EXPECT_EQ(reopened.get_view(3), 999);
}

TEST_F(ChunkedColumnTest, TableMigratesLegacyFixedPropertyColumns) {
  Table table({"value", "name"},
              {DataType(DataTypeId::kInt64), DataType(DataTypeId::kVarchar)});
  table.Init(*ckp_, MemoryLevel::kInMemory);

  auto legacy = std::make_unique<TypedColumn<int64_t>>();
  legacy->Open(*ckp_, ModuleDescriptor{}, MemoryLevel::kInMemory);
  legacy->resize(3);
  legacy->set_value(0, 11);
  legacy->set_value(1, 22);
  legacy->set_value(2, 33);
  table.SetColumn(0, std::move(legacy));

  EXPECT_TRUE(table.HasLegacyPropertyColumns());
  EXPECT_TRUE(
      table.MigrateLegacyPropertyColumns(*ckp_, MemoryLevel::kInMemory));
  EXPECT_FALSE(table.HasLegacyPropertyColumns());
  EXPECT_FALSE(
      table.MigrateLegacyPropertyColumns(*ckp_, MemoryLevel::kInMemory));

  auto* chunked =
      dynamic_cast<ChunkedColumn<int64_t>*>(table.get_column_by_id(0));
  ASSERT_NE(chunked, nullptr);
  EXPECT_EQ(chunked->get_view(0), 11);
  EXPECT_EQ(chunked->get_view(1), 22);
  EXPECT_EQ(chunked->get_view(2), 33);
  EXPECT_EQ(
      dynamic_cast<ChunkedColumn<std::string_view>*>(table.get_column_by_id(1)),
      nullptr);
}

TEST_F(ChunkedColumnTest, CloneIsolatesOriginalWritesBeforeCloneWrites) {
  ChunkedColumn<int64_t> original(4);
  original.Open(*ckp_, ModuleDescriptor{}, MemoryLevel::kInMemory);
  original.resize(8);
  original.set_value(1, 11);
  original.set_value(5, 55);
  auto module = original.Clone();
  auto* clone = dynamic_cast<ChunkedColumn<int64_t>*>(module.get());
  original.set_value(1, 22);
  EXPECT_EQ(clone->get_view(1), 11);
  clone->set_value(5, 66);
  EXPECT_EQ(original.get_view(5), 55);
}

TEST_F(ChunkedColumnTest, NullAndWriteBoundsMatchLegacyColumn) {
  ChunkedColumn<int64_t> col(4);
  TypedColumn<int64_t> legacy;
  col.Open(*ckp_, ModuleDescriptor{}, MemoryLevel::kInMemory);
  legacy.Open(*ckp_, ModuleDescriptor{}, MemoryLevel::kInMemory);
  col.resize(3);
  legacy.resize(3);
  const Value null{DataType(DataTypeId::kInt64)};
  col.set_value(1, 42);
  legacy.set_value(1, 42);
  col.set_any(1, null, true);
  legacy.set_any(1, null, true);
  EXPECT_EQ(col.get_view(1), legacy.get_view(1));
  EXPECT_EQ(col.get_view(1), 0);
  EXPECT_THROW(col.set_value(3, 1), exception::RuntimeError);
  EXPECT_THROW(col.set_any(3, Value::INT64(1), false), exception::RuntimeError);
}

TEST_F(ChunkedColumnTest, SparseUpdateCopiesAndPersistsOnePhysicalPage) {
  ChunkedColumn<int64_t> original;
  original.Open(*ckp_, ModuleDescriptor{}, MemoryLevel::kInMemory);
  original.resize(original.rows_per_chunk());
  original.set_value(0, 100);
  CheckpointManifest first;
  original.Dump(*ckp_, first, "col");
  ckp_->FinalizeObjectWriter(first);
  auto module = original.Clone();
  auto* clone = dynamic_cast<ChunkedColumn<int64_t>*>(module.get());
  clone->set_value(1, 200);
  clone->set_value(2, 300);
  EXPECT_EQ(clone->cow_bytes_copied(), 4096u);
  EXPECT_EQ(original.get_view(1), 0);
  CheckpointManifest second;
  clone->Dump(*ckp_, second, "col");
  ckp_->FinalizeObjectWriter(second);
  auto decode = [&](const CheckpointManifest& meta) {
    auto path = meta.FindModule("col")->get_path(kChunkDirPath);
    auto file = ckp_->OpenFile(*path, MemoryLevel::kInMemory);
    return DecodeChunkDirectory(file->GetData(), file->GetDataSize());
  };
  const auto before = decode(first), after = decode(second);
  ASSERT_EQ(after.pages.size(), 64u);
  EXPECT_NE(before.object_ids[before.pages[0].prefix.object_id],
            after.object_ids[after.pages[0].prefix.object_id]);
  for (size_t i = 1; i < after.pages.size(); ++i)
    EXPECT_EQ(before.object_ids[before.pages[i].prefix.object_id],
              after.object_ids[after.pages[i].prefix.object_id]);
  auto payload =
      ckp_->OpenObject(after.object_ids[after.pages[0].prefix.object_id],
                       MemoryLevel::kInMemory);
  EXPECT_EQ(payload->GetDataSize(), 4096u);
}

TEST_F(ChunkedColumnTest, PayloadChecksumIsCheckedOnFirstAccess) {
  ChunkedColumn<int64_t> col(4);
  col.Open(*ckp_, ModuleDescriptor{}, MemoryLevel::kInMemory);
  col.resize(4);
  col.set_value(0, 99);
  CheckpointManifest meta;
  col.Dump(*ckp_, meta, "col");
  ckp_->FinalizeObjectWriter(meta);
  auto desc = *meta.FindModule("col");
  auto file =
      ckp_->OpenFile(*desc.get_path(kChunkDirPath), MemoryLevel::kInMemory);
  auto dir = DecodeChunkDirectory(file->GetData(), file->GetDataSize());
  auto object = ckp_->OpenObject(dir.object_ids[0], MemoryLevel::kInMemory);
  // Corrupt only the test's private mapping, leaving directory metadata valid.
  static_cast<char*>(object->GetData())[dir.pages[0].prefix.offset] ^= 1;
  ChunkedColumn<int64_t> reopened;
  EXPECT_NO_THROW(reopened.Open(*ckp_, desc, MemoryLevel::kSyncToFile));
  EXPECT_THROW(reopened.get_view(0), exception::CheckpointException);
}

TEST_F(ChunkedColumnTest, SpanReaderVerifiesEachNewPage) {
  ChunkedColumn<int64_t> col(4);
  col.Open(*ckp_, ModuleDescriptor{}, MemoryLevel::kInMemory);
  col.resize(8);
  col.set_value(0, 99);
  CheckpointManifest meta;
  col.Dump(*ckp_, meta, "col");
  ckp_->FinalizeObjectWriter(meta);
  auto desc = *meta.FindModule("col");
  auto file =
      ckp_->OpenFile(*desc.get_path(kChunkDirPath), MemoryLevel::kInMemory);
  auto dir = DecodeChunkDirectory(file->GetData(), file->GetDataSize());
  const auto& slice = dir.pages[1].prefix;
  auto object =
      ckp_->OpenObject(dir.object_ids[slice.object_id], MemoryLevel::kInMemory);
  static_cast<char*>(object->GetData())[slice.offset] ^= 1;
  ChunkedColumn<int64_t> reopened(4);
  reopened.Open(*ckp_, desc, MemoryLevel::kInMemory);
  auto ref = std::dynamic_pointer_cast<TypedRefColumn<int64_t>>(
      CreateRefColumn(reopened));
  PropertyColumnReader<int64_t> reader(*ref);
  EXPECT_EQ(reader.get_view(0), 99);
  EXPECT_EQ(reader.get_view(3), 0);
  EXPECT_THROW(reader.get_view(4), exception::CheckpointException);
}

TEST_F(ChunkedColumnTest, BuilderOwnsSegmentsAfterColumnDestruction) {
  CheckpointManifest meta;
  {
    ChunkedColumn<int64_t> col(4);
    col.Open(*ckp_, ModuleDescriptor{}, MemoryLevel::kInMemory);
    col.resize(4);
    col.set_value(0, 99);
    col.Dump(*ckp_, meta, "col");
  }
  ckp_->FinalizeObjectWriter(meta);
  ChunkedColumn<int64_t> reopened(4);
  reopened.Open(*ckp_, *meta.FindModule("col"), MemoryLevel::kInMemory);
  EXPECT_EQ(reopened.get_view(0), 99);
}

TEST_F(ChunkedColumnTest, ConcurrentAppendAfterReopenUsesStableSuffix) {
  ChunkedColumn<int64_t> original;
  original.Open(*ckp_, ModuleDescriptor{}, MemoryLevel::kInMemory);
  original.resize(1024);
  original.set_value(0, 123);
  CheckpointManifest first;
  original.Dump(*ckp_, first, "col");
  ckp_->FinalizeObjectWriter(first);
  ChunkedColumn<int64_t> col;
  col.Open(*ckp_, *first.FindModule("col"), MemoryLevel::kInMemory);
  col.PrepareForInsert(1);
  const size_t copied = col.cow_bytes_copied();
  std::atomic<bool> done{false};
  std::atomic<bool> reader_ok{true};
  std::thread reader([&] {
    while (!done.load())
      if (col.get_view(0) != 123)
        reader_ok.store(false);
  });
  std::atomic<size_t> ready{0};
  std::vector<std::thread> writers;
  for (size_t thread = 0; thread < 8; ++thread)
    writers.emplace_back([&, thread] {
      ready.fetch_add(1);
      while (ready.load() != 8)
        std::this_thread::yield();
      for (size_t row = thread + 1; row < 1024; row += 8)
        col.set_any(row, Value::INT64(row * 10), false);
    });
  for (auto& writer : writers)
    writer.join();
  done.store(true);
  reader.join();
  EXPECT_TRUE(reader_ok.load());
  EXPECT_EQ(col.cow_bytes_copied(), copied);
  for (size_t row = 1; row < 1024; ++row)
    EXPECT_EQ(col.get_view(row), row * 10);
  CheckpointManifest second;
  col.Dump(*ckp_, second, "col");
  ckp_->FinalizeObjectWriter(second);
  ChunkedColumn<int64_t> reopened;
  reopened.Open(*ckp_, *second.FindModule("col"), MemoryLevel::kInMemory);
  EXPECT_EQ(reopened.get_view(0), 123);
  for (size_t row = 1; row < 1024; ++row)
    EXPECT_EQ(reopened.get_view(row), row * 10);
}

// Column-level manifestation of W18: a reader pins a Clone (an independent page
// directory that shares the frozen, shared-mapped segments) while a writer
// performs covering writes on the original. Every write to a shared segment
// forks it via Copy() instead of mutating in place, so the pinned snapshot stays
// stable. Under TSAN this exercises the concurrent surfaces the design admits:
// the shared_ptr refcount, Segment::Verify()'s call_once/atomic on the shared
// frozen source (hit by both the reader and the writer's pre-copy Verify), and
// the fork-on-first-covering-write of a shared segment. Concurrent covering
// writes to the *same* column object are deliberately not exercised: they are
// single-writer by admission control (SnapshotCowWriteTransaction).
TEST_F(ChunkedColumnTest, ConcurrentCowForkUnderPinnedSnapshotReader) {
  ChunkedColumn<int64_t> original(4);
  original.Open(*ckp_, ModuleDescriptor{}, MemoryLevel::kInMemory);
  original.resize(1024);
  for (size_t row = 0; row < 1024; ++row)
    original.set_value(row, static_cast<int64_t>(row));
  CheckpointManifest first;
  original.Dump(*ckp_, first, "col");
  ckp_->FinalizeObjectWriter(first);

  // Reopen so segments are frozen and shared-mapped from immutable objects.
  ChunkedColumn<int64_t> col(4);
  col.Open(*ckp_, *first.FindModule("col"), MemoryLevel::kInMemory);

  // Pinned snapshot: independent page directory sharing the frozen segments.
  auto snapshot_module = col.Clone();
  auto& snapshot = dynamic_cast<ChunkedColumn<int64_t>&>(*snapshot_module);

  std::atomic<bool> done{false};
  std::atomic<bool> reader_ok{true};
  std::thread reader([&] {
    while (!done.load(std::memory_order_relaxed)) {
      for (size_t row = 0; row < 1024; ++row) {
        if (snapshot.get_view(row) != static_cast<int64_t>(row)) {
          reader_ok.store(false);
          break;
        }
      }
    }
  });

  // Writer forks each shared segment via a covering write on the original.
  for (size_t row = 0; row < 1024; ++row)
    col.set_value(row, static_cast<int64_t>(row * 10));

  done.store(true);
  reader.join();

  EXPECT_TRUE(reader_ok.load());
  for (size_t row = 0; row < 1024; ++row) {
    EXPECT_EQ(col.get_view(row), static_cast<int64_t>(row * 10));
    EXPECT_EQ(snapshot.get_view(row), static_cast<int64_t>(row));
  }
}

TEST_F(ChunkedColumnTest, AppendFrontAdvancesAcrossCheckpointAndCow) {
  ChunkedColumn<int64_t> col(4);
  col.Open(*ckp_, ModuleDescriptor{}, MemoryLevel::kInMemory);
  col.resize(12);
  col.set_value(0, 10);
  col.PrepareForInsert(1);
  col.set_any(1, Value::INT64(20), false);
  auto snapshot = col.Clone();
  col.PrepareForInsert(2);
  col.set_any(2, Value::INT64(30), false);
  col.PrepareForInsert(4);
  col.set_any(4, Value::INT64(40), false);
  EXPECT_EQ(dynamic_cast<ChunkedColumn<int64_t>*>(snapshot.get())->get_view(0),
            10);
  EXPECT_EQ(dynamic_cast<ChunkedColumn<int64_t>*>(snapshot.get())->get_view(1),
            20);
  CheckpointManifest meta;
  col.Dump(*ckp_, meta, "col");
  ckp_->FinalizeObjectWriter(meta);
  ChunkedColumn<int64_t> reopened(4);
  reopened.Open(*ckp_, *meta.FindModule("col"), MemoryLevel::kInMemory);
  EXPECT_EQ(reopened.get_view(0), 10);
  EXPECT_EQ(reopened.get_view(1), 20);
  EXPECT_EQ(reopened.get_view(2), 30);
  EXPECT_EQ(reopened.get_view(4), 40);
}

TEST_F(ChunkedColumnTest, SharedObjectsAreMappedOnceInDiskMode) {
  ChunkedColumn<int32_t> left(4);
  ChunkedColumn<int64_t> right(4);
  left.Open(*ckp_, ModuleDescriptor{}, MemoryLevel::kSyncToFile);
  right.Open(*ckp_, ModuleDescriptor{}, MemoryLevel::kSyncToFile);
  left.resize(4);
  right.resize(4);
  left.set_value(0, 10);
  right.set_value(0, 20);
  CheckpointManifest meta;
  left.Dump(*ckp_, meta, "left");
  right.Dump(*ckp_, meta, "right");
  ckp_->FinalizeObjectWriter(meta);
  auto file = ckp_->OpenFile(*meta.FindModule("left")->get_path(kChunkDirPath),
                             MemoryLevel::kInMemory);
  auto d = DecodeChunkDirectory(file->GetData(), file->GetDataSize());
  const auto& id = d.object_ids[0];
  auto first = ckp_->OpenObject(id, MemoryLevel::kSyncToFile);
  auto second = ckp_->OpenObject(id, MemoryLevel::kSyncToFile);
  EXPECT_EQ(first.get(), second.get());
  EXPECT_EQ(first->GetContainerType(), ContainerType::kFilePrivateMMap);
  ChunkedColumn<int64_t> reopened(4);
  reopened.Open(*ckp_, *meta.FindModule("right"), MemoryLevel::kSyncToFile);
  EXPECT_EQ(reopened.get_view(0), 20);
}

TEST_F(ChunkedColumnTest, HugePageChunksShareOneArena) {
  auto one =
      ckp_->AllocateChunkBuffer(256 * 1024, MemoryLevel::kHugePagePreferred);
  auto two =
      ckp_->AllocateChunkBuffer(256 * 1024, MemoryLevel::kHugePagePreferred);
  EXPECT_EQ(one.container.get(), two.container.get());
  EXPECT_EQ(two.offset - one.offset, 256u * 1024);
  EXPECT_EQ(one.container->GetDataSize(), 2u * 1024 * 1024);
}

TEST(ChunkDirTest, VersionedDirectoryRejectsMetadataCorruption) {
  ChunkDirectory d;
  d.row_width = 8;
  d.rows_per_chunk = 4;
  d.rows_per_page = 4;
  d.row_count = 4;
  d.object_ids = {"object"};
  ChunkDirectoryPage page;
  page.prefix_rows = 4;
  page.prefix.length = 32;
  d.pages = {page};
  auto blob = EncodeChunkDirectory(d);
  auto decoded = DecodeChunkDirectory(blob.data(), blob.size());
  EXPECT_EQ(decoded.row_width, 8u);
  blob[24] ^= 1;
  EXPECT_THROW(DecodeChunkDirectory(blob.data(), blob.size()),
               exception::CheckpointException);
  EXPECT_THROW(ExtractChunkDirObjectIds(blob.data(), blob.size()),
               exception::CheckpointException);
}

TEST(ChunkedColumnFactoryTest, RegisteredForReopen) {
  // A checkpoint storing chunked_column<T> modules must be reopenable via the
  // ModuleFactory, so ChunkedColumn<T> has to be registered for creation.
  auto& factory = ModuleFactory::instance();
  ChunkedColumn<int64_t> probe(4);
  auto module = factory.Create(probe.ModuleTypeName());
  ASSERT_NE(module, nullptr);
  EXPECT_NE(dynamic_cast<ChunkedColumn<int64_t>*>(module.get()), nullptr);
}

TEST_F(ChunkedColumnTest, AbortedCowKeepsLiveAppendWritable) {
  ChunkedColumn<int64_t> live;
  live.Open(*ckp_, ModuleDescriptor{}, MemoryLevel::kInMemory);
  live.resize(1024);
  live.PrepareForInsert(0);
  live.set_any(0, Value::INT64(10), false);
  {
    auto private_copy = live.Clone();
    auto& clone = dynamic_cast<ChunkedColumn<int64_t>&>(*private_copy);
    clone.PrepareForInsert(1);
    EXPECT_EQ(clone.cow_bytes_copied(), 4096u);
  }
  EXPECT_NO_THROW(live.set_any(1, Value::INT64(20), false));
  EXPECT_EQ(live.get_view(0), 10);
  EXPECT_EQ(live.get_view(1), 20);
}

TEST_F(ChunkedColumnTest, CleanLegacyMigrationPreservesPinnedVertexSnapshot) {
  Schema schema;
  schema.AddVertexLabel("v", {DataType::INT64}, {"val"},
                        {std::make_tuple(DataType::INT64, "id", 0)}, 4096, "",
                        {Value::INT64(0)});
  VertexTable table(schema.get_vertex_schema(0));
  table.Init(ckp_, MemoryLevel::kInMemory);
  table.EnsureCapacity(16);
  auto legacy = std::make_unique<TypedColumn<int64_t>>();
  legacy->Open(*ckp_, ModuleDescriptor{}, MemoryLevel::kInMemory);
  legacy->resize(table.Capacity());
  table.SetColumn(0, std::move(legacy));
  vid_t vid;
  ASSERT_TRUE(
      table.AddVertex(Value::INT64(1), {Value::INT64(99)}, vid, 1, false));
  auto first = mgr_.CreateStaging();
  auto checkpoint = first.checkpoint();
  CheckpointManifest meta(1);
  meta.SetSchema(schema);
  std::unique_ptr<ColumnBase> keys;
  std::unique_ptr<TypedColumn<vid_t>> indices;
  meta.SetModule(VertexTable::KeyIndexer("v"),
                 table.get_indexer().Dump(*checkpoint, keys, indices));
  keys->Dump(*checkpoint, meta, VertexTable::KeyKeys("v"));
  indices->Dump(*checkpoint, meta, VertexTable::KeyIndices("v"));
  table.TakeVertexTimestamp()->Dump(*checkpoint, meta,
                                    VertexTable::KeyVertexTimestamp("v"));
  table.TakeTable()->get_column_by_id(0)->Dump(
      *checkpoint, meta, VertexTable::KeyProperty("v", 0));
  checkpoint->SetManifest(std::move(meta));
  first.Publish();
  PropertyGraph base;
  base.Open(checkpoint, MemoryLevel::kInMemory);
  EXPECT_FALSE(base.dirty_tracker().IsVertexDirty(0));
  auto& base_keys = dynamic_cast<const TypedColumn<int64_t>&>(
      base.get_vertex_table(0).get_indexer().get_keys());
  ASSERT_NE(base_keys.data(), nullptr);
  auto private_graph = base.Clone();
  auto detached = CowDetachState::FromSchema(private_graph->schema());
  private_graph->DetachDirtyModulesForCheckpoint(detached);
  auto second = mgr_.CreateStaging();
  auto migrated = second.checkpoint();
  private_graph->DumpDirtyAndReopen(migrated, 2);
  ASSERT_NE(base_keys.data(), nullptr)
      << "A clean legacy table migration closed the pinned base graph's keys";
  second.Publish();
  EXPECT_EQ(base_keys.get_view(0), 1);
  EXPECT_EQ(base.GetVertexPropertyColumn(0, 0)->get_any(0).GetValue<int64_t>(),
            99);
  EXPECT_TRUE(base.get_vertex_table(0).get_table().HasLegacyPropertyColumns());
  EXPECT_FALSE(private_graph->get_vertex_table(0)
                   .get_table()
                   .HasLegacyPropertyColumns());
  const auto key = VertexTable::KeyKeys("v");
  EXPECT_EQ(checkpoint->manifest().FindModule(key)->paths(),
            migrated->manifest().FindModule(key)->paths());
  PropertyGraph reopened;
  reopened.Open(migrated, MemoryLevel::kInMemory);
  EXPECT_EQ(
      reopened.GetVertexPropertyColumn(0, 0)->get_any(0).GetValue<int64_t>(),
      99);
  EXPECT_TRUE(reopened.get_lid(0, Value::INT64(1), vid, 2));
}

TEST_F(ChunkedColumnTest, CleanColumnsRebindAfterCheckpointRotation) {
  Schema schema;
  schema.AddVertexLabel("v", {DataType::INT64}, {"val"},
                        {std::make_tuple(DataType::INT64, "id", 0)}, 4096, "",
                        {Value::INT64(0)});
  schema.AddEdgeLabel("v", "v", "e", {DataType::INT64, DataType::INT64},
                      {"a", "b"});
  CheckpointManifest initial;
  initial.SetSchema(schema);
  ckp_->SetManifest(std::move(initial));
  PropertyGraph graph;
  graph.Open(ckp_, MemoryLevel::kInMemory);
  ASSERT_TRUE(graph.EnsureCapacity(0, 16).ok());
  vid_t vid;
  ASSERT_TRUE(
      graph.AddVertex(0, Value::INT64(1), {Value::INT64(99)}, vid, 1).ok());
  Allocator alloc(MemoryLevel::kInMemory, temp_dir_.string());
  int32_t offset;
  const void* property;
  ASSERT_TRUE(graph
                  .AddEdge(0, vid, 0, vid, 0,
                           {Value::INT64(11), Value::INT64(22)}, 1, alloc,
                           offset, property, true)
                  .ok());
  // Low-level PropertyGraph writes leave dirty tracking to their caller.
  graph.MarkVertexTableDirty(0);
  graph.MarkEdgeTableDirty(0, 0, 0);
  auto first = mgr_.CreateStaging();
  graph.Compact();
  graph.DumpAndClear(first.checkpoint());
  auto old = first.Publish();
  graph.Open(old, MemoryLevel::kInMemory);
  auto* before = graph.get_vertex_table(0).get_table().get_column_by_id(0);
  auto* edge_before =
      graph.get_edge_table(0, 0, 0).table()->get_column_by_id(0);
  std::weak_ptr<Checkpoint> old_weak = old;
  auto second = mgr_.CreateStaging();
  graph.DumpDirtyAndReopen(second.checkpoint(), 2);
  second.Publish();
  old.reset();
  auto* after = graph.get_vertex_table(0).get_table().get_column_by_id(0);
  ASSERT_EQ(before, after);
  ASSERT_EQ(edge_before,
            graph.get_edge_table(0, 0, 0).table()->get_column_by_id(0));
  ASSERT_TRUE(old_weak.expired());
  // Exercise the normal post-rotation Insert -> next COW preparation sequence.
  ASSERT_TRUE(
      graph.AddVertex(0, Value::INT64(2), {Value::INT64(100)}, vid, 3).ok());
  ASSERT_TRUE(graph
                  .AddEdge(0, vid, 0, vid, 0,
                           {Value::INT64(33), Value::INT64(44)}, 3, alloc,
                           offset, property)
                  .ok());
  graph.MarkVertexTableDirty(0);
  graph.MarkEdgeTableDirty(0, 0, 0);
  auto next = graph.Clone();
  EXPECT_NO_THROW(next->PrepareForInsert());
  EXPECT_EQ(
      next->GetVertexPropertyColumn(0, 0)->get_any(vid).GetValue<int64_t>(),
      100);
  EXPECT_EQ(next->get_edge_table(0, 0, 0)
                .table()
                ->get_column_by_id(0)
                ->get_any(1)
                .GetValue<int64_t>(),
            33);
  next.reset();
  auto third = mgr_.CreateStaging();
  graph.DumpDirtyAndReopen(third.checkpoint(), 4);
  auto latest = third.Publish();
  PropertyGraph reopened;
  reopened.Open(latest, MemoryLevel::kInMemory);
  EXPECT_EQ(
      reopened.GetVertexPropertyColumn(0, 0)->get_any(vid).GetValue<int64_t>(),
      100);
}

TEST_F(ChunkedColumnTest, CleanLegacyMigrationPreservesPinnedEdgeSnapshot) {
  Schema schema;
  schema.AddVertexLabel("v", {}, {},
                        {std::make_tuple(DataType::INT64, "id", 0)}, 4096);
  schema.AddEdgeLabel("v", "v", "e", {DataType::INT64, DataType::INT64},
                      {"a", "b"}, EdgeStrategy::kMultiple,
                      EdgeStrategy::kMultiple, true, true);
  VertexTable vertices(schema.get_vertex_schema(0));
  vertices.Init(ckp_, MemoryLevel::kInMemory);
  vertices.EnsureCapacity(16);
  vid_t vid;
  ASSERT_TRUE(vertices.AddVertex(Value::INT64(1), {}, vid, 1, false));
  ASSERT_TRUE(vertices.AddVertex(Value::INT64(2), {}, vid, 1, false));
  EdgeTable edges(schema.get_edge_schema(0, 0, 0));
  edges.Init(ckp_, MemoryLevel::kInMemory);
  edges.EnsureCapacity(16, 16, 16);
  for (size_t i = 0; i < 2; ++i) {
    auto legacy = std::make_unique<TypedColumn<int64_t>>();
    legacy->Open(*ckp_, ModuleDescriptor{}, MemoryLevel::kInMemory);
    legacy->resize(edges.Capacity());
    edges.table()->SetColumn(i, std::move(legacy));
  }
  Allocator alloc(MemoryLevel::kInMemory, temp_dir_.string());
  edges.AddEdge(0, 1, {Value::INT64(11), Value::INT64(22)}, 1, alloc, false);
  // Write the legacy format explicitly: DisassembleTo itself migrates columns.
  auto first = mgr_.CreateStaging();
  auto old = first.checkpoint();
  CheckpointManifest meta(1);
  meta.SetSchema(schema);
  ModuleBroker modules;
  vertices.DisassembleTo(modules, meta, *old);
  modules.SetModule(EdgeTable::KeyOutCsr("v", "e", "v"), edges.TakeOutCsr());
  modules.SetModule(EdgeTable::KeyInCsr("v", "e", "v"), edges.TakeInCsr());
  for (size_t i = 0; i < 2; ++i)
    edges.table()->get_column_by_id(i)->Dump(
        *old, meta, EdgeTable::KeyProperty("v", "e", "v", i));
  meta.SetScalar(EdgeTable::ScalarKey("v", "e", "v", "table_idx"), "1");
  meta.SetScalar(EdgeTable::ScalarKey("v", "e", "v", "capacity"),
                 std::to_string(edges.Capacity()));
  modules.Dump(*old, meta);
  old->SetManifest(std::move(meta));
  first.Publish();
  PropertyGraph base;
  base.Open(old, MemoryLevel::kInMemory);
  auto next = base.Clone();
  auto detached = CowDetachState::FromSchema(next->schema());
  next->DetachDirtyModulesForCheckpoint(detached);
  auto second = mgr_.CreateStaging();
  auto migrated = second.checkpoint();
  next->DumpDirtyAndReopen(migrated, 2);
  second.Publish();
  for (const auto& key : {EdgeTable::KeyOutCsr("v", "e", "v"),
                          EdgeTable::KeyInCsr("v", "e", "v")}) {
    EXPECT_EQ(old->manifest().FindModule(key)->paths(),
              migrated->manifest().FindModule(key)->paths());
  }
  EXPECT_TRUE(base.get_edge_table(0, 0, 0).table()->HasLegacyPropertyColumns());
  EXPECT_FALSE(
      next->get_edge_table(0, 0, 0).table()->HasLegacyPropertyColumns());
  PropertyGraph reopened;
  reopened.Open(migrated, MemoryLevel::kInMemory);
  for (const auto* graph : {&base, next.get(), &reopened}) {
    const auto& edge = graph->get_edge_table(0, 0, 0);
    auto outgoing = edge.get_outgoing_view(MAX_TIMESTAMP).get_edges(0);
    auto incoming = edge.get_incoming_view(MAX_TIMESTAMP).get_edges(1);
    ASSERT_NE(outgoing.begin(), outgoing.end());
    ASSERT_NE(incoming.begin(), incoming.end());
    EXPECT_EQ(outgoing.begin().get_vertex(), 1);
    EXPECT_EQ(incoming.begin().get_vertex(), 0);
    EXPECT_EQ(edge.table()->get_column_by_id(0)->get_any(0).GetValue<int64_t>(),
              11);
    EXPECT_EQ(edge.table()->get_column_by_id(1)->get_any(0).GetValue<int64_t>(),
              22);
  }
}

TEST_F(ChunkedColumnTest, SpanReadersCoverSplitPagesAndIndependentTraversal) {
  ChunkedColumn<int64_t> chunked(4);
  chunked.Open(*ckp_, ModuleDescriptor{}, MemoryLevel::kInMemory);
  chunked.resize(11);
  for (size_t row = 0; row < chunked.size(); ++row)
    chunked.set_value(row, row * 10);
  chunked.PrepareForInsert(2);  // First page contains a prefix and a suffix.
  auto ref = std::dynamic_pointer_cast<TypedRefColumn<int64_t>>(
      CreateRefColumn(chunked));
  ASSERT_NE(ref, nullptr);
  EXPECT_EQ(ref->get_span(0).size(), 2);
  EXPECT_EQ(ref->get_span(2).size(), 2);
  EXPECT_TRUE(ref->get_span(11).empty());
  EXPECT_THROW(ref->get_span(12), exception::RuntimeError);
  std::atomic<bool> correct{true};
  auto scan = [&](bool reverse) {
    PropertyColumnReader<int64_t> reader(*ref);
    for (size_t repeat = 0; repeat < 100; ++repeat)
      for (size_t i = 0; i < 11; ++i) {
        size_t row = reverse ? 10 - i : i;
        if (reader.get_view(row) != static_cast<int64_t>(row * 10))
          correct.store(false);
      }
  };
  std::thread forward(scan, false), backward(scan, true);
  forward.join();
  backward.join();
  EXPECT_TRUE(correct.load());
  PropertyColumnReader<int64_t> gathered(*ref);
  for (size_t row : {3, 1, 9, 5, 8, 0, 10})
    EXPECT_EQ(gathered.get_view(row), row * 10);
}

// ---------------------------------------------------------------------------
// DEMO (not an assertion suite): materialize real on-disk checkpoint files
// under /tmp/neug_chunk_demo and print them, so the object-file layout can be
// inspected directly. It shows:
//   (1) two columns packing into ONE shared payload object (cross-column);
//   (2) an update writing ONLY the dirty 4 KiB page into a NEW object, while
//       the clean column keeps referencing the OLD object by ObjectSlice;
//   (3) the per-column chunk directory mapping each physical page to a slice.
TEST(ChunkDiskDemo, PrintRealCheckpointFiles) {
  namespace fs = std::filesystem;
  const fs::path demo = "/tmp/neug_chunk_demo";
  std::error_code ec;
  fs::remove_all(demo, ec);
  fs::create_directories(demo, ec);
  const fs::path objects = demo / "checkpoint" / "objects";

  auto list_objects = [&](const char* tag) {
    std::printf("\n[objects/] %s\n", tag);
    std::vector<fs::path> files;
    for (auto& e : fs::directory_iterator(objects, ec))
      if (e.is_regular_file()) files.push_back(e.path());
    std::sort(files.begin(), files.end());
    uint64_t total = 0;
    for (auto& f : files) {
      const auto sz = fs::file_size(f, ec);
      total += sz;
      std::printf("  %-38s %8llu bytes\n", f.filename().string().c_str(),
                  static_cast<unsigned long long>(sz));
    }
    std::printf("  -> %zu files, %llu bytes total\n", files.size(),
                static_cast<unsigned long long>(total));
  };

  auto dump_dir = [&](const std::shared_ptr<Checkpoint>& ckp,
                      const std::string& col) {
    const auto* mod = ckp->manifest().FindModule(col);
    auto path = mod->get_path(kChunkDirPath);
    auto file = ckp->OpenFile(*path, MemoryLevel::kInMemory);
    auto d = DecodeChunkDirectory(file->GetData(), file->GetDataSize());
    std::printf("  column '%s': row_width=%u rows_per_chunk=%llu "
                "rows_per_page=%llu row_count=%llu\n",
                col.c_str(), d.row_width,
                static_cast<unsigned long long>(d.rows_per_chunk),
                static_cast<unsigned long long>(d.rows_per_page),
                static_cast<unsigned long long>(d.row_count));
    for (size_t i = 0; i < d.object_ids.size(); ++i)
      std::printf("      object_ids[%zu] = %s\n", i, d.object_ids[i].c_str());
    for (size_t i = 0; i < d.pages.size(); ++i) {
      const auto& p = d.pages[i];
      std::printf("      page[%zu]: prefix_rows=%u -> ObjectSlice{obj_idx=%llu,"
                  " offset=%llu, length=%u, crc=%08x}\n",
                  i, p.prefix_rows,
                  static_cast<unsigned long long>(p.prefix.object_id),
                  static_cast<unsigned long long>(p.prefix.offset),
                  static_cast<unsigned>(p.prefix.length),
                  static_cast<unsigned>(p.prefix.crc32c));
    }
  };

  CheckpointManager mgr;
  mgr.Open(demo.string());

  // ---- Checkpoint 1: two INT64 columns dumped into ONE checkpoint ----
  std::shared_ptr<Checkpoint> cp1;
  {
    auto staging = mgr.CreateStaging();
    auto ckp = staging.checkpoint();
    CheckpointManifest meta;
    meta.SetSchema(Schema());
    ChunkedColumn<int64_t> age, score;  // production sizing => 4 KiB pages
    age.Open(*ckp, ModuleDescriptor{}, MemoryLevel::kInMemory);
    score.Open(*ckp, ModuleDescriptor{}, MemoryLevel::kInMemory);
    age.resize(8);
    score.resize(8);
    for (size_t i = 0; i < 8; ++i) {
      age.set_value(i, static_cast<int64_t>(100 + i));
      score.set_value(i, static_cast<int64_t>(200 + i));
    }
    age.Dump(*ckp, meta, "age");
    score.Dump(*ckp, meta, "score");
    ckp->SetManifest(std::move(meta));
    cp1 = staging.Publish();
  }
  std::printf("\n============ CHECKPOINT 1: fresh age + score ============\n");
  list_objects("after checkpoint 1");
  std::printf("\n[chunk directories @ cp1]\n");
  dump_dir(cp1, "age");
  dump_dir(cp1, "score");

  // ---- Update ONE value in 'age', leave 'score' clean, checkpoint again ----
  std::shared_ptr<Checkpoint> cp2;
  {
    ChunkedColumn<int64_t> age, score;
    age.Open(*cp1, *cp1->manifest().FindModule("age"), MemoryLevel::kInMemory);
    score.Open(*cp1, *cp1->manifest().FindModule("score"),
               MemoryLevel::kInMemory);
    age.set_value(3, 999);  // forks ONLY the 4 KiB page holding row 3
    std::printf("\n======== UPDATE: age[3] = 999 (score untouched) ========\n");
    std::printf("  age.cow_bytes_copied() = %zu bytes (one 4 KiB page)\n",
                age.cow_bytes_copied());

    auto staging = mgr.CreateStaging();
    auto ckp = staging.checkpoint();
    CheckpointManifest meta;
    meta.SetSchema(Schema());
    age.Dump(*ckp, meta, "age");
    score.Dump(*ckp, meta, "score");
    ckp->SetManifest(std::move(meta));
    cp2 = staging.Publish();
  }
  std::printf("\n============ CHECKPOINT 2: after update ============\n");
  list_objects("after checkpoint 2 (cp1 objects persist until GC)");
  std::printf("\n[chunk directories @ cp2]\n");
  dump_dir(cp2, "age");
  dump_dir(cp2, "score");

  mgr.Close();
  std::printf("\n(demo files left under %s for inspection)\n", demo.c_str());
}

// ---------------------------------------------------------------------------
// DEMO 2: ONE column spanning MULTIPLE physical pages (600 rows / 512 rows per
// page = 2 pages), then a single-value update that dirties ONLY the 2nd page.
// Shows (a) a directory holding MANY page pointers, and (b) within ONE column,
// clean pages keep referencing the OLD object while the dirty page moves to a
// NEW object.
TEST(ChunkDiskDemo, PrintMultiPageDirectory) {
  namespace fs = std::filesystem;
  const fs::path demo = "/tmp/neug_chunk_demo2";
  std::error_code ec;
  fs::remove_all(demo, ec);
  fs::create_directories(demo, ec);
  const fs::path objects = demo / "checkpoint" / "objects";

  auto list_objects = [&](const char* tag) {
    std::printf("\n[objects/] %s\n", tag);
    std::vector<fs::path> files;
    for (auto& e : fs::directory_iterator(objects, ec))
      if (e.is_regular_file()) files.push_back(e.path());
    std::sort(files.begin(), files.end());
    for (auto& f : files)
      std::printf("  %-38s %8llu bytes\n", f.filename().string().c_str(),
                  static_cast<unsigned long long>(fs::file_size(f, ec)));
    std::printf("  -> %zu files\n", files.size());
  };
  auto dump_dir = [&](const std::shared_ptr<Checkpoint>& ckp,
                      const std::string& col) {
    const auto* mod = ckp->manifest().FindModule(col);
    auto path = mod->get_path(kChunkDirPath);
    auto file = ckp->OpenFile(*path, MemoryLevel::kInMemory);
    auto d = DecodeChunkDirectory(file->GetData(), file->GetDataSize());
    std::printf("  column '%s': rows_per_page=%llu row_count=%llu PAGES=%zu\n",
                col.c_str(), static_cast<unsigned long long>(d.rows_per_page),
                static_cast<unsigned long long>(d.row_count), d.pages.size());
    for (size_t i = 0; i < d.object_ids.size(); ++i)
      std::printf("      object_ids[%zu] = %s\n", i, d.object_ids[i].c_str());
    for (size_t i = 0; i < d.pages.size(); ++i) {
      const auto& p = d.pages[i];
      std::printf("      page[%zu]: prefix_rows=%u -> ObjectSlice{obj_idx=%llu,"
                  " offset=%llu, length=%u, crc=%08x}\n",
                  i, p.prefix_rows,
                  static_cast<unsigned long long>(p.prefix.object_id),
                  static_cast<unsigned long long>(p.prefix.offset),
                  static_cast<unsigned>(p.prefix.length),
                  static_cast<unsigned>(p.prefix.crc32c));
    }
  };

  CheckpointManager mgr;
  mgr.Open(demo.string());

  // ---- CP1: one INT64 column, 600 rows => 2 physical pages (512 rows each) --
  std::shared_ptr<Checkpoint> cp1;
  {
    auto staging = mgr.CreateStaging();
    auto ckp = staging.checkpoint();
    CheckpointManifest meta;
    meta.SetSchema(Schema());
    ChunkedColumn<int64_t> age;
    age.Open(*ckp, ModuleDescriptor{}, MemoryLevel::kInMemory);
    age.resize(600);
    for (size_t i = 0; i < 600; ++i) age.set_value(i, static_cast<int64_t>(i));
    age.Dump(*ckp, meta, "age");
    ckp->SetManifest(std::move(meta));
    cp1 = staging.Publish();
  }
  std::printf("\n===== DEMO2 CP1: one column, 600 rows (2 pages) =====\n");
  list_objects("cp1");
  dump_dir(cp1, "age");

  // ---- Update ONE value in the 2nd page (row 550); page 0 stays clean ----
  std::shared_ptr<Checkpoint> cp2;
  {
    ChunkedColumn<int64_t> age;
    age.Open(*cp1, *cp1->manifest().FindModule("age"), MemoryLevel::kInMemory);
    age.set_value(550, 99999);  // row 550 lives in page 1 (rows 512..1023)
    std::printf("\n== UPDATE age[550]=99999 (page1 dirty, page0 clean) ==\n");
    std::printf("  age.cow_bytes_copied() = %zu bytes\n", age.cow_bytes_copied());
    auto staging = mgr.CreateStaging();
    auto ckp = staging.checkpoint();
    CheckpointManifest meta;
    meta.SetSchema(Schema());
    age.Dump(*ckp, meta, "age");
    ckp->SetManifest(std::move(meta));
    cp2 = staging.Publish();
  }
  std::printf("\n===== DEMO2 CP2: after update =====\n");
  list_objects("cp2");
  dump_dir(cp2, "age");

  mgr.Close();
  std::printf("\n(demo2 files left under %s)\n", demo.c_str());
}

// ---------------------------------------------------------------------------
// DEMO 3: the prefix + suffix split of ONE physical page. A page is filled
// with 300 rows and checkpointed (all prefix). Then 100 MORE rows are appended
// into the SAME page: PrepareForInsert splits it into an immutable prefix
// (rows 0..299, reused by reference) and a writable suffix (rows 300..511).
// The cp2 directory then shows ONE page carrying TWO ObjectSlices.
TEST(ChunkDiskDemo, PrintAppendPrefixSuffix) {
  namespace fs = std::filesystem;
  const fs::path demo = "/tmp/neug_chunk_demo3";
  std::error_code ec;
  fs::remove_all(demo, ec);
  fs::create_directories(demo, ec);
  const fs::path objects = demo / "checkpoint" / "objects";

  auto list_objects = [&](const char* tag) {
    std::printf("\n[objects/] %s\n", tag);
    std::vector<fs::path> files;
    for (auto& e : fs::directory_iterator(objects, ec))
      if (e.is_regular_file()) files.push_back(e.path());
    std::sort(files.begin(), files.end());
    for (auto& f : files)
      std::printf("  %-38s %8llu bytes\n", f.filename().string().c_str(),
                  static_cast<unsigned long long>(fs::file_size(f, ec)));
    std::printf("  -> %zu files\n", files.size());
  };
  auto dump_dir = [&](const std::shared_ptr<Checkpoint>& ckp,
                      const std::string& col) {
    const auto* mod = ckp->manifest().FindModule(col);
    auto path = mod->get_path(kChunkDirPath);
    auto file = ckp->OpenFile(*path, MemoryLevel::kInMemory);
    auto d = DecodeChunkDirectory(file->GetData(), file->GetDataSize());
    std::printf("  column '%s': rows_per_page=%llu row_count=%llu PAGES=%zu\n",
                col.c_str(), static_cast<unsigned long long>(d.rows_per_page),
                static_cast<unsigned long long>(d.row_count), d.pages.size());
    for (size_t i = 0; i < d.object_ids.size(); ++i)
      std::printf("      object_ids[%zu] = %s\n", i, d.object_ids[i].c_str());
    for (size_t i = 0; i < d.pages.size(); ++i) {
      const auto& p = d.pages[i];
      std::printf("      page[%zu]: prefix_rows=%u\n", i, p.prefix_rows);
      std::printf("          prefix -> ObjectSlice{obj_idx=%llu, offset=%llu,"
                  " length=%u, crc=%08x}\n",
                  static_cast<unsigned long long>(p.prefix.object_id),
                  static_cast<unsigned long long>(p.prefix.offset),
                  static_cast<unsigned>(p.prefix.length),
                  static_cast<unsigned>(p.prefix.crc32c));
      if (p.prefix_rows < d.rows_per_page)
        std::printf("          suffix -> ObjectSlice{obj_idx=%llu, offset=%llu,"
                    " length=%u, crc=%08x}\n",
                    static_cast<unsigned long long>(p.suffix.object_id),
                    static_cast<unsigned long long>(p.suffix.offset),
                    static_cast<unsigned>(p.suffix.length),
                    static_cast<unsigned>(p.suffix.crc32c));
      else
        std::printf("          suffix -> (none: page is full prefix)\n");
    }
  };

  CheckpointManager mgr;
  mgr.Open(demo.string());

  // ---- CP1: 300 rows in ONE page, entirely prefix (no suffix yet) ----
  std::shared_ptr<Checkpoint> cp1;
  {
    auto staging = mgr.CreateStaging();
    auto ckp = staging.checkpoint();
    CheckpointManifest meta;
    meta.SetSchema(Schema());
    ChunkedColumn<int64_t> age;
    age.Open(*ckp, ModuleDescriptor{}, MemoryLevel::kInMemory);
    age.resize(300);
    for (size_t i = 0; i < 300; ++i)
      age.set_value(i, static_cast<int64_t>(i + 1));  // 1..300
    age.Dump(*ckp, meta, "age");
    ckp->SetManifest(std::move(meta));
    cp1 = staging.Publish();
  }
  std::printf("\n===== DEMO3 CP1: 300 rows, one page (prefix only) =====\n");
  list_objects("cp1");
  dump_dir(cp1, "age");

  // ---- APPEND rows 300..399 into the SAME page => prefix + suffix ----
  std::shared_ptr<Checkpoint> cp2;
  {
    ChunkedColumn<int64_t> age;
    age.Open(*cp1, *cp1->manifest().FindModule("age"), MemoryLevel::kInMemory);
    age.resize(400);            // grow logical row count 300 -> 400
    age.PrepareForInsert(300);  // split page0: prefix[0..300) frozen +
                                //              suffix[300..512) writable
    std::printf("\n== APPEND rows 300..399 (prefix reused, suffix new) ==\n");
    std::printf("  cow_bytes_copied() after PrepareForInsert = %zu bytes"
                " (only the 212-row suffix; the 300-row prefix is aliased)\n",
                age.cow_bytes_copied());
    for (size_t i = 300; i < 400; ++i)
      age.set_any(i, Value::INT64(static_cast<int64_t>(i + 1)), false);
    auto staging = mgr.CreateStaging();
    auto ckp = staging.checkpoint();
    CheckpointManifest meta;
    meta.SetSchema(Schema());
    age.Dump(*ckp, meta, "age");
    ckp->SetManifest(std::move(meta));
    cp2 = staging.Publish();
  }
  std::printf("\n===== DEMO3 CP2: after append (page0 = prefix + suffix) =====\n");
  list_objects("cp2");
  dump_dir(cp2, "age");

  // ---- Round-trip: reopen cp2 and verify both segments read back correctly --
  ChunkedColumn<int64_t> check;
  check.Open(*cp2, *cp2->manifest().FindModule("age"), MemoryLevel::kInMemory);
  std::printf("\n  round-trip: size=%zu age[0]=%lld age[299]=%lld "
              "age[300]=%lld age[399]=%lld\n",
              check.size(), static_cast<long long>(check.get_view(0)),
              static_cast<long long>(check.get_view(299)),
              static_cast<long long>(check.get_view(300)),
              static_cast<long long>(check.get_view(399)));

  mgr.Close();
  std::printf("\n(demo3 files left under %s)\n", demo.c_str());
}

// ---------------------------------------------------------------------------
// DEMO 4: TWO vertex labels (person + software) in ONE real graph checkpoint.
// Answers "if I create different vertex types, how are the data files and the
// directory files organised?". On a genuine PropertyGraph dump it shows:
//   (a) EVERY property column of EVERY label gets its OWN chunk-directory
//       object, keyed vertex_<label>_prop_<i>  (directory == per column);
//   (b) all labels' dirty property pages are packed into the SAME shared DATA
//       object by the one checkpoint-wide object writer (data == cross-label
//       shared), exactly like DEMO1's two columns;
//   (c) the remaining objects/ files are the per-label indexer / keys /
//       indices / vertex-timestamp modules.
TEST(ChunkDiskDemo, PrintMultiLabelGraph) {
  namespace fs = std::filesystem;
  const fs::path demo = "/tmp/neug_chunk_demo4";
  std::error_code ec;
  fs::remove_all(demo, ec);
  fs::create_directories(demo, ec);
  const fs::path objects = demo / "checkpoint" / "objects";

  CheckpointManager mgr;
  mgr.Open(demo.string());

  // ---- schema: person(id PK, age INT64) + software(id PK, since INT64) ----
  Schema schema;
  schema.AddVertexLabel("person", {DataType::INT64}, {"age"},
                        {std::make_tuple(DataType::INT64, "id", 0)}, 4096, "",
                        {Value::INT64(0)});
  schema.AddVertexLabel("software", {DataType::INT64}, {"since"},
                        {std::make_tuple(DataType::INT64, "id", 0)}, 4096, "",
                        {Value::INT64(0)});

  // seed checkpoint carrying ONLY the schema, so the graph opens empty.
  std::shared_ptr<Checkpoint> seed;
  {
    auto staging = mgr.CreateStaging();
    auto ckp = staging.checkpoint();
    CheckpointManifest meta;
    meta.SetSchema(schema);
    ckp->SetManifest(std::move(meta));
    seed = staging.Publish();
  }

  // ---- open the graph, insert into BOTH labels ----
  PropertyGraph graph;
  graph.Open(seed, MemoryLevel::kInMemory);
  ASSERT_TRUE(graph.EnsureCapacity(0, 16).ok());  // person
  ASSERT_TRUE(graph.EnsureCapacity(1, 16).ok());  // software
  vid_t vid;
  ASSERT_TRUE(
      graph.AddVertex(0, Value::INT64(1), {Value::INT64(30)}, vid, 1).ok());
  ASSERT_TRUE(
      graph.AddVertex(0, Value::INT64(2), {Value::INT64(40)}, vid, 1).ok());
  ASSERT_TRUE(
      graph.AddVertex(1, Value::INT64(10), {Value::INT64(2010)}, vid, 1).ok());
  ASSERT_TRUE(
      graph.AddVertex(1, Value::INT64(11), {Value::INT64(2011)}, vid, 1).ok());
  graph.MarkVertexTableDirty(0);
  graph.MarkVertexTableDirty(1);

  // ---- ONE real graph checkpoint (shared object writer across labels) ----
  std::shared_ptr<Checkpoint> cp1;
  {
    auto staging = mgr.CreateStaging();
    graph.Compact();
    graph.DumpAndClear(staging.checkpoint());
    cp1 = staging.Publish();
  }

  // ---- pass 1: find every chunked property column, decode its directory ----
  std::set<std::string> dir_objs, data_objs;
  std::map<std::string, ChunkDirectory> dirs;
  for (const auto& kv : cp1->manifest().Modules()) {
    auto p = kv.second.get_path(kChunkDirPath);
    if (!p) continue;  // not a chunked column
    dir_objs.insert(fs::path(*p).filename().string());
    auto file = cp1->OpenFile(*p, MemoryLevel::kInMemory);
    auto d = DecodeChunkDirectory(file->GetData(), file->GetDataSize());
    for (const auto& oid : d.object_ids) data_objs.insert(oid);
    dirs.emplace(kv.first, std::move(d));
  }

  // ---- pass 2: classify every objects/ file ----
  std::printf("\n===== DEMO4: person + software in ONE graph checkpoint =====\n");
  std::printf("\n[objects/] every file, classified\n");
  std::vector<fs::path> files;
  for (auto& e : fs::directory_iterator(objects, ec))
    if (e.is_regular_file()) files.push_back(e.path());
  std::sort(files.begin(), files.end());
  for (auto& f : files) {
    const auto name = f.filename().string();
    const char* role =
        dir_objs.count(name)   ? "DIRECTORY (one column's chunk_dir)"
        : data_objs.count(name) ? "DATA (shared property pages)"
                                : "OTHER (indexer/keys/indices/v_ts)";
    std::printf("  %-38s %8llu bytes  %s\n", name.c_str(),
                static_cast<unsigned long long>(fs::file_size(f, ec)), role);
  }
  std::printf("  -> %zu files total\n", files.size());

  // ---- pass 3: per-column chunk directories (sorted for stable output) ----
  std::printf("\n[chunk directories] ONE per property column, per label\n");
  for (const auto& kv : dirs) {
    const auto& d = kv.second;
    std::printf("  column '%s': row_width=%u rows_per_page=%llu row_count=%llu"
                " PAGES=%zu\n",
                kv.first.c_str(), d.row_width,
                static_cast<unsigned long long>(d.rows_per_page),
                static_cast<unsigned long long>(d.row_count), d.pages.size());
    for (size_t i = 0; i < d.object_ids.size(); ++i)
      std::printf("      object_ids[%zu] = %s   (DATA object)\n", i,
                  d.object_ids[i].c_str());
    for (size_t i = 0; i < d.pages.size(); ++i) {
      const auto& p = d.pages[i];
      std::printf("      page[%zu]: prefix_rows=%u prefix->obj_idx=%llu"
                  " off=%llu len=%u crc=%08x\n",
                  i, p.prefix_rows,
                  static_cast<unsigned long long>(p.prefix.object_id),
                  static_cast<unsigned long long>(p.prefix.offset),
                  static_cast<unsigned>(p.prefix.length),
                  static_cast<unsigned>(p.prefix.crc32c));
    }
  }

  // ---- verdict: do the two labels share ONE data object? ----
  std::string person_oid, software_oid;
  auto pit = dirs.find("vertex_person_prop_0");
  if (pit != dirs.end() && !pit->second.object_ids.empty())
    person_oid = pit->second.object_ids[0];
  auto sit = dirs.find("vertex_software_prop_0");
  if (sit != dirs.end() && !sit->second.object_ids.empty())
    software_oid = sit->second.object_ids[0];
  std::printf("\n[verdict] person.age    DATA object = %s\n",
              person_oid.c_str());
  std::printf("          software.since DATA object = %s\n",
              software_oid.c_str());
  std::printf("          => %s\n",
              (!person_oid.empty() && person_oid == software_oid)
                  ? "SHARED: both labels' property pages live in ONE object"
                  : "separate data objects");

  // ---- round-trip: reopen the real checkpoint and read BOTH labels back ----
  PropertyGraph reopened;
  reopened.Open(cp1, MemoryLevel::kInMemory);
  auto person_age = reopened.GetVertexPropertyColumn(0, 0);
  auto software_since = reopened.GetVertexPropertyColumn(1, 0);
  std::printf("\n[round-trip] person.age[0]=%lld [1]=%lld ;"
              " software.since[0]=%lld [1]=%lld\n",
              static_cast<long long>(person_age->get_any(0).GetValue<int64_t>()),
              static_cast<long long>(person_age->get_any(1).GetValue<int64_t>()),
              static_cast<long long>(
                  software_since->get_any(0).GetValue<int64_t>()),
              static_cast<long long>(
                  software_since->get_any(1).GetValue<int64_t>()));

  mgr.Close();
  std::printf("\n(demo4 files left under %s)\n", demo.c_str());
}

}  // namespace
}  // namespace neug
