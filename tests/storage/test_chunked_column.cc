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

#include <atomic>
#include <chrono>
#include <filesystem>
#include <memory>
#include <string>
#include <thread>

#include "neug/storages/checkpoint_manager.h"
#include "neug/storages/checkpoint_manifest.h"
#include "neug/storages/graph/schema.h"
#include "neug/storages/module/module_factory.h"
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

}  // namespace
}  // namespace neug
