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

#include <chrono>
#include <filesystem>
#include <memory>
#include <string>

#include "neug/storages/checkpoint_manager.h"
#include "neug/storages/checkpoint_manifest.h"
#include "neug/storages/graph/schema.h"
#include "neug/storages/module/module_factory.h"

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

TEST_F(ChunkedColumnTest, DetachForksAllChunks) {
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

  // After a full detach, writing any chunk is isolated from the original.
  cow->set_value(5, 555);  // chunk 1
  EXPECT_EQ(cow->get_view(5), 555);
  EXPECT_EQ(original.get_view(5), 5);
  cow->set_value(0, 111);  // chunk 0
  EXPECT_EQ(cow->get_view(0), 111);
  EXPECT_EQ(original.get_view(0), 0);
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

  // Reopen from the serialized chunk directory (crc32c verified on load).
  ChunkedColumn<int64_t> reopened(kRowsPerChunk);
  reopened.Open(*ckp_, *meta.FindModule("chunked_col"), MemoryLevel::kInMemory);
  EXPECT_EQ(reopened.size(), 10u);
  EXPECT_EQ(reopened.rows_per_chunk(), kRowsPerChunk);
  for (size_t i = 0; i < 10; ++i) {
    EXPECT_EQ(reopened.get_view(i), static_cast<int64_t>(i * 7));
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

TEST(ChunkDirTest, ExtractChunkDirObjectPathsParsesAndIsBoundsChecked) {
  // Build a directory blob in the ChunkedColumn::Dump format: header +
  // object-path table + per-chunk ObjectSlice section.
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
  const std::string p0 = "objects/aaa";
  const std::string p1 = "objects/bbb";
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

  auto paths = ExtractChunkDirObjectPaths(blob.data(), blob.size());
  ASSERT_EQ(paths.size(), 2u);
  EXPECT_EQ(paths[0], p0);
  EXPECT_EQ(paths[1], p1);

  // Truncating so p1's declared length exceeds the remaining bytes yields only
  // the first path (bounds-checked).
  auto partial =
      ExtractChunkDirObjectPaths(blob.data(), p1_bytes_at + p1.size() - 1);
  EXPECT_EQ(partial.size(), 1u);
  EXPECT_EQ(partial[0], p0);
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
  ChunkedColumn<int64_t> reopened(kRowsPerChunk);
  reopened.Open(*ckp_, *meta.FindModule("conv"), MemoryLevel::kInMemory);
  EXPECT_EQ(reopened.get_view(3), 999);
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
