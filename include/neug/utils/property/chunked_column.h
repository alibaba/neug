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
#pragma once

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "neug/common/types/value.h"
#include "neug/storages/checkpoint.h"
#include "neug/storages/chunk/chunk_block.h"
#include "neug/storages/module/module.h"
#include "neug/storages/module/type_name.h"
#include "neug/utils/exception/exception.h"
#include "neug/utils/property/column.h"
#include "neug/utils/property/types.h"

namespace neug {

/// Fixed-length property column stored as power-of-two chunks.
///
/// Read is an O(1) flat index: chunk_id = row >> shift, local_slot = row & mask
/// (see chunked_cow_checkpoint_plan.md §2 "读路径实测约束"). Mutation uses
/// chunk-granular copy-on-write: after Clone(), only the chunks actually
/// written are forked, so a sparse update copies one chunk rather than the
/// whole column. Object-slice persistence (Dump/Open of a chunk directory) is
/// added in a later step; this is the in-memory core.
template <typename T>
class ChunkedColumn : public ColumnBase {
 public:
  /// Production chunk payload floor (plan §2). Tests may use smaller values.
  static constexpr size_t kMinChunkPayloadBytes = 256 * 1024;
  // The chunk-directory descriptor key (kChunkDirPath) is shared with
  // checkpoint GC via chunk_types.h.

  /// Default constructor for the module factory. rows_per_chunk defaults to the
  /// production value for T; Open() overrides it from the persisted directory.
  ChunkedColumn() : ChunkedColumn(ComputeRowsPerChunk(sizeof(T))) {}

  /// @p rows_per_chunk must be a power of two.
  explicit ChunkedColumn(size_t rows_per_chunk)
      : rows_per_chunk_(rows_per_chunk),
        shift_(ComputeShift(rows_per_chunk)),
        mask_(rows_per_chunk - 1) {}

  /// Smallest power-of-two row count whose payload reaches the floor.
  static size_t ComputeRowsPerChunk(size_t row_width) {
    size_t rows = 1;
    while (rows * row_width < kMinChunkPayloadBytes)
      rows <<= 1;
    return rows;
  }

  /// Migration hook (plan §7): build a ChunkedColumn copying the values of a
  /// legacy TypedColumn. The result is a fresh, writable, in-memory column.
  static std::unique_ptr<ChunkedColumn<T>> FromLegacy(
      Checkpoint& ckp, MemoryLevel level, const TypedColumn<T>& legacy) {
    auto col =
        std::make_unique<ChunkedColumn<T>>(ComputeRowsPerChunk(sizeof(T)));
    col->Open(ckp, ModuleDescriptor{}, level);
    const size_t n = legacy.size();
    col->resize(n);
    for (size_t i = 0; i < n; ++i) {
      col->set_value(i, legacy.get_view(i));
    }
    return col;
  }

  /// Bind the checkpoint and, when @p desc references a chunk directory, load
  /// the persisted chunks: open each unique packed object once and locate every
  /// chunk at its slice offset, verifying crc32c. Loaded chunks are shared
  /// (copy-on-write), so a later write copies just that chunk's region.
  void Open(Checkpoint& ckp, const ModuleDescriptor& desc,
            MemoryLevel level) override {
    ckp_ = &ckp;
    level_ = level;
    const auto dir_path = desc.get_path(kChunkDirPath);
    if (!dir_path.has_value()) {
      return;  // fresh empty column
    }
    auto dir = ckp.OpenFile(*dir_path, level);
    const char* cursor = static_cast<const char*>(dir->GetData());
    rows_per_chunk_ = ReadU64(cursor);
    shift_ = ComputeShift(rows_per_chunk_);
    mask_ = rows_per_chunk_ - 1;
    const uint64_t chunk_count = ReadU64(cursor);
    size_ = ReadU64(cursor);
    const uint64_t object_count = ReadU64(cursor);
    // Open each unique object once; chunks index into this table.
    std::vector<std::shared_ptr<IDataContainer>> objects;
    std::vector<std::string> object_paths;
    objects.reserve(object_count);
    object_paths.reserve(object_count);
    for (uint64_t o = 0; o < object_count; ++o) {
      const uint32_t path_len = ReadU32(cursor);
      const std::string path(cursor, path_len);
      cursor += path_len;
      object_paths.push_back(path);
      objects.push_back(ckp.OpenFile(path, level));
    }
    blocks_.clear();
    chunk_owned_.clear();
    for (uint64_t i = 0; i < chunk_count; ++i) {
      const uint32_t obj_index = ReadU32(cursor);
      const uint64_t offset = ReadU64(cursor);
      const uint32_t length = ReadU32(cursor);
      const uint32_t crc = ReadU32(cursor);
      ChunkBlock block;
      block.mem = objects[obj_index];
      block.slice.offset = offset;
      block.slice.length = length;
      block.slice.crc32c = crc;
      block.object_path = object_paths[obj_index];
      block.read_offset = offset;
      block.dirty = false;
      if (Crc32c(BlockData(block), length) != crc) {
        THROW_CHECKPOINT_EXCEPTION(
            "ChunkedColumn::Open: crc32c mismatch for chunk " +
            std::to_string(i));
      }
      blocks_.push_back(std::move(block));
      chunk_owned_.push_back(false);  // shared object region; copy on write
    }
  }

  /// Pack dirty chunks into shared immutable objects via the checkpoint's
  /// ObjectWriter (clean chunks reuse the object they already live in), then
  /// serialize a self-contained directory: a local object-path table plus one
  /// ObjectSlice (object index, offset, length, crc32c) per chunk.
  void Dump(Checkpoint& ckp, CheckpointManifest& meta,
            const std::string& key) override {
    const size_t chunk_bytes = rows_per_chunk_ * sizeof(T);

    // Phase 1: append dirty chunks to the object writer; clean chunks reuse
    // their existing object. Record each chunk's resolved persist location.
    std::vector<std::string> chunk_path(blocks_.size());
    std::vector<uint64_t> chunk_offset(blocks_.size(), 0);
    std::vector<uint32_t> chunk_length(blocks_.size(), 0);
    std::vector<uint32_t> chunk_crc(blocks_.size(), 0);
    std::vector<size_t> dirty_slice(blocks_.size(), 0);
    std::vector<char> is_dirty(blocks_.size(), 0);
    bool any_dirty = false;
    for (size_t i = 0; i < blocks_.size(); ++i) {
      const ChunkBlock& b = blocks_[i];
      if (!b.object_path.empty()) {
        chunk_path[i] = b.object_path;
        chunk_offset[i] = b.slice.offset;
        chunk_length[i] = b.slice.length;
        chunk_crc[i] = b.slice.crc32c;
      } else {
        dirty_slice[i] = ckp.object_writer().AppendBlock(
            BlockData(b), static_cast<uint32_t>(chunk_bytes));
        is_dirty[i] = 1;
        any_dirty = true;
      }
    }
    // Commit the packed objects so dirty slices get their final object_id.
    if (any_dirty) {
      ckp.SealObjects();
      const auto& slices = ckp.object_writer().slices();
      const auto& object_table = ckp.object_table();
      for (size_t i = 0; i < blocks_.size(); ++i) {
        if (!is_dirty[i]) {
          continue;
        }
        const ObjectSlice& s = slices[dirty_slice[i]];
        chunk_path[i] = object_table[s.object_id];
        chunk_offset[i] = s.offset;
        chunk_length[i] = s.length;
        chunk_crc[i] = s.crc32c;
        // The chunk now lives in a committed object. Reads keep using the
        // private block (read_offset 0); slice records the persisted location
        // so the next Dump reuses it without re-committing.
        blocks_[i].object_path = chunk_path[i];
        blocks_[i].slice.offset = s.offset;
        blocks_[i].slice.length = s.length;
        blocks_[i].slice.crc32c = s.crc32c;
        blocks_[i].dirty = false;
        chunk_owned_[i] = false;
      }
    }

    // Phase 2: build the directory — a local object-path table (unique paths)
    // plus one ObjectSlice per chunk indexing into it.
    std::vector<std::string> object_paths;
    std::unordered_map<std::string, uint32_t> path_to_index;
    std::vector<char> chunk_section;
    for (size_t i = 0; i < blocks_.size(); ++i) {
      auto it = path_to_index.find(chunk_path[i]);
      uint32_t obj_index;
      if (it == path_to_index.end()) {
        obj_index = static_cast<uint32_t>(object_paths.size());
        path_to_index.emplace(chunk_path[i], obj_index);
        object_paths.push_back(chunk_path[i]);
      } else {
        obj_index = it->second;
      }
      AppendU32(chunk_section, obj_index);
      AppendU64(chunk_section, chunk_offset[i]);
      AppendU32(chunk_section, chunk_length[i]);
      AppendU32(chunk_section, chunk_crc[i]);
    }
    std::vector<char> blob;
    AppendU64(blob, rows_per_chunk_);
    AppendU64(blob, blocks_.size());
    AppendU64(blob, size_);
    AppendU64(blob, object_paths.size());
    for (const auto& p : object_paths) {
      AppendU32(blob, static_cast<uint32_t>(p.size()));
      blob.insert(blob.end(), p.begin(), p.end());
    }
    blob.insert(blob.end(), chunk_section.begin(), chunk_section.end());

    auto dir = ckp.CreateRuntimeContainer(blob.size(), level_);
    std::memcpy(dir->GetData(), blob.data(), blob.size());
    ModuleDescriptor desc;
    desc.module_type = ModuleTypeName();
    desc.set_path(kChunkDirPath, ckp.Commit(*dir));
    meta.SetModule(key, std::move(desc));
  }

  size_t size() const override { return size_; }
  size_t rows_per_chunk() const { return rows_per_chunk_; }
  DataTypeId type() const override { return ValueConverter<T>::type().id(); }

  /// Grow to @p n rows, allocating chunk containers as needed.
  void resize(size_t n) override {
    const size_t needed = (n + rows_per_chunk_ - 1) / rows_per_chunk_;
    while (blocks_.size() < needed) {
      ChunkBlock block;
      block.mem =
          ckp_->CreateRuntimeContainer(rows_per_chunk_ * sizeof(T), level_);
      T* data = static_cast<T*>(block.mem->GetData());
      for (size_t i = 0; i < rows_per_chunk_; ++i) {
        data[i] = T();
      }
      block.dirty = true;
      blocks_.push_back(std::move(block));
      chunk_owned_.push_back(true);
    }
    size_ = n;
  }

  void resize(size_t size, const Value& default_value) override {
    const size_t old = size_;
    resize(size);
    const T default_typed = default_value.GetValue<T>();
    for (size_t i = old; i < size; ++i) {
      set_value(i, default_typed);
    }
  }

  T get_view(size_t row) const {
    const ChunkBlock& block = blocks_[row >> shift_];
    return static_cast<const T*>(BlockData(block))[row & mask_];
  }

  Value get_any(size_t row) const override {
    return Value::CreateValue<T>(get_view(row));
  }

  void set_value(size_t row, const T& value) {
    const size_t chunk = row >> shift_;
    EnsureWritable(chunk);
    static_cast<T*>(BlockData(blocks_[chunk]))[row & mask_] = value;
  }

  void set_any(size_t row, const Value& value, bool /*insert_safe*/) override {
    set_value(row, value.GetValue<T>());
  }

  /// Zero-copy clone: chunk containers are shared until written.
  std::unique_ptr<Module> Clone() const override {
    auto clone = std::make_unique<ChunkedColumn<T>>(rows_per_chunk_);
    clone->blocks_ = blocks_;
    clone->chunk_owned_.assign(blocks_.size(), false);
    clone->size_ = size_;
    clone->ckp_ = ckp_;
    clone->level_ = level_;
    return clone;
  }

  /// Eagerly fork every chunk so this instance owns all of its data.
  void Detach(Checkpoint& ckp, MemoryLevel level) override {
    ckp_ = &ckp;
    level_ = level;
    for (size_t i = 0; i < blocks_.size(); ++i) {
      EnsureWritable(i);
    }
  }

  static std::string type_name() {
    return "chunked_column<" + type_name_string<T>() + ">";
  }

  std::string ModuleTypeName() const override { return type_name(); }

 private:
  static int ComputeShift(size_t rows_per_chunk) {
    int shift = 0;
    while ((static_cast<size_t>(1) << shift) < rows_per_chunk) {
      ++shift;
    }
    return shift;
  }

  // Little-endian serialization helpers for the chunk directory blob.
  static void AppendU64(std::vector<char>& blob, uint64_t v) {
    for (int i = 0; i < 8; ++i) {
      blob.push_back(static_cast<char>((v >> (8 * i)) & 0xFF));
    }
  }
  static void AppendU32(std::vector<char>& blob, uint32_t v) {
    for (int i = 0; i < 4; ++i) {
      blob.push_back(static_cast<char>((v >> (8 * i)) & 0xFF));
    }
  }
  static uint64_t ReadU64(const char*& cursor) {
    uint64_t v = 0;
    for (int i = 0; i < 8; ++i) {
      v |= static_cast<uint64_t>(static_cast<unsigned char>(*cursor++))
           << (8 * i);
    }
    return v;
  }
  static uint32_t ReadU32(const char*& cursor) {
    uint32_t v = 0;
    for (int i = 0; i < 4; ++i) {
      v |= static_cast<uint32_t>(static_cast<unsigned char>(*cursor++))
           << (8 * i);
    }
    return v;
  }

  static void* BlockData(const ChunkBlock& block) {
    // The chunk's payload lives at read_offset within its backing container
    // (0 for a private writable block, the slice offset for a packed object).
    return static_cast<char*>(block.mem->GetData()) + block.read_offset;
  }

  /// Make @p chunk_id privately writable if it is still shared (a packed object
  /// region or a clone source). Copies just this chunk's region: the backing
  /// object may pack several chunks, so it must not be forked whole.
  void EnsureWritable(size_t chunk_id) {
    if (!chunk_owned_[chunk_id]) {
      ChunkBlock& b = blocks_[chunk_id];
      const size_t chunk_bytes = rows_per_chunk_ * sizeof(T);
      auto writable = ckp_->CreateRuntimeContainer(chunk_bytes, level_);
      std::memcpy(writable->GetData(), BlockData(b), chunk_bytes);
      b.mem = writable;
      b.read_offset = 0;
      b.slice.offset = 0;
      b.object_path.clear();
      chunk_owned_[chunk_id] = true;
      b.dirty = true;
    }
  }

  size_t rows_per_chunk_;
  int shift_;
  size_t mask_;
  size_t size_ = 0;
  std::vector<ChunkBlock> blocks_;
  std::vector<bool> chunk_owned_;
  Checkpoint* ckp_ = nullptr;
  MemoryLevel level_ = MemoryLevel::kInMemory;
};

/// Read-only reference to a ChunkedColumn, mirroring TypedRefColumn but with
/// chunk-localized access.
template <typename T>
class ChunkedRefColumn : public RefColumnBase {
 public:
  using value_type = T;

  explicit ChunkedRefColumn(const ChunkedColumn<T>& column) : column_(column) {}

  T get_view(size_t index) const { return column_.get_view(index); }

  Value get_any(size_t index) const override { return column_.get_any(index); }

  DataTypeId type() const override { return ValueConverter<T>::type().id(); }

  ColType col_type() const override { return ColType::kInternal; }

 private:
  const ChunkedColumn<T>& column_;
};

}  // namespace neug
