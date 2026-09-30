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

#include <algorithm>
#include <atomic>
#include <cassert>
#include <cstring>
#include <limits>
#include <memory>
#include <mutex>
#include <span>
#include <unordered_map>
#include <vector>

#include "neug/storages/checkpoint.h"
#include "neug/storages/module/type_name.h"
#include "neug/utils/exception/exception.h"
#include "neug/utils/property/column.h"

namespace neug {

/// Stable logical chunks contain physical pages of at most 4 KiB. Covering
/// writes fork one page; concurrent inserts write a prepared, stable suffix.
/// Clone/resize/PrepareForInsert/Dump require COW or maintenance admission.
/// Concurrent set_any(..., false) is permitted only for distinct reserved rows.
template <typename T>
class ChunkedColumn : public ColumnBase {
 public:
  static constexpr size_t kMinChunkPayloadBytes = 256 * 1024;
  static constexpr size_t kCowPageBytes = 4096;

  ChunkedColumn() : ChunkedColumn(ComputeRowsPerChunk(sizeof(T))) {}
  explicit ChunkedColumn(size_t rows_per_chunk)
      : rows_per_chunk_(ValidateRows(rows_per_chunk)),
        rows_per_page_(std::min(rows_per_chunk, ComputePageRows())),
        page_shift_(Shift(rows_per_page_)),
        page_mask_(rows_per_page_ - 1) {}

  static size_t ComputeRowsPerChunk(size_t width) {
    if (width == 0 || width > kMinChunkPayloadBytes)
      THROW_INVALID_ARGUMENT_EXCEPTION("Invalid fixed-width chunk row size");
    size_t rows = 1;
    while (rows < (kMinChunkPayloadBytes + width - 1) / width)
      rows <<= 1;
    return rows;
  }
  static std::unique_ptr<ChunkedColumn<T>> FromLegacy(
      Checkpoint& ckp, MemoryLevel level, const TypedColumn<T>& legacy) {
    auto col = std::make_unique<ChunkedColumn<T>>();
    col->Open(ckp, ModuleDescriptor{}, level);
    col->resize(legacy.size());
    for (size_t i = 0; i < legacy.size(); ++i)
      col->set_value(i, legacy.get_view(i));
    return col;
  }

  void Open(Checkpoint& ckp, const ModuleDescriptor& desc,
            MemoryLevel level) override {
    ckp_ = &ckp;
    level_ = level;
    pages_.clear();
    size_ = 0;
    append_prepared_ = false;
    append_begin_ = 0;
    auto path = desc.get_path(kChunkDirPath);
    if (!path)
      return;
    auto file = ckp.OpenFile(*path, MemoryLevel::kInMemory);
    auto d = DecodeChunkDirectory(file->GetData(), file->GetDataSize());
    if (d.row_width && d.row_width != sizeof(T))
      THROW_CHECKPOINT_EXCEPTION("Chunk directory row type mismatch");
    rows_per_chunk_ = ValidateRows(d.rows_per_chunk);
    rows_per_page_ = std::min(rows_per_chunk_, ComputePageRows());
    if (d.row_count > std::numeric_limits<size_t>::max())
      THROW_CHECKPOINT_EXCEPTION("Chunk row count overflow");
    size_ = d.row_count;
    page_shift_ = Shift(rows_per_page_);
    page_mask_ = rows_per_page_ - 1;
    std::vector<std::shared_ptr<IDataContainer>> objects;
    for (const auto& id : d.object_ids)
      objects.push_back(ckp.OpenObject(id, level));
    auto load = [&](const ObjectSlice& slice, size_t rows) {
      auto mem = objects.at(slice.object_id);
      if (slice.length != rows * sizeof(T) ||
          slice.offset > mem->GetDataSize() ||
          slice.length > mem->GetDataSize() - slice.offset)
        THROW_CHECKPOINT_EXCEPTION("Chunk slice is outside its object");
      auto segment = std::make_shared<Segment>();
      segment->mem = std::move(mem);
      segment->data = reinterpret_cast<T*>(
          static_cast<char*>(segment->mem->GetData()) + slice.offset);
      // ObjectWriter packs typed payloads on their natural alignment.
      if (slice.offset % alignof(T) != 0)
        THROW_CHECKPOINT_EXCEPTION("Misaligned chunk slice");
      segment->bytes = slice.length;
      segment->slice = slice;
      segment->object_id = d.object_ids[slice.object_id];
      segment->frozen = true;
      segment->verified.store(false);
      segment->dirty.store(false);
      return segment;
    };
    if (d.row_width != 0) {
      if (d.rows_per_page != rows_per_page_)
        THROW_CHECKPOINT_EXCEPTION("Unsupported physical chunk page size");
      for (const auto& page : d.pages) {
        Page entry{load(page.prefix, page.prefix_rows), nullptr,
                   page.prefix_rows};
        if (page.prefix_rows < rows_per_page_)
          entry.suffix = load(page.suffix, rows_per_page_ - page.prefix_rows);
        pages_.push_back(std::move(entry));
      }
    } else {
      // One-time conversion of the initial whole-chunk directory. Its payload
      // CRC protects the old slice; new checkpoints encode physical page CRCs.
      for (const auto& page : d.pages) {
        auto source = load(page.prefix, rows_per_chunk_);
        source->Verify();
        for (size_t offset = 0;
             offset < rows_per_chunk_ && pages_.size() < PageCount(size_);
             offset += rows_per_page_) {
          auto segment = Slice(source, offset, rows_per_page_, true);
          pages_.push_back(Page{std::move(segment), nullptr,
                                static_cast<uint32_t>(rows_per_page_)});
        }
      }
    }
  }

  void Dump(Checkpoint& ckp, CheckpointManifest& meta,
            const std::string& key) override {
    // The builder owns every segment, so owners may disassemble/destroy columns
    // before checkpoint-wide sealing. No callback borrows a column or manifest.
    auto builder = std::make_shared<Builder>();
    builder->rows_per_chunk = rows_per_chunk_;
    builder->rows_per_page = rows_per_page_;
    builder->row_count = size_;
    builder->pages = pages_;
    builder->key = key;
    for (const auto& page : builder->pages) {
      builder->Append(ckp, page.prefix);
      if (page.suffix)
        builder->Append(ckp, page.suffix);
    }
    if (builder->pending.empty())
      builder->Commit(ckp, meta);
    else
      ckp.RegisterObjectFinalizer(
          [builder](Checkpoint& checkpoint, CheckpointManifest& manifest) {
            builder->Commit(checkpoint, manifest);
          });
  }

  size_t size() const override { return size_; }
  size_t rows_per_chunk() const { return rows_per_chunk_; }
  size_t rows_per_page() const { return rows_per_page_; }
  size_t cow_bytes_copied() const { return cow_bytes_copied_; }
  DataTypeId type() const override { return ValueConverter<T>::type().id(); }

  void resize(size_t n) override {
    const size_t count = PageCount(n);
    if (count < pages_.size())
      pages_.resize(count);
    while (pages_.size() < count) {
      const size_t allocation_pages =
          std::min(rows_per_chunk_ / rows_per_page_, count - pages_.size());
      auto allocation = ckp_->AllocateChunkBuffer(
          allocation_pages * rows_per_page_ * sizeof(T), level_);
      T* data = reinterpret_cast<T*>(
          static_cast<char*>(allocation.container->GetData()) +
          allocation.offset);
      std::fill_n(data, allocation_pages * rows_per_page_, T());
      for (size_t i = 0; i < allocation_pages; ++i) {
        auto segment = std::make_shared<Segment>();
        segment->mem = allocation.container;
        segment->data = data + i * rows_per_page_;
        segment->bytes = rows_per_page_ * sizeof(T);
        pages_.push_back(Page{std::move(segment), nullptr,
                              static_cast<uint32_t>(rows_per_page_)});
      }
    }
    size_ = n;
    append_prepared_ = false;
  }
  void resize(size_t n, const Value& value) override {
    if (!value.IsNull() && value.type().id() != type())
      THROW_RUNTIME_ERROR("Default value type mismatch");
    const auto old = size_;
    resize(n);
    const T typed = value.IsNull() ? T() : value.GetValue<T>();
    for (size_t i = old; i < n; ++i)
      set_value(i, typed);
  }

  T get_view(size_t row) const {
    assert(row < size_);
    const auto& page = pages_[row >> page_shift_];
    const auto slot = row & page_mask_;
    const auto& segment = slot < page.prefix_rows ? page.prefix : page.suffix;
    segment->Verify();
    return segment
        ->data[slot < page.prefix_rows ? slot : slot - page.prefix_rows];
  }
  std::span<const T> get_span(size_t row) const {
    if (row >= size_)
      THROW_RUNTIME_ERROR("Chunk row out of range");
    const auto& page = pages_[row >> page_shift_];
    const auto slot = row & page_mask_;
    const bool prefix = slot < page.prefix_rows;
    const auto& segment = prefix ? page.prefix : page.suffix;
    segment->Verify();
    const auto local = prefix ? slot : slot - page.prefix_rows;
    return {segment->data + local,
            std::min(size_ - row, segment->bytes / sizeof(T) - local)};
  }
  Value get_any(size_t row) const override {
    return Value::CreateValue<T>(get_view(row));
  }

  void set_value(size_t row, const T& value) {
    if (row >= size_)
      THROW_RUNTIME_ERROR("Chunk row out of range");
    auto& page = pages_[row >> page_shift_];
    const auto slot = row & page_mask_;
    auto& segment = slot < page.prefix_rows ? page.prefix : page.suffix;
    if (segment->frozen || segment.use_count() > 1) {
      segment->Verify();
      const auto bytes = segment->bytes;
      segment = Copy(segment->data, bytes);
    }
    segment->data[slot < page.prefix_rows ? slot : slot - page.prefix_rows] =
        value;
    segment->MarkDirty();
  }
  void set_any(size_t row, const Value& value, bool insert_safe) override {
    const T typed = value.IsNull() ? T() : value.GetValue<T>();
    if (insert_safe)
      set_value(row, typed);
    else
      InsertValue(row, typed);
  }

  void PrepareForInsert(size_t begin) override {
    if (begin > size_)
      THROW_RUNTIME_ERROR("Invalid append high-water mark");
    if (append_prepared_ && begin == append_begin_)
      return;
    for (size_t i = begin >> page_shift_; i < pages_.size(); ++i) {
      auto& page = pages_[i];
      const size_t local =
          i == (begin >> page_shift_) ? (begin & page_mask_) : 0;
      if (local == 0 && !page.suffix && !page.prefix->frozen)
        continue;
      if (page.suffix && page.prefix_rows != local) {
        // At most one boundary page needs materialization as the append front
        // advances. Other physical pages keep their immutable/mutable versions.
        auto merged = Copy(nullptr, rows_per_page_ * sizeof(T));
        page.prefix->Verify();
        page.suffix->Verify();
        std::copy_n(page.prefix->data, page.prefix_rows, merged->data);
        std::copy_n(page.suffix->data, rows_per_page_ - page.prefix_rows,
                    merged->data + page.prefix_rows);
        cow_bytes_copied_ += rows_per_page_ * sizeof(T);
        page = Page{std::move(merged), nullptr,
                    static_cast<uint32_t>(rows_per_page_)};
      }
      if (local != 0) {
        if (!page.suffix) {
          auto suffix =
              Slice(page.prefix, local, rows_per_page_ - local, false);
          page.prefix = Slice(page.prefix, 0, local, true);
          page.prefix_rows = local;
          page.suffix = std::move(suffix);
        } else if (page.suffix->frozen) {
          page.suffix = Slice(page.suffix, 0, rows_per_page_ - local, false);
        }
      } else {
        page.prefix = Slice(page.prefix, 0, rows_per_page_, false);
      }
    }
    append_begin_ = begin;
    append_prepared_ = true;
  }

  std::unique_ptr<Module> Clone() const override {
    auto clone = std::make_unique<ChunkedColumn<T>>(rows_per_chunk_);
    clone->pages_ = pages_;
    clone->size_ = size_;
    clone->ckp_ = ckp_;
    clone->level_ = level_;
    // Shared segment ownership makes both original and clone fork on covering
    // writes. Prepared append slots are unreachable from retained snapshots.
    return clone;
  }
  void Detach(Checkpoint& ckp, MemoryLevel level) override {
    RebindCheckpoint(ckp);
    level_ = level;
  }
  void RebindCheckpoint(Checkpoint& ckp) override { ckp_ = &ckp; }
  static std::string type_name() {
    return "chunked_column<" + type_name_string<T>() + ">";
  }
  std::string ModuleTypeName() const override { return type_name(); }

 private:
  struct Segment {
    std::shared_ptr<IDataContainer> mem;
    T* data = nullptr;
    size_t bytes = 0;
    std::string object_id;
    ObjectSlice slice;
    bool frozen = false;
    mutable std::atomic<bool> verified{true};
    mutable std::once_flag verify_once;
    std::atomic<bool> dirty{true};
    void Verify() const {
      if (verified.load(std::memory_order_acquire))
        return;
      std::call_once(verify_once, [this] {
        if (Crc32c(data, bytes) != slice.crc32c)
          THROW_CHECKPOINT_EXCEPTION("Chunk payload checksum mismatch");
        verified.store(true, std::memory_order_release);
      });
    }
    void MarkDirty() {
      if (!dirty.load(std::memory_order_relaxed))
        dirty.store(true, std::memory_order_relaxed);
    }
  };
  struct Page {
    std::shared_ptr<Segment> prefix, suffix;
    uint32_t prefix_rows;
  };
  struct Builder {
    size_t rows_per_chunk, rows_per_page, row_count;
    std::string key;
    std::vector<Page> pages;
    std::vector<std::pair<std::shared_ptr<Segment>, size_t>> pending;
    std::unordered_map<std::string, bool> reusable_objects;
    void Append(Checkpoint& ckp, const std::shared_ptr<Segment>& s) {
      // Failed staging/GC may remove an unpublished physical object. Retain
      // the live bytes and rewrite it instead of trusting a stale object ID.
      bool reusable = !s->dirty.load() && !s->object_id.empty();
      if (reusable) {
        auto [it, added] = reusable_objects.emplace(s->object_id, false);
        if (added) {
          try {
            (void) ckp.OpenObject(s->object_id, MemoryLevel::kInMemory);
            it->second = true;
          } catch (const exception::CheckpointException&) {}
        }
        reusable = it->second;
      }
      if (!reusable) {
        s->Verify();
        pending.emplace_back(
            s, ckp.object_writer().AppendBlock(s->data, s->bytes));
      }
    }
    void Commit(Checkpoint& ckp, CheckpointManifest& meta) {
      const auto& slices = ckp.object_writer().slices();
      for (auto& [segment, index] : pending) {
        const auto slice = slices.at(index);
        segment->object_id =
            ckp.ObjectIdForPath(ckp.object_table().at(slice.object_id));
        segment->slice = slice;
        segment->dirty.store(false);
        segment->frozen = true;
      }
      ChunkDirectory d;
      d.row_width = sizeof(T);
      d.rows_per_chunk = rows_per_chunk;
      d.rows_per_page = rows_per_page;
      d.row_count = row_count;
      std::unordered_map<std::string, uint32_t> indices;
      auto slice_for = [&](const std::shared_ptr<Segment>& s) {
        auto [it, added] = indices.emplace(s->object_id, d.object_ids.size());
        if (added)
          d.object_ids.push_back(s->object_id);
        auto slice = s->slice;
        slice.object_id = it->second;
        return slice;
      };
      for (const auto& page : pages) {
        ChunkDirectoryPage entry;
        entry.prefix_rows = page.prefix_rows;
        entry.prefix = slice_for(page.prefix);
        if (page.suffix)
          entry.suffix = slice_for(page.suffix);
        d.pages.push_back(entry);
      }
      auto blob = EncodeChunkDirectory(d);
      auto file =
          ckp.CreateRuntimeContainer(blob.size(), MemoryLevel::kInMemory);
      std::memcpy(file->GetData(), blob.data(), blob.size());
      ModuleDescriptor desc;
      desc.module_type = ChunkedColumn<T>::type_name();
      desc.set_path(kChunkDirPath, ckp.Commit(*file));
      meta.SetModule(key, std::move(desc));
    }
  };

  static size_t ValidateRows(uint64_t rows) {
    if (rows == 0 || (rows & (rows - 1)) != 0 ||
        rows > std::numeric_limits<uint32_t>::max() / sizeof(T))
      THROW_CHECKPOINT_EXCEPTION("Invalid logical chunk row count");
    return rows;
  }
  static size_t ComputePageRows() {
    size_t rows = 1;
    while ((rows << 1) <= kCowPageBytes / sizeof(T))
      rows <<= 1;
    return rows;
  }
  static int Shift(size_t rows) {
    int shift = 0;
    while ((size_t{1} << shift) < rows)
      ++shift;
    return shift;
  }
  size_t PageCount(size_t rows) const {
    return rows / rows_per_page_ + (rows % rows_per_page_ != 0);
  }
  std::shared_ptr<Segment> Copy(const T* source, size_t bytes) {
    auto allocation = ckp_->AllocateChunkBuffer(bytes, level_);
    auto result = std::make_shared<Segment>();
    result->mem = allocation.container;
    result->data = reinterpret_cast<T*>(
        static_cast<char*>(result->mem->GetData()) + allocation.offset);
    result->bytes = bytes;
    if (source) {
      std::memcpy(result->data, source, bytes);
      cow_bytes_copied_ += bytes;
    }
    return result;
  }
  std::shared_ptr<Segment> Slice(const std::shared_ptr<Segment>& source,
                                 size_t row, size_t rows, bool frozen) {
    source->Verify();
    const size_t bytes = rows * sizeof(T);
    std::shared_ptr<Segment> result;
    if (frozen && source->frozen) {
      // Only already immutable segments can be aliased by a new descriptor.
      // Freezing a shared mutable source here would disable inserts into the
      // published graph if this private COW workspace is subsequently aborted.
      result = std::make_shared<Segment>();
      result->mem = source->mem;
      result->data = source->data + row;
      result->bytes = bytes;
    } else
      result = Copy(source->data + row, bytes);
    result->frozen = frozen;
    if (!source->dirty.load() && !source->object_id.empty()) {
      result->object_id = source->object_id;
      result->slice = source->slice;
      result->slice.offset += row * sizeof(T);
      result->slice.length = bytes;
      result->slice.crc32c = Crc32c(result->data, bytes);
      result->dirty.store(false);
    }
    return result;
  }
  void InsertValue(size_t row, const T& value) {
    if (row >= size_ || (append_prepared_ && row < append_begin_))
      THROW_RUNTIME_ERROR("Insert row was not reserved in the append area");
    auto& page = pages_[row >> page_shift_];
    const auto slot = row & page_mask_;
    const auto& segment = slot < page.prefix_rows ? page.prefix : page.suffix;
    if (segment->frozen)
      THROW_STORAGE_EXCEPTION("Insert append area was not prepared before WAL");
    if (!append_prepared_ && segment.use_count() > 1)
      THROW_STORAGE_EXCEPTION("Shared insert append area was not prepared");
    segment->data[slot < page.prefix_rows ? slot : slot - page.prefix_rows] =
        value;
    segment->MarkDirty();
  }

  size_t rows_per_chunk_, rows_per_page_;
  int page_shift_;
  size_t page_mask_;
  size_t size_ = 0, append_begin_ = 0, cow_bytes_copied_ = 0;
  bool append_prepared_ = false;
  std::vector<Page> pages_;
  Checkpoint* ckp_ = nullptr;
  MemoryLevel level_ = MemoryLevel::kInMemory;
};

template <typename T>
class ChunkedRefColumn : public TypedRefColumn<T> {
 public:
  using value_type = T;
  explicit ChunkedRefColumn(const ChunkedColumn<T>& column)
      : TypedRefColumn<T>(
            &column, column.size(),
            [](const void* source, size_t row) {
              return static_cast<const ChunkedColumn<T>*>(source)->get_view(
                  row);
            },
            [](const void* source, size_t row) {
              return static_cast<const ChunkedColumn<T>*>(source)->get_span(
                  row);
            }),
        column_(column) {}
  T get_view(size_t row) const { return column_.get_view(row); }

 private:
  const ChunkedColumn<T>& column_;
};
}  // namespace neug
