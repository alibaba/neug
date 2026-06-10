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
#include <unordered_map>
#include <utility>
#include <vector>

#include "neug/common/types.h"
#include "neug/execution/common/context_chunk.h"
#include "neug/execution/common/data_chunk.h"
#include "neug/utils/result.h"

namespace neug {
class StorageReadInterface;

namespace execution {
class IContextColumn;

/**
 * @brief Context is a multi-chunk container passed between operators.
 *
 * A Context holds one or more ContextChunks (DataChunk + head pairs) that
 * share the same schema. Operators iterate chunks via `apply_chunks`, whose
 * callback receives a `ContextChunk&&` (by-value ownership) and returns
 * `result<ContextChunk>`. The single-chunk case (today's typical query path)
 * always has exactly one chunk at index 0; multi-chunk support enables batch
 * IO scenarios where data arrives in chunks.
 */
class Context {
 public:
  Context();

  ~Context() = default;

  void clear();

  // --- Chunk access ---

  /// Returns the number of chunks (always >= 1).
  size_t chunk_num() const;

  /// Returns a mutable reference to the chunk at the given index.
  ContextChunk& chunk(size_t idx);

  /// Returns a const reference to the chunk at the given index.
  const ContextChunk& chunk(size_t idx) const;

  /// Returns a mutable reference to all chunks.
  std::vector<ContextChunk>& chunks();

  /// Returns a const reference to all chunks.
  const std::vector<ContextChunk>& chunks() const;

  /// Appends a chunk (with no head) to this Context.
  void append_chunk(DataChunk&& chunk);

  /// Appends a chunk with the given head column.
  void append_chunk(DataChunk&& chunk, std::shared_ptr<IContextColumn> head);

  /// Appends a fully-formed ContextChunk to this Context.
  void append_chunk(ContextChunk&& chunk);

  /// Applies a chunk-level operation to every chunk in this Context.
  template <typename F>
  neug::result<Context> apply_chunks(F&& func) {
    Context out;
    out.tag_ids = std::move(tag_ids);
    std::vector<ContextChunk> in = std::move(chunks_);
    out.chunks_.reserve(in.size());
    for (auto& cc : in) {
      auto r = func(std::move(cc));
      if (!r) {
        return tl::make_unexpected(r.error());
      }
      out.chunks_.push_back(std::move(*r));
    }
    return out;
  }

  // --- Cross-chunk operations ---

  /// Merges all chunks into a single chunk using ContextChunk::union_with.
  /// No-op if already single-chunk or empty.
  void flatten();

  /// Ensures single-chunk. If multi-chunk, logs a warning and flattens.
  void ensure_single_chunk(const char* caller);

  /// Returns the number of columns (from the first chunk, 0 if empty).
  size_t col_num() const;

  /// Returns the total number of rows across all chunks.
  size_t row_num() const;

  std::vector<int> tag_ids;

 private:
  std::vector<ContextChunk> chunks_;
};

class ContextMeta {
 public:
  ContextMeta() = default;
  ~ContextMeta() = default;

  bool exist(int alias) const {
    return alias_set_.find(alias) != alias_set_.end();
  }

  void set(int32_t alias, const DataType& type) {
    if (alias >= 0) {
      alias_set_.emplace(alias, type);
    }
  }

  DataType get(int32_t alias) const { return alias_set_.at(alias); }

  const std::unordered_map<int32_t, DataType>& columns() const {
    return alias_set_;
  }

 private:
  std::unordered_map<int32_t, DataType> alias_set_;
};

}  // namespace execution

}  // namespace neug
