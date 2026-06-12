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

#include <memory>
#include <utility>
#include <vector>

#include "neug/execution/common/data_chunk.h"

namespace neug {

namespace execution {

/**
 * @brief A ContextChunk pairs a DataChunk with its execution-time `head`
 * column.
 *
 * A ContextChunk is the unit of execution: operators and helpers consume and
 * mutate ContextChunks, while DataChunk underneath stays a pure data
 * container.
 *
 * The `head` column tracks the most recently produced column and backs
 * anonymous reads/writes (alias = -1) inside an operator pipeline. set/get
 * with alias = -1 operate on `head`; reshuffle / optional_reshuffle keep
 * `head` in sync with the named columns (sharing pointer identity when
 * `head` aliases one of the named columns).
 */
class ContextChunk {
 public:
  ContextChunk() = default;
  ~ContextChunk() = default;

  ContextChunk(DataChunk&& chunk);  // NOLINT(runtime/explicit)
  ContextChunk(DataChunk&& chunk, std::shared_ptr<IContextColumn> head);

  ContextChunk(const ContextChunk& other) = default;
  ContextChunk& operator=(const ContextChunk& other) = default;
  ContextChunk(ContextChunk&& other) noexcept = default;
  ContextChunk& operator=(ContextChunk&& other) noexcept = default;

  // ---- access ----

  DataChunk& chunk();
  const DataChunk& chunk() const;
  std::shared_ptr<IContextColumn>& head();
  const std::shared_ptr<IContextColumn>& head() const;

  std::vector<std::shared_ptr<IContextColumn>>& columns();
  const std::vector<std::shared_ptr<IContextColumn>>& columns() const;

  // ---- mutation ----

  void clear();

  /// Stores `col`. If alias >= 0 the column is also placed in the chunk
  /// under that alias; head is updated either way.
  void set(int alias, std::shared_ptr<IContextColumn> col);

  /// Returns the column referenced by alias. alias = -1 returns head.
  std::shared_ptr<IContextColumn> get(int alias) const;

  void set_with_reshuffle(int alias, std::shared_ptr<IContextColumn> col,
                          const std::vector<size_t>& offsets);

  void remove(int alias);

  bool exist(int alias) const;

  /// Row count of the context. Falls back to the `head` column when there are
  /// no named columns, mirroring the pre-refactor Context::row_num()
  /// semantics. DataChunk::row_num() (a pure data container) ignores head by
  /// design; ContextChunk is the successor of Context and therefore restores
  /// the head fallback so head-only seeds (e.g. the single-row seed produced
  /// by DummySourceOpr for `RETURN <literal>` or a leading UNWIND of a
  /// constant list) report the correct number of rows.
  size_t row_num() const;
  size_t col_num() const;

  /// Reshuffles every column in the chunk and the head column. If head is
  /// aliased to one of the named columns the shuffled column is reused so
  /// that pointer identity is preserved for subsequent get(-1).
  void reshuffle(const std::vector<size_t>& offsets);

  void optional_reshuffle(const std::vector<size_t>& offsets);

  /// Concatenates `other` into this ContextChunk. Heads are merged in
  /// lock-step, preserving alias-to-head pointer identity if possible.
  ContextChunk union_with(const ContextChunk& other) const;

 private:
  DataChunk chunk_;
  std::shared_ptr<IContextColumn> head_;
};

}  // namespace execution
}  // namespace neug
