/** Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * This file is based on the DuckDB project
 * (https://github.com/duckdb/duckdb) Licensed under the MIT License.
 */

// DuckDB execution/index/art/art.hpp, adapted to standalone NeuG storage.
// BoundIndex/catalog/constraints/deletion/merge/vacuum/legacy storage removed.

#pragma once

#include <shared_mutex>
#include <span>
#include "neug/storages/index/art_tree/art_key.h"
#include "neug/storages/index/art_tree/node.h"
#include "neug/utils/api.h"
#include "neug/utils/property/types.h"

namespace neug {
class Checkpoint;
struct ModuleDescriptor;
}  // namespace neug

namespace neug::art {

// The unique-constraint alternative is removed for append-only candidate IDs.
enum class ARTConflictType : uint8_t { NO_CONFLICT = 0 };

struct ARTKeyValue {
  vector<uint8_t> key;
  index_id_t value;
};

class NEUG_API ArtTree {
 public:
  friend class Leaf;
  struct ReadState;

 public:
  class ReadSession;
  class PrefixCursor;

 public:
  //! FixedSizeAllocator count of the ART (the deprecated LEAF allocator
  //! removed).
  static constexpr uint8_t ALLOCATOR_COUNT = 8;

 public:
  // NeuG adaptation of ART's database-bound constructor: an owned standalone
  // tree.
  explicit ArtTree(Checkpoint& checkpoint,
                   MemoryLevel level = MemoryLevel::kInMemory,
                   uint8_t prefix_capacity = 15);
  void Detach(Checkpoint& checkpoint, MemoryLevel level);
  ~ArtTree();
  ArtTree(const ArtTree&) = delete;
  ArtTree& operator=(const ArtTree&) = delete;

 public:
  //! Insert a chunk. DataChunk/Vector are adapted to spans of NeuG Values/IDs.
  // One arena and one lock per batch; NULLs and existing pairs are skipped.
  size_t Insert(std::span<const Value> keys, std::span<const index_id_t> ids);
  bool Insert(const Value& key, index_id_t id);
  // Caller-generated, prefix-free encoded keys. No Value/string re-encoding.
  bool Insert(const ARTKey& key, index_id_t value);
  vector<index_id_t> Lookup(const ARTKey& key) const;
  ReadSession OpenRead() const;

  //! Build an ART from sorted keys and their row IDs, using ARTBuilder.
  // Input must contain non-NULL sorted Values; the destination must be empty.
  ARTConflictType Build(std::span<const Value> keys,
                        std::span<const index_id_t> ids);
  // Build caller-encoded keys without re-encoding. Keys must be non-empty,
  // strictly sorted and prefix-free; values may repeat (including zero).
  // The destination must be empty. Input bytes need only live until return.
  ARTConflictType BuildEncoded(std::span<const ARTKey> keys,
                               std::span<const index_id_t> values);

  // NeuG Value-facing query adapters. Traversal uses the upstream Search
  // methods.
  bool Contains(const Value& key, index_id_t id) const;
  std::vector<index_id_t> Lookup(const Value& key) const;
  std::vector<index_id_t> Range(const std::optional<Value>& lower,
                                const std::optional<Value>& upper,
                                bool lower_inclusive = true,
                                bool upper_inclusive = true) const;

  // Replace descriptor with the current ART checkpoint metadata on success.
  void Save(Checkpoint& checkpoint, ModuleDescriptor& descriptor) const;
  static std::unique_ptr<ArtTree> Load(
      const ModuleDescriptor& descriptor, Checkpoint& checkpoint,
      MemoryLevel level = MemoryLevel::kInMemory);

  // Report memory requested by the ART index, excluding additional overhead
  // introduced by the underlying allocator's size-rounding policy.
  MemoryUsage GetMemoryUsage() const;

 public:
  //! ART key generation. DataChunk/Vector are adapted to Value/ID spans.
  void GenerateKeys(ARTArenaAllocator& allocator, std::span<const Value> input,
                    unsafe_vector<ARTKey>& keys) const;
  void GenerateKeyVectors(ARTArenaAllocator& allocator,
                          std::span<const Value> input,
                          std::span<const index_id_t> row_ids,
                          unsafe_vector<ARTKey>& keys,
                          unsafe_vector<ARTKey>& row_id_keys);

  //! Release empty buffers after bulk construction; live node addresses stay
  //! unchanged.
  void RemoveEmptyBuffers();

  //! Verifies the nodes.
  void Verify();
  //! Verifies that the node allocations match the node counts.
  void VerifyAllocations();
  //! Verifies the index buffers.
  void VerifyBuffers();
  //! Returns string representation of the ART.
  string ToString(bool display_ascii = false);
  //! Returns the configured prefix byte capacity.
  uint8_t PrefixCount() const { return prefix_count; }

 private:
  friend class NodePtr;
  friend class ARTOperator;
  //! Root of the tree.
  NodePtr tree = NodePtr();
  //! Fixed-size allocators holding the ART nodes (owned by this standalone
  //! tree).
  std::unique_ptr<array<unsafe_unique_ptr<FixedSizeAllocator>, ALLOCATOR_COUNT>>
      allocators;
  //! The number of bytes fitting in the prefix.
  uint8_t prefix_count;
  // NeuG snapshots share this tree; the storage adapter supplies operation
  // locks.
  mutable std::shared_mutex mutex;

  //! Insert keys and row_ids into ART. Constraint/rollback branches removed.
  size_t InsertKeys(ARTArenaAllocator& arena, const unsafe_vector<ARTKey>& keys,
                    const unsafe_vector<ARTKey>& row_id_keys);
  ARTConflictType Build(unsafe_vector<ARTKey>& keys,
                        unsafe_vector<ARTKey>& row_ids, idx_t row_count);

  OptionalNodePtr FindEncodedKey(const ARTKey& key, bool inserting) const;
  vector<index_id_t> LookupEncodedUnlocked(const ARTKey& key) const;
  bool FullScan(idx_t max_count, set<row_t>& row_ids) const;
  bool SearchEqual(const ARTKey& key, idx_t max_count,
                   set<row_t>& row_ids) const;
  bool SearchGreater(const ARTKey& key, bool equal, idx_t max_count,
                     set<row_t>& row_ids) const;
  bool SearchLess(const ARTKey& upper_bound, bool equal, idx_t max_count,
                  set<row_t>& row_ids) const;
  bool SearchCloseRange(const ARTKey& lower_bound, const ARTKey& upper_bound,
                        bool left_equal, bool right_equal, idx_t max_count,
                        set<row_t>& row_ids) const;

  string ToStringInternal(bool display_ascii);
  void VerifyInternal();
  void VerifyAllocationsInternal();
};
// Sessions share one read lock, including across cursor resets and multiple
// cursors. The tree must outlive its sessions/cursors.
class NEUG_API ArtTree::ReadSession {
 public:
  vector<index_id_t> Lookup(const ARTKey& key) const;
  PrefixCursor ScanPrefix(const ARTKey& prefix) const;

 private:
  friend class ArtTree;
  explicit ReadSession(std::shared_ptr<ReadState> state)
      : state(std::move(state)) {}
  std::shared_ptr<ReadState> state;
};
class NEUG_API ArtTree::PrefixCursor {
 public:
  ~PrefixCursor();
  PrefixCursor(PrefixCursor&&) noexcept;
  PrefixCursor& operator=(PrefixCursor&&) noexcept;
  PrefixCursor(const PrefixCursor&) = delete;
  PrefixCursor& operator=(const PrefixCursor&) = delete;
  std::optional<ARTKeyValue> Next();
  void Reset();

 private:
  friend class ReadSession;
  struct State;
  PrefixCursor(std::shared_ptr<ReadState> state, const ARTKey& prefix);
  std::unique_ptr<State> state;
};
}  // namespace neug::art
