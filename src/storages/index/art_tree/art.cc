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

#include "neug/storages/index/art_tree/art.h"

#include <algorithm>
#include <charconv>
#include "neug/storages/checkpoint.h"
#include "neug/storages/index/art_tree/art_builder.h"
#include "neug/storages/index/art_tree/art_operator.h"
#include "neug/storages/index/art_tree/art_scanner.h"
#include "neug/storages/index/art_tree/base_leaf.h"
#include "neug/storages/index/art_tree/base_node.h"
#include "neug/storages/index/art_tree/iterator.h"
#include "neug/storages/index/art_tree/node256.h"
#include "neug/storages/index/art_tree/node256_leaf.h"
#include "neug/storages/index/art_tree/node48.h"
#include "neug/storages/index/art_tree/prefix.h"
#include "neug/storages/module_descriptor.h"
namespace neug::art {
ArtTree::ArtTree(Checkpoint& checkpoint, MemoryLevel level, uint8_t capacity)
    : prefix_count(capacity) {
  // Prefix child refs must remain naturally aligned, as in the original layout.
  if (!capacity || capacity > 239 || (capacity + 1) % 8)
    throw InternalException("Invalid ART prefix capacity");
  allocators = std::make_unique<
      array<unsafe_unique_ptr<FixedSizeAllocator>, ALLOCATOR_COUNT>>();
  const idx_t sizes[] = {idx_t(capacity) + Prefix::METADATA_SIZE,
                         sizeof(Node4),
                         sizeof(Node16),
                         sizeof(Node48),
                         sizeof(Node256),
                         sizeof(Node7Leaf),
                         sizeof(Node15Leaf),
                         sizeof(Node256Leaf)};
  for (idx_t i = 0; i < ALLOCATOR_COUNT; ++i)
    (*allocators)[i] =
        std::make_unique<FixedSizeAllocator>(sizes[i], checkpoint, level);
}
ArtTree::~ArtTree() = default;
void ArtTree::Detach(Checkpoint& checkpoint, MemoryLevel level) {
  std::unique_lock lock(mutex);
  for (auto& allocator : *allocators)
    allocator->Detach(checkpoint, level);
}
// Adapted from DuckDB's GenerateKeys/GenerateKeyVectors batch path. NeuG
// supplies Values directly, so DataChunk, selection vectors and compound-key
// conversion are not needed. All encoded bytes share this batch's arena.
void ArtTree::GenerateKeys(ARTArenaAllocator& arena,
                           std::span<const neug::Value> values,
                           vector<ARTKey>& keys) const {
  keys.resize(values.size());
  for (size_t i = 0; i < values.size(); ++i)
    keys[i] = ARTKey::CreateKey(arena, values[i]);
}
void ArtTree::GenerateKeyVectors(ARTArenaAllocator& arena,
                                 std::span<const neug::Value> values,
                                 std::span<const index_id_t> ids,
                                 vector<ARTKey>& keys,
                                 vector<ARTKey>& row_id_keys) {
  if (values.size() != ids.size())
    throw InternalException("ART batch key/id count mismatch");
  for (auto id : ids)
    if (id == INVALID_INDEX_ID)
      throw InternalException("Invalid NeuG index id");
  GenerateKeys(arena, values, keys);
  row_id_keys.resize(ids.size());
  for (size_t i = 0; i < ids.size(); ++i)
    row_id_keys[i] = ARTKey::CreateARTKey<row_t>(arena, ids[i]);
}
namespace {
const string ALLOCATOR_PREFIX = "art_allocator_";
uint64_t ParseMetadata(const string& text) {
  uint64_t value;
  const auto [end, error] =
      std::from_chars(text.data(), text.data() + text.size(), value);
  if (error != std::errc{} || end != text.data() + text.size())
    throw SerializationException("Invalid ART metadata integer");
  return value;
}
uint64_t ReadMetadata(const ModuleDescriptor& descriptor, const string& key) {
  auto value = descriptor.get(key);
  if (!value)
    throw SerializationException("Missing ART metadata: " + key);
  return ParseMetadata(*value);
}

}  // namespace

void ArtTree::Save(Checkpoint& checkpoint, ModuleDescriptor& descriptor) const {
  std::unique_lock lock(mutex);
  // Replace the descriptor only after every block has been stored successfully.
  ModuleDescriptor next;
  next.set("art_endian",
           std::endian::native == std::endian::little ? "1" : "2");
  next.set("art_prefix_count", std::to_string(prefix_count));
  next.set("art_root", std::to_string(tree.Get()));
  next.set("art_allocator_count", std::to_string(ALLOCATOR_COUNT));
  next.set("art_block_size",
           std::to_string(FixedSizeBuffer::BUFFER_ALLOC_SIZE));
  auto writer = checkpoint.CreateObjectWriter();
  for (idx_t i = 0; i < ALLOCATOR_COUNT; ++i) {
    auto& allocator = *(*allocators)[i];
    allocator.RemoveEmptyBuffers();
    allocator.Save(writer);
  }
  writer.Commit();
  for (idx_t i = 0; i < ALLOCATOR_COUNT; ++i) {
    auto& allocator = *(*allocators)[i];
    const auto prefix = ALLOCATOR_PREFIX + std::to_string(i) + "_";
    next.set(prefix + "segment_size",
             std::to_string(allocator.GetSegmentSize()));
    next.set(prefix + "segment_count",
             std::to_string(allocator.GetSegmentCount()));
    const auto buffer_prefix = prefix + "buffer_";
    const auto& blocks = allocator.SaveMetadata(next, buffer_prefix);
    next.set(prefix + "buffer_count", std::to_string(blocks.size()));
  }
  descriptor = std::move(next);
}
std::unique_ptr<ArtTree> ArtTree::Load(const ModuleDescriptor& descriptor,
                                       Checkpoint& checkpoint,
                                       MemoryLevel level) {
  if (ReadMetadata(descriptor, "art_allocator_count") != ALLOCATOR_COUNT ||
      ReadMetadata(descriptor, "art_block_size") !=
          FixedSizeBuffer::BUFFER_ALLOC_SIZE)
    throw SerializationException("Unsupported ART block format");
  if (ReadMetadata(descriptor, "art_endian") !=
      (std::endian::native == std::endian::little ? 1 : 2))
    throw SerializationException("ART payload endian mismatch");
  auto prefix_count = ReadMetadata(descriptor, "art_prefix_count");
  if (!prefix_count || prefix_count > 239 || (prefix_count + 1) % 8)
    throw SerializationException("Invalid ART prefix size");
  auto result = std::make_unique<ArtTree>(checkpoint, level, prefix_count);
  auto reader = checkpoint.CreateObjectReader();
  idx_t total_nodes = 0;
  for (idx_t i = 0; i < ALLOCATOR_COUNT; ++i) {
    auto& allocator = *(*result->allocators)[i];
    const auto prefix = ALLOCATOR_PREFIX + std::to_string(i) + "_";
    if (ReadMetadata(descriptor, prefix + "segment_size") !=
        allocator.GetSegmentSize())
      throw SerializationException("ART segment size mismatch");
    const auto buffer_prefix = prefix + "buffer_";
    const auto& paths = descriptor.paths();
    const auto buffer_count =
        std::count_if(paths.begin(), paths.end(), [&](const auto& entry) {
          return entry.first.starts_with(buffer_prefix);
        });
    if (static_cast<uint64_t>(buffer_count) !=
        ReadMetadata(descriptor, prefix + "buffer_count"))
      throw SerializationException("ART buffer count mismatch");
    allocator.Load(checkpoint, reader, descriptor, buffer_prefix);
    if (allocator.GetSegmentCount() !=
        ReadMetadata(descriptor, prefix + "segment_count"))
      throw SerializationException("ART segment count mismatch");
    total_nodes += allocator.GetSegmentCount();
  }
  result->tree.Set(ReadMetadata(descriptor, "art_root"));
  if (result->tree.Get() != 0) {
    idx_t visited_nodes = 0;
    // The scanner calls this before dereferencing each node.
    ARTConstScanPreorder(
        *result, result->tree, [&](const NodePtr& node) -> OptionalNodePtr {
          const auto type = node.GetType();
          if (type == NType::LEAF_INLINED)
            return {};

          NodePtr::GetAllocator(*result, type).VerifyReference(node);
          if (++visited_nodes > total_nodes)
            throw SerializationException(
                "ART traversal exceeds allocated nodes");

          if (type == NType::PREFIX) {
            const ConstNodeHandle handle(*result, node);
            const auto length = handle.GetPtr()[prefix_count];
            if (length == 0 || length > prefix_count)
              throw SerializationException("Invalid ART prefix length");
          }
          return node;
        });
  }
  return result;
}

MemoryUsage ArtTree::GetMemoryUsage() const {
  std::shared_lock lock(mutex);
  MemoryUsage usage;
  usage.directory_bytes = sizeof(ArtTree) + sizeof(*allocators);
  for (const auto& allocator : *allocators)
    usage += allocator->GetMemoryUsage();
  return usage;
}
string ArtTree::ToString(bool display_ascii) {
  std::unique_lock lock(mutex);
  return ToStringInternal(display_ascii);
}

string ArtTree::ToStringInternal(bool display_ascii) {
  if (tree.HasMetadata()) {
    return "\nART: \n" +
           tree.ToString(*this, ToStringOptions(false, display_ascii, nullptr,
                                                0, 0, false));
  }
  return "[empty]";
}

void ArtTree::Verify() {
  std::unique_lock lock(mutex);
  VerifyInternal();
}

void ArtTree::VerifyInternal() {
  if (tree.HasMetadata()) {
    tree.Verify(*this);
  }
}

void ArtTree::VerifyAllocations() {
  std::unique_lock lock(mutex);
  return VerifyAllocationsInternal();
}

void ArtTree::VerifyAllocationsInternal() {
#ifdef DEBUG
  unordered_map<uint8_t, idx_t> node_counts;
  for (idx_t i = 0; i < allocators->size(); i++) {
    node_counts[UnsafeNumericCast<uint8_t>(i)] = 0;
  }

  if (tree.HasMetadata()) {
    tree.VerifyAllocations(*this, node_counts);
  }

  for (idx_t i = 0; i < allocators->size(); i++) {
    auto segment_count = (*allocators)[i]->GetSegmentCount();
    D_ASSERT(segment_count == node_counts[UnsafeNumericCast<uint8_t>(i)]);
  }
#endif
}

void ArtTree::RemoveEmptyBuffers() {
  std::unique_lock lock(mutex);
  for (auto& allocator : *allocators)
    allocator->RemoveEmptyBuffers();
}

void ArtTree::VerifyBuffers() {
  std::unique_lock lock(mutex);
  for (auto& allocator : *allocators) {
    allocator->VerifyBuffers();
  }
}

bool ArtTree::FullScan(idx_t max_count, set<row_t>& row_ids) const {
  if (!tree.HasMetadata()) {
    return true;
  }
  Iterator it(*this);
  it.FindMinimum(tree);
  const auto empty_key = ARTKey();
  RowIdSetOutput output(row_ids, max_count);
  return it.Scan(empty_key, output, false) == ARTScanResult::COMPLETED;
}

bool ArtTree::SearchEqual(const ARTKey& key, idx_t max_count,
                          set<row_t>& row_ids) const {
  auto leaf = ARTOperator::Lookup(*this, tree, key, 0);
  if (!leaf) {
    return true;
  }

  Iterator it(*this);
  it.FindMinimum(leaf.Get());
  const auto empty_key = ARTKey();
  RowIdSetOutput output(row_ids, max_count);
  return it.Scan(empty_key, output, false) == ARTScanResult::COMPLETED;
}

bool ArtTree::SearchGreater(const ARTKey& key, bool equal, idx_t max_count,
                            set<row_t>& row_ids) const {
  if (!tree.HasMetadata()) {
    return true;
  }

  // Find the lowest value that satisfies the predicate.
  Iterator it(*this);

  // Early-out, if the maximum value in the ArtTree is lower than the lower
  // bound.
  if (!it.LowerBound(tree, key, equal)) {
    return true;
  }

  // We continue the scan. We do not check the bounds as any value following
  // this value is greater and satisfies our predicate.
  RowIdSetOutput output(row_ids, max_count);
  return it.Scan(ARTKey(), output, false) == ARTScanResult::COMPLETED;
}

bool ArtTree::SearchLess(const ARTKey& upper_bound, bool equal, idx_t max_count,
                         set<row_t>& row_ids) const {
  if (!tree.HasMetadata()) {
    return true;
  }

  // Find the minimum value in the ArtTree: we start scanning from this value.
  Iterator it(*this);
  it.FindMinimum(tree);

  // Early-out, if the minimum value is higher than the upper bound.
  if (it.current_key.GreaterThan(upper_bound, equal, it.GetNestedDepth())) {
    return true;
  }

  // Continue the scan until we reach the upper bound.
  RowIdSetOutput output(row_ids, max_count);
  return it.Scan(upper_bound, output, equal) == ARTScanResult::COMPLETED;
}

bool ArtTree::SearchCloseRange(const ARTKey& lower_bound,
                               const ARTKey& upper_bound, bool left_equal,
                               bool right_equal, idx_t max_count,
                               set<row_t>& row_ids) const {
  if (!tree.HasMetadata()) {
    return true;
  }

  // Find the first node that satisfies the left predicate.
  Iterator it(*this);

  // Early-out, if the maximum value in the ArtTree is lower than the lower
  // bound.
  if (!it.LowerBound(tree, lower_bound, left_equal)) {
    return true;
  }

  // Continue the scan until we reach the upper bound.
  RowIdSetOutput output(row_ids, max_count);
  return it.Scan(upper_bound, output, right_equal) == ARTScanResult::COMPLETED;
}

bool ArtTree::Insert(const neug::Value& key, index_id_t id) {
  if (id == std::numeric_limits<index_id_t>::max())
    throw InternalException("Invalid NeuG index id");
  std::unique_lock lock(mutex);
  ARTArenaAllocator arena;
  auto encoded = ARTKey::CreateKey(arena, key);
  if (encoded.Empty())
    return false;
  auto row = ARTKey::CreateARTKey<row_t>(arena, id);
  auto leaf = ARTOperator::Lookup(*this, tree, encoded, 0);
  if (leaf && ARTOperator::LookupInLeaf(*this, leaf.Get(), row))
    return false;
  ARTOperator::Insert(arena, *this, tree, encoded, 0, row,
                      GateStatus::GATE_NOT_SET);
  return true;
}
size_t ArtTree::Insert(std::span<const neug::Value> values,
                       std::span<const index_id_t> ids) {
  std::unique_lock lock(mutex);
  ARTArenaAllocator arena;
  vector<ARTKey> keys, row_id_keys;
  GenerateKeyVectors(arena, values, ids, keys, row_id_keys);
  return InsertKeys(arena, keys, row_id_keys);
}
// Preserve the upstream InsertKeys loop, adapted to NeuG append-only
// semantics: skip NULLs and existing pairs rather than constraint rollback.
size_t ArtTree::InsertKeys(ARTArenaAllocator& arena, const vector<ARTKey>& keys,
                           const vector<ARTKey>& row_id_keys) {
  size_t inserted = 0;
  for (size_t i = 0; i < keys.size(); ++i) {
    if (keys[i].Empty())
      continue;
    auto leaf = ARTOperator::Lookup(*this, tree, keys[i], 0);
    if (leaf && ARTOperator::LookupInLeaf(*this, leaf.Get(), row_id_keys[i]))
      continue;
    ARTOperator::Insert(arena, *this, tree, keys[i], 0, row_id_keys[i],
                        GateStatus::GATE_NOT_SET);
    ++inserted;
  }
  return inserted;
}
bool ArtTree::Contains(const neug::Value& key, index_id_t id) const {
  std::shared_lock lock(mutex);
  ARTArenaAllocator arena;
  auto encoded = ARTKey::CreateKey(arena, key);
  if (encoded.Empty() || id == INVALID_INDEX_ID)
    return false;
  auto row = ARTKey::CreateARTKey<row_t>(arena, id);
  auto leaf = ARTOperator::Lookup(*this, tree, encoded, 0);
  return leaf && ARTOperator::LookupInLeaf(*this, leaf.Get(), row);
}
std::vector<index_id_t> ArtTree::Lookup(const neug::Value& key) const {
  return Range(key, key);
}
std::vector<index_id_t> ArtTree::Range(const std::optional<neug::Value>& lower,
                                       const std::optional<neug::Value>& upper,
                                       bool lower_inclusive,
                                       bool upper_inclusive) const {
  std::shared_lock lock(mutex);
  std::vector<index_id_t> result;
  ARTArenaAllocator arena;
  ARTKey lo, hi;
  if (lower)
    lo = ARTKey::CreateKey(arena, *lower);
  if (upper)
    hi = ARTKey::CreateKey(arena, *upper);
  if ((lower && lo.Empty()) || (upper && hi.Empty()) || !tree.HasMetadata())
    return result;
  if (lower && upper &&
      (lo > hi || (lo == hi && (!lower_inclusive || !upper_inclusive))))
    return result;
  set<row_t> ids;
  const auto max_count = ARTNumericLimits<idx_t>::Maximum();
  if (lower && upper) {
    if (lo == hi)
      SearchEqual(lo, max_count, ids);
    else
      SearchCloseRange(lo, hi, lower_inclusive, upper_inclusive, max_count,
                       ids);
  } else if (lower) {
    SearchGreater(lo, lower_inclusive, max_count, ids);
  } else if (upper) {
    SearchLess(hi, upper_inclusive, max_count, ids);
  } else {
    FullScan(max_count, ids);
  }
  result.reserve(ids.size());
  for (auto id : ids) {
    if (id < 0 || uint64_t(id) >= std::numeric_limits<index_id_t>::max())
      throw InternalException("ART id outside NeuG index id range");
    result.push_back(static_cast<index_id_t>(id));
  }
  return result;
}

ARTConflictType ArtTree::Build(std::span<const Value> values,
                               std::span<const index_id_t> ids) {
  std::unique_lock lock(mutex);
  if (tree.HasMetadata())
    throw InternalException("ART Build requires an empty tree");
  ARTArenaAllocator arena;
  unsafe_vector<ARTKey> keys, row_id_keys;
  GenerateKeyVectors(arena, values, ids, keys, row_id_keys);
  if (keys.empty())
    return ARTConflictType::NO_CONFLICT;
  for (idx_t i = 0; i < keys.size(); ++i) {
    if (keys[i].Empty() || (i && keys[i - 1] > keys[i]))
      throw InternalException("ART Build requires sorted non-NULL keys");
  }
  auto result = Build(keys, row_id_keys, keys.size());
  for (auto& allocator : *allocators)
    allocator->RemoveEmptyBuffers();
  return result;
}

ARTConflictType ArtTree::BuildEncoded(std::span<const ARTKey> input,
                                      std::span<const index_id_t> values) {
  std::unique_lock lock(mutex);
  if (tree.HasMetadata())
    throw InternalException("ART Build requires an empty tree");
  if (input.size() != values.size())
    throw InternalException("ART encoded build key/value count mismatch");
  // Validate the entire input before ARTBuilder mutates the tree. A prefix
  // pair would otherwise make the builder read beyond the shorter key.
  for (size_t i = 0; i < input.size(); ++i) {
    if (input[i].Empty() || !input[i].data || values[i] == INVALID_INDEX_ID)
      throw InternalException("Invalid encoded ART key or value");
    if (i) {
      auto common = std::min(input[i - 1].len, input[i].len);
      if (std::memcmp(input[i - 1].data, input[i].data, common) >= 0)
        throw InternalException(
            "ART encoded build requires sorted unique prefix-free keys");
    }
  }
  if (input.empty())
    return ARTConflictType::NO_CONFLICT;
  ARTArenaAllocator arena;
  unsafe_vector<ARTKey> row_keys;
  row_keys.reserve(values.size());
  for (auto value : values)
    row_keys.push_back(ARTKey::CreateARTKey<row_t>(arena, value));
  // Unlike the Value/row-ID build adapter, payloads need not be distinct.
  ARTBuilder builder(arena, *this, input, row_keys);
  builder.Init(tree, input.size() - 1);
  auto result = builder.Build();
  for (auto& allocator : *allocators)
    allocator->RemoveEmptyBuffers();
  return result;
}

ARTConflictType ArtTree::Build(unsafe_vector<ARTKey>& keys,
                               unsafe_vector<ARTKey>& row_ids,
                               const idx_t row_count) {
  ARTArenaAllocator arena;
  ARTBuilder builder(arena, *this, keys, row_ids);
  builder.Init(tree, row_count - 1);

  auto result = builder.Build();
  if (result != ARTConflictType::NO_CONFLICT) {
    return result;
  }

#ifdef DEBUG
  set<row_t> row_ids_debug;
  Iterator it(*this);
  it.FindMinimum(tree);
  ARTKey empty_key = ARTKey();
  RowIdSetOutput output(row_ids_debug, ARTNumericLimits<idx_t>().Maximum());
  it.Scan(empty_key, output, false);
  D_ASSERT(row_count == row_ids_debug.size());
#endif

  return ARTConflictType::NO_CONFLICT;
}

OptionalNodePtr ArtTree::FindEncodedKey(const ARTKey& key,
                                        bool inserting) const {
  auto current = tree;
  idx_t depth = 0;
  auto prefix_conflict = [&]() -> OptionalNodePtr {
    if (inserting)
      throw InternalException("Encoded ART keys must be prefix-free");
    return {};
  };
  while (current.HasMetadata()) {
    if (current.IsAnyLeaf() || current.GetGateStatus() == GateStatus::GATE_SET)
      return depth == key.len ? OptionalNodePtr(current) : prefix_conflict();
    if (current.GetType() == NType::PREFIX) {
      ConstNodeHandle handle(*this, current);
      auto data = handle.GetPtr();
      for (idx_t i = 0; i < data[PrefixCount()]; ++i) {
        if (depth == key.len)
          return prefix_conflict();
        if (data[i] != key[depth++])
          return {};
      }
      current = ConstPrefixHandle::ChildRef(*this, handle);
    } else {
      if (depth == key.len)
        return prefix_conflict();
      auto child = current.GetChildNode(*this, key[depth++]);
      if (!child)
        return {};
      current = child.Get();
    }
  }
  return {};
}
bool ArtTree::Insert(const ARTKey& key, index_id_t value) {
  if (key.Empty() || !key.data || value == INVALID_INDEX_ID)
    throw InternalException("Invalid encoded ART key or value");
  std::unique_lock lock(mutex);
  ARTArenaAllocator arena;
  auto row = ARTKey::CreateARTKey<row_t>(arena, value);
  auto leaf = FindEncodedKey(key, true);
  if (leaf && ARTOperator::LookupInLeaf(*this, leaf.Get(), row))
    return false;
  ARTOperator::Insert(arena, *this, tree, key, 0, row,
                      GateStatus::GATE_NOT_SET);
  return true;
}
vector<index_id_t> ArtTree::LookupEncodedUnlocked(const ARTKey& key) const {
  vector<index_id_t> result;
  if (key.Empty() || !key.data || !FindEncodedKey(key, false))
    return result;
  set<row_t> values;
  SearchEqual(key, ARTNumericLimits<idx_t>::Maximum(), values);
  for (auto value : values) {
    if (value < 0 || uint64_t(value) >= INVALID_INDEX_ID)
      throw InternalException("ART value outside NeuG index id range");
    result.push_back(static_cast<index_id_t>(value));
  }
  return result;
}
vector<index_id_t> ArtTree::Lookup(const ARTKey& key) const {
  std::shared_lock lock(mutex);
  return LookupEncodedUnlocked(key);
}
struct ArtTree::ReadState {
  const ArtTree& art;
  std::shared_lock<std::shared_mutex> lock;
  explicit ReadState(const ArtTree& art) : art(art), lock(art.mutex) {}
};
struct ArtTree::PrefixCursor::State {
  std::shared_ptr<ReadState> read;
  vector<uint8_t> prefix;
  std::unique_ptr<Iterator> iterator;
  bool complete = false;
  State(std::shared_ptr<ReadState> read, const ARTKey& key)
      : read(std::move(read)) {
    if (key.len)
      prefix.assign(key.data, key.data + key.len);
    Reset();
  }
  void Reset() {
    iterator = std::make_unique<Iterator>(read->art);
    ARTKey key(prefix.data(), prefix.size());
    complete = !iterator->SeekPrefix(read->art.tree, key);
  }
};
ArtTree::ReadSession ArtTree::OpenRead() const {
  return ReadSession(std::make_shared<ReadState>(*this));
}
vector<index_id_t> ArtTree::ReadSession::Lookup(const ARTKey& key) const {
  return state->art.LookupEncodedUnlocked(key);
}
ArtTree::PrefixCursor ArtTree::ReadSession::ScanPrefix(
    const ARTKey& prefix) const {
  return PrefixCursor(state, prefix);
}
ArtTree::PrefixCursor::PrefixCursor(std::shared_ptr<ReadState> read,
                                    const ARTKey& key)
    : state(std::make_unique<State>(std::move(read), key)) {}
ArtTree::PrefixCursor::~PrefixCursor() = default;
ArtTree::PrefixCursor::PrefixCursor(PrefixCursor&&) noexcept = default;
ArtTree::PrefixCursor& ArtTree::PrefixCursor::operator=(
    PrefixCursor&&) noexcept = default;
void ArtTree::PrefixCursor::Reset() { state->Reset(); }
std::optional<ARTKeyValue> ArtTree::PrefixCursor::Next() {
  if (state->complete)
    return {};
  vector<ARTKeyValue> rows;
  KeyValueOutput output{rows, 1, {}};
  state->complete =
      state->iterator->Scan(ARTKey(), output, true) == ARTScanResult::COMPLETED;
  if (rows.empty())
    return {};
  auto& row = rows.front();
  if (row.key.size() < state->prefix.size() ||
      !std::equal(state->prefix.begin(), state->prefix.end(),
                  row.key.begin())) {
    state->complete = true;
    return {};
  }
  return std::move(row);
}
}  // namespace neug::art
