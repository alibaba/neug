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

#include "bundled_edge_csr_loader.h"

#include <glog/logging.h>

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <functional>
#include <limits>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "neug/common/columns/value_columns.h"
#include "neug/common/types/container_types.h"
#include "neug/storages/csr/mutable_csr.h"
#include "neug/storages/loader/chunk_pipeline_utils.h"
#include "neug/storages/loader/loader_utils.h"
#include "neug/utils/property/types.h"

namespace neug {

namespace internal {

/// Internal append-only builder used while a fresh CSR is staged. The CSR is
/// not published until both passes complete, so this bypasses transactional
/// incremental growth.
template <typename EDATA_T>
class BundledEdgeCsrLoader::MutableWriter {
 public:
  using csr_t = MutableCsr<EDATA_T>;
  using nbr_t = typename csr_t::nbr_t;
  static constexpr EdgeStrategy kStrategy = EdgeStrategy::kMultiple;

  explicit MutableWriter(csr_t& csr) : csr_(csr) {}

  void PrepareBuild(vid_t vertex_count) {
    CHECK(csr_.adj_list_buffer_ != nullptr);
    CHECK(csr_.degree_list_ != nullptr);
    CHECK(csr_.cap_list_ != nullptr);
    CHECK(csr_.nbr_list_ != nullptr);
    csr_.adj_list_buffer_->Resize(static_cast<size_t>(vertex_count) *
                                  sizeof(nbr_t*));
    csr_.degree_list_->Resize(static_cast<size_t>(vertex_count) * sizeof(int));
    csr_.cap_list_->Resize(static_cast<size_t>(vertex_count) * sizeof(int));
    csr_.locks_ = std::make_unique<SpinLock[]>(vertex_count);
    refresh_metadata_ptrs();
    CHECK(adj_lists_ != nullptr || vertex_count == 0);
    CHECK(degrees_ != nullptr || vertex_count == 0);
    CHECK(capacities_ != nullptr || vertex_count == 0);
    for (vid_t i = 0; i < vertex_count; ++i) {
      adj_lists_[i] = nullptr;
      degrees_[i].store(0, std::memory_order_relaxed);
      capacities_[i] = 0;
    }
    csr_.edge_num_.store(0, std::memory_order_relaxed);
    csr_.unsorted_since_ = 0;
  }

  void CountSerial(vid_t src, int count = 1) {
    DCHECK_LT(src, vertex_capacity_);
    CHECK_GT(count, 0);
    auto degree = degrees_[src].load(std::memory_order_relaxed);
    CHECK_LE(degree, std::numeric_limits<int>::max() - count);
    degrees_[src].store(degree + count, std::memory_order_relaxed);
  }

  void CountConcurrent(vid_t src, int count = 1) {
    DCHECK_LT(src, vertex_capacity_);
    CHECK_GT(count, 0);
    const auto degree =
        degrees_[src].fetch_add(count, std::memory_order_relaxed);
    CHECK_LE(degree, std::numeric_limits<int>::max() - count);
  }

  void AllocateFromCounts() {
    refresh_metadata_ptrs();
    size_t total_capacity = 0;
    for (size_t i = 0; i < vertex_capacity_; ++i) {
      const auto degree = degrees_[i].load(std::memory_order_relaxed);
      const auto storage_capacity =
          mutable_csr_detail::capacity_with_reserve(degree);
      // During fill cap_list_ stores the exact expected degree, allowing both
      // passes to be validated without another O(V) metadata array.
      capacities_[i] = degree;
      CHECK_LE(static_cast<size_t>(storage_capacity),
               std::numeric_limits<size_t>::max() - total_capacity);
      total_capacity += static_cast<size_t>(storage_capacity);
    }
    CHECK_LE(total_capacity,
             std::numeric_limits<size_t>::max() / sizeof(nbr_t));
    csr_.nbr_list_->Resize(total_capacity * sizeof(nbr_t));
    nbrs_ = reinterpret_cast<nbr_t*>(csr_.nbr_list_->GetData());
    CHECK(nbrs_ != nullptr || total_capacity == 0);

    size_t offset = 0;
    for (size_t i = 0; i < vertex_capacity_; ++i) {
      const auto storage_capacity =
          mutable_csr_detail::capacity_with_reserve(capacities_[i]);
      adj_lists_[i] = storage_capacity == 0 ? nullptr : nbrs_ + offset;
      offset += static_cast<size_t>(storage_capacity);
      degrees_[i].store(0, std::memory_order_relaxed);
    }
  }

  int ReserveSerial(vid_t src, int count) {
    CHECK_LT(src, vertex_capacity_);
    CHECK_GT(count, 0);
    const auto slot = degrees_[src].load(std::memory_order_relaxed);
    CHECK_LE(slot, capacities_[src] - count);
    degrees_[src].store(slot + count, std::memory_order_relaxed);
    return slot;
  }

  int ReserveConcurrent(vid_t src, int count) {
    CHECK_LT(src, vertex_capacity_);
    CHECK_GT(count, 0);
    CHECK_LE(count, capacities_[src]);
    const auto slot = degrees_[src].fetch_add(count, std::memory_order_relaxed);
    CHECK_LE(slot, capacities_[src] - count);
    return slot;
  }

  void PutAt(vid_t src, int slot, vid_t dst, const EDATA_T& data,
             timestamp_t ts) {
    DCHECK_LT(src, vertex_capacity_);
    DCHECK_GE(slot, 0);
    DCHECK_LT(slot, capacities_[src]);
    auto& nbr = adj_lists_[src][slot];
    nbr.neighbor = dst;
    nbr.data = data;
    nbr.timestamp.store(ts, std::memory_order_relaxed);
  }

  void PutSerial(vid_t src, vid_t dst, const EDATA_T& data, timestamp_t ts) {
    PutAt(src, ReserveSerial(src, 1), dst, data, ts);
  }

  void Finish(uint64_t filled_edge_count) {
    uint64_t edge_num = 0;
    for (size_t i = 0; i < vertex_capacity_; ++i) {
      const auto degree = degrees_[i].load(std::memory_order_relaxed);
      CHECK_EQ(degree, capacities_[i])
          << "Bulk edge count/fill mismatch for vertex " << i;
      edge_num += static_cast<uint64_t>(degree);
      capacities_[i] = mutable_csr_detail::capacity_with_reserve(degree);
    }
    CHECK_EQ(edge_num, filled_edge_count);
    csr_.edge_num_.store(edge_num, std::memory_order_relaxed);
    csr_.refresh_prefetch_policy();
  }

 private:
  void refresh_metadata_ptrs() {
    vertex_capacity_ = csr_.vertex_capacity();
    adj_lists_ = reinterpret_cast<nbr_t**>(
        csr_.adj_list_buffer_ == nullptr ? nullptr
                                         : csr_.adj_list_buffer_->GetData());
    degrees_ = reinterpret_cast<std::atomic<int>*>(
        csr_.degree_list_ == nullptr ? nullptr : csr_.degree_list_->GetData());
    capacities_ = reinterpret_cast<int*>(
        csr_.cap_list_ == nullptr ? nullptr : csr_.cap_list_->GetData());
  }

  csr_t& csr_;
  size_t vertex_capacity_ = 0;
  nbr_t** adj_lists_ = nullptr;
  std::atomic<int>* degrees_ = nullptr;
  int* capacities_ = nullptr;
  nbr_t* nbrs_ = nullptr;
};

/// Builder for single-edge CSR layouts. A degree pass can reuse timestamp as a
/// private seen marker. Unique endpoints are filled concurrently; duplicates
/// fall back to ordered last-write-wins fill.
template <typename EDATA_T>
class BundledEdgeCsrLoader::SingleMutableWriter {
 public:
  using csr_t = SingleMutableCsr<EDATA_T>;
  using nbr_t = typename csr_t::nbr_t;
  static constexpr EdgeStrategy kStrategy = EdgeStrategy::kSingle;

  explicit SingleMutableWriter(csr_t& csr) : csr_(csr) {}

  void PrepareBuild(vid_t vertex_count) {
    CHECK(csr_.nbr_list_ != nullptr);
    csr_.nbr_list_->Resize(static_cast<size_t>(vertex_count) * sizeof(nbr_t));
    refresh_ptrs();
    CHECK(nbrs_ != nullptr || vertex_count == 0);
    for (vid_t i = 0; i < vertex_count; ++i) {
      nbrs_[i].timestamp.store(INVALID_TIMESTAMP, std::memory_order_relaxed);
    }
    csr_.edge_num_.store(0, std::memory_order_relaxed);
  }

  bool MarkSeenAndCheckDuplicateSerial(vid_t src) {
    DCHECK_LT(src, vertex_capacity_);
    auto& timestamp = nbrs_[src].timestamp;
    const auto previous = timestamp.load(std::memory_order_relaxed);
    timestamp.store(0, std::memory_order_relaxed);
    return previous != INVALID_TIMESTAMP;
  }

  bool MarkSeenAndCheckDuplicateConcurrent(vid_t src) {
    DCHECK_LT(src, vertex_capacity_);
    const auto previous =
        nbrs_[src].timestamp.exchange(0, std::memory_order_relaxed);
    return previous != INVALID_TIMESTAMP;
  }

  void PutSerial(vid_t src, vid_t dst, const EDATA_T& data, timestamp_t ts) {
    PutAtVertex(src, dst, data, ts);
  }

  void PutUnique(vid_t src, vid_t dst, const EDATA_T& data, timestamp_t ts) {
    PutAtVertex(src, dst, data, ts);
  }

  void Finish(uint64_t filled_edge_count) {
    csr_.edge_num_.store(filled_edge_count, std::memory_order_relaxed);
    csr_.refresh_prefetch_policy();
  }

 private:
  void PutAtVertex(vid_t src, vid_t dst, const EDATA_T& data, timestamp_t ts) {
    DCHECK_LT(src, vertex_capacity_);
    auto& nbr = nbrs_[src];
    nbr.neighbor = dst;
    nbr.data = data;
    nbr.timestamp.store(ts, std::memory_order_relaxed);
  }

  void refresh_ptrs() {
    vertex_capacity_ = csr_.vertex_capacity();
    nbrs_ = reinterpret_cast<nbr_t*>(
        csr_.nbr_list_ == nullptr ? nullptr : csr_.nbr_list_->GetData());
  }

  csr_t& csr_;
  size_t vertex_capacity_ = 0;
  nbr_t* nbrs_ = nullptr;
};

}  // namespace internal

namespace {

class EmptyCsrBulkWriter {
 public:
  static constexpr EdgeStrategy kStrategy = EdgeStrategy::kNone;

  void PrepareBuild(vid_t /*vertex_count*/) {}
  void Finish(uint64_t /*filled_edge_count*/) {}
};

template <typename EDATA_T, typename F>
bool with_csr_bulk_writer(CsrBase* csr, F&& callback) {
  if (auto* typed = dynamic_cast<MutableCsr<EDATA_T>*>(csr)) {
    internal::BundledEdgeCsrLoader::MutableWriter<EDATA_T> writer(*typed);
    std::forward<F>(callback)(writer);
    return true;
  }
  if (auto* typed = dynamic_cast<SingleMutableCsr<EDATA_T>*>(csr)) {
    internal::BundledEdgeCsrLoader::SingleMutableWriter<EDATA_T> writer(*typed);
    std::forward<F>(callback)(writer);
    return true;
  }
  if (dynamic_cast<EmptyCsr<EDATA_T>*>(csr) != nullptr) {
    EmptyCsrBulkWriter writer;
    std::forward<F>(callback)(writer);
    return true;
  }
  return false;
}

template <typename EDATA_T>
class BulkEdgeDataReader {
 public:
  explicit BulkEdgeDataReader(const std::shared_ptr<IContextColumn>& column)
      : column_(column.get()) {
    CHECK(column_ != nullptr);
    auto values = std::dynamic_pointer_cast<ValueColumn<EDATA_T>>(column);
    if (values) {
      values_ = &values->data();
    }
  }

  EDATA_T Get(size_t row) const {
    if (values_ != nullptr) {
      return (*values_)[row];
    }
    return column_->get_elem(row).template GetValue<EDATA_T>();
  }

 private:
  const IContextColumn* column_;
  const vector_t<EDATA_T>* values_ = nullptr;
};

template <>
class BulkEdgeDataReader<EmptyType> {
 public:
  explicit BulkEdgeDataReader(
      const std::shared_ptr<IContextColumn>& /*column*/) {}

  EmptyType Get(size_t /*row*/) const { return EmptyType(); }
};

constexpr uint32_t kInvalidBulkEdgeIndex = std::numeric_limits<uint32_t>::max();

struct BulkEdgeGroup {
  uint32_t count = 0;
  uint32_t head = kInvalidBulkEdgeIndex;
  uint32_t tail = kInvalidBulkEdgeIndex;
};

struct BulkEdgeWorkerScratch {
  std::vector<vid_t> src_lids;
  std::vector<vid_t> dst_lids;
  flat_hash_map<vid_t, BulkEdgeGroup> out_groups;
  flat_hash_map<vid_t, BulkEdgeGroup> in_groups;
  std::vector<uint32_t> out_next;
  std::vector<uint32_t> in_next;
  bool out_single_duplicate = false;
  bool in_single_duplicate = false;
  uint64_t filled_edge_count = 0;
};

void index_bulk_edge_endpoints(const std::shared_ptr<DataChunk>& chunk,
                               const IndexerType& src_indexer,
                               const IndexerType& dst_indexer,
                               BulkEdgeWorkerScratch& scratch) {
  CHECK(chunk != nullptr);
  CHECK_GE(chunk->col_num(), 2);
  auto src_column = chunk->get(0);
  auto dst_column = chunk->get(1);
  CHECK(src_column != nullptr);
  CHECK(dst_column != nullptr);
  CHECK_EQ(src_column->size(), dst_column->size());

  scratch.src_lids.clear();
  scratch.dst_lids.clear();
  src_indexer.get_index(*src_column, scratch.src_lids);
  dst_indexer.get_index(*dst_column, scratch.dst_lids);
  CHECK_EQ(scratch.src_lids.size(), scratch.dst_lids.size());
}

inline bool is_valid_bulk_edge(vid_t src, vid_t dst) {
  return src != std::numeric_limits<vid_t>::max() &&
         dst != std::numeric_limits<vid_t>::max();
}

struct BulkEdgeCountSummary {
  bool out_single_duplicate = false;
  bool in_single_duplicate = false;
};

constexpr size_t kBulkEdgeInitialGroupReserve = 4096;

size_t bulk_edge_group_reserve(size_t rows) {
  // Eagerly reserving one hash slot per edge wastes substantial memory for a
  // supernode. The map can grow past this cap for high-cardinality chunks, and
  // the worker-local scratch reuses that larger backing store on later chunks.
  return std::min(rows, kBulkEdgeInitialGroupReserve);
}

void count_bulk_edge_group(flat_hash_map<vid_t, BulkEdgeGroup>& groups,
                           vid_t vertex) {
  auto& group = groups[vertex];
  CHECK_LT(group.count, std::numeric_limits<uint32_t>::max());
  ++group.count;
}

template <typename OutWriter, typename InWriter>
void count_bulk_edge_chunk(BulkEdgeWorkerScratch& scratch, OutWriter& out,
                           InWriter& in, bool concurrent) {
  CHECK_LE(scratch.src_lids.size(),
           static_cast<size_t>(std::numeric_limits<uint32_t>::max()));

  if (concurrent) {
    if constexpr (OutWriter::kStrategy == EdgeStrategy::kMultiple) {
      scratch.out_groups.clear();
      scratch.out_groups.reserve(
          bulk_edge_group_reserve(scratch.src_lids.size()));
    }
    if constexpr (InWriter::kStrategy == EdgeStrategy::kMultiple) {
      scratch.in_groups.clear();
      scratch.in_groups.reserve(
          bulk_edge_group_reserve(scratch.dst_lids.size()));
    }
  }
  for (size_t row = 0; row < scratch.src_lids.size(); ++row) {
    const auto src = scratch.src_lids[row];
    const auto dst = scratch.dst_lids[row];
    if (!is_valid_bulk_edge(src, dst)) {
      continue;
    }
    if (concurrent) {
      if constexpr (OutWriter::kStrategy == EdgeStrategy::kMultiple) {
        count_bulk_edge_group(scratch.out_groups, src);
      }
      if constexpr (InWriter::kStrategy == EdgeStrategy::kMultiple) {
        count_bulk_edge_group(scratch.in_groups, dst);
      }
      if constexpr (OutWriter::kStrategy == EdgeStrategy::kSingle) {
        if (!scratch.out_single_duplicate) {
          scratch.out_single_duplicate =
              out.MarkSeenAndCheckDuplicateConcurrent(src);
        }
      }
      if constexpr (InWriter::kStrategy == EdgeStrategy::kSingle) {
        if (!scratch.in_single_duplicate) {
          scratch.in_single_duplicate =
              in.MarkSeenAndCheckDuplicateConcurrent(dst);
        }
      }
    } else {
      if constexpr (OutWriter::kStrategy == EdgeStrategy::kMultiple) {
        out.CountSerial(src);
      }
      if constexpr (InWriter::kStrategy == EdgeStrategy::kMultiple) {
        in.CountSerial(dst);
      }
      if constexpr (OutWriter::kStrategy == EdgeStrategy::kSingle) {
        if (!scratch.out_single_duplicate) {
          scratch.out_single_duplicate =
              out.MarkSeenAndCheckDuplicateSerial(src);
        }
      }
      if constexpr (InWriter::kStrategy == EdgeStrategy::kSingle) {
        if (!scratch.in_single_duplicate) {
          scratch.in_single_duplicate = in.MarkSeenAndCheckDuplicateSerial(dst);
        }
      }
    }
  }
  if (concurrent) {
    if constexpr (OutWriter::kStrategy == EdgeStrategy::kMultiple) {
      for (const auto& [src, group] : scratch.out_groups) {
        CHECK_LE(group.count,
                 static_cast<uint32_t>(std::numeric_limits<int>::max()));
        out.CountConcurrent(src, static_cast<int>(group.count));
      }
    }
    if constexpr (InWriter::kStrategy == EdgeStrategy::kMultiple) {
      for (const auto& [dst, group] : scratch.in_groups) {
        CHECK_LE(group.count,
                 static_cast<uint32_t>(std::numeric_limits<int>::max()));
        in.CountConcurrent(dst, static_cast<int>(group.count));
      }
    }
  }
}

using BulkEdgeCountChunk = std::function<void(BulkEdgeWorkerScratch&, bool)>;

BulkEdgeCountSummary count_bulk_edges(
    const std::shared_ptr<IDataChunkSource>& source,
    const IndexerType& src_indexer, const IndexerType& dst_indexer,
    ChunkSourceOptions options, std::vector<BulkEdgeWorkerScratch>& scratches,
    const BulkEdgeCountChunk& count_chunk) {
  // Degree accumulation and single-slot uniqueness checks are commutative, so
  // this pass never needs input order even when fill may later fall back to the
  // ordered last-write-wins path.
  // The degree pass only needs endpoint OIDs, so edge properties are not
  // parsed, typed, allocated, and discarded during the first pass.
  options.projected_columns = {0, 1};
  auto supplier = source->Open(options);
  CHECK(supplier != nullptr);

  const auto worker_count = options.consumer_count;
  CHECK_EQ(scratches.size(), static_cast<size_t>(worker_count));
  const bool concurrent = worker_count > 1;
  auto count = [&](int32_t worker, const std::shared_ptr<DataChunk>& chunk) {
    CHECK_GE(worker, 0);
    CHECK_LT(worker, worker_count);
    auto& scratch = scratches[static_cast<size_t>(worker)];
    index_bulk_edge_endpoints(chunk, src_indexer, dst_indexer, scratch);
    count_chunk(scratch, concurrent);
  };
  consume_supplier_indexed(*supplier, options, count);
  BulkEdgeCountSummary summary;
  for (const auto& scratch : scratches) {
    summary.out_single_duplicate =
        summary.out_single_duplicate || scratch.out_single_duplicate;
    summary.in_single_duplicate =
        summary.in_single_duplicate || scratch.in_single_duplicate;
  }
  return summary;
}

void append_bulk_edge_group(flat_hash_map<vid_t, BulkEdgeGroup>& groups,
                            std::vector<uint32_t>& next, vid_t vertex,
                            uint32_t edge_index) {
  auto [it, inserted] = groups.try_emplace(vertex);
  auto& group = it->second;
  if (inserted) {
    group.head = edge_index;
  } else {
    CHECK_NE(group.tail, kInvalidBulkEdgeIndex);
    next[group.tail] = edge_index;
  }
  group.tail = edge_index;
  CHECK_LT(group.count, std::numeric_limits<uint32_t>::max());
  ++group.count;
}

template <typename OutWriter, typename InWriter>
size_t group_bulk_edge_chunk(BulkEdgeWorkerScratch& scratch) {
  CHECK_LE(scratch.src_lids.size(),
           static_cast<size_t>(std::numeric_limits<uint32_t>::max()));
  if constexpr (OutWriter::kStrategy == EdgeStrategy::kMultiple) {
    scratch.out_groups.clear();
    scratch.out_groups.reserve(
        bulk_edge_group_reserve(scratch.src_lids.size()));
    scratch.out_next.assign(scratch.src_lids.size(), kInvalidBulkEdgeIndex);
  }
  if constexpr (InWriter::kStrategy == EdgeStrategy::kMultiple) {
    scratch.in_groups.clear();
    scratch.in_groups.reserve(bulk_edge_group_reserve(scratch.dst_lids.size()));
    scratch.in_next.assign(scratch.dst_lids.size(), kInvalidBulkEdgeIndex);
  }
  size_t valid_edges = 0;
  for (size_t row = 0; row < scratch.src_lids.size(); ++row) {
    const auto src = scratch.src_lids[row];
    const auto dst = scratch.dst_lids[row];
    if (!is_valid_bulk_edge(src, dst)) {
      continue;
    }
    ++valid_edges;
    const auto edge_index = static_cast<uint32_t>(row);
    if constexpr (OutWriter::kStrategy == EdgeStrategy::kMultiple) {
      append_bulk_edge_group(scratch.out_groups, scratch.out_next, src,
                             edge_index);
    }
    if constexpr (InWriter::kStrategy == EdgeStrategy::kMultiple) {
      append_bulk_edge_group(scratch.in_groups, scratch.in_next, dst,
                             edge_index);
    }
  }
  return valid_edges;
}

template <typename EDATA_T, typename OutWriter, typename InWriter>
void fill_bulk_edge_chunk_serial(BulkEdgeWorkerScratch& scratch,
                                 const BulkEdgeDataReader<EDATA_T>& data_reader,
                                 OutWriter& out, InWriter& in) {
  size_t valid_edges = 0;
  for (size_t row = 0; row < scratch.src_lids.size(); ++row) {
    const auto src = scratch.src_lids[row];
    const auto dst = scratch.dst_lids[row];
    if (!is_valid_bulk_edge(src, dst)) {
      continue;
    }
    ++valid_edges;
    const auto data = data_reader.Get(row);
    if constexpr (OutWriter::kStrategy != EdgeStrategy::kNone) {
      out.PutSerial(src, dst, data, 0);
    }
    if constexpr (InWriter::kStrategy != EdgeStrategy::kNone) {
      in.PutSerial(dst, src, data, 0);
    }
  }
  CHECK_LE(valid_edges,
           std::numeric_limits<uint64_t>::max() - scratch.filled_edge_count);
  scratch.filled_edge_count += static_cast<uint64_t>(valid_edges);
}

template <typename EDATA_T, typename OutWriter, typename InWriter>
void fill_bulk_edge_chunk_concurrent(
    BulkEdgeWorkerScratch& scratch,
    const BulkEdgeDataReader<EDATA_T>& data_reader, OutWriter& out,
    InWriter& in) {
  const auto valid_edges = group_bulk_edge_chunk<OutWriter, InWriter>(scratch);
  constexpr bool kDirectOut = OutWriter::kStrategy == EdgeStrategy::kSingle;
  constexpr bool kDirectIn = InWriter::kStrategy == EdgeStrategy::kSingle;
  if constexpr (kDirectOut || kDirectIn) {
    for (size_t row = 0; row < scratch.src_lids.size(); ++row) {
      const auto src = scratch.src_lids[row];
      const auto dst = scratch.dst_lids[row];
      if (!is_valid_bulk_edge(src, dst)) {
        continue;
      }
      const auto data = data_reader.Get(row);
      if constexpr (kDirectOut) {
        out.PutUnique(src, dst, data, 0);
      }
      if constexpr (kDirectIn) {
        in.PutUnique(dst, src, data, 0);
      }
    }
  }
  if constexpr (OutWriter::kStrategy == EdgeStrategy::kMultiple) {
    for (const auto& [src, group] : scratch.out_groups) {
      CHECK_LE(group.count,
               static_cast<uint32_t>(std::numeric_limits<int>::max()));
      auto slot = out.ReserveConcurrent(src, static_cast<int>(group.count));
      auto edge_index = group.head;
      for (uint32_t i = 0; i < group.count; ++i) {
        CHECK_NE(edge_index, kInvalidBulkEdgeIndex);
        const auto data = data_reader.Get(edge_index);
        out.PutAt(src, slot++, scratch.dst_lids[edge_index], data, 0);
        edge_index = scratch.out_next[edge_index];
      }
      CHECK_EQ(edge_index, kInvalidBulkEdgeIndex);
    }
  }
  if constexpr (InWriter::kStrategy == EdgeStrategy::kMultiple) {
    for (const auto& [dst, group] : scratch.in_groups) {
      CHECK_LE(group.count,
               static_cast<uint32_t>(std::numeric_limits<int>::max()));
      auto slot = in.ReserveConcurrent(dst, static_cast<int>(group.count));
      auto edge_index = group.head;
      for (uint32_t i = 0; i < group.count; ++i) {
        CHECK_NE(edge_index, kInvalidBulkEdgeIndex);
        const auto data = data_reader.Get(edge_index);
        in.PutAt(dst, slot++, scratch.src_lids[edge_index], data, 0);
        edge_index = scratch.in_next[edge_index];
      }
      CHECK_EQ(edge_index, kInvalidBulkEdgeIndex);
    }
  }
  CHECK_LE(valid_edges,
           std::numeric_limits<uint64_t>::max() - scratch.filled_edge_count);
  scratch.filled_edge_count += static_cast<uint64_t>(valid_edges);
}

template <typename EDATA_T>
using BulkEdgeFillChunk = std::function<void(const BulkEdgeDataReader<EDATA_T>&,
                                             BulkEdgeWorkerScratch&, bool)>;

template <typename EDATA_T>
struct BulkEdgeBuildOps {
  bool needs_degree_count = false;
  bool checks_out_single = false;
  bool checks_in_single = false;
  BulkEdgeCountChunk count_chunk;
  std::function<void()> allocate_from_counts;
  BulkEdgeFillChunk<EDATA_T> fill_chunk;
};

template <typename EDATA_T, typename OutWriter, typename InWriter>
BulkEdgeBuildOps<EDATA_T> make_bulk_edge_build_ops(OutWriter& out,
                                                   InWriter& in) {
  BulkEdgeBuildOps<EDATA_T> ops;
  constexpr bool kStoresAnyDirection =
      OutWriter::kStrategy != EdgeStrategy::kNone ||
      InWriter::kStrategy != EdgeStrategy::kNone;
  constexpr bool kNeedsAnyDegreeCount =
      OutWriter::kStrategy == EdgeStrategy::kMultiple ||
      InWriter::kStrategy == EdgeStrategy::kMultiple;
  ops.needs_degree_count = kNeedsAnyDegreeCount;
  ops.checks_out_single = OutWriter::kStrategy == EdgeStrategy::kSingle;
  ops.checks_in_single = InWriter::kStrategy == EdgeStrategy::kSingle;
  ops.allocate_from_counts = [&]() {
    if constexpr (OutWriter::kStrategy == EdgeStrategy::kMultiple) {
      out.AllocateFromCounts();
    }
    if constexpr (InWriter::kStrategy == EdgeStrategy::kMultiple) {
      in.AllocateFromCounts();
    }
  };
  if constexpr (kNeedsAnyDegreeCount) {
    ops.count_chunk = [&out, &in](BulkEdgeWorkerScratch& scratch,
                                  bool concurrent) {
      count_bulk_edge_chunk(scratch, out, in, concurrent);
    };
  }
  if constexpr (kStoresAnyDirection) {
    ops.fill_chunk = [&out, &in](const BulkEdgeDataReader<EDATA_T>& data_reader,
                                 BulkEdgeWorkerScratch& scratch,
                                 bool concurrent) {
      if (concurrent) {
        fill_bulk_edge_chunk_concurrent(scratch, data_reader, out, in);
      } else {
        fill_bulk_edge_chunk_serial(scratch, data_reader, out, in);
      }
    };
  }
  return ops;
}

template <typename EDATA_T>
uint64_t fill_bulk_edges(const std::shared_ptr<IDataChunkSource>& source,
                         const IndexerType& src_indexer,
                         const IndexerType& dst_indexer,
                         const ChunkSourceOptions& options,
                         bool allow_concurrent_fill,
                         std::vector<BulkEdgeWorkerScratch>& scratches,
                         const BulkEdgeBuildOps<EDATA_T>& ops) {
  CHECK(!allow_concurrent_fill || !options.preserve_order);
  auto supplier = source->Open(options);
  CHECK(supplier != nullptr);

  const auto worker_count = options.consumer_count;
  CHECK_GE(scratches.size(), static_cast<size_t>(worker_count));
  for (auto& scratch : scratches) {
    scratch.filled_edge_count = 0;
  }
  if (allow_concurrent_fill && worker_count > 1) {
    auto fill = [&](int32_t worker, const std::shared_ptr<DataChunk>& chunk) {
      CHECK_GE(worker, 0);
      CHECK_LT(worker, worker_count);
      auto& scratch = scratches[static_cast<size_t>(worker)];
      index_bulk_edge_endpoints(chunk, src_indexer, dst_indexer, scratch);
      const auto data_column = chunk->col_num() > 2 ? chunk->get(2) : nullptr;
      BulkEdgeDataReader<EDATA_T> data_reader(data_column);
      ops.fill_chunk(data_reader, scratch, true);
    };

    consume_supplier_indexed(*supplier, options, fill);
  } else {
    auto& scratch = scratches.front();
    while (auto chunk = supplier->GetNextChunk()) {
      index_bulk_edge_endpoints(chunk, src_indexer, dst_indexer, scratch);
      const auto data_column = chunk->col_num() > 2 ? chunk->get(2) : nullptr;
      BulkEdgeDataReader<EDATA_T> data_reader(data_column);
      ops.fill_chunk(data_reader, scratch, false);
    }
  }

  uint64_t filled_edge_count = 0;
  for (const auto& scratch : scratches) {
    CHECK_LE(scratch.filled_edge_count,
             std::numeric_limits<uint64_t>::max() - filled_edge_count);
    filled_edge_count += scratch.filled_edge_count;
  }
  return filled_edge_count;
}

template <typename EDATA_T>
uint64_t build_bundled_edges_with_ops(
    const BulkEdgeBuildOps<EDATA_T>& ops, const IndexerType& src_indexer,
    const IndexerType& dst_indexer,
    const std::shared_ptr<IDataChunkSource>& source, int64_t source_bytes) {
  // VertexTable owns its reserve policy. Edge storage consumes the resulting
  // indexer capacity instead of duplicating PropertyGraph::Dump's policy.
  if (!ops.fill_chunk) {
    ops.allocate_from_counts();
    return 0;
  }
  const auto count_options = ResolveBulkBuildSourceOptions(
      source_bytes, source->ParallelEnabled(), false,
      BulkBuildWorkerStrategy::kBalancedProducerConsumer);
  const bool needs_degree_count = ops.needs_degree_count;
  std::vector<BulkEdgeWorkerScratch> scratches;
  if (needs_degree_count) {
    scratches.resize(static_cast<size_t>(count_options.consumer_count));
  }
  const bool has_single_direction =
      ops.checks_out_single || ops.checks_in_single;
  BulkEdgeCountSummary count_summary;
  if (needs_degree_count) {
    CHECK(static_cast<bool>(ops.count_chunk));
    count_summary = count_bulk_edges(source, src_indexer, dst_indexer,
                                     count_options, scratches, ops.count_chunk);
  }
  const bool single_duplicate =
      (ops.checks_out_single && count_summary.out_single_duplicate) ||
      (ops.checks_in_single && count_summary.in_single_duplicate);
  const bool preserve_fill_order =
      has_single_direction && (!needs_degree_count || single_duplicate);
  const bool allow_concurrent_fill = !preserve_fill_order;
  const auto fill_options =
      preserve_fill_order
          ? ResolveBulkBuildSourceOptions(
                source_bytes, source->ParallelEnabled(), true,
                BulkBuildWorkerStrategy::kBalancedProducerConsumer)
          : count_options;
  if (scratches.size() < static_cast<size_t>(fill_options.consumer_count)) {
    scratches.resize(static_cast<size_t>(fill_options.consumer_count));
  }
  ops.allocate_from_counts();
  return fill_bulk_edges<EDATA_T>(source, src_indexer, dst_indexer,
                                  fill_options, allow_concurrent_fill,
                                  scratches, ops);
}

template <typename EDATA_T>
bool build_bundled_edges_typed(CsrBase* out_csr, CsrBase* in_csr,
                               const IndexerType& src_indexer,
                               const IndexerType& dst_indexer,
                               const std::shared_ptr<IDataChunkSource>& source,
                               int64_t source_bytes, vid_t src_vertex_capacity,
                               vid_t dst_vertex_capacity) {
  bool built = false;
  const bool out_supported =
      with_csr_bulk_writer<EDATA_T>(out_csr, [&](auto& out) {
        const bool in_supported =
            with_csr_bulk_writer<EDATA_T>(in_csr, [&](auto& in) {
              out.PrepareBuild(src_vertex_capacity);
              in.PrepareBuild(dst_vertex_capacity);
              auto ops = make_bulk_edge_build_ops<EDATA_T>(out, in);
              const auto filled_edge_count = build_bundled_edges_with_ops(
                  ops, src_indexer, dst_indexer, source, source_bytes);
              out.Finish(filled_edge_count);
              in.Finish(filled_edge_count);
            });
        built = in_supported;
      });
  return out_supported && built;
}

bool build_bundled_edges(CsrBase* out_csr, CsrBase* in_csr,
                         const EdgeSchema& schema,
                         const IndexerType& src_indexer,
                         const IndexerType& dst_indexer,
                         const std::shared_ptr<IDataChunkSource>& source,
                         int64_t source_bytes, vid_t src_vertex_capacity,
                         vid_t dst_vertex_capacity) {
  const auto property_type = schema.properties.empty()
                                 ? DataTypeId::kEmpty
                                 : schema.properties[0].id();
  switch (property_type) {
#define TYPE_DISPATCHER(enum_val, cpp_type)                              \
  case DataTypeId::enum_val:                                             \
    return build_bundled_edges_typed<cpp_type>(                          \
        out_csr, in_csr, src_indexer, dst_indexer, source, source_bytes, \
        src_vertex_capacity, dst_vertex_capacity);
    FOR_EACH_DATA_TYPE_NO_STRING(TYPE_DISPATCHER)
#undef TYPE_DISPATCHER
  case DataTypeId::kEmpty:
    return build_bundled_edges_typed<EmptyType>(
        out_csr, in_csr, src_indexer, dst_indexer, source, source_bytes,
        src_vertex_capacity, dst_vertex_capacity);
  default:
    return false;
  }
}

}  // namespace

bool internal::BundledEdgeCsrLoader::TryBuild(
    CsrBase& out_csr, CsrBase& in_csr, const EdgeSchema& schema,
    const IndexerType& src_indexer, const IndexerType& dst_indexer,
    const std::shared_ptr<IDataChunkSource>& source, int64_t source_bytes,
    vid_t src_vertex_capacity, vid_t dst_vertex_capacity) {
  if (!source) {
    return false;
  }
  return build_bundled_edges(&out_csr, &in_csr, schema, src_indexer,
                             dst_indexer, source, source_bytes,
                             src_vertex_capacity, dst_vertex_capacity);
}

}  // namespace neug
