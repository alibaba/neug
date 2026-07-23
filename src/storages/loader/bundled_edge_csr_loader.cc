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
#include <exception>
#include <fstream>
#include <limits>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

#include "neug/common/columns/value_columns.h"
#include "neug/common/types/container_types.h"
#include "neug/storages/checkpoint.h"
#include "neug/storages/csr/mutable_csr.h"
#include "neug/storages/loader/chunk_pipeline_utils.h"
#include "neug/storages/loader/loader_utils.h"
#include "neug/utils/exception/exception.h"
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
      : column_(column.get()),
        value_column_(dynamic_cast<const ValueColumn<EDATA_T>*>(column.get())) {
    if (column_ == nullptr) {
      THROW_SCHEMA_MISMATCH("Bundled edge property column is missing");
    }
  }

  EDATA_T Get(size_t row) const {
    if (value_column_ != nullptr) {
      return value_column_->get_value(row);
    }
    return column_->get_elem(row).template GetValue<EDATA_T>();
  }

 private:
  const IContextColumn* column_;
  const ValueColumn<EDATA_T>* value_column_;
};

template <>
class BulkEdgeDataReader<EmptyType> {
 public:
  explicit BulkEdgeDataReader(
      const std::shared_ptr<IContextColumn>& /*column*/) {}

  EmptyType Get(size_t /*row*/) const { return EmptyType(); }
};

template <typename EDATA_T>
struct BulkEdgeSpillRecord {
  vid_t src;
  vid_t dst;
  EDATA_T data;
};

template <typename EDATA_T>
class BulkEdgeSpillSegment {
 public:
  using record_t = BulkEdgeSpillRecord<EDATA_T>;

  // Retained across ordered runs so each spill segment opens its file and
  // allocates its replay buffer once. Adjacent ranges continue reading from
  // the current position without another seek.
  class Reader {
   public:
    Reader(std::string path, uint64_t record_count)
        : path_(std::move(path)),
          record_count_(record_count),
          input_(path_, std::ios::binary),
          records_(kRecordsPerBuffer) {
      if (!input_) {
        THROW_IO_EXCEPTION("Failed to open edge spill file: " + path_);
      }
    }

    Reader(const Reader&) = delete;
    Reader& operator=(const Reader&) = delete;

    template <typename Consume>
    void ReplayRange(uint64_t offset, uint64_t count, Consume&& consume) {
      CHECK_LE(offset, record_count_);
      CHECK_LE(count, record_count_ - offset);
      if (next_record_offset_ != offset) {
        Seek(offset);
      }

      auto remaining = count;
      while (remaining > 0) {
        const auto requested =
            static_cast<size_t>(std::min<uint64_t>(remaining, records_.size()));
        input_.read(reinterpret_cast<char*>(records_.data()),
                    static_cast<std::streamsize>(requested * sizeof(record_t)));
        const auto bytes = input_.gcount();
        if (bytes !=
            static_cast<std::streamsize>(requested * sizeof(record_t))) {
          THROW_IO_EXCEPTION("Truncated edge spill file: " + path_);
        }
        next_record_offset_ += requested;
        consume(records_.data(), requested);
        remaining -= requested;
      }
    }

   private:
    void Seek(uint64_t offset) {
      if (offset >
          static_cast<uint64_t>(std::numeric_limits<std::streamoff>::max()) /
              sizeof(record_t)) {
        THROW_IO_EXCEPTION("Edge spill offset exceeds stream capacity: " +
                           path_);
      }
      input_.clear();
      input_.seekg(static_cast<std::streamoff>(offset * sizeof(record_t)));
      if (!input_) {
        THROW_IO_EXCEPTION("Failed to seek edge spill file: " + path_);
      }
      next_record_offset_ = offset;
    }

    std::string path_;
    uint64_t record_count_;
    std::ifstream input_;
    std::vector<record_t> records_;
    uint64_t next_record_offset_ = std::numeric_limits<uint64_t>::max();
  };

  explicit BulkEdgeSpillSegment(Checkpoint& checkpoint)
      : file_(checkpoint.CreateRuntimeFile()),
        output_(file_.path(), std::ios::binary | std::ios::trunc) {
    static_assert(std::is_trivially_copyable_v<record_t>);
    if (!output_) {
      THROW_IO_EXCEPTION("Failed to create edge spill file: " + file_.path());
    }
    buffer_.reserve(kRecordsPerBuffer);
  }

  ~BulkEdgeSpillSegment() = default;
  BulkEdgeSpillSegment(const BulkEdgeSpillSegment&) = delete;
  BulkEdgeSpillSegment& operator=(const BulkEdgeSpillSegment&) = delete;

  void Append(vid_t src, vid_t dst, const EDATA_T& data) {
    CHECK(!finalized_);
    CHECK_LT(record_count_, std::numeric_limits<uint64_t>::max());
    buffer_.push_back({src, dst, data});
    ++record_count_;
    if (buffer_.size() == kRecordsPerBuffer) {
      Flush();
    }
  }

  uint64_t RecordCount() const { return record_count_; }

  void Finalize() {
    if (finalized_) {
      return;
    }
    Flush();
    output_.close();
    if (output_.fail()) {
      THROW_IO_EXCEPTION("Failed to finalize edge spill file: " + file_.path());
    }
    finalized_ = true;
  }

  template <typename Consume>
  void Replay(Consume&& consume) const {
    auto reader = OpenReader();
    reader->ReplayRange(0, record_count_, std::forward<Consume>(consume));
  }

  std::unique_ptr<Reader> OpenReader() const {
    CHECK(finalized_);
    return std::make_unique<Reader>(file_.path(), record_count_);
  }

 private:
  void Flush() {
    if (buffer_.empty()) {
      return;
    }
    output_.write(
        reinterpret_cast<const char*>(buffer_.data()),
        static_cast<std::streamsize>(buffer_.size() * sizeof(record_t)));
    if (!output_) {
      THROW_IO_EXCEPTION("Failed to write edge spill file: " + file_.path());
    }
    buffer_.clear();
  }

  static constexpr size_t kRecordsPerBuffer = 4096;
  CheckpointFileManager::RuntimeFileHandle file_;
  std::ofstream output_;
  std::vector<record_t> buffer_;
  uint64_t record_count_ = 0;
  bool finalized_ = false;
};

struct BulkEdgeSpillRun {
  size_t segment = 0;
  uint64_t record_offset = 0;
  uint64_t record_count = 0;
  uint64_t first_row_ordinal = 0;
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

template <typename EDATA_T>
struct BulkEdgeMaterialization {
  std::vector<std::unique_ptr<BulkEdgeSpillSegment<EDATA_T>>> segments;
  std::vector<std::vector<BulkEdgeSpillRun>> runs;
};

template <typename EDATA_T, typename OutWriter, typename InWriter>
BulkEdgeMaterialization<EDATA_T> materialize_bulk_edges(
    IDataChunkSource& source, const IndexerType& src_indexer,
    const IndexerType& dst_indexer, const ChunkSourceOptions& options,
    Checkpoint& checkpoint, std::vector<BulkEdgeWorkerScratch>& scratches,
    OutWriter& out, InWriter& in) {
  auto supplier = open_data_chunk_source(source, options);
  CHECK(supplier != nullptr);
  if (options.consumer_count > 1 && source.ProvidesStableRowOrdinals()) {
    CHECK(supplier->ProvidesStableRowOrdinals());
  }

  const auto worker_count = std::max<int32_t>(1, options.consumer_count);
  CHECK_EQ(scratches.size(), static_cast<size_t>(worker_count));
  BulkEdgeMaterialization<EDATA_T> result;
  result.segments.reserve(static_cast<size_t>(worker_count));
  result.runs.resize(static_cast<size_t>(worker_count));
  for (int32_t worker = 0; worker < worker_count; ++worker) {
    result.segments.push_back(
        std::make_unique<BulkEdgeSpillSegment<EDATA_T>>(checkpoint));
  }

  const bool concurrent = worker_count > 1;
  auto materialize = [&](int32_t worker, const SequencedDataChunk& sequenced) {
    CHECK_GE(worker, 0);
    CHECK_LT(worker, worker_count);
    const auto& chunk = sequenced.chunk;
    auto& scratch = scratches[static_cast<size_t>(worker)];
    index_bulk_edge_endpoints(chunk, src_indexer, dst_indexer, scratch);
    count_bulk_edge_chunk(scratch, out, in, concurrent);
    const auto data_column = chunk->col_num() > 2 ? chunk->get(2) : nullptr;
    BulkEdgeDataReader<EDATA_T> data_reader(data_column);
    auto& segment = *result.segments[static_cast<size_t>(worker)];
    const auto record_offset = segment.RecordCount();
    for (size_t row = 0; row < scratch.src_lids.size(); ++row) {
      const auto src = scratch.src_lids[row];
      const auto dst = scratch.dst_lids[row];
      if (is_valid_bulk_edge(src, dst)) {
        segment.Append(src, dst, data_reader.Get(row));
      }
    }
    const auto record_count = segment.RecordCount() - record_offset;
    if (record_count > 0) {
      result.runs[static_cast<size_t>(worker)].push_back(
          {static_cast<size_t>(worker), record_offset, record_count,
           sequenced.first_row_ordinal});
    }
  };
  consume_supplier_indexed(*supplier, options, materialize);
  for (auto& segment : result.segments) {
    segment->Finalize();
  }
  return result;
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

template <typename DataReader, typename OutWriter, typename InWriter>
void fill_bulk_edge_chunk_serial(BulkEdgeWorkerScratch& scratch,
                                 const DataReader& data_reader, OutWriter& out,
                                 InWriter& in) {
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

template <typename DataReader, typename OutWriter, typename InWriter>
void fill_bulk_edge_chunk_concurrent(BulkEdgeWorkerScratch& scratch,
                                     const DataReader& data_reader,
                                     OutWriter& out, InWriter& in) {
  CHECK_LE(scratch.src_lids.size(),
           static_cast<size_t>(std::numeric_limits<uint32_t>::max()));
  constexpr bool kDirectOut = OutWriter::kStrategy == EdgeStrategy::kSingle;
  constexpr bool kDirectIn = InWriter::kStrategy == EdgeStrategy::kSingle;
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
    if constexpr (kDirectOut || kDirectIn) {
      const auto data = data_reader.Get(row);
      if constexpr (kDirectOut) {
        out.PutSerial(src, dst, data, 0);
      }
      if constexpr (kDirectIn) {
        in.PutSerial(dst, src, data, 0);
      }
    }
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
class BulkEdgeSpillDataReader {
 public:
  explicit BulkEdgeSpillDataReader(const BulkEdgeSpillRecord<EDATA_T>* records)
      : records_(records) {}

  EDATA_T Get(size_t row) const { return records_[row].data; }

 private:
  const BulkEdgeSpillRecord<EDATA_T>* records_;
};

template <typename EDATA_T, typename OutWriter, typename InWriter>
uint64_t replay_bulk_edges(
    const BulkEdgeMaterialization<EDATA_T>& materialization, bool concurrent,
    bool preserve_input_order, std::vector<BulkEdgeWorkerScratch>& scratches,
    OutWriter& out, InWriter& in) {
  const auto& segments = materialization.segments;
  CHECK_EQ(scratches.size(), segments.size());
  for (auto& scratch : scratches) {
    scratch.filled_edge_count = 0;
  }

  auto replay_segment = [&](size_t worker) {
    auto& scratch = scratches[worker];
    segments[worker]->Replay(
        [&](const BulkEdgeSpillRecord<EDATA_T>* records, size_t count) {
          scratch.src_lids.resize(count);
          scratch.dst_lids.resize(count);
          for (size_t row = 0; row < count; ++row) {
            scratch.src_lids[row] = records[row].src;
            scratch.dst_lids[row] = records[row].dst;
          }
          BulkEdgeSpillDataReader<EDATA_T> data_reader(records);
          if (concurrent) {
            fill_bulk_edge_chunk_concurrent(scratch, data_reader, out, in);
          } else {
            fill_bulk_edge_chunk_serial(scratch, data_reader, out, in);
          }
        });
  };

  if (preserve_input_order) {
    std::vector<BulkEdgeSpillRun> ordered_runs;
    for (const auto& runs : materialization.runs) {
      ordered_runs.insert(ordered_runs.end(), runs.begin(), runs.end());
    }
    std::stable_sort(ordered_runs.begin(), ordered_runs.end(),
                     [](const auto& lhs, const auto& rhs) {
                       return lhs.first_row_ordinal < rhs.first_row_ordinal;
                     });
    using spill_reader_t = typename BulkEdgeSpillSegment<EDATA_T>::Reader;
    std::vector<std::unique_ptr<spill_reader_t>> readers(segments.size());
    for (const auto& run : ordered_runs) {
      CHECK_LT(run.segment, segments.size());
      auto& scratch = scratches[run.segment];
      auto& reader = readers[run.segment];
      if (!reader) {
        reader = segments[run.segment]->OpenReader();
      }
      reader->ReplayRange(
          run.record_offset, run.record_count,
          [&](const BulkEdgeSpillRecord<EDATA_T>* records, size_t count) {
            scratch.src_lids.resize(count);
            scratch.dst_lids.resize(count);
            for (size_t row = 0; row < count; ++row) {
              scratch.src_lids[row] = records[row].src;
              scratch.dst_lids[row] = records[row].dst;
            }
            BulkEdgeSpillDataReader<EDATA_T> data_reader(records);
            fill_bulk_edge_chunk_serial(scratch, data_reader, out, in);
          });
    }
  } else if (concurrent && segments.size() > 1) {
    std::atomic<bool> cancelled{false};
    std::mutex error_mutex;
    std::exception_ptr first_error;
    std::vector<std::thread> workers;
    workers.reserve(segments.size());
    for (size_t worker = 0; worker < segments.size(); ++worker) {
      workers.emplace_back([&, worker] {
        try {
          if (!cancelled.load(std::memory_order_acquire)) {
            replay_segment(worker);
          }
        } catch (...) {
          bool expected = false;
          if (cancelled.compare_exchange_strong(expected, true,
                                                std::memory_order_acq_rel)) {
            std::lock_guard<std::mutex> lock(error_mutex);
            first_error = std::current_exception();
          }
        }
      });
    }
    for (auto& worker : workers) {
      worker.join();
    }
    if (first_error) {
      std::rethrow_exception(first_error);
    }
  } else {
    for (size_t worker = 0; worker < segments.size(); ++worker) {
      replay_segment(worker);
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

template <typename EDATA_T, typename OutWriter, typename InWriter>
uint64_t build_bundled_edges_with_writers(OutWriter& out, InWriter& in,
                                          const IndexerType& src_indexer,
                                          const IndexerType& dst_indexer,
                                          IDataChunkSource& source,
                                          int64_t source_bytes,
                                          Checkpoint& checkpoint,
                                          BulkLoadOptions bulk_load_options) {
  constexpr bool kStoresAnyDirection =
      OutWriter::kStrategy != EdgeStrategy::kNone ||
      InWriter::kStrategy != EdgeStrategy::kNone;
  if constexpr (!kStoresAnyDirection) {
    return 0;
  } else {
    constexpr bool kHasSingleDirection =
        OutWriter::kStrategy == EdgeStrategy::kSingle ||
        InWriter::kStrategy == EdgeStrategy::kSingle;
    auto source_options = ResolveBulkBuildSourceOptions(
        source_bytes, source.ParallelEnabled(), bulk_load_options.worker_budget,
        BulkBuildWorkerStrategy::kBalancedProducerConsumer);
    if constexpr (kHasSingleDirection) {
      if (!source.ProvidesStableRowOrdinals()) {
        // Without stable ordinals a one-shot parallel source cannot recover
        // last-write-wins order if a single-edge duplicate is discovered.
        source_options = ChunkSourceOptions{};
        source_options.worker_budget =
            std::max<int32_t>(1, bulk_load_options.worker_budget);
      }
    }
    std::vector<BulkEdgeWorkerScratch> scratches;
    scratches.resize(static_cast<size_t>(
        std::max<int32_t>(1, source_options.consumer_count)));
    auto materialization = materialize_bulk_edges<EDATA_T>(
        source, src_indexer, dst_indexer, source_options, checkpoint, scratches,
        out, in);
    if constexpr (OutWriter::kStrategy == EdgeStrategy::kMultiple) {
      out.AllocateFromCounts();
    }
    if constexpr (InWriter::kStrategy == EdgeStrategy::kMultiple) {
      in.AllocateFromCounts();
    }
    const bool single_duplicate =
        kHasSingleDirection &&
        std::any_of(scratches.begin(), scratches.end(),
                    [](const auto& scratch) {
                      return scratch.out_single_duplicate ||
                             scratch.in_single_duplicate;
                    });
    const bool concurrent_replay =
        source_options.consumer_count > 1 && !single_duplicate;
    return replay_bulk_edges<EDATA_T>(materialization, concurrent_replay,
                                      single_duplicate, scratches, out, in);
  }
}

template <typename EDATA_T>
bool build_bundled_edges_typed(
    CsrBase* out_csr, CsrBase* in_csr, const IndexerType& src_indexer,
    const IndexerType& dst_indexer, IDataChunkSource& source,
    int64_t source_bytes, vid_t src_vertex_capacity, vid_t dst_vertex_capacity,
    Checkpoint& checkpoint, BulkLoadOptions bulk_load_options) {
  bool built = false;
  const bool out_supported =
      with_csr_bulk_writer<EDATA_T>(out_csr, [&](auto& out) {
        const bool in_supported =
            with_csr_bulk_writer<EDATA_T>(in_csr, [&](auto& in) {
              out.PrepareBuild(src_vertex_capacity);
              in.PrepareBuild(dst_vertex_capacity);
              const auto filled_edge_count =
                  build_bundled_edges_with_writers<EDATA_T>(
                      out, in, src_indexer, dst_indexer, source, source_bytes,
                      checkpoint, bulk_load_options);
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
                         IDataChunkSource& source, int64_t source_bytes,
                         vid_t src_vertex_capacity, vid_t dst_vertex_capacity,
                         Checkpoint& checkpoint,
                         BulkLoadOptions bulk_load_options) {
  const auto property_type = schema.properties.empty()
                                 ? DataTypeId::kEmpty
                                 : schema.properties[0].id();
  switch (property_type) {
#define TYPE_DISPATCHER(enum_val, cpp_type)                              \
  case DataTypeId::enum_val:                                             \
    return build_bundled_edges_typed<cpp_type>(                          \
        out_csr, in_csr, src_indexer, dst_indexer, source, source_bytes, \
        src_vertex_capacity, dst_vertex_capacity, checkpoint,            \
        bulk_load_options);
    FOR_EACH_DATA_TYPE_NO_STRING(TYPE_DISPATCHER)
#undef TYPE_DISPATCHER
  case DataTypeId::kEmpty:
    return build_bundled_edges_typed<EmptyType>(
        out_csr, in_csr, src_indexer, dst_indexer, source, source_bytes,
        src_vertex_capacity, dst_vertex_capacity, checkpoint,
        bulk_load_options);
  default:
    return false;
  }
}

}  // namespace

bool internal::BundledEdgeCsrLoader::TryBuild(
    CsrBase& out_csr, CsrBase& in_csr, const EdgeSchema& schema,
    const IndexerType& src_indexer, const IndexerType& dst_indexer,
    IDataChunkSource& source, int64_t source_bytes, vid_t src_vertex_capacity,
    vid_t dst_vertex_capacity, Checkpoint& checkpoint,
    BulkLoadOptions options) {
  return build_bundled_edges(
      &out_csr, &in_csr, schema, src_indexer, dst_indexer, source, source_bytes,
      src_vertex_capacity, dst_vertex_capacity, checkpoint, options);
}

}  // namespace neug
