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

// Storage-level A/B benchmark: legacy whole-buffer TypedColumn vs the chunked
// COW ChunkedColumn, run over identical workloads on the same machine/data.
//
// This is a reproducible harness for the handover's step-2 question: "does a
// small change really copy less and write less to disk?". It isolates the
// property-column layer (no query engine), so the reported copy/write bytes are
// deterministic and comparable across the two layouts. It also covers the read
// side (W7 point-read latency, W8 scan throughput + cross-object count) to
// verify whether chunking introduces read amplification.
//
// It is built as a gtest executable (chunk_benchmark) that is NOT registered
// with ctest, so it never slows CI. Run it manually:
//   build/tests/storage/chunk_benchmark
// Scale is controlled by env vars (defaults suit a laptop):
//   NEUG_BENCH_ROWS        base column rows          (default 4194304 = 32 MiB)
//   NEUG_BENCH_UPDATES     updates per COW workload   (default 1000)
//   NEUG_BENCH_BATCHES     multi-batch COPY rounds    (default 20)
//   NEUG_BENCH_BATCH_ROWS  rows appended per round    (default 100000)
//   NEUG_BENCH_READS       point reads per pass       (default 100000)
//   NEUG_BENCH_SCAN_ROUNDS scattered-update checkpoints (default 5)

#include <gtest/gtest.h>
#include <sys/resource.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <filesystem>
#include <functional>
#include <memory>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include "neug/storages/chunk/chunk_types.h"
#include "neug/storages/checkpoint_manager.h"
#include "neug/storages/checkpoint_manifest.h"
#include "neug/storages/graph/schema.h"
#include "neug/utils/property/chunked_column.h"
#include "neug/utils/property/column.h"

namespace neug {
namespace {

using Clock = std::chrono::steady_clock;

size_t EnvOr(const char* name, size_t fallback) {
  if (const char* v = std::getenv(name)) {
    if (size_t parsed = std::strtoull(v, nullptr, 10))
      return parsed;
  }
  return fallback;
}

double EnvOrD(const char* name, double fallback) {
  if (const char* v = std::getenv(name)) {
    char* end = nullptr;
    const double parsed = std::strtod(v, &end);
    if (end != v)
      return parsed;
  }
  return fallback;
}

struct Config {
  size_t rows = EnvOr("NEUG_BENCH_ROWS", 1ull << 22);
  size_t updates = EnvOr("NEUG_BENCH_UPDATES", 1000);
  size_t batches = EnvOr("NEUG_BENCH_BATCHES", 20);
  size_t batch_rows = EnvOr("NEUG_BENCH_BATCH_ROWS", 100000);
  size_t reads = EnvOr("NEUG_BENCH_READS", 100000);
  size_t scan_rounds = EnvOr("NEUG_BENCH_SCAN_ROUNDS", 5);
  // W4 hot-spot skew: theta in [0,1); 0 is uniform, ->1 concentrates on a few
  // hot rows. W6 single-row incremental insert scale + checkpoint cadence.
  // Dense high-fraction update touches dense_fraction % of the column.
  double zipf_theta = EnvOrD("NEUG_BENCH_ZIPF_THETA", 0.99);
  size_t inserts = EnvOr("NEUG_BENCH_INSERTS", 20000);
  size_t insert_ckp_every = EnvOr("NEUG_BENCH_INSERT_CKP_EVERY", 1000);
  size_t dense_fraction = EnvOr("NEUG_BENCH_DENSE_FRACTION", 50);
};

struct Result {
  std::string layout;
  std::string workload;
  uint64_t copied_bytes = 0;
  uint64_t write_bytes = 0;
  uint64_t dir_bytes = 0;  // chunk-directory blob rewritten (metadata face)
  double wall_ms = 0.0;
  double ckp_ms = 0.0;     // checkpoint (Dump+Finalize) stall
  double p99_us = 0.0;
  long rss_delta_mb = 0;
  size_t objects = 0;
};

long PeakRssMb() {
  rusage usage{};
  getrusage(RUSAGE_SELF, &usage);
#if defined(__APPLE__)
  return static_cast<long>(usage.ru_maxrss / (1024 * 1024));  // bytes
#else
  return static_cast<long>(usage.ru_maxrss / 1024);  // kilobytes
#endif
}

uint64_t DirBytes(const std::filesystem::path& dir) {
  uint64_t total = 0;
  std::error_code ec;
  for (auto it = std::filesystem::recursive_directory_iterator(dir, ec);
       it != std::filesystem::recursive_directory_iterator(); it.increment(ec)) {
    if (ec)
      break;
    if (it->is_regular_file(ec))
      total += static_cast<uint64_t>(it->file_size(ec));
  }
  return total;
}

size_t DirFiles(const std::filesystem::path& dir) {
  size_t count = 0;
  std::error_code ec;
  for (auto it = std::filesystem::recursive_directory_iterator(dir, ec);
       it != std::filesystem::recursive_directory_iterator(); it.increment(ec)) {
    if (ec)
      break;
    if (it->is_regular_file(ec))
      ++count;
  }
  return count;
}

double PercentileUs(std::vector<double>& samples_us, double pct) {
  if (samples_us.empty())
    return 0.0;
  std::sort(samples_us.begin(), samples_us.end());
  size_t idx = static_cast<size_t>((pct / 100.0) * (samples_us.size() - 1));
  return samples_us[idx];
}

// A self-contained checkpoint sandbox: one temp dir, one staging Checkpoint.
// Object files land under the temp dir, so DirBytes/DirFiles deltas measure the
// bytes/objects a Dump actually persisted.
class Sandbox {
 public:
  explicit Sandbox(const std::string& tag) {
    dir_ = std::filesystem::temp_directory_path() /
           ("neug_bench_" + tag + "_" +
            std::to_string(
                Clock::now().time_since_epoch().count()));
    std::filesystem::remove_all(dir_);
    std::filesystem::create_directories(dir_);
    mgr_.Open(dir_.string());
    auto staging = mgr_.CreateStaging();
    ckp_ = staging.checkpoint();
    CheckpointManifest meta;
    meta.SetSchema(Schema());
    ckp_->SetManifest(std::move(meta));
    staging.Discard();
  }
  ~Sandbox() { std::filesystem::remove_all(dir_); }
  Checkpoint& ckp() { return *ckp_; }
  const std::filesystem::path& dir() const { return dir_; }

 private:
  std::filesystem::path dir_;
  CheckpointManager mgr_;
  std::shared_ptr<Checkpoint> ckp_;
};

// Factories only construct the column; the harness Opens it (empty for a fresh
// base, or from a committed descriptor when reopening a live column). This
// split matters because Dump() consumes a legacy buffer, so a live COW source
// must be reopened from the last checkpoint rather than reused after Dump.
using Factory = std::function<std::unique_ptr<ColumnBase>()>;

Factory LegacyFactory() {
  return [] { return std::make_unique<TypedColumn<int64_t>>(); };
}

Factory ChunkedFactory() {
  return [] { return std::make_unique<ChunkedColumn<int64_t>>(); };
}

// Open a column: from a committed descriptor when @p desc is set (the live
// state a write transaction would clone after a checkpoint), else empty.
std::unique_ptr<ColumnBase> OpenColumn(const Factory& factory, Checkpoint& ckp,
                                       const ModuleDescriptor* desc) {
  auto col = factory();
  if (desc != nullptr) {
    col->Open(ckp, *desc, MemoryLevel::kInMemory);
  } else {
    col->Open(ckp, ModuleDescriptor{}, MemoryLevel::kInMemory);
  }
  return col;
}

uint64_t ChunkedCopiedBytes(const ColumnBase& col) {
  if (auto* chunked =
          dynamic_cast<const ChunkedColumn<int64_t>*>(&col))
    return chunked->cow_bytes_copied();
  return 0;
}

bool IsChunked(const ColumnBase& col) {
  return dynamic_cast<const ChunkedColumn<int64_t>*>(&col) != nullptr;
}

// ---------------------------------------------------------------------------
// COW update workload: build a base column, checkpoint it, then Clone+Detach a
// private copy, apply updates, and measure the incremental checkpoint cost.
// This mirrors a SnapshotCowWriteTransaction over one property column.
// ---------------------------------------------------------------------------
using ApplyFn = std::function<void(ColumnBase&, std::vector<double>&)>;

Result RunCowUpdate(const std::string& layout, const Factory& factory,
                    const std::string& workload, const Config& cfg,
                    const ApplyFn& apply) {
  Result r{layout, workload};
  Sandbox box(layout + "_" + workload);
  auto& ckp = box.ckp();

  // Build the base column and take checkpoint N. Dump() consumes a legacy
  // buffer, so the base is dropped and a live column is reopened from the
  // committed descriptor before cloning (this is the state a write transaction
  // would clone after recovery).
  auto base = OpenColumn(factory, ckp, nullptr);
  base->resize(cfg.rows);
  for (size_t i = 0; i < cfg.rows; ++i)
    base->set_any(i, Value::INT64(static_cast<int64_t>(i)), true);
  CheckpointManifest base_meta;
  base->Dump(ckp, base_meta, "col");
  ckp.FinalizeObjectWriter(base_meta);
  base.reset();

  auto live = OpenColumn(factory, ckp, base_meta.FindModule("col"));

  // Private COW workspace, as a write transaction would create. Clone()
  // returns a Module; downcast to ColumnBase so set_any/Dump/cow metrics are
  // reachable through the same handle.
  auto cow_module = live->Clone();
  auto* cow = dynamic_cast<ColumnBase*>(cow_module.get());
  if (cow == nullptr) {
    ADD_FAILURE() << "Clone() did not yield a ColumnBase for " << layout;
    return r;
  }
  const long rss0 = PeakRssMb();
  const uint64_t files0 = DirFiles(box.dir());
  std::vector<double> latencies_us;
  latencies_us.reserve(cfg.updates);

  const auto t0 = Clock::now();
  cow->Detach(ckp, MemoryLevel::kInMemory);
  apply(*cow, latencies_us);
  const auto t1 = Clock::now();

  // Copied bytes: chunked forks only touched pages; legacy forks the whole
  // buffer at Detach (size * sizeof(int64)).
  r.copied_bytes =
      IsChunked(*cow) ? ChunkedCopiedBytes(*cow) : cfg.rows * sizeof(int64_t);

  const uint64_t bytes0 = DirBytes(box.dir());
  const auto td0 = Clock::now();
  CheckpointManifest cow_meta;
  cow->Dump(ckp, cow_meta, "col");
  ckp.FinalizeObjectWriter(cow_meta);
  r.ckp_ms =
      std::chrono::duration<double, std::milli>(Clock::now() - td0).count();
  const uint64_t bytes1 = DirBytes(box.dir());

  r.write_bytes = bytes1 - bytes0;
  r.objects = DirFiles(box.dir()) - files0;
  // Metadata face: the chunk-directory blob this checkpoint rewrote whole
  // (O(pages), the P0 leftover scheme A attacks). Legacy has no separate
  // directory, so it stays 0.
  if (IsChunked(*cow)) {
    if (auto path = cow_meta.FindModule("col")->get_path(kChunkDirPath)) {
      auto file = ckp.OpenFile(*path, MemoryLevel::kInMemory);
      r.dir_bytes = file->GetDataSize();
    }
  }
  r.wall_ms =
      std::chrono::duration<double, std::milli>(t1 - t0).count();
  r.p99_us = PercentileUs(latencies_us, 99.0);
  r.rss_delta_mb = PeakRssMb() - rss0;
  return r;
}

// ---------------------------------------------------------------------------
// Multi-batch append workload: K rounds of (grow + reserved-append + checkpoint)
// models repeated COPY into the same table. Legacy rewrites the whole growing
// buffer each round (quadratic); chunked writes only new/changed chunks
// (linear). copied/write bytes are summed across all rounds.
// ---------------------------------------------------------------------------
Result RunMultiBatchAppend(const std::string& layout, const Factory& factory,
                           const Config& cfg) {
  Result r{layout, "multi_batch_copy"};
  Sandbox box(layout + "_multibatch");
  auto& ckp = box.ckp();

  auto col = OpenColumn(factory, ckp, nullptr);
  col->resize(0);
  size_t rows = 0;
  const auto t0 = Clock::now();
  for (size_t b = 0; b < cfg.batches; ++b) {
    const size_t begin = rows;
    rows += cfg.batch_rows;
    col->resize(rows);
    col->PrepareForInsert(begin);
    for (size_t i = begin; i < rows; ++i)
      col->set_any(i, Value::INT64(static_cast<int64_t>(i)), false);
    // Per-round copy cost: a legacy resize memcpy's the whole prior buffer into
    // a fresh mapping (quadratic across rounds); a chunked append reuses the
    // existing chunks and only forks a touched tail page (near zero).
    if (IsChunked(*col)) {
      r.copied_bytes += ChunkedCopiedBytes(*col);
    } else {
      r.copied_bytes += static_cast<uint64_t>(begin) * sizeof(int64_t);
    }
    const uint64_t bytes0 = DirBytes(box.dir());
    CheckpointManifest meta;
    col->Dump(ckp, meta, "col");
    ckp.FinalizeObjectWriter(meta);
    r.write_bytes += DirBytes(box.dir()) - bytes0;
    // Dump consumed the buffer; reopen the live column for the next round.
    col = OpenColumn(factory, ckp, meta.FindModule("col"));
  }
  const auto t1 = Clock::now();
  r.objects = DirFiles(box.dir());
  r.wall_ms = std::chrono::duration<double, std::milli>(t1 - t0).count();
  return r;
}

// ---------------------------------------------------------------------------
// Read-side helpers and results (W7 point read, W8 scan / read amplification).
// ---------------------------------------------------------------------------
struct ReadResult {
  std::string layout;
  std::string workload;
  double wall_ms = 0.0;
  double p50_us = 0.0;
  double p99_us = 0.0;
  double mrows_per_s = 0.0;
  size_t cross_objects = 0;
};

void PrintReadRow(const ReadResult& r) {
  std::printf("| %-9s | %-22s | %9.2f | %8.3f | %8.3f | %9.3f | %6zu |\n",
              r.layout.c_str(), r.workload.c_str(), r.wall_ms, r.p50_us,
              r.p99_us, r.mrows_per_s, r.cross_objects);
}

void PrintReadHeader() {
  std::printf("| %-9s | %-22s | %9s | %8s | %8s | %9s | %6s |\n", "layout",
              "workload", "wall_ms", "p50_us", "p99_us", "Mrows/s", "xobjs");
  std::printf(
      "|-----------|------------------------|-----------|----------|"
      "----------|-----------|--------|\n");
}

std::vector<ReadResult> g_read_results;

void RecordRead(ReadResult r) {
  PrintReadRow(r);
  g_read_results.push_back(std::move(r));
}

// Raw single-row read that bypasses Value boxing, so the measured cost is the
// layout's true per-access path: legacy does one direct buffer index; chunked
// resolves a page, picks prefix/suffix, and verifies the segment CRC on first
// touch (cached afterwards via the segment's verified flag).
int64_t ReadRow(const ColumnBase& col, size_t row) {
  if (auto* chunked = dynamic_cast<const ChunkedColumn<int64_t>*>(&col))
    return chunked->get_view(row);
  if (auto* legacy = dynamic_cast<const TypedColumn<int64_t>*>(&col))
    return legacy->get_view(row);
  return col.get_any(row).GetValue<int64_t>();
}

// Distinct physical objects a full sequential scan must map. Legacy stores the
// whole column in one committed data object (scan touches 1). Chunked spreads
// pages across objects; the persisted chunk directory lists exactly the objects
// its pages reference (also what checkpoint GC retains), so its size is the
// scan's cross-object count. Its growth across checkpoints is the read
// amplification the first-set physical merge (P1) is meant to bound.
size_t CountScanObjects(Checkpoint& ckp, const ModuleDescriptor& desc) {
  if (auto path = desc.get_path(kChunkDirPath)) {
    auto file = ckp.OpenFile(*path, MemoryLevel::kInMemory);
    auto d = DecodeChunkDirectory(file->GetData(), file->GetDataSize());
    return d.object_ids.size();
  }
  return 1;  // legacy: one whole-buffer object
}

// ---------------------------------------------------------------------------
// W7 point-read latency: build + checkpoint a base column, reopen it read-only
// (the state a read transaction sees), then time random single-row reads. Two
// passes expose the chunked segment CRC verify as a one-time cold cost: pass 1
// (cold) pays CRC on each segment's first touch, pass 2 (warm) hits the cache.
// The same fixed row sequence is replayed so the passes differ only by the
// verify cache, not by which rows are touched.
// ---------------------------------------------------------------------------
std::vector<ReadResult> RunPointRead(const std::string& layout,
                                     const Factory& factory,
                                     const Config& cfg) {
  std::vector<ReadResult> out;
  Sandbox box(layout + "_pointread");
  auto& ckp = box.ckp();

  auto base = OpenColumn(factory, ckp, nullptr);
  base->resize(cfg.rows);
  for (size_t i = 0; i < cfg.rows; ++i)
    base->set_any(i, Value::INT64(static_cast<int64_t>(i)), true);
  CheckpointManifest base_meta;
  base->Dump(ckp, base_meta, "col");
  ckp.FinalizeObjectWriter(base_meta);
  base.reset();

  const auto* desc = base_meta.FindModule("col");
  auto live = OpenColumn(factory, ckp, desc);
  const size_t xobjs = CountScanObjects(ckp, *desc);

  std::mt19937_64 rng(1234);
  std::uniform_int_distribution<size_t> dist(0, cfg.rows - 1);
  std::vector<size_t> rows(cfg.reads);
  for (auto& x : rows)
    x = dist(rng);

  for (int pass = 0; pass < 2; ++pass) {
    ReadResult r{layout, pass == 0 ? "point_read_cold" : "point_read_warm"};
    std::vector<double> lat;
    lat.reserve(cfg.reads);
    volatile int64_t sink = 0;  // defeat dead-code elimination of the reads
    const auto t0 = Clock::now();
    for (size_t k = 0; k < cfg.reads; ++k) {
      const auto s = Clock::now();
      sink += ReadRow(*live, rows[k]);
      lat.push_back(
          std::chrono::duration<double, std::micro>(Clock::now() - s).count());
    }
    const auto t1 = Clock::now();
    r.wall_ms = std::chrono::duration<double, std::milli>(t1 - t0).count();
    r.p50_us = PercentileUs(lat, 50.0);
    r.p99_us = PercentileUs(lat, 99.0);
    r.cross_objects = xobjs;
    out.push_back(std::move(r));
  }
  return out;
}

// ---------------------------------------------------------------------------
// W8 scan throughput + cross-object count: build a base column, then run
// scan_rounds of scattered-update checkpoints WITHOUT any physical merge. After
// each checkpoint, reopen and time one full sequential scan and count the
// distinct objects it spans. Legacy rewrites one whole object each round (scan
// always spans 1: no read amplification, but heavy write cost already measured
// by the COW workloads); chunked writes only touched pages into new objects
// while untouched pages keep referencing old ones, so its cross-object count
// grows with rounds -- the read amplification this benchmark exists to surface.
// ---------------------------------------------------------------------------
std::vector<ReadResult> RunScanReadAmplification(const std::string& layout,
                                                 const Factory& factory,
                                                 const Config& cfg) {
  std::vector<ReadResult> out;
  Sandbox box(layout + "_scanamp");
  auto& ckp = box.ckp();

  auto col = OpenColumn(factory, ckp, nullptr);
  col->resize(cfg.rows);
  for (size_t i = 0; i < cfg.rows; ++i)
    col->set_any(i, Value::INT64(static_cast<int64_t>(i)), true);
  CheckpointManifest meta;
  col->Dump(ckp, meta, "col");
  ckp.FinalizeObjectWriter(meta);
  col.reset();

  auto scan_once = [&](const ModuleDescriptor* desc, const std::string& name) {
    auto read_col = OpenColumn(factory, ckp, desc);
    ReadResult r{layout, name};
    volatile int64_t sink = 0;
    const auto t0 = Clock::now();
    for (size_t i = 0; i < cfg.rows; ++i)
      sink += ReadRow(*read_col, i);
    const auto t1 = Clock::now();
    r.wall_ms = std::chrono::duration<double, std::milli>(t1 - t0).count();
    r.mrows_per_s = cfg.rows / (r.wall_ms * 1000.0);
    r.cross_objects = CountScanObjects(ckp, *desc);
    return r;
  };

  out.push_back(scan_once(meta.FindModule("col"), "scan_ckpt_r0"));

  std::mt19937_64 rng(1234);
  std::uniform_int_distribution<size_t> dist(0, cfg.rows - 1);
  auto live = OpenColumn(factory, ckp, meta.FindModule("col"));
  for (size_t round = 1; round <= cfg.scan_rounds; ++round) {
    auto cow_module = live->Clone();
    auto* cow = dynamic_cast<ColumnBase*>(cow_module.get());
    if (cow == nullptr) {
      ADD_FAILURE() << "Clone() did not yield a ColumnBase for " << layout;
      return out;
    }
    cow->Detach(ckp, MemoryLevel::kInMemory);
    for (size_t k = 0; k < cfg.updates; ++k)
      cow->set_any(dist(rng), Value::INT64(static_cast<int64_t>(round)), true);
    CheckpointManifest round_meta;
    cow->Dump(ckp, round_meta, "col");
    ckp.FinalizeObjectWriter(round_meta);
    out.push_back(scan_once(round_meta.FindModule("col"),
                            "scan_ckpt_r" + std::to_string(round)));
    live = OpenColumn(factory, ckp, round_meta.FindModule("col"));
  }
  return out;
}

void PrintRow(const Result& r) {
  std::printf(
      "| %-9s | %-22s | %14llu | %14llu | %11llu | %9.2f | %8.2f | %8.2f | "
      "%7ld | %6zu |\n",
      r.layout.c_str(), r.workload.c_str(),
      static_cast<unsigned long long>(r.copied_bytes),
      static_cast<unsigned long long>(r.write_bytes),
      static_cast<unsigned long long>(r.dir_bytes), r.wall_ms, r.ckp_ms,
      r.p99_us, r.rss_delta_mb, r.objects);
}

void PrintHeader() {
  std::printf(
      "| %-9s | %-22s | %14s | %14s | %11s | %9s | %8s | %8s | %7s | %6s |\n",
      "layout", "workload", "copied_bytes", "write_bytes", "dir_bytes",
      "wall_ms", "ckp_ms", "p99_us", "rss_dMB", "objs");
  std::printf(
      "|-----------|------------------------|----------------|"
      "----------------|-------------|-----------|----------|----------|"
      "---------|--------|\n");
}

std::vector<Result> g_results;

void Record(Result r) {
  PrintRow(r);
  g_results.push_back(std::move(r));
}

// Apply helpers --------------------------------------------------------------

ApplyFn SingleValueUpdate(const Config& cfg) {
  return [cfg](ColumnBase& col, std::vector<double>& lat) {
    for (size_t k = 0; k < cfg.updates; ++k) {
      const auto s = Clock::now();
      col.set_any(cfg.rows / 2, Value::INT64(static_cast<int64_t>(k)), true);
      lat.push_back(
          std::chrono::duration<double, std::micro>(Clock::now() - s).count());
    }
  };
}

ApplyFn RandomUpdate(const Config& cfg) {
  return [cfg](ColumnBase& col, std::vector<double>& lat) {
    std::mt19937_64 rng(1234);
    std::uniform_int_distribution<size_t> dist(0, cfg.rows - 1);
    for (size_t k = 0; k < cfg.updates; ++k) {
      const size_t row = dist(rng);
      const auto s = Clock::now();
      col.set_any(row, Value::INT64(static_cast<int64_t>(k)), true);
      lat.push_back(
          std::chrono::duration<double, std::micro>(Clock::now() - s).count());
    }
  };
}

ApplyFn DenseUpdate(const Config& cfg) {
  // A contiguous run that fills whole 4 KiB pages (worst case for sub-page
  // COW: degenerates toward whole-chunk copy when every page is touched).
  return [cfg](ColumnBase& col, std::vector<double>& lat) {
    const size_t start = cfg.rows / 4;
    for (size_t k = 0; k < cfg.updates; ++k) {
      const auto s = Clock::now();
      col.set_any(start + k, Value::INT64(static_cast<int64_t>(k)), true);
      lat.push_back(
          std::chrono::duration<double, std::micro>(Clock::now() - s).count());
    }
  };
}

// YCSB-style Zipfian generator over [0, n). theta in [0,1): theta=0 is uniform,
// theta->1 concentrates draws on a small hot set. Models W4's skewed hot-spot
// update access, where a few chunks take most writes -- the case where the
// chunked layout's "fork only touched pages" advantage over a whole-buffer
// legacy dump is largest (the hot pages are re-forked, the cold tail is not).
class Zipf {
 public:
  Zipf(size_t n, double theta, uint64_t seed) : rng_(seed), n_(n), theta_(theta) {
    alpha_ = 1.0 / (1.0 - theta);
    zeta2_ = Zeta(2, theta);
    zetan_ = Zeta(n, theta);
    eta_ = (1.0 - std::pow(2.0 / static_cast<double>(n), 1.0 - theta)) /
           (1.0 - zeta2_ / zetan_);
  }
  size_t operator()() {
    const double u = std::uniform_real_distribution<double>(0.0, 1.0)(rng_);
    const double uz = u * zetan_;
    if (uz < 1.0)
      return 0;
    if (uz < 1.0 + std::pow(0.5, theta_))
      return 1;
    const size_t k = static_cast<size_t>(static_cast<double>(n_) *
                                         std::pow(eta_ * u - eta_ + 1.0, alpha_));
    return k < n_ ? k : n_ - 1;
  }

 private:
  static double Zeta(size_t n, double theta) {
    double sum = 0.0;
    for (size_t i = 1; i <= n; ++i)
      sum += std::pow(1.0 / static_cast<double>(i), theta);
    return sum;
  }
  std::mt19937_64 rng_;
  size_t n_;
  double theta_, alpha_, zeta2_, zetan_, eta_;
};

// W4 hot-spot update: same op count as RandomUpdate but Zipf-skewed, so writes
// concentrate on a few hot pages instead of spreading uniformly.
ApplyFn ZipfUpdate(const Config& cfg) {
  return [cfg](ColumnBase& col, std::vector<double>& lat) {
    Zipf zipf(cfg.rows, cfg.zipf_theta, 1234);
    for (size_t k = 0; k < cfg.updates; ++k) {
      const size_t row = zipf();
      const auto s = Clock::now();
      col.set_any(row, Value::INT64(static_cast<int64_t>(k)), true);
      lat.push_back(
          std::chrono::duration<double, std::micro>(Clock::now() - s).count());
    }
  };
}

// Dense high-fraction update: touch dense_fraction % of the whole column as one
// contiguous run (default 50%). This is the honest worst case for sub-page COW
// -- when the updated fraction is high, nearly every page in the window forks
// and the chunked layout converges toward a whole-buffer legacy rewrite, so its
// copied/write bytes approach legacy's. Reports the crossover cost, not a win.
ApplyFn DenseHighFractionUpdate(const Config& cfg) {
  return [cfg](ColumnBase& col, std::vector<double>& lat) {
    const size_t frac = std::min<size_t>(cfg.dense_fraction, 100);
    const size_t span = std::max<size_t>(1, cfg.rows * frac / 100);
    const size_t start = (cfg.rows - span) / 3;  // fixed offset, both layouts
    for (size_t k = 0; k < span; ++k) {
      const auto s = Clock::now();
      col.set_any(start + k, Value::INT64(static_cast<int64_t>(k)), true);
      lat.push_back(
          std::chrono::duration<double, std::micro>(Clock::now() - s).count());
    }
  };
}

// ---------------------------------------------------------------------------
// Page-table metadata scaling (P0 leftover). The chunked column already writes
// data pages incrementally, but the page table that locates those pages is
// still copied whole in two places: Clone() deep-copies the in-memory page
// descriptor array (O(page_count)); Dump() re-encodes the whole ChunkDirectory
// blob (O(page_count)). The two tests below sweep column size and measure each
// cost in isolation, so we can read off at what scale the page-table metadata,
// not the data, starts to dominate a single-value change. Neither test changes
// storage code; they only measure the current implementation.
// ---------------------------------------------------------------------------
struct DirStats {
  size_t page_count = 0;
  size_t object_count = 0;
  uint64_t rows_per_page = 0;
  uint64_t blob_bytes = 0;
};

DirStats ReadDirStats(Checkpoint& ckp, const ModuleDescriptor& desc) {
  DirStats s;
  if (auto path = desc.get_path(kChunkDirPath)) {
    auto file = ckp.OpenFile(*path, MemoryLevel::kInMemory);
    s.blob_bytes = file->GetDataSize();
    auto d = DecodeChunkDirectory(file->GetData(), file->GetDataSize());
    s.page_count = d.pages.size();
    s.object_count = d.object_ids.size();
    s.rows_per_page = d.rows_per_page;
  }
  return s;
}

// Build an N-row chunked int64 column and publish checkpoint 1. Dump consumes
// the buffer, so callers reopen a live column from out_meta afterwards.
void BuildChunkedBase(Checkpoint& ckp, size_t rows,
                      CheckpointManifest& out_meta) {
  auto base = OpenColumn(ChunkedFactory(), ckp, nullptr);
  base->resize(rows);
  for (size_t i = 0; i < rows; ++i)
    base->set_any(i, Value::INT64(static_cast<int64_t>(i)), true);
  base->Dump(ckp, out_meta, "col");
  ckp.FinalizeObjectWriter(out_meta);
}

// Count only object files (data objects + directory objects), excluding the
// manifest/CURRENT noise at the sandbox root, so a checkpoint delta reflects
// exactly the bytes the object store gained.
uint64_t ObjectsBytes(const Sandbox& box) {
  return DirBytes(box.dir() / "checkpoint" / "objects");
}

std::vector<size_t> ScaleSweep() {
  std::vector<size_t> v = {1ull << 20, 1ull << 22, 1ull << 24, 1ull << 26};
  if (EnvOr("NEUG_BENCH_HUGE", 0) != 0)
    v.push_back(1ull << 28);  // 256M rows: heavy, opt-in via NEUG_BENCH_HUGE=1
  return v;
}

double UsSince(const Clock::time_point& t0) {
  return std::chrono::duration<double, std::micro>(Clock::now() - t0).count();
}

// ---------------------------------------------------------------------------
// Concurrency results (W17 concurrent append, W18 reader-on-clone). Both are
// chunked-only: the legacy whole-buffer column has no prepared-append admission
// contract, and its Clone() shares the *mutable* buffer with no copy-on-write,
// so concurrent append into reserved rows / reader-on-clone-while-writer-fork
// are unsafe there (a real data race, not a supported path). The no-race side is
// pinned by the TSAN tests ConcurrentAppendAfterReopenUsesStableSuffix (W17) and
// ConcurrentCowForkUnderPinnedSnapshotReader (W18); these benchmarks measure the
// performance face -- throughput and copy-freedom.
// ---------------------------------------------------------------------------
struct ConcResult {
  std::string workload;
  size_t threads = 0;
  size_t rows = 0;
  double wall_ms = 0.0;
  double mrows_per_s = 0.0;
  uint64_t copied_bytes = 0;
  bool ok = false;  // values verified / snapshot isolation held
};

void PrintConcHeader() {
  std::printf("| %-24s | %7s | %10s | %9s | %9s | %14s | %4s |\n", "workload",
              "threads", "rows", "wall_ms", "Mrows/s", "copied_bytes", "ok");
  std::printf(
      "|--------------------------|---------|------------|----------|------"
      "----|----------------|------|\n");
}

void PrintConcRow(const ConcResult& r) {
  std::printf("| %-24s | %7zu | %10zu | %9.2f | %9.3f | %14llu | %4s |\n",
              r.workload.c_str(), r.threads, r.rows, r.wall_ms, r.mrows_per_s,
              static_cast<unsigned long long>(r.copied_bytes),
              r.ok ? "yes" : "NO");
}

// W17 concurrent append after checkpoint: build+checkpoint a frozen base of
// base_rows, reopen, reserve an append tail (resize + PrepareForInsert) exactly
// as the reload path does, then have `threads` writers concurrently fill
// distinct reserved rows via set_any(..., false) (InsertValue). The frozen base
// chunks are never copied -- appends land in the prepared, already mutable
// suffix -- so copied_bytes stays at the one-time boundary-prepare cost no matter
// how many rows the writers append. Reports append throughput.
ConcResult RunConcurrentAppend(Checkpoint& ckp, size_t threads, size_t base_rows,
                              size_t append_rows) {
  ConcResult r{"w17_concurrent_append", threads, append_rows};
  auto base = OpenColumn(ChunkedFactory(), ckp, nullptr);
  base->resize(base_rows);
  for (size_t i = 0; i < base_rows; ++i)
    base->set_any(i, Value::INT64(static_cast<int64_t>(i)), true);
  CheckpointManifest base_meta;
  base->Dump(ckp, base_meta, "col");
  ckp.FinalizeObjectWriter(base_meta);
  base.reset();

  auto col = OpenColumn(ChunkedFactory(), ckp, base_meta.FindModule("col"));
  // Reserve + prepare the append tail (reload path: EnsureCapacity -> resize +
  // PrepareForInsert(high-water mark)). This one-time prepare is the only copy.
  col->resize(base_rows + append_rows);
  col->PrepareForInsert(base_rows);
  auto* chunked = dynamic_cast<ChunkedColumn<int64_t>*>(col.get());
  const uint64_t copied_before = chunked->cow_bytes_copied();

  std::atomic<size_t> ready{0};
  std::vector<std::thread> writers;
  writers.reserve(threads);
  const auto t0 = Clock::now();
  for (size_t t = 0; t < threads; ++t) {
    writers.emplace_back([&, t] {
      ready.fetch_add(1);
      while (ready.load() != threads)
        std::this_thread::yield();
      // Strided so every writer touches a distinct set of reserved rows.
      for (size_t i = t; i < append_rows; i += threads)
        col->set_any(base_rows + i, Value::INT64(static_cast<int64_t>(i)),
                     false);
    });
  }
  for (auto& w : writers)
    w.join();
  const auto t1 = Clock::now();

  r.wall_ms = std::chrono::duration<double, std::milli>(t1 - t0).count();
  r.mrows_per_s =
      r.wall_ms > 0 ? (static_cast<double>(append_rows) / 1e6) /
                          (r.wall_ms / 1e3)
                    : 0.0;
  r.copied_bytes = chunked->cow_bytes_copied() - copied_before;
  bool ok = true;
  for (size_t i = 0; i < append_rows; ++i)
    if (chunked->get_view(base_rows + i) != static_cast<int64_t>(i)) {
      ok = false;
      break;
    }
  r.ok = ok;
  return r;
}

// W18 reader-on-clone while a single writer forks: build+checkpoint a frozen
// base, reopen, Clone() a pinned snapshot (an independent page directory sharing
// the frozen segments -- zero data copy), then run `readers` threads scanning the
// snapshot and verifying every value stays at its original while ONE writer
// performs covering set_value on the original. Covering writes are single-writer
// by admission control (set_value mutates the page directory), so W18 uses one
// writer. Each write forks only the touched page (Copy), so the snapshot's shared
// frozen segments are never mutated and readers keep seeing the old values.
// Reports reader scan throughput, the writer's fork copied_bytes (touched pages
// only) and whether isolation held; the Clone itself copies 0 data bytes.
ConcResult RunReaderOnClone(Checkpoint& ckp, size_t readers, size_t rows,
                           size_t writes) {
  ConcResult r{"w18_reader_on_clone", readers + 1, rows};
  auto base = OpenColumn(ChunkedFactory(), ckp, nullptr);
  base->resize(rows);
  for (size_t i = 0; i < rows; ++i)
    base->set_any(i, Value::INT64(static_cast<int64_t>(i)), true);
  CheckpointManifest base_meta;
  base->Dump(ckp, base_meta, "col");
  ckp.FinalizeObjectWriter(base_meta);
  base.reset();

  auto live = OpenColumn(ChunkedFactory(), ckp, base_meta.FindModule("col"));
  auto* original = dynamic_cast<ChunkedColumn<int64_t>*>(live.get());
  // Pinned snapshot: Clone copies the page directory only, no data copy.
  auto snap_module = live->Clone();
  auto* snapshot = dynamic_cast<ChunkedColumn<int64_t>*>(snap_module.get());

  std::atomic<bool> stop{false};
  std::atomic<bool> isolation_ok{true};
  std::atomic<size_t> reads{0};
  std::vector<std::thread> th;
  th.reserve(readers + 1);
  const auto t0 = Clock::now();
  for (size_t i = 0; i < readers; ++i) {
    th.emplace_back([&] {
      while (!stop.load(std::memory_order_relaxed)) {
        for (size_t row = 0; row < rows; ++row)
          if (snapshot->get_view(row) != static_cast<int64_t>(row))
            isolation_ok.store(false, std::memory_order_relaxed);
        reads.fetch_add(rows, std::memory_order_relaxed);
      }
    });
  }
  th.emplace_back([&] {
    for (size_t k = 0; k < writes; ++k) {
      const size_t row = k % rows;
      original->set_value(row, static_cast<int64_t>(row) + 1000000);
    }
  });
  // Join the single writer (last thread), then stop the readers.
  th.back().join();
  stop.store(true);
  for (size_t i = 0; i < readers; ++i)
    th[i].join();
  const auto t1 = Clock::now();

  r.wall_ms = std::chrono::duration<double, std::milli>(t1 - t0).count();
  const size_t total_reads = reads.load();
  r.mrows_per_s =
      r.wall_ms > 0
          ? (static_cast<double>(total_reads) / 1e6) / (r.wall_ms / 1e3)
          : 0.0;
  r.rows = total_reads;  // reader-side rows scanned during the window
  r.copied_bytes = original->cow_bytes_copied();  // writer forked pages only
  r.ok = isolation_ok.load();
  return r;
}

}  // namespace

TEST(ChunkBenchmark, CowUpdateWorkloads) {
  Config cfg;
  std::printf("\n=== Chunked COW storage A/B benchmark ===\n");
  std::printf("rows=%zu updates=%zu batches=%zu batch_rows=%zu\n\n", cfg.rows,
              cfg.updates, cfg.batches, cfg.batch_rows);
  PrintHeader();

  const struct {
    std::string name;
    ApplyFn fn;
  } workloads[] = {
      {"single_value_update", SingleValueUpdate(cfg)},
      {"random_update", RandomUpdate(cfg)},
      {"zipf_update", ZipfUpdate(cfg)},
      {"dense_update", DenseUpdate(cfg)},
      {"dense_high_fraction", DenseHighFractionUpdate(cfg)},
  };

  for (const auto& w : workloads) {
    Record(RunCowUpdate("legacy", LegacyFactory(), w.name, cfg, w.fn));
    Record(RunCowUpdate("chunked", ChunkedFactory(), w.name, cfg, w.fn));
  }
}

TEST(ChunkBenchmark, MultiBatchCopy) {
  Config cfg;
  PrintHeader();
  Record(RunMultiBatchAppend("legacy", LegacyFactory(), cfg));
  Record(RunMultiBatchAppend("chunked", ChunkedFactory(), cfg));
}

// ---------------------------------------------------------------------------
// W6 single-row incremental insert: fill one reserved row at a time, growing
// the column capacity geometrically exactly as VertexTable does, checkpointing
// every insert_ckp_every rows. Scale is bounded by NEUG_BENCH_INSERTS (not
// NEUG_BENCH_ROWS).
//
// Cadence (this models the real production path, not a pathological one): a
// single-row TP insert does NOT resize per row. VertexTable::insert_vertices
// grows capacity geometrically (cap<4096 ? 4096 : cap+cap/4) and calls
// resize+PrepareForInsert only when the next row crosses the reserved capacity
// (EnsureCapacity, vertex_table.cc:244-258); each insert then writes a reserved
// row via InsertValue with no copy. A per-row resize+PrepareForInsert would
// re-run the boundary merge+split on every row and grossly overstate chunked
// cost, so we amortize prepare across geometric growth instead.
//
// At each checkpoint the column is dumped at its reserved size (size_ ==
// capacity, matching DisassembleTo which persists the EnsureCapacity-resized
// column, headroom included) and reopened; the reopened pages are frozen, so we
// re-prepare the append area (PrepareForInsert(high_water)) exactly as the
// property_graph reload path does. Under this cadence the legacy whole-buffer
// column re-copies its prior buffer at every geometric growth (O(n) amortized
// total, NOT the O(n^2) a per-row resize would imply), while the chunked append
// reuses existing chunks and only forks the boundary page at each growth plus
// the reserved headroom tail at each checkpoint reopen. copied/write bytes, the
// summed checkpoint pause and the object count are reported.
// ---------------------------------------------------------------------------
Result RunSingleRowInsert(const std::string& layout, const Factory& factory,
                          const Config& cfg) {
  Result r{layout, "single_row_insert"};
  Sandbox box(layout + "_singlerow");
  auto& ckp = box.ckp();
  auto col = OpenColumn(factory, ckp, nullptr);
  col->resize(0);

  const long rss0 = PeakRssMb();
  const uint64_t files0 = DirFiles(box.dir());
  size_t capacity = 0;    // reserved rows the column currently holds
  size_t high_water = 0;  // logically filled rows (= VertexTable indexer size)
  const auto t0 = Clock::now();
  for (size_t k = 0; k < cfg.inserts; ++k) {
    if (high_water >= capacity) {
      // Geometric growth identical to VertexTable::insert_vertices.
      const size_t new_cap = capacity < 4096 ? 4096 : capacity + capacity / 4;
      col->resize(new_cap);
      if (!IsChunked(*col))
        r.copied_bytes += capacity * sizeof(int64_t);  // prior buffer re-copied
      col->PrepareForInsert(high_water);
      capacity = new_cap;
    }
    col->set_any(high_water, Value::INT64(static_cast<int64_t>(high_water)),
                 false);
    ++high_water;
    if ((k + 1) % cfg.insert_ckp_every == 0) {
      // Chunked copied bytes are cumulative per live column; read them right
      // before Dump (end of this checkpoint interval), then reopen resets them.
      if (IsChunked(*col))
        r.copied_bytes += ChunkedCopiedBytes(*col);
      const uint64_t bytes0 = DirBytes(box.dir());
      const auto tc0 = Clock::now();
      CheckpointManifest meta;
      col->Dump(ckp, meta, "col");
      ckp.FinalizeObjectWriter(meta);
      r.ckp_ms +=
          std::chrono::duration<double, std::milli>(Clock::now() - tc0).count();
      r.write_bytes += DirBytes(box.dir()) - bytes0;
      // Reopen reloads the reserved capacity as frozen pages (size_ == capacity,
      // unchanged); production re-prepares the append area on load.
      col = OpenColumn(factory, ckp, meta.FindModule("col"));
      col->PrepareForInsert(high_water);
    }
  }
  if (IsChunked(*col))
    r.copied_bytes += ChunkedCopiedBytes(*col);  // tail interval
  const auto t1 = Clock::now();
  r.objects = DirFiles(box.dir()) - files0;
  r.wall_ms = std::chrono::duration<double, std::milli>(t1 - t0).count();
  r.rss_delta_mb = PeakRssMb() - rss0;
  return r;
}

TEST(ChunkBenchmark, SingleRowInsert) {
  Config cfg;
  std::printf("\n=== W6 single-row incremental insert (legacy vs chunked) ===\n");
  std::printf("inserts=%zu checkpoint_every=%zu\n\n", cfg.inserts,
              cfg.insert_ckp_every);
  PrintHeader();
  Record(RunSingleRowInsert("legacy", LegacyFactory(), cfg));
  Record(RunSingleRowInsert("chunked", ChunkedFactory(), cfg));
}

// W17: concurrent append into a prepared reserved tail after a checkpoint
// reopen. Chunked-only (see RunConcurrentAppend). Scale via NEUG_BENCH_W17_*.
TEST(ChunkBenchmark, ConcurrentAppendW17) {
  Sandbox box("w17");
  const size_t threads = EnvOr("NEUG_BENCH_W17_THREADS", 8);
  const size_t base_rows = EnvOr("NEUG_BENCH_W17_BASE", 1ull << 20);
  const size_t append_rows = EnvOr("NEUG_BENCH_W17_APPEND", 1ull << 20);
  std::printf(
      "\n=== W17 concurrent append after checkpoint (chunked only) ===\n");
  std::printf("base_rows=%zu append_rows=%zu threads=%zu\n\n", base_rows,
              append_rows, threads);
  PrintConcHeader();
  PrintConcRow(RunConcurrentAppend(box.ckp(), threads, base_rows, append_rows));
}

// W18: readers pin a Clone snapshot while one writer forks the original.
// Chunked-only (see RunReaderOnClone). Scale via NEUG_BENCH_W18_*.
TEST(ChunkBenchmark, ReaderOnCloneW18) {
  Sandbox box("w18");
  const size_t readers = EnvOr("NEUG_BENCH_W18_READERS", 4);
  const size_t rows = EnvOr("NEUG_BENCH_W18_ROWS", 1ull << 20);
  const size_t writes = EnvOr("NEUG_BENCH_W18_WRITES", 200000);
  std::printf("\n=== W18 reader-on-clone x single-writer fork (chunked only) ===\n");
  std::printf("rows=%zu readers=%zu writer=1 writes=%zu\n\n", rows, readers,
              writes);
  PrintConcHeader();
  PrintConcRow(RunReaderOnClone(box.ckp(), readers, rows, writes));
}

TEST(ChunkBenchmark, PointRead) {
  Config cfg;
  std::printf("\n=== W7 point-read latency (legacy vs chunked) ===\n");
  std::printf("rows=%zu reads=%zu\n\n", cfg.rows, cfg.reads);
  PrintReadHeader();
  for (auto& r : RunPointRead("legacy", LegacyFactory(), cfg))
    RecordRead(std::move(r));
  for (auto& r : RunPointRead("chunked", ChunkedFactory(), cfg))
    RecordRead(std::move(r));
}

TEST(ChunkBenchmark, ScanReadAmplification) {
  Config cfg;
  std::printf("\n=== W8 scan throughput + cross-object count ===\n");
  std::printf("rows=%zu updates=%zu scan_rounds=%zu\n\n", cfg.rows, cfg.updates,
              cfg.scan_rounds);
  PrintReadHeader();
  for (auto& r : RunScanReadAmplification("legacy", LegacyFactory(), cfg))
    RecordRead(std::move(r));
  for (auto& r : RunScanReadAmplification("chunked", ChunkedFactory(), cfg))
    RecordRead(std::move(r));
}

// Group 1: in-memory Clone cost vs column scale. Times live->Clone() alone:
// the page-descriptor array deep-copy plus 2*page_count shared_ptr increfs,
// which a SnapshotCowWriteTransaction pays once per write-transaction fork.
// The update_us column is the real data-page COW (one 4 KiB page) for
// contrast: at large N the descriptor copy is a fixed floor that does not
// shrink with how little actually changed.
TEST(ChunkBenchmark, PageTableCloneScaling) {
  std::printf(
      "\n=== Page-table Clone scaling (in-memory descriptor copy) ===\n");
  std::printf("| %10s | %9s | %10s | %10s | %11s | %10s |\n", "rows", "pages",
              "clone_us", "desc_KB", "ns_per_page", "update_us");
  std::printf(
      "|------------|-----------|------------|------------|-------------|"
      "------------|\n");
  for (size_t rows : ScaleSweep()) {
    Sandbox box("clonescale");
    auto& ckp = box.ckp();
    CheckpointManifest base_meta;
    BuildChunkedBase(ckp, rows, base_meta);
    const auto* desc = base_meta.FindModule("col");
    DirStats st = ReadDirStats(ckp, *desc);
    auto live = OpenColumn(ChunkedFactory(), ckp, desc);

    const auto t0 = Clock::now();
    auto clone = live->Clone();
    const double clone_us = UsSince(t0);
    // In-memory page descriptor ~40B (two shared_ptr + uint32, aligned). Peak
    // RSS cannot show this copy (the base column already saturated the peak),
    // so report the analytical descriptor bytes instead.
    const double desc_kb = st.page_count * 40.0 / 1024.0;

    auto* cow = dynamic_cast<ColumnBase*>(clone.get());
    cow->Detach(ckp, MemoryLevel::kInMemory);
    const auto t1 = Clock::now();
    cow->set_any(rows / 2, Value::INT64(1), true);  // one data page COW
    const double update_us = UsSince(t1);

    const double ns_per_page =
        st.page_count ? clone_us * 1000.0 / static_cast<double>(st.page_count)
                      : 0.0;
    std::printf("| %10zu | %9zu | %10.2f | %10.1f | %11.2f | %10.2f |\n", rows,
                st.page_count, clone_us, desc_kb, ns_per_page, update_us);
  }
}

// Group 2: on-disk directory rewrite cost vs column scale. Applies ONE
// single-value update to an N-row column, checkpoints, then splits the object
// bytes gained into the chunk-directory blob (rewritten whole, O(pages)) and
// the data object (one 4 KiB page). The dir/data ratio is the write
// amplification a paged directory (scheme A) would remove.
TEST(ChunkBenchmark, PageTableDirectoryScaling) {
  std::printf(
      "\n=== Page-table directory scaling (on-disk rewrite, one update) ===\n");
  std::printf("| %10s | %9s | %9s | %12s | %12s | %12s | %8s |\n", "rows",
              "pages", "dump_ms", "write_bytes", "dir_bytes", "data_bytes",
              "dir/data");
  std::printf(
      "|------------|-----------|-----------|--------------|--------------|"
      "--------------|----------|\n");
  for (size_t rows : ScaleSweep()) {
    Sandbox box("dirscale");
    auto& ckp = box.ckp();
    CheckpointManifest base_meta;
    BuildChunkedBase(ckp, rows, base_meta);
    const auto* desc = base_meta.FindModule("col");

    auto live = OpenColumn(ChunkedFactory(), ckp, desc);
    auto clone = live->Clone();
    auto* cow = dynamic_cast<ColumnBase*>(clone.get());
    cow->Detach(ckp, MemoryLevel::kInMemory);
    cow->set_any(rows / 2, Value::INT64(1), true);  // ONE value

    const uint64_t bytes0 = ObjectsBytes(box);
    const auto t0 = Clock::now();
    CheckpointManifest cow_meta;
    cow->Dump(ckp, cow_meta, "col");
    ckp.FinalizeObjectWriter(cow_meta);
    const double dump_ms =
        std::chrono::duration<double, std::milli>(Clock::now() - t0).count();
    const uint64_t bytes1 = ObjectsBytes(box);

    DirStats st = ReadDirStats(ckp, *cow_meta.FindModule("col"));
    const uint64_t write_bytes = bytes1 - bytes0;
    const uint64_t dir_bytes = st.blob_bytes;
    const uint64_t data_bytes =
        write_bytes > dir_bytes ? write_bytes - dir_bytes : 0;
    const double ratio =
        data_bytes ? static_cast<double>(dir_bytes) / data_bytes : 0.0;
    std::printf(
        "| %10zu | %9zu | %9.2f | %12llu | %12llu | %12llu | %8.1f |\n", rows,
        st.page_count, dump_ms, static_cast<unsigned long long>(write_bytes),
        static_cast<unsigned long long>(dir_bytes),
        static_cast<unsigned long long>(data_bytes), ratio);
  }
}

// Group 3: high-frequency small writes in a transaction cadence. Models a TP
// write workload on one N-row chunked column: txn_count write transactions,
// each = Clone the published column + Detach + one single-value update, then
// publish the clone as the new snapshot; every ckp_every transactions take a
// checkpoint (Dump + reopen, where reopen re-decodes the O(pages) directory).
// It reports steady-state throughput, the checkpoint pause, and the page-table
// share = (clone + checkpoint) / wall, i.e. the fraction of the storage write
// path that scheme A would attack. This is a storage-layer cadence model: it
// excludes WAL / version-manager / snapshot-publish, which are per-transaction
// constants orthogonal to page-table size.
TEST(ChunkBenchmark, PageTableHighFreqSmallWrite) {
  const size_t txns = EnvOr("NEUG_BENCH_TXNS", 2000);
  const size_t ckp_every = EnvOr("NEUG_BENCH_CKP_EVERY", 100);
  const size_t fanout = EnvOr("NEUG_BENCH_FANOUT", 128);
  std::printf(
      "\n=== Page-table high-frequency small writes (txn cadence) ===\n");
  std::printf("txns=%zu checkpoint_every=%zu scheme_A_fanout=%zu\n", txns,
              ckp_every, fanout);
  std::printf(
      "| %10s | %9s | %8s | %9s | %9s | %8s | %7s | %9s | %9s | %9s |\n",
      "rows", "pages", "txn/s", "clone_ms", "free_ms", "ckp_ms", "meta%",
      "ckp_pause", "A_txn/s", "A_pause");
  std::printf(
      "|------------|-----------|----------|-----------|-----------|----------|"
      "---------|-----------|-----------|-----------|\n");
  for (size_t rows : ScaleSweep()) {
    Sandbox box("hifreq");
    auto& ckp = box.ckp();
    CheckpointManifest base_meta;
    BuildChunkedBase(ckp, rows, base_meta);
    const auto* base_desc = base_meta.FindModule("col");
    DirStats st = ReadDirStats(ckp, *base_desc);

    std::unique_ptr<Module> owner =
        OpenColumn(ChunkedFactory(), ckp, base_desc);
    auto* cur = dynamic_cast<ColumnBase*>(owner.get());

    std::mt19937_64 rng(1234);
    std::uniform_int_distribution<size_t> dist(0, rows - 1);
    double clone_us = 0.0, free_us = 0.0, ckp_ms = 0.0;
    size_t dumps = 0;

    const auto t_start = Clock::now();
    for (size_t t = 0; t < txns; ++t) {
      const auto c0 = Clock::now();
      auto clone = cur->Clone();
      clone_us += UsSince(c0);
      auto* cow = dynamic_cast<ColumnBase*>(clone.get());
      cow->Detach(ckp, MemoryLevel::kInMemory);
      cow->set_any(dist(rng), Value::INT64(static_cast<int64_t>(t)), true);

      // Publish: free the previous snapshot's O(pages) descriptor array, then
      // adopt the clone. Segments survive (shared with the clone); only the
      // descriptor vector is released. This free is the mirror cost of Clone
      // and is also O(pages), so scheme A shrinks it by the same factor.
      const auto f0 = Clock::now();
      owner.reset();
      owner = std::move(clone);
      free_us += UsSince(f0);
      cur = dynamic_cast<ColumnBase*>(owner.get());

      if ((t + 1) % ckp_every == 0) {
        const auto d0 = Clock::now();
        CheckpointManifest meta;
        cur->Dump(ckp, meta, "col");
        ckp.FinalizeObjectWriter(meta);
        auto reopened =
            OpenColumn(ChunkedFactory(), ckp, meta.FindModule("col"));
        owner = std::move(reopened);
        cur = dynamic_cast<ColumnBase*>(owner.get());
        ckp_ms +=
            std::chrono::duration<double, std::milli>(Clock::now() - d0).count();
        ++dumps;
      }
    }
    const double wall_ms = std::chrono::duration<double, std::milli>(
                               Clock::now() - t_start)
                               .count();
    const double clone_ms = clone_us / 1000.0;
    const double free_ms = free_us / 1000.0;
    const double meta_ms = clone_ms + free_ms + ckp_ms;
    const double data_ms = wall_ms > meta_ms ? wall_ms - meta_ms : 0.0;
    const double meta_pct = wall_ms > 0 ? 100.0 * meta_ms / wall_ms : 0.0;
    const double txn_per_s = wall_ms > 0 ? txns / (wall_ms / 1000.0) : 0.0;
    const double ckp_pause = dumps ? ckp_ms / static_cast<double>(dumps) : 0.0;
    // Scheme-A projection: clone, free, and the directory encode/decode all
    // scale with page count, so divide them by fanout; the data-page work is
    // unchanged. Conservative, since the measured clone speedup exceeds fanout.
    const double proj_wall = data_ms + meta_ms / static_cast<double>(fanout);
    const double proj_txn_s = proj_wall > 0 ? txns / (proj_wall / 1000.0) : 0.0;
    const double proj_pause = ckp_pause / static_cast<double>(fanout);
    std::printf(
        "| %10zu | %9zu | %8.0f | %9.1f | %9.1f | %8.1f | %6.1f%% | %9.2f | "
        "%9.0f | %9.2f |\n",
        rows, st.page_count, txn_per_s, clone_ms, free_ms, ckp_ms, meta_pct,
        ckp_pause, proj_txn_s, proj_pause);
  }
}

// Group 4: scheme-A mock. Scheme A stores the page table as a chunked COW
// column of page descriptors, so its Clone and directory scale with the number
// of descriptor pages = data_pages / fanout, not data_pages. A real
// ChunkedColumn<int64> sized to N/fanout rows has exactly that page count, the
// same 40B Page struct and the same 24B directory entry, so its measured Clone
// and directory blob are a faithful stand-in for scheme A's descriptor column,
// residual flat directory included. fanout = descriptors per 4 KiB page, a
// power of two: 128 for a 24B descriptor, 64 for a wider 40-52B one.
TEST(ChunkBenchmark, PageTableSchemeAMock) {
  const size_t fanout = EnvOr("NEUG_BENCH_FANOUT", 128);
  std::printf("\n=== Scheme-A mock (page table as a descriptor column) ===\n");
  std::printf("fanout=%zu descriptors per 4KiB page\n", fanout);
  std::printf(
      "| %10s | %9s | %12s | %12s | %8s | %11s | %11s | %8s |\n", "rows",
      "data_pgs", "cur_clone_us", "mock_clone_us", "cl_spd", "cur_dir_KB",
      "mock_dir_KB", "dir_spd");
  std::printf(
      "|------------|-----------|--------------|--------------|----------|"
      "-------------|-------------|----------|\n");
  for (size_t rows : ScaleSweep()) {
    // Current: a real N-row data column, page table copied whole.
    Sandbox box_cur("mock_cur");
    CheckpointManifest meta_cur;
    BuildChunkedBase(box_cur.ckp(), rows, meta_cur);
    const auto* desc_cur = meta_cur.FindModule("col");
    DirStats st_cur = ReadDirStats(box_cur.ckp(), *desc_cur);
    auto live_cur = OpenColumn(ChunkedFactory(), box_cur.ckp(), desc_cur);
    const auto c0 = Clock::now();
    auto clone_cur = live_cur->Clone();
    const double cur_clone_us = UsSince(c0);
    const double cur_dir_kb = st_cur.blob_bytes / 1024.0;

    // Mock: scheme A's descriptor column has data_pages/fanout pages, which is
    // the page count of an N/fanout-row int64 column.
    const size_t mock_rows = std::max<size_t>(512, rows / fanout);
    Sandbox box_mock("mock_a");
    CheckpointManifest meta_mock;
    BuildChunkedBase(box_mock.ckp(), mock_rows, meta_mock);
    const auto* desc_mock = meta_mock.FindModule("col");
    DirStats st_mock = ReadDirStats(box_mock.ckp(), *desc_mock);
    auto live_mock = OpenColumn(ChunkedFactory(), box_mock.ckp(), desc_mock);
    const auto c1 = Clock::now();
    auto clone_mock = live_mock->Clone();
    const double mock_clone_us = UsSince(c1);
    const double mock_dir_kb = st_mock.blob_bytes / 1024.0;

    const double cl_spd =
        mock_clone_us > 0 ? cur_clone_us / mock_clone_us : 0.0;
    const double dir_spd = mock_dir_kb > 0 ? cur_dir_kb / mock_dir_kb : 0.0;
    std::printf(
        "| %10zu | %9zu | %12.2f | %12.2f | %7.0fx | %11.1f | %11.1f | %7.0fx "
        "|\n",
        rows, st_cur.page_count, cur_clone_us, mock_clone_us, cl_spd,
        cur_dir_kb, mock_dir_kb, dir_spd);
  }
}

}  // namespace neug
