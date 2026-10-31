/** Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// End-to-end write benchmark for the chunked-COW storage optimization.
//
// This program deliberately depends ONLY on the public NeugDB API
// (Open / Connect / Query / Close), so the exact same source file compiles and
// runs unchanged on two build trees:
//   * the legacy baseline (property columns backed by TypedColumn<T>), and
//   * the optimized tree   (property columns backed by ChunkedColumn<T>).
// Running it on both and diffing the printed table gives an honest A/B of the
// storage layout at the whole-database level, with zero production-code
// instrumentation.
//
// Signals reported per phase:
//   * wall_ms       – wall time of the write workload itself
//   * p50/p99/max_us– per-operation latency distribution (write ops only)
//   * ckpt_ms       – stall of the closing checkpoint (checkpoint_on_close)
//   * wr_bytes      – bytes written by THIS checkpoint (new object files only;
//                     reused immutable chunk objects are not counted). This is
//                     the end-to-end write-amplification signal: the legacy
//                     layout re-dumps whole column buffers, the chunked layout
//                     only re-dumps the chunks that actually changed.
//   * new_objs      – number of new object files written by this checkpoint
//   * ckpt_bytes    – total live checkpoint footprint after the phase
//   * live_objs     – object files retained after the closing GC, i.e. exactly
//                     the distinct objects the live checkpoint references.
//                     Grows across rounds for the chunked layout (clean slices
//                     reuse old objects, so one live checkpoint spans multiple
//                     generations) while legacy stays flat (whole-column rewrite
//                     lets GC reclaim the prior object). This is the space face
//                     of the cross-object-count growth, reclaimed by P2 merge.
//   * rss_mb        – peak resident set size of the process so far
//
// NOTE ON LATENCY: each write is issued as its own implicit transaction with a
// distinct query string, so per-op latency includes parse/plan cost. That cost
// is common-mode across the two trees and cancels out in the A/B diff; the
// cleanest layout signals are ckpt_ms / wr_bytes / rss_mb.

#include <glog/logging.h>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <map>
#include <random>
#include <string>
#include <vector>

#if defined(__APPLE__) || defined(__unix__)
#include <sys/resource.h>
#include <sys/time.h>
#define NEUG_E2E_HAVE_RUSAGE 1
#endif

#include "neug/neug.h"

namespace {

namespace fs = std::filesystem;
using Clock = std::chrono::steady_clock;

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------
struct Config {
  std::string data_dir = "/tmp/neug_chunk_e2e";
  std::string csv_path;             // generated bulk-load input (default derived)
  int64_t rows = 1000000;           // bulk-loaded vertex count
  int rounds = 3;                   // update+checkpoint rounds after the load
  int64_t updates = 20000;          // rows updated per round
  // Phase-2 workload:
  //   update - point|range SET (row count constant)
  //   delete - W10: delete a disjoint batch each round (count shrinks)
  //   churn  - W11: delete a batch + insert an equal batch (count ~constant)
  //   oltp   - W12: mixed point-read / point-update / insert (count grows)
  std::string workload = "update";
  std::string pattern = "point";    // "point" | "range" (update workload only)
  int64_t range_block = 4096;       // rows per range-update statement
  int threads = 4;
  bool keep = false;                // keep data_dir/csv on exit
  bool skip_load = false;           // reuse an existing data_dir (no bulk load)
};

void PrintUsage(const char* prog) {
  std::printf(
      "Usage: %s [options]\n"
      "  --data-dir PATH    database directory (default /tmp/neug_chunk_e2e)\n"
      "  --csv PATH         bulk-load CSV path (default <data_dir>_items.csv)\n"
      "  --rows N           vertices to bulk load (default 1000000)\n"
      "  --rounds N         update+checkpoint rounds (default 3)\n"
      "  --updates N        rows updated per round (default 20000)\n"
      "  --workload W       update | delete | churn | oltp (default update)\n"
      "  --pattern P        point | range (update workload only, default point)\n"
      "  --range-block N    rows per range statement (default 4096)\n"
      "  --threads N        db query capacity (default 4)\n"
      "  --skip-load        reuse existing data_dir, skip bulk load\n"
      "  --keep             do not delete data_dir/csv on exit\n"
      "  --help             show this message\n",
      prog);
}

Config ParseArgs(int argc, char** argv) {
  Config c;
  for (int i = 1; i < argc; ++i) {
    std::string a = argv[i];
    auto next = [&](const char* name) -> std::string {
      CHECK_LT(i + 1, argc) << "missing value for " << name;
      return argv[++i];
    };
    if (a == "--data-dir") {
      c.data_dir = next("--data-dir");
    } else if (a == "--csv") {
      c.csv_path = next("--csv");
    } else if (a == "--rows") {
      c.rows = std::atoll(next("--rows").c_str());
    } else if (a == "--rounds") {
      c.rounds = std::atoi(next("--rounds").c_str());
    } else if (a == "--updates") {
      c.updates = std::atoll(next("--updates").c_str());
    } else if (a == "--workload") {
      c.workload = next("--workload");
    } else if (a == "--pattern") {
      c.pattern = next("--pattern");
    } else if (a == "--range-block") {
      c.range_block = std::atoll(next("--range-block").c_str());
    } else if (a == "--threads") {
      c.threads = std::atoi(next("--threads").c_str());
    } else if (a == "--skip-load") {
      c.skip_load = true;
    } else if (a == "--keep") {
      c.keep = true;
    } else if (a == "--help") {
      PrintUsage(argv[0]);
      std::exit(0);
    } else {
      LOG(ERROR) << "unknown argument: " << a;
      PrintUsage(argv[0]);
      std::exit(2);
    }
  }
  CHECK(c.pattern == "point" || c.pattern == "range")
      << "--pattern must be point|range";
  CHECK(c.workload == "update" || c.workload == "delete" ||
        c.workload == "churn" || c.workload == "oltp")
      << "--workload must be update|delete|churn|oltp";
  CHECK_GT(c.rows, 0);
  CHECK_GT(c.range_block, 0);
  if (c.csv_path.empty()) c.csv_path = c.data_dir + "_items.csv";
  return c;
}

// ---------------------------------------------------------------------------
// Small utilities
// ---------------------------------------------------------------------------
double MsSince(Clock::time_point start) {
  return std::chrono::duration<double, std::milli>(Clock::now() - start)
      .count();
}

double PeakRssMB() {
#if defined(NEUG_E2E_HAVE_RUSAGE)
  struct rusage ru {};
  if (getrusage(RUSAGE_SELF, &ru) != 0) return 0.0;
  // ru_maxrss is bytes on macOS, kilobytes on Linux.
#if defined(__APPLE__)
  return static_cast<double>(ru.ru_maxrss) / (1024.0 * 1024.0);
#else
  return static_cast<double>(ru.ru_maxrss) / 1024.0;
#endif
#else
  return 0.0;
#endif
}

struct Latency {
  double p50_us = 0, p99_us = 0, max_us = 0, mean_us = 0;
};

Latency Summarize(std::vector<double>& samples_us) {
  Latency out;
  if (samples_us.empty()) return out;
  std::sort(samples_us.begin(), samples_us.end());
  const size_t n = samples_us.size();
  auto pick = [&](double q) {
    size_t idx = static_cast<size_t>(q * static_cast<double>(n - 1));
    return samples_us[idx];
  };
  out.p50_us = pick(0.50);
  out.p99_us = pick(0.99);
  out.max_us = samples_us.back();
  double sum = 0;
  for (double v : samples_us) sum += v;
  out.mean_us = sum / static_cast<double>(n);
  return out;
}

uint64_t DirBytes(const fs::path& dir) {
  uint64_t total = 0;
  std::error_code ec;
  if (!fs::exists(dir, ec)) return 0;
  for (auto it = fs::recursive_directory_iterator(dir, ec);
       !ec && it != fs::recursive_directory_iterator(); it.increment(ec)) {
    if (it->is_regular_file(ec)) total += it->file_size(ec);
  }
  return total;
}

// filename -> size snapshot of the immutable checkpoint object store.
std::map<std::string, uint64_t> SnapshotObjects(const fs::path& objects_dir) {
  std::map<std::string, uint64_t> snap;
  std::error_code ec;
  if (!fs::exists(objects_dir, ec)) return snap;
  for (auto it = fs::directory_iterator(objects_dir, ec);
       !ec && it != fs::directory_iterator(); it.increment(ec)) {
    if (it->is_regular_file(ec)) {
      snap[it->path().filename().string()] = it->file_size(ec);
    }
  }
  return snap;
}

struct WriteAmp {
  uint64_t bytes = 0;
  size_t new_objects = 0;
};

// Bytes/objects that appear (or change size) in `after` but not in `before`.
// Reused immutable chunk objects keep their filename and are not counted, so
// this isolates what a single checkpoint actually wrote to disk.
WriteAmp DiffObjects(const std::map<std::string, uint64_t>& before,
                     const std::map<std::string, uint64_t>& after) {
  WriteAmp wa;
  for (const auto& [name, size] : after) {
    auto found = before.find(name);
    if (found == before.end() || found->second != size) {
      wa.bytes += size;
      ++wa.new_objects;
    }
  }
  return wa;
}

void WriteCsv(const fs::path& path, int64_t rows) {
  std::ofstream out(path);
  CHECK(out) << "cannot open csv for writing: " << path;
  out << "id,val,score\n";
  std::string line;
  line.reserve(48);
  for (int64_t i = 0; i < rows; ++i) {
    line.clear();
    line += std::to_string(i);
    line += ',';
    line += std::to_string(i * 3 + 1);
    line += ',';
    line += std::to_string(static_cast<double>(i) * 0.5);
    line += '\n';
    out << line;
  }
  out.flush();
  CHECK(out) << "failed writing csv: " << path;
}

// ---------------------------------------------------------------------------
// Result table
// ---------------------------------------------------------------------------
struct Row {
  std::string phase;
  int64_t ops = 0;
  double wall_ms = 0;
  double p50_us = 0, p99_us = 0, max_us = 0;
  double ckpt_ms = 0;
  uint64_t wr_bytes = 0;
  size_t new_objs = 0;
  uint64_t ckpt_bytes = 0;
  size_t live_objs = 0;  // objects retained after GC = referenced by live ckpt
  double rss_mb = 0;
};

void PrintTableHeader() {
  std::printf(
      "\n%-16s %10s %10s %9s %9s %9s %14s %9s %14s %10s %9s\n", "phase",
      "ops", "wall_ms", "p50_us", "p99_us", "max_us", "wr_bytes", "new_objs",
      "ckpt_bytes", "live_objs", "rss_mb");
  std::printf("%s\n", std::string(131, '-').c_str());
}

void PrintRow(const Row& r) {
  std::printf(
      "%-16s %10lld %10.2f %9.1f %9.1f %9.1f %14llu %9zu %14llu %10zu %9.1f\n",
      r.phase.c_str(), static_cast<long long>(r.ops), r.wall_ms, r.p50_us,
      r.p99_us, r.max_us, static_cast<unsigned long long>(r.wr_bytes),
      r.new_objs, static_cast<unsigned long long>(r.ckpt_bytes), r.live_objs,
      r.rss_mb);
}

// Open a fresh read-write handle on the data dir with checkpoint-on-close.
std::unique_ptr<neug::NeugDB> OpenRW(const Config& cfg) {
  auto db = std::make_unique<neug::NeugDB>();
  bool ok = db->Open(cfg.data_dir, cfg.threads, neug::DBMode::READ_WRITE,
                     "gopt", /*checkpoint_on_close=*/true);
  CHECK(ok) << "failed to open database at " << cfg.data_dir;
  return db;
}

int64_t CountItems(neug::Connection& conn) {
  auto r = conn.Query("MATCH (n:Item) RETURN count(n);", "read");
  CHECK(r) << "count query failed: " << r.error().ToString();
  auto& qr = r.value();
  if (!qr.hasNext()) return -1;
  return qr.GetInt64(0);
}

}  // namespace

int main(int argc, char** argv) {
  Config cfg = ParseArgs(argc, argv);
  const fs::path data_dir = cfg.data_dir;
  const fs::path objects_dir = data_dir / "checkpoint" / "objects";
  const fs::path csv_path = cfg.csv_path;

  std::printf("chunk_e2e_benchmark config:\n");
  std::printf("  data_dir    = %s\n", cfg.data_dir.c_str());
  std::printf("  rows        = %lld\n", static_cast<long long>(cfg.rows));
  std::printf("  rounds      = %d\n", cfg.rounds);
  std::printf("  updates     = %lld rows/round\n",
              static_cast<long long>(cfg.updates));
  std::printf("  workload    = %s\n", cfg.workload.c_str());
  std::printf("  pattern     = %s (range_block=%lld)\n", cfg.pattern.c_str(),
              static_cast<long long>(cfg.range_block));
  std::printf("  threads     = %d\n", cfg.threads);
  std::printf("  version     = %s\n", neug::NeugDB().Version());

  std::vector<Row> rows;
  std::mt19937_64 rng(0xC0FFEEULL);  // fixed seed: identical ops on both trees

  // -------------------------------------------------------------------------
  // Phase 1: bulk load (schema + COPY) into a fresh database, then checkpoint.
  // -------------------------------------------------------------------------
  if (!cfg.skip_load) {
    std::error_code ec;
    fs::remove_all(data_dir, ec);
    fs::create_directories(data_dir, ec);
    LOG(INFO) << "generating bulk-load CSV: " << csv_path << " (" << cfg.rows
              << " rows)";
    auto csv_t0 = Clock::now();
    WriteCsv(csv_path, cfg.rows);
    const double csv_ms = MsSince(csv_t0);
    std::printf("  csv_gen_ms  = %.1f\n", csv_ms);

    auto db = OpenRW(cfg);
    auto conn = db->Connect();

    auto t0 = Clock::now();
    auto ddl = conn->Query(
        "CREATE NODE TABLE Item(id INT64, val INT64, score DOUBLE, "
        "PRIMARY KEY(id));",
        "schema");
    CHECK(ddl) << "DDL failed: " << ddl.error().ToString();
    auto copy = conn->Query("COPY Item FROM \"" + csv_path.string() +
                                "\" (HEADER=true, DELIMITER=',');",
                            "update");
    CHECK(copy) << "COPY failed: " << copy.error().ToString();
    const double load_ms = MsSince(t0);

    const int64_t loaded = CountItems(*conn);
    CHECK_EQ(loaded, cfg.rows) << "bulk load row count mismatch";
    conn->Close();

    // The closing checkpoint persists the freshly loaded columns.
    auto before = SnapshotObjects(objects_dir);
    auto tc = Clock::now();
    db->Close();
    const double ckpt_ms = MsSince(tc);
    auto after = SnapshotObjects(objects_dir);
    WriteAmp wa = DiffObjects(before, after);

    Row r;
    r.phase = "load";
    r.ops = cfg.rows;
    r.wall_ms = load_ms;
    r.ckpt_ms = ckpt_ms;
    r.wr_bytes = wa.bytes;
    r.new_objs = wa.new_objects;
    r.ckpt_bytes = DirBytes(objects_dir);
    r.live_objs = after.size();
    r.rss_mb = PeakRssMB();
    rows.push_back(r);
  } else {
    LOG(INFO) << "--skip-load: reusing existing database at " << cfg.data_dir;
  }

  // -------------------------------------------------------------------------
  // Phase 2: repeated (reopen -> workload -> checkpoint) rounds. Each round's
  // checkpoint is the incremental write-amplification / space measurement. The
  // --workload selector picks the phase-2 body:
  //   update - point|range SET; row count constant.
  //   delete - W10: remove a disjoint batch each round; count shrinks. This is
  //            the space curve the disk report deferred to W10 -- deleted rows
  //            leave partially-valid chunk objects that no merge reclaims, so
  //            ckpt_bytes / live_objs show how the footprint decays (or not).
  //   churn  - W11: delete a batch and insert an equal batch of fresh ids each
  //            round; count ~constant, models OLTP turnover (delete + append).
  //   oltp   - W12: mixed point-read / point-update / insert each round; count
  //            grows slowly, latency spans reads and writes.
  // All workloads run on ONE connection (a READ_WRITE db permits a single
  // connection), so W17/W18 concurrency is measured at the storage layer in
  // bench_chunked_column, not here.
  // -------------------------------------------------------------------------
  int64_t expected = cfg.rows;         // live row count tracked across rounds
  int64_t next_insert_id = cfg.rows;   // fresh ids for churn/oltp inserts
  std::vector<int64_t> delete_pool;    // shuffled existing ids for delete/churn
  size_t delete_cursor = 0;
  if (cfg.workload == "delete" || cfg.workload == "churn") {
    delete_pool.resize(static_cast<size_t>(cfg.rows));
    for (int64_t i = 0; i < cfg.rows; ++i)
      delete_pool[static_cast<size_t>(i)] = i;
    std::shuffle(delete_pool.begin(), delete_pool.end(), rng);
  }

  for (int round = 1; round <= cfg.rounds; ++round) {
    auto db = OpenRW(cfg);
    auto conn = db->Connect();

    auto before = SnapshotObjects(objects_dir);
    std::vector<double> lat_us;
    lat_us.reserve(static_cast<size_t>(cfg.updates));
    // Time one statement and record its latency; fail loudly on any error.
    auto timed = [&](const std::string& q, const char* mode) {
      auto s = Clock::now();
      auto res = conn->Query(q, mode);
      lat_us.push_back(
          std::chrono::duration<double, std::micro>(Clock::now() - s).count());
      CHECK(res) << "query failed [" << mode << "]: " << q << " : "
                 << res.error().ToString();
    };

    auto t0 = Clock::now();
    int64_t ops = 0;
    if (cfg.workload == "delete") {
      for (int64_t i = 0;
           i < cfg.updates && delete_cursor < delete_pool.size();
           ++i, ++delete_cursor) {
        timed("MATCH (n:Item {id: " +
                  std::to_string(delete_pool[delete_cursor]) + "}) DELETE n;",
              "update");
        --expected;
        ++ops;
      }
    } else if (cfg.workload == "churn") {
      const int64_t half = cfg.updates / 2;
      for (int64_t i = 0; i < half && delete_cursor < delete_pool.size();
           ++i, ++delete_cursor) {
        timed("MATCH (n:Item {id: " +
                  std::to_string(delete_pool[delete_cursor]) + "}) DELETE n;",
              "update");
        --expected;
        ++ops;
      }
      for (int64_t i = half; i < cfg.updates; ++i) {
        const int64_t id = next_insert_id++;
        timed("CREATE (n:Item {id: " + std::to_string(id) + ", val: " +
                  std::to_string(id * 3 + 1) + ", score: " +
                  std::to_string(static_cast<double>(id) * 0.5) + "});",
              "insert");
        ++expected;
        ++ops;
      }
    } else if (cfg.workload == "oltp") {
      for (int64_t i = 0; i < cfg.updates; ++i) {
        const int64_t roll = static_cast<int64_t>(rng() % 100);
        if (roll < 70) {  // 70% point reads
          const int64_t id =
              static_cast<int64_t>(rng() % static_cast<uint64_t>(expected));
          timed("MATCH (n:Item {id: " + std::to_string(id) +
                    "}) RETURN n.val;",
                "read");
        } else if (roll < 95) {  // 25% point updates
          const int64_t id =
              static_cast<int64_t>(rng() % static_cast<uint64_t>(expected));
          timed("MATCH (n:Item {id: " + std::to_string(id) +
                    "}) SET n.val = " +
                    std::to_string(static_cast<int64_t>(rng())) + ";",
                "update");
        } else {  // 5% inserts of fresh ids
          const int64_t id = next_insert_id++;
          timed("CREATE (n:Item {id: " + std::to_string(id) + ", val: " +
                    std::to_string(id * 3 + 1) + ", score: " +
                    std::to_string(static_cast<double>(id) * 0.5) + "});",
                "insert");
          ++expected;
        }
        ++ops;
      }
    } else if (cfg.pattern == "point") {
      for (int64_t i = 0; i < cfg.updates; ++i) {
        const int64_t id =
            static_cast<int64_t>(rng() % static_cast<uint64_t>(cfg.rows));
        const int64_t val = static_cast<int64_t>(rng());
        timed("MATCH (n:Item {id: " + std::to_string(id) +
                  "}) SET n.val = " + std::to_string(val) + ";",
              "update");
        ++ops;
      }
    } else {  // update + range: one contiguous window in range_block slices.
      // A localized (dense) update touches only the few chunks covering the
      // window, which is where the chunked layout's write-amplification
      // advantage over whole-buffer legacy dumps is largest.
      const int64_t span = std::max<int64_t>(1, cfg.rows - cfg.updates);
      const int64_t window_start =
          static_cast<int64_t>(rng() % static_cast<uint64_t>(span));
      int64_t done = 0;
      while (done < cfg.updates) {
        const int64_t start = window_start + done;
        const int64_t end = std::min(cfg.rows, start + cfg.range_block);
        const int64_t val = static_cast<int64_t>(rng());
        timed("MATCH (n:Item) WHERE n.id >= " + std::to_string(start) +
                  " AND n.id < " + std::to_string(end) + " SET n.val = " +
                  std::to_string(val) + ";",
              "update");
        ++ops;
        done += (end - start);
      }
    }
    const double wall_ms = MsSince(t0);

    // Correctness guard: the live count must match the tracked expectation
    // (constant for update, shrinking for delete, ~constant for churn, growing
    // for oltp) after every reopen cycle.
    const int64_t cnt = CountItems(*conn);
    CHECK_EQ(cnt, expected) << "row count mismatch after round " << round;
    conn->Close();

    auto tc = Clock::now();
    db->Close();
    const double ckpt_ms = MsSince(tc);
    auto after = SnapshotObjects(objects_dir);
    WriteAmp wa = DiffObjects(before, after);
    Latency lat = Summarize(lat_us);

    Row r;
    r.phase = (cfg.workload == "update" ? ("update_" + cfg.pattern)
                                        : cfg.workload) +
              "#" + std::to_string(round);
    r.ops = ops;
    r.wall_ms = wall_ms;
    r.p50_us = lat.p50_us;
    r.p99_us = lat.p99_us;
    r.max_us = lat.max_us;
    r.ckpt_ms = ckpt_ms;
    r.wr_bytes = wa.bytes;
    r.new_objs = wa.new_objects;
    r.ckpt_bytes = DirBytes(objects_dir);
    r.live_objs = after.size();
    r.rss_mb = PeakRssMB();
    rows.push_back(r);
  }

  // -------------------------------------------------------------------------
  // Report
  // -------------------------------------------------------------------------
  PrintTableHeader();
  for (const Row& r : rows) PrintRow(r);

  if (!cfg.keep) {
    std::error_code ec;
    fs::remove_all(data_dir, ec);
    fs::remove(csv_path, ec);
  } else {
    std::printf("\n(--keep) left data_dir=%s csv=%s\n", cfg.data_dir.c_str(),
                cfg.csv_path.c_str());
  }
  return 0;
}
