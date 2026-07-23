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

#include "neug/main/neug_db.h"

#include <glog/logging.h>
#include <unistd.h>
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <exception>
#include <filesystem>
#include <limits>
#include <sstream>
#include <utility>
#include <vector>

#include "neug/compiler/planner/gopt_planner.h"
#include "neug/compiler/planner/graph_planner.h"
#include "neug/execution/execute/plan_parser.h"
#include "neug/execution/execute/query_cache.h"
#include "neug/main/checkpoint_coordinator.h"
#include "neug/main/connection_manager.h"
#include "neug/main/file_lock.h"
#include "neug/main/query_processor.h"
#include "neug/storages/allocators.h"
#include "neug/storages/checkpoint.h"
#include "neug/storages/checkpoint_manager.h"
#include "neug/storages/checkpoint_manifest.h"
#include "neug/storages/graph/schema.h"
#include "neug/transaction/compact_transaction.h"
#include "neug/transaction/wal/wal.h"
#include "neug/utils/exception/exception.h"
#include "neug/utils/io/file/file_utils.h"
#include "neug/utils/result.h"

namespace neug {

class Connection;
static void IngestWalRange(
    PropertyGraph& graph,
    const std::vector<std::shared_ptr<Allocator>>& allocators,
    const IWalParser& parser, uint32_t from, uint32_t to) {
  if (from >= to) {
    return;
  }
  // Build a single writable GraphView covering the whole replay range.
  // read_ts = MAX_TIMESTAMP so vertices inserted earlier in the loop are
  // visible to later edge-resolution lookups regardless of the per-unit
  // commit timestamp.
  GraphView view(graph);
  for (size_t j = from; j < to; ++j) {
    const auto& unit = parser.get_insert_wal(j);
    InsertTransaction::IngestWal(view, j, unit.ptr, unit.size,
                                 *allocators.at(0));
    if (j % 1000000 == 0) {
      LOG(INFO) << "Ingested " << j << " WALs";
    }
  }
}

NeugDB::NeugDB() : closed_(true), is_pure_memory_(false), max_thread_num_(1) {}

NeugDB::~NeugDB() {
  try {
    Close();
  } catch (const std::exception& e) {
    LOG(ERROR) << "Checkpoint failed while destroying NeugDB: " << e.what();
  } catch (...) { LOG(ERROR) << "Checkpoint failed while destroying NeugDB"; }
  WalWriterFactory::Finalize();
  WalParserFactory::Finalize();
  // We put the removal of temp dir here to avoid the situation that
  //  starting tp service with database opened in memory mode. In this case,
  //  pydatabase will call close and then reopen, so we need to keep the temp
  //  dir until the db is destructed.
  try {
    if (is_pure_memory_) {
      VLOG(10) << "Removing temp NeugDB at: " << config_.data_dir;
      remove_directory(config_.data_dir);
    }
  } catch (const std::exception& e) {
    LOG(WARNING) << "Failed to remove temp dir for " << work_dir() << ": "
                 << e.what();
  } catch (...) {
    LOG(WARNING) << "Failed to remove temp dir for " << work_dir();
  }
}

bool NeugDB::Open(const std::string& data_dir, int32_t max_thread_num,
                  const DBMode mode, const std::string& planner_kind,
                  bool checkpoint_on_close) {
  NeugDBConfig config(data_dir, max_thread_num);
  config.mode = mode;
  config.planner_kind = planner_kind;
  config.checkpoint_on_close = checkpoint_on_close;
  return Open(config);
}

bool NeugDB::Open(const NeugDBConfig& config) {
  if (!closed_.load(std::memory_order_acquire)) {
    THROW_RUNTIME_ERROR("NeugDB instance is already open.");
  }
  config_ = config;
  preprocessConfig();
  config_.data_dir = std::filesystem::absolute(config_.data_dir).string();
  const bool recover_workspace =
      config_.mode == DBMode::READ_WRITE || is_pure_memory_;
  if (recover_workspace) {
    std::filesystem::create_directories(config_.data_dir);
  } else if (!std::filesystem::is_directory(config_.data_dir)) {
    THROW_NO_CHECKPOINT_EXCEPTION(
        "NeugDB::Open: no checkpoint found in read-only database: " +
        config_.data_dir);
  }

  file_lock_ = std::make_unique<FileLock>(config_.data_dir);

  std::string error_msg;
  if (!file_lock_->lock(error_msg, config.mode)) {
    THROW_DATABASE_LOCKED_EXCEPTION("Failed to lock data directory: " +
                                    config_.data_dir + ", error: " + error_msg);
  }
  try {
    checkpoint_mgr_.Open(config_.data_dir, recover_workspace);
    VLOG(1) << "Opening NeuGDB at " << checkpoint_mgr_.db_dir();
    neug::execution::PlanParser::get().init();
    const auto recovered_wal_timestamp = openGraphAndIngestWals();
    checkpoint_coordinator_ = std::make_unique<CheckpointCoordinator>(
        checkpoint_mgr_, *snapshot_store_, config_.memory_level,
        recovered_wal_timestamp, [this](const std::string& allocator_dir) {
          reopenAllocators(allocator_dir);
        });
    if (recovered_wal_timestamp > 0 && config.checkpoint_on_recovery &&
        config_.mode == DBMode::READ_WRITE) {
      LOG(INFO) << "Creating checkpoint after recovery at ts "
                << recovered_wal_timestamp;
      createCheckpointAfterRecovery();
    }
    if (config_.mode == DBMode::READ_WRITE) {
      checkpoint_mgr_.CleanupRetiredCheckpoints();
    }
    initPlannerAndQueryProcessor();
  } catch (...) {
    checkpoint_coordinator_.reset();
    snapshot_store_.reset();
    allocators_.clear();
    checkpoint_mgr_.Close();
    if (file_lock_) {
      file_lock_->unlock();
      file_lock_.reset();
    }
    throw;
  }

  LOG(INFO) << "NeugDB opened successfully";
  closed_.store(false, std::memory_order_release);
  return true;
}

void NeugDB::Close() {
  if (closed_.exchange(true, std::memory_order_acq_rel)) {
    return;
  }
  if (connection_manager_) {
    connection_manager_->Close();
    connection_manager_.reset();
  }

  if (query_processor_) {
    query_processor_.reset();
  }
  if (global_query_cache_) {
    global_query_cache_.reset();
  }
  if (planner_) {
    planner_.reset();
  }

  if (config_.checkpoint_on_close && config_.mode == DBMode::READ_WRITE) {
    VLOG(1) << "Creating checkpoint on close...";
    try {
      createCheckpointOnClose();
    } catch (const std::exception& e) {
      LOG(ERROR) << "Checkpoint on close failed: " << e.what();
    } catch (...) { LOG(ERROR) << "Checkpoint on close failed"; }
  }

  // Clear GraphSnapshotStore instead of graph_
  checkpoint_coordinator_.reset();
  snapshot_store_.reset();
  allocators_.clear();
  checkpoint_mgr_.Close();

  if (file_lock_) {
    file_lock_->unlock();
    file_lock_.reset();
  }
}

std::shared_ptr<Connection> NeugDB::Connect() {
  return connection_manager_->CreateConnection();
}

CheckpointCoordinator& NeugDB::checkpoint_coordinator() {
  return *checkpoint_coordinator_;
}

const CheckpointCoordinator& NeugDB::checkpoint_coordinator() const {
  return *checkpoint_coordinator_;
}

void NeugDB::RemoveConnection(std::shared_ptr<Connection> conn) {
  connection_manager_->RemoveConnection(conn);
}

void NeugDB::CloseAllConnection() { connection_manager_->Close(); }

void NeugDB::PrepareForServing() {
  if (IsClosed()) {
    THROW_RUNTIME_ERROR("NeugDB instance is not ready for serving!");
  }
  CloseAllConnection();
  if (config_.mode == DBMode::READ_WRITE) {
    createCheckpointAfterRecovery();
  }
  initQueryRuntime();
}

void NeugDB::preprocessConfig() {
  if (config_.max_thread_num < 0) {
    THROW_INVALID_ARGUMENT_EXCEPTION(
        "Invalid max_thread_num: " + std::to_string(config_.max_thread_num) +
        ". Must be a non-negative integer.");
  }
  if (config_.max_thread_num == 0) {
    config_.max_thread_num =
        static_cast<int>(std::thread::hardware_concurrency());
    if (config_.max_thread_num == 0) {
      config_.max_thread_num = 1;
    }
  }
  auto db_dir = config_.data_dir;
  if (db_dir.empty() || db_dir == ":memory" || db_dir == ":memory:") {
    std::string db_dir_prefix;
    char* prefix_env = std::getenv("NEUG_DB_TMP_DIR");
    if (prefix_env) {
      db_dir_prefix = std::string(prefix_env);
    } else {
      db_dir_prefix = "/tmp";
    }
    std::stringstream ss;
    auto now = std::chrono::system_clock::now();
    auto duration = now.time_since_epoch();
    ss << "neug_db_"
       << std::chrono::duration_cast<std::chrono::microseconds>(duration)
              .count();
    db_dir = db_dir_prefix + "/" + ss.str();
    is_pure_memory_ = true;
    LOG(INFO) << "Creating temp NeugDB with: " << db_dir << " in "
              << config_.mode << " mode";
    config_.data_dir = db_dir;
  } else {
    is_pure_memory_ = false;
    LOG(INFO) << "Creating NeugDB with: " << db_dir << " in " << config_.mode
              << " mode";
  }
}

void NeugDB::initAllocators(const std::string& allocator_dir) {
  assert(config_.max_thread_num > 0);
  allocators_.clear();
  remove_directory(allocator_dir);
  std::filesystem::create_directories(allocator_dir);
  allocators_.reserve(static_cast<size_t>(config_.max_thread_num));
  for (int32_t i = 0; i < config_.max_thread_num; ++i) {
    allocators_.emplace_back(std::make_shared<Allocator>(
        config_.memory_level,
        config_.memory_level == MemoryLevel::kSyncToFile
            ? allocator_prefix(allocator_dir, static_cast<size_t>(i))
            : ""));
  }
}

void NeugDB::reopenAllocators(const std::string& allocator_dir) {
  // Precompute every prefix before touching live allocator state: a
  // mid-rotation allocation failure would otherwise leave earlier allocators
  // on the new generation and later ones on the retired one.
  std::vector<std::string> prefixes;
  prefixes.reserve(allocators_.size());
  for (size_t i = 0; i < allocators_.size(); ++i) {
    prefixes.emplace_back(config_.memory_level == MemoryLevel::kSyncToFile
                              ? allocator_prefix(allocator_dir, i)
                              : "");
  }
  for (size_t i = 0; i < allocators_.size(); ++i) {
    allocators_[i]->Reopen(config_.memory_level, std::move(prefixes[i]));
  }
}

timestamp_t NeugDB::openGraphAndIngestWals() {
  max_thread_num_ = config_.max_thread_num;
  timestamp_t recovered_wal_timestamp = 0;
  try {
    auto ckp = checkpoint_mgr_.CurrentCheckpoint();
    if (ckp == nullptr) {
      if (config_.mode == DBMode::READ_ONLY && !is_pure_memory_) {
        THROW_NO_CHECKPOINT_EXCEPTION(
            "NeugDB::Open: no checkpoint found in read-only database: " +
            checkpoint_mgr_.db_dir());
      }
      auto staging_checkpoint = checkpoint_mgr_.CreateStagingCheckpoint();
      ckp = staging_checkpoint.checkpoint();
      CheckpointManifest meta;
      meta.SetSchema(Schema());
      ckp->UpdateMeta(std::move(meta));
      ckp = staging_checkpoint.Commit();
      LOG(INFO) << "No checkpoint found, created initial checkpoint: "
                << ckp->path();
    }
    LOG(INFO) << "Opening graph from checkpoint " << ckp->path();
    auto graph = std::make_shared<PropertyGraph>();
    graph->Open(ckp, config_.memory_level);

    // Init allocators before ingesting wals
    initAllocators(ckp->allocator_dir());

    neug::WalParserFactory::Init();
    auto wal_parser = WalParserFactory::CreateWalParser(ckp->wal_dir());
    ingestWals(*wal_parser, *graph);
    recovered_wal_timestamp = wal_parser->last_ts();

    // Create GraphSnapshotStore with the graph at timestamp 0
    snapshot_store_ =
        std::make_unique<GraphSnapshotStore>(config_.storage_slot_num, graph);

  } catch (const neug::exception::NoCheckpointException&) {
    throw;
  } catch (std::exception& e) {
    LOG(ERROR) << "Exception: " << e.what();
    THROW_INTERNAL_EXCEPTION(e.what());
  }
  return recovered_wal_timestamp;
}

void NeugDB::ingestWals(IWalParser& parser, PropertyGraph& graph) {
  uint32_t from_ts = 1;
  LOG(INFO) << "Ingesting update wals size: "
            << parser.get_update_wals().size();

  for (auto& update_wal : parser.get_update_wals()) {
    uint32_t to_ts = update_wal.timestamp;
    if (from_ts < to_ts) {
      IngestWalRange(graph, allocators_, parser, from_ts, to_ts);
    }
    if (update_wal.size == 0) {
      graph.Compact();
    } else {
      UpdateTransaction::IngestWal(graph, to_ts, update_wal.ptr,
                                   update_wal.size, *allocators_.at(0));
    }
    from_ts = to_ts + 1;
  }
  if (from_ts <= parser.last_ts()) {
    IngestWalRange(graph, allocators_, parser, from_ts, parser.last_ts() + 1);
  }
  LOG(INFO) << "Finish ingesting wals up to timestamp: " << parser.last_ts();
}

void NeugDB::initPlanner() {
  if (config_.planner_kind == "gopt") {
    planner_ = std::make_shared<GOptPlanner>();
  } else {
    THROW_INVALID_ARGUMENT_EXCEPTION("Invalid planner kind: " +
                                     config_.planner_kind);
  }
  LOG(INFO) << "Finish initializing planner";
}

void NeugDB::initQueryRuntime() {
  if (!planner_) {
    THROW_RUNTIME_ERROR("Planner is not initialized");
  }
  global_query_cache_ = std::make_shared<execution::GlobalQueryCache>(planner_);

  query_processor_ = std::make_shared<QueryProcessor>(
      *checkpoint_coordinator_, *snapshot_store_, planner_, global_query_cache_,
      *allocators_.at(0), max_thread_num_, config_.mode == DBMode::READ_ONLY);

  connection_manager_ = std::make_unique<ConnectionManager>(
      *snapshot_store_, planner_, query_processor_, config_);
}

void NeugDB::initPlannerAndQueryProcessor() {
  initPlanner();
  initQueryRuntime();
}

void NeugDB::createCheckpointAfterRecovery() {
  std::lock_guard<std::mutex> lock(mutex_);
  {
    SnapshotGuard guard(*snapshot_store_);
    auto* live_graph = guard.get().mutable_graph();
    if (!live_graph->IsModified()) {
      return;
    }
  }
  auto outcome = checkpoint_coordinator_->ExecuteRecovery();
  if (!outcome.ok()) {
    if (outcome.error_code() == StatusCode::ERR_IO_ERROR) {
      THROW_IO_EXCEPTION(outcome.error_message());
    }
    THROW_INTERNAL_EXCEPTION(outcome.error_message());
  }
}

void NeugDB::createCheckpointOnClose() {
  std::lock_guard<std::mutex> lock(mutex_);
  {
    SnapshotGuard guard(*snapshot_store_);
    auto* live_graph = guard.get().mutable_graph();
    if (!live_graph->IsModified()) {
      return;
    }
  }
  auto outcome = checkpoint_coordinator_->PrepareShutdown();
  if (!outcome.ok()) {
    if (outcome.error_code() == StatusCode::ERR_IO_ERROR) {
      THROW_IO_EXCEPTION(outcome.error_message());
    }
    THROW_INTERNAL_EXCEPTION(outcome.error_message());
  }
  // Close-path checkpointing does not reopen a live graph. Release all
  // snapshot/container/mmap resources before deleting the retired checkpoint.
  checkpoint_coordinator_.reset();
  snapshot_store_.reset();
  allocators_.clear();
  try {
    checkpoint_mgr_.CleanupRetiredCheckpoints();
  } catch (const std::exception& e) {
    LOG(WARNING) << "Checkpoint GC failed: " << e.what();
  } catch (...) { LOG(WARNING) << "Checkpoint GC failed"; }
}

}  // namespace neug
