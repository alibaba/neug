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
#ifndef _WIN32
#include <unistd.h>
#else
#include <windows.h>
#endif
#include <algorithm>
#include <atomic>
#include <cerrno>
#include <cstdint>
#include <cstdlib>
#include <exception>
#include <filesystem>
#include <limits>
#include <random>
#include <system_error>
#include <utility>
#include <vector>

#include "neug/compiler/extension/extension_manager.h"
#include "neug/compiler/planner/gopt_planner.h"
#include "neug/compiler/planner/graph_planner.h"
#include "neug/execution/execute/plan_parser.h"
#include "neug/execution/execute/query_cache.h"
#include "neug/main/checkpoint_coordinator.h"
#include "neug/main/connection_manager.h"
#include "neug/main/execution_slot.h"
#include "neug/main/file_lock.h"
#include "neug/storages/allocators.h"
#include "neug/storages/checkpoint_manager.h"
#include "neug/storages/checkpoint_manifest.h"
#include "neug/storages/graph/schema.h"
#include "neug/transaction/compact_transaction.h"
#include "neug/transaction/version_manager.h"
#include "neug/transaction/wal/wal.h"
#include "neug/utils/exception/exception.h"
#include "neug/utils/io/file/file_utils.h"
#include "neug/utils/result.h"

namespace neug {

#ifdef _WIN32
// MSVC's CRT has no mkdtemp(); emulate it by replacing the trailing
// "XXXXXX" with random characters and retrying until a fresh directory is
// created.  Sets errno and returns false on failure, mirroring mkdtemp.
static bool mkdtemp_win(std::string& path_template) {
  constexpr char kChars[] =
      "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
  constexpr size_t kSuffixLen = 6;
  if (path_template.size() < kSuffixLen ||
      path_template.compare(path_template.size() - kSuffixLen, kSuffixLen,
                            "XXXXXX") != 0) {
    errno = EINVAL;
    return false;
  }
  std::random_device rd;
  std::mt19937_64 gen(rd());
  std::uniform_int_distribution<size_t> dist(0, sizeof(kChars) - 2);
  for (int attempt = 0; attempt < 100; ++attempt) {
    for (size_t i = path_template.size() - kSuffixLen; i < path_template.size();
         ++i) {
      path_template[i] = kChars[dist(gen)];
    }
    std::error_code ec;
    if (std::filesystem::create_directory(path_template, ec)) {
      return true;
    }
    if (ec && ec != std::errc::file_exists) {
      // Map the system error to its POSIX equivalent so the errno-based
      // reporting at the call site stays meaningful.
      errno = ec.default_error_condition().value();
      return false;
    }
    // Name collision: retry with a new random suffix.
  }
  errno = EEXIST;
  return false;
}
#endif

inline std::string allocator_prefix(const std::string& allocator_dir,
                                    int slot_id) {
  return (std::filesystem::path(allocator_dir) /
          ("allocator_" + std::to_string(slot_id) + "_"))
      .string();
}

class Connection;
static void IngestWalRange(PropertyGraph& graph,
                           std::vector<std::shared_ptr<Allocator>>& allocators,
                           const IWalParser& parser, uint32_t from,
                           uint32_t to) {
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
    InsertTransaction::IngestWal(view, j, unit.ptr, unit.size, *allocators[0]);
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
    // Fail fast: if Close() cannot complete (e.g. a NeugDBService is still
    // associated), the service would be left holding a reference to a
    // destroyed database and its destructor would call back into freed
    // memory. Continuing teardown here is undefined behavior.
    LOG(FATAL) << "Failed to close NeugDB in destructor: " << e.what();
  } catch (...) {
    LOG(FATAL) << "Failed to close NeugDB in destructor: unknown error";
  }
  WalWriterFactory::Finalize();
  WalParserFactory::Finalize();
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
  try {
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

    config_.data_dir =
        std::filesystem::weakly_canonical(config_.data_dir).string();

    file_lock_ = std::make_unique<FileLock>(config_.data_dir);

    std::string error_msg;
    // A pure-memory database owns a fresh private workspace, so it needs an
    // exclusive lock to initialize the lock file even when its data access
    // mode is read-only. Persistent read-only databases use a shared lock and
    // may create missing runtime coordination metadata for legacy databases.
    const auto lock_mode = is_pure_memory_ ? DBMode::READ_WRITE : config.mode;
    if (!file_lock_->lock(error_msg, lock_mode)) {
      THROW_DATABASE_LOCKED_EXCEPTION(
          "Failed to lock data directory: " + config_.data_dir +
          ", error: " + error_msg);
    }

    checkpoint_mgr_.Open(config_.data_dir, recover_workspace);
    VLOG(1) << "Opening NeuGDB at " << checkpoint_mgr_.database_dir();
    neug::execution::PlanParser::get().init();
    timestamp_t initial_visibility_ts = openGraphAndIngestWals();
    checkpoint_coordinator_ = std::make_unique<CheckpointCoordinator>(
        checkpoint_mgr_, *snapshot_store_, config_.memory_level,
        [this](const std::string& allocator_dir) {
          reopenAllocators(allocator_dir);
        });
    if (initial_visibility_ts > 0 && config.checkpoint_on_recovery &&
        config_.mode == DBMode::READ_WRITE) {
      LOG(INFO) << "Creating checkpoint after recovery at ts "
                << initial_visibility_ts;
      if (createCheckpointAfterRecovery()) {
        initial_visibility_ts = 0;
      }
    }
    if (config_.mode == DBMode::READ_WRITE) {
      checkpoint_mgr_.CollectGarbage();
    }
    initVersionManager(initial_visibility_ts);
    extension_manager_ = std::make_unique<ExtensionManager>();
    initPlanner();
    initQueryRuntime();
  } catch (...) {
    clearQueryRuntime();
    planner_.reset();
    extension_manager_.reset();
    version_manager_.reset();
    checkpoint_coordinator_.reset();
    snapshot_store_.reset();
    allocators_.clear();
    checkpoint_mgr_.Close();
    cleanupTemporaryWorkspace();
    if (file_lock_) {
      file_lock_->unlock();
      file_lock_.reset();
    }
    throw;
  }

  LOG(INFO) << "NeugDB opened successfully";
  closed_.store(false);
  return true;
}

void NeugDB::Close() {
  {
    // Serialized with registerService(): the active-service check and the
    // closed flag update are atomic with respect to service registration,
    // so no rollback or re-check is needed and Close() stays idempotent.
    std::lock_guard<std::mutex> lock(service_mutex_);
    if (active_service_ != nullptr) {
      THROW_RUNTIME_ERROR(
          "Cannot close NeugDB while a NeugDBService is still associated "
          "with it. Stop and destroy the service first.");
    }
    if (closed_.exchange(true)) {
      return;
    }
  }
  // Once closed_ is set with no active service, registerService() rejects
  // new registrations and concurrent Close() calls return early, so the
  // remaining cleanup does not need the lock.
  clearQueryRuntime();
  if (planner_) {
    planner_.reset();
  }

  if (config_.checkpoint_on_close && config_.mode == DBMode::READ_WRITE) {
    VLOG(1) << "Creating checkpoint on close...";
    try {
      createCheckpointOnClose();
    } catch (const std::exception& e) {
      LOG(ERROR) << "Checkpoint on close failed: " << e.what();
    }
  }

  version_manager_.reset();
  checkpoint_coordinator_.reset();
  snapshot_store_.reset();
  extension_manager_.reset();
  allocators_.clear();
  checkpoint_mgr_.Close();
  cleanupTemporaryWorkspace();

  if (file_lock_) {
    file_lock_->unlock();
    file_lock_.reset();
  }
}

std::shared_ptr<Connection> NeugDB::Connect() {
  std::lock_guard<std::mutex> lock(service_mutex_);
  if (IsClosed()) {
    THROW_RUNTIME_ERROR(
        "Cannot create connection on a closed NeugDB instance.");
  }
  if (active_service_ != nullptr) {
    THROW_RUNTIME_ERROR(
        "Cannot create connection while the database is being served by a "
        "NeugDBService.");
  }
  return connection_manager_->CreateConnection();
}

bool NeugDB::HasActiveService() const {
  std::lock_guard<std::mutex> lock(service_mutex_);
  return active_service_ != nullptr;
}

bool NeugDB::HasOpenConnections() const {
  std::lock_guard<std::mutex> lock(service_mutex_);
  return connection_manager_ && connection_manager_->HasOpenConnections();
}

void NeugDB::registerService(NeugDBService* svc) {
  // Serialized with Close(): either the database is closed first (and this
  // registration is rejected), or the service registers first (and Close()
  // fails fast). A service can therefore never be registered onto a closed
  // or closing database.
  std::lock_guard<std::mutex> lock(service_mutex_);
  if (IsClosed()) {
    THROW_RUNTIME_ERROR(
        "Cannot register a NeugDBService on a closed NeugDB instance.");
  }
  if (active_service_ != nullptr) {
    THROW_RUNTIME_ERROR(
        "NeugDB instance is already associated with a NeugDBService. Only "
        "one service instance is allowed per database.");
  }
  if (connection_manager_ && connection_manager_->HasOpenConnections()) {
    THROW_RUNTIME_ERROR(
        "Cannot switch NeugDB to TP mode while local connections are open. "
        "Close all Connection objects before starting the service.");
  }
  active_service_ = svc;

  try {
    closeAllConnections();
  } catch (...) {
    active_service_ = nullptr;
    throw;
  }
}

void NeugDB::unregisterService(NeugDBService* svc) noexcept {
  std::lock_guard<std::mutex> lock(service_mutex_);
  if (active_service_ != svc) {
    LOG(WARNING) << "unregisterService: the given service is not the active "
                    "service of this database.";
    return;
  }
  active_service_ = nullptr;
}

void NeugDB::closeAllConnections() {
  if (connection_manager_) {
    connection_manager_->Close();
  }
}

void NeugDB::PrepareForServing() {
  std::lock_guard<std::mutex> lock(service_mutex_);
  if (IsClosed()) {
    THROW_RUNTIME_ERROR("NeugDB instance is not ready for serving!");
  }
  if (active_service_ != nullptr) {
    THROW_RUNTIME_ERROR(
        "Cannot prepare NeugDB for serving while a NeugDBService is already "
        "associated with it.");
  }
  if (connection_manager_ && connection_manager_->HasOpenConnections()) {
    THROW_RUNTIME_ERROR(
        "Cannot switch NeugDB to TP mode while local connections are open. "
        "Close all Connection objects before starting the service.");
  }
  closeAllConnections();
  clearQueryRuntime();
  bool checkpoint_created = false;
  if (config_.mode == DBMode::READ_WRITE) {
    checkpoint_created = createCheckpointAfterRecovery();
  }
  if (checkpoint_created) {
    // Replacing the VM is safe only after publishing a new checkpoint whose
    // WAL directory starts a fresh transaction timeline. A clean graph may
    // still have WAL records (for example an in-place TP checkpoint), so keep
    // the current VM in that case.
    initVersionManager(0);
  }
  initQueryRuntime();
}

void NeugDB::preprocessConfig() {
  if (config_.max_thread_num < 0) {
    THROW_INVALID_ARGUMENT_EXCEPTION(
        "Invalid max_thread_num: " + std::to_string(config_.max_thread_num) +
        ". Must be a non-negative integer.");
  }

  // 0 means auto-select from the host's hardware concurrency. A positive
  // value is honored as-is: databases legitimately oversubscribe worker
  // threads beyond physical cores, and tests depend on explicit counts.
  // Guardrails against over-sized values live at the Python API boundary and
  // the service-layer thread_num clamp.
  if (config_.max_thread_num == 0) {
    int hardware_concurrency =
        static_cast<int>(std::thread::hardware_concurrency());
    config_.max_thread_num =
        hardware_concurrency > 0 ? hardware_concurrency : 1;
  }
  if (config_.data_dir.empty() || config_.data_dir == ":memory" ||
      config_.data_dir == ":memory:") {
    std::filesystem::path db_dir_prefix;
    char* prefix_env = std::getenv("NEUG_DB_TMP_DIR");
    if (prefix_env) {
      db_dir_prefix = prefix_env;
    } else {
#ifdef _WIN32
      // On Windows, use the system temp directory (e.g.
      // C:\Users\<user>\AppData\Local\Temp)
      char tmp_path[MAX_PATH];
      DWORD len = GetTempPathA(MAX_PATH, tmp_path);
      if (len > 0) {
        // Remove trailing backslash
        if (tmp_path[len - 1] == '\\' || tmp_path[len - 1] == '/') {
          tmp_path[len - 1] = '\0';
          len--;
        }
        db_dir_prefix = std::string(tmp_path);
      } else {
        db_dir_prefix = ".";
      }
#else
      db_dir_prefix = "/tmp";
#endif
    }
    db_dir_prefix = std::filesystem::absolute(db_dir_prefix);
    std::filesystem::create_directories(db_dir_prefix);
    auto path_template = (db_dir_prefix / "neug_db_XXXXXX").string();
#ifdef _WIN32
    if (!mkdtemp_win(path_template)) {
#else
    if (::mkdtemp(path_template.data()) == nullptr) {
#endif
      const auto error = std::error_code(errno, std::generic_category());
      THROW_IO_EXCEPTION("Failed to create temporary NeugDB under " +
                         db_dir_prefix.string() + ": " + error.message());
    }
    config_.data_dir.swap(path_template);
    is_pure_memory_ = true;
    LOG(INFO) << "Creating temp NeugDB with: " << config_.data_dir << " in "
              << config_.mode << " mode";
  } else {
    LOG(INFO) << "Creating NeugDB with: " << config_.data_dir << " in "
              << config_.mode << " mode";
  }
}

void NeugDB::cleanupTemporaryWorkspace() noexcept {
  if (!is_pure_memory_) {
    return;
  }
  is_pure_memory_ = false;
  try {
    VLOG(10) << "Removing temp NeugDB at: " << config_.data_dir;
    remove_directory(config_.data_dir);
  } catch (const std::exception& e) {
    LOG(WARNING) << "Failed to remove temporary NeugDB " << config_.data_dir
                 << "; leaving it on disk: " << e.what();
  } catch (...) {
    LOG(WARNING) << "Failed to remove temporary NeugDB " << config_.data_dir
                 << "; leaving it on disk";
  }
}

void NeugDB::initAllocators(const std::string& allocator_dir) {
  // WAL replay needs an allocator even in read-only mode. Read-only opens
  // must not alter the checkpoint allocator workspace, so use transient
  // in-memory backing there. Read-write opens retain the durable workspace.
  allocators_.clear();
  const bool read_only = config_.mode == DBMode::READ_ONLY;
  if (!read_only) {
    remove_directory(allocator_dir);
    std::filesystem::create_directories(allocator_dir);
  }
  assert(config_.max_thread_num > 0);
  for (int i = 0; i < config_.max_thread_num; ++i) {
    allocators_.emplace_back(std::make_shared<Allocator>(
        read_only ? MemoryLevel::kInMemory : config_.memory_level,
        !read_only && config_.memory_level == MemoryLevel::kSyncToFile
            ? allocator_prefix(allocator_dir, i)
            : ""));
  }
}

void NeugDB::reopenAllocators(const std::string& allocator_dir) {
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
  try {
    auto ckp = checkpoint_mgr_.Current();
    if (ckp == nullptr) {
      if (config_.mode == DBMode::READ_ONLY && !is_pure_memory_) {
        THROW_NO_CHECKPOINT_EXCEPTION(
            "NeugDB::Open: no checkpoint found in read-only database: " +
            checkpoint_mgr_.database_dir());
      }
      auto staging_checkpoint = checkpoint_mgr_.CreateStaging();
      ckp = staging_checkpoint.checkpoint();
      CheckpointManifest meta;
      meta.SetSchema(Schema());
      ckp->SetManifest(std::move(meta));
      ckp = staging_checkpoint.Publish();
      LOG(INFO) << "No checkpoint found, created initial checkpoint: "
                << ckp->manifest_path();
    }
    LOG(INFO) << "Opening graph from checkpoint " << ckp->manifest_path();
    auto graph = std::make_shared<PropertyGraph>();
    graph->Open(ckp, config_.memory_level);

    // Init allocators before ingesting wals
    initAllocators(ckp->allocator_dir());

    neug::WalParserFactory::Init();
    auto wal_parser = WalParserFactory::CreateWalParser(ckp->wal_dir());
    const timestamp_t recovered_wal_timestamp =
        ingestWals(*wal_parser, *graph,
                   static_cast<timestamp_t>(ckp->manifest().base_timestamp()));

    // VersionManager is initialized by the caller with recovered_wal_timestamp.
    snapshot_store_ =
        std::make_unique<GraphSnapshotStore>(config_.storage_slot_num, graph);
    return recovered_wal_timestamp;

  } catch (const neug::exception::NoCheckpointException&) {
    throw;
  } catch (std::exception& e) {
    LOG(ERROR) << "Exception: " << e.what();
    THROW_INTERNAL_EXCEPTION(e.what());
  }
}

timestamp_t NeugDB::ingestWals(IWalParser& parser, PropertyGraph& graph,
                               timestamp_t base_timestamp) {
  uint32_t from_ts = base_timestamp + 1;
  LOG(INFO) << "Ingesting update wals size: "
            << parser.get_update_wals().size();

  for (auto& update_wal : parser.get_update_wals()) {
    uint32_t to_ts = update_wal.timestamp;
    // A checkpoint already contains every change through base_timestamp.
    // Normally its WAL epoch is fresh, but ignore stale records defensively
    // so recovery never re-applies a checkpointed update.
    if (to_ts <= base_timestamp) {
      continue;
    }
    if (from_ts < to_ts) {
      IngestWalRange(graph, allocators_, parser, from_ts, to_ts);
    }
    if (update_wal.size == 0) {
      graph.Compact();
    } else {
      UpdateTransaction::IngestWal(graph, to_ts, update_wal.ptr,
                                   update_wal.size, *allocators_[0]);
    }
    from_ts = to_ts + 1;
  }
  if (from_ts <= parser.last_ts()) {
    IngestWalRange(graph, allocators_, parser, from_ts, parser.last_ts() + 1);
  }
  LOG(INFO) << "Finish ingesting wals up to timestamp: " << parser.last_ts();
  return std::max(base_timestamp, parser.last_ts());
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

void NeugDB::initVersionManager(timestamp_t initial_visibility_ts) {
  auto version_manager = std::make_unique<VersionManager>();
  SnapshotGuard snapshot(*snapshot_store_);
  const PublishedReadView initial_read_view{
      initial_visibility_ts, snapshot.get().snapshot_generation()};
  version_manager->init_ts(initial_read_view, max_thread_num_);
  version_manager_ = std::move(version_manager);
}

std::unique_ptr<ExecutionSlot> NeugDB::createExecutionSlot(size_t slot_id) {
  CHECK(snapshot_store_ != nullptr);
  CHECK(planner_ != nullptr);
  CHECK(global_query_cache_ != nullptr);
  CHECK(version_manager_ != nullptr);
  CHECK(checkpoint_coordinator_ != nullptr);
  CHECK_LT(slot_id, allocators_.size());
  return std::unique_ptr<ExecutionSlot>(new ExecutionSlot(
      *snapshot_store_, planner_, global_query_cache_, *version_manager_,
      *allocators_.at(slot_id), QueryExecutionStrategy::kDirect,
      /*wal_writer=*/nullptr, *checkpoint_coordinator_, *extension_manager_,
      config_, static_cast<int>(slot_id)));
}

void NeugDB::initQueryRuntime() {
  if (!planner_) {
    THROW_RUNTIME_ERROR("Planner is not initialized");
  }
  auto global_query_cache =
      std::make_shared<execution::GlobalQueryCache>(planner_);
  auto connection_manager = std::make_unique<ConnectionManager>(*this, config_);
  CHECK(!global_query_cache_);
  CHECK(!connection_manager_);
  global_query_cache_ = std::move(global_query_cache);
  connection_manager_ = std::move(connection_manager);
}

void NeugDB::clearQueryRuntime() noexcept {
  if (connection_manager_) {
    try {
      connection_manager_->Close();
    } catch (const std::exception& e) {
      LOG(WARNING) << "Failed to close query runtime connections: " << e.what();
    } catch (...) {
      LOG(WARNING) << "Failed to close query runtime connections";
    }
    connection_manager_.reset();
  }
  global_query_cache_.reset();
}

bool NeugDB::createCheckpointAfterRecovery() {
  std::lock_guard<std::mutex> lock(mutex_);
  {
    SnapshotGuard guard(*snapshot_store_);
    auto* live_graph = guard.get().mutable_graph();
    if (!live_graph->IsModified()) {
      return false;
    }
  }
  auto outcome = checkpoint_coordinator_->PublishRecoveryCheckpoint();
  if (!outcome.ok()) {
    if (outcome.error_code() == StatusCode::ERR_IO_ERROR) {
      THROW_IO_EXCEPTION(outcome.error_message());
    }
    THROW_INTERNAL_EXCEPTION(outcome.error_message());
  }
  return true;
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
  auto outcome = checkpoint_coordinator_->PublishShutdownCheckpoint();
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
  checkpoint_mgr_.CollectGarbage();
}

}  // namespace neug
