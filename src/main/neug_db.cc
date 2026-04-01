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
#include <vector>

#include "neug/compiler/planner/gopt_planner.h"
#include "neug/compiler/planner/graph_planner.h"
#include "neug/execution/execute/plan_parser.h"
#include "neug/execution/execute/query_cache.h"
#include "neug/main/connection_manager.h"
#include "neug/main/file_lock.h"
#include "neug/main/query_processor.h"
#include "neug/server/neug_db_session.h"
#include "neug/storages/file_names.h"
#include "neug/storages/graph/schema.h"
#include "neug/transaction/compact_transaction.h"
#include "neug/transaction/wal/wal.h"
#include "neug/utils/allocators.h"
#include "neug/utils/exception/exception.h"
#include "neug/utils/file_utils.h"
#include "neug/utils/result.h"

namespace neug {

class Connection;
static void IngestWalRange(PropertyGraph& graph,
                           std::vector<std::shared_ptr<Allocator>>& allocators,
                           const IWalParser& parser, uint32_t from, uint32_t to,
                           const std::string& work_dir) {
  if (from >= to) {
    return;
  }
  for (size_t j = from; j < to; ++j) {
    const auto& unit = parser.get_insert_wal(j);
    InsertTransaction::IngestWal(graph, j, unit.ptr, unit.size, *allocators[0]);
    if (j % 1000000 == 0) {
      LOG(INFO) << "Ingested " << j << " WALs";
    }
  }
}

NeugDB::NeugDB()
    : last_compaction_ts_(0),
      last_ts_(0),
      closed_(true),
      is_pure_memory_(false),
      thread_num_(1) {}

NeugDB::~NeugDB() {
  Close();
  WalWriterFactory::Finalize();
  WalParserFactory::Finalize();
  // We put the removal of temp dir here to avoid the situation that
  //  starting tp service with database opened in memory mode. In this case,
  //  pydatabase will call close and then reopen, so we need to keep the temp
  //  dir until the db is destructed.
  if (is_pure_memory_) {
    VLOG(10) << "Removing temp NeugDB at: " << work_dir_;
    remove_directory(work_dir_);
  } else {
    remove_directory(tmp_dir(work_dir_));
  }
}

bool NeugDB::Open(const std::string& data_dir, int32_t max_num_threads,
                  const DBMode mode, const std::string& planner_kind,
                  bool enable_auto_compaction, bool compact_csr,
                  bool compact_on_close, bool checkpoint_on_close) {
  NeugDBConfig config(data_dir, max_num_threads);
  config.mode = mode;
  config.planner_kind = planner_kind;
  config.enable_auto_compaction = enable_auto_compaction;
  config.compact_csr = compact_csr;
  config.compact_on_close = compact_on_close;
  config.checkpoint_on_close = checkpoint_on_close;
  return Open(config);
}

bool NeugDB::Open(const NeugDBConfig& config) {
  config_ = config;
  preprocessConfig();

  work_dir_ = config_.data_dir;
  VLOG(1) << "Opening NeuGDB at " << work_dir_;
  if (!std::filesystem::exists(work_dir_)) {
    std::filesystem::create_directories(work_dir_);
  }
  file_lock_ = std::make_unique<FileLock>(work_dir_);

  if (!file_lock_->lock(work_dir_, config.mode)) {
    THROW_IO_EXCEPTION("Failed to lock data directory: " + work_dir_);
  }
  neug::execution::PlanParser::get().init();
  initAllocators();
  openGraphAndSchema();
  ingestWals();
  initPlannerAndQueryProcessor();

  LOG(INFO) << "NeugDB opened successfully";
  closed_.store(false);
  if (last_ts_ > 0 && config.checkpoint_after_recovery) {
    LOG(INFO) << "Creating checkpoint after recovery at ts " << last_ts_;
    createCheckpoint(true);
  }
  return true;
}

void NeugDB::Close() {
  // atomic flag to avoid double close
  if (closed_.exchange(true)) {
    return;
  }
  closed_.store(true);
  if (connection_manager_) {
    connection_manager_->Close();
    connection_manager_.reset();
  }
  if (planner_) {
    planner_.reset();
  }
  if (query_processor_) {
    query_processor_.reset();
  }
  // -----------Create checkpoint if needed----------------
  if (config_.checkpoint_on_close) {
    createCheckpoint(false, false);
  }
  graph_.Clear();

  if (file_lock_) {
    file_lock_->unlock();
  }
}

std::shared_ptr<Connection> NeugDB::Connect() {
  return connection_manager_->CreateConnection();
}

void NeugDB::RemoveConnection(std::shared_ptr<Connection> conn) {
  connection_manager_->RemoveConnection(conn);
}

void NeugDB::CloseAllConnection() { connection_manager_->Close(); }

void NeugDB::preprocessConfig() {
  if (config_.thread_num == 0) {
    config_.thread_num = std::thread::hardware_concurrency();
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

void NeugDB::initAllocators() {
  // Initialize the default allocator for ingesting wals
  remove_directory(allocator_dir(work_dir_));
  std::filesystem::create_directories(allocator_dir(work_dir_));
  assert(config_.thread_num > 0);
  for (int i = 0; i < config_.thread_num; ++i) {
    allocators_.emplace_back(std::make_shared<Allocator>(
        config_.memory_level, config_.memory_level != MemoryLevel::kSyncToFile
                                  ? ""
                                  : wal_ingest_allocator_prefix(work_dir_, i)));
  }
}

void NeugDB::openGraphAndSchema() {
  if (!std::filesystem::exists(work_dir_)) {
    std::filesystem::create_directories(work_dir_);
  }

  thread_num_ = config_.thread_num;
  try {
    graph_.Open(work_dir_, config_.memory_level);
  } catch (std::exception& e) {
    LOG(ERROR) << "Exception: " << e.what();
    THROW_INTERNAL_EXCEPTION(e.what());
  }
}

void NeugDB::ingestWals() {
  auto wal_uri = parse_wal_uri(config_.wal_uri, work_dir_);
  neug::WalParserFactory::Init();
  auto wal_parser = WalParserFactory::CreateWalParser(wal_uri);
  ingestWals(*wal_parser, work_dir_);
}

void NeugDB::ingestWals(IWalParser& parser, const std::string& work_dir) {
  uint32_t from_ts = 1;
  LOG(INFO) << "Ingesting update wals size: "
            << parser.get_update_wals().size();
  for (auto& update_wal : parser.get_update_wals()) {
    uint32_t to_ts = update_wal.timestamp;
    if (from_ts < to_ts) {
      IngestWalRange(graph_, allocators_, parser, from_ts, to_ts, work_dir);
    }
    if (update_wal.size == 0) {
      graph_.Compact(config_.compact_csr, config_.csr_reserve_ratio,
                     update_wal.timestamp);
      last_compaction_ts_ = update_wal.timestamp;
    } else {
      UpdateTransaction::IngestWal(graph_, work_dir, to_ts, update_wal.ptr,
                                   update_wal.size, *allocators_[0]);
    }
    from_ts = to_ts + 1;
  }
  if (from_ts <= parser.last_ts()) {
    IngestWalRange(graph_, allocators_, parser, from_ts, parser.last_ts() + 1,
                   work_dir);
  }
  LOG(INFO) << "Finish ingesting wals up to timestamp: " << parser.last_ts();
  last_ts_ = parser.last_ts();
}

void NeugDB::initPlannerAndQueryProcessor() {
  if (config_.planner_kind == "gopt") {
    // Gopt planner is the default planner, so we don't need to create it.
    planner_ = std::make_shared<GOptPlanner>();
  } else {
    THROW_INVALID_ARGUMENT_EXCEPTION("Invalid planner kind: " +
                                     config_.planner_kind);
  }
  planner_->update_meta(schema().to_yaml().value());
  planner_->update_statistics(graph().get_statistics_json());
  LOG(INFO) << "Finish initializing planner with schema and statistics";

  global_query_cache_ = std::make_shared<execution::GlobalQueryCache>(planner_);

  query_processor_ = std::make_shared<QueryProcessor>(
      graph_, planner_, global_query_cache_, *allocators_[0], thread_num_,
      config_.mode == DBMode::READ_ONLY);

  connection_manager_ = std::make_unique<ConnectionManager>(
      graph_, planner_, query_processor_, config_);
}

void NeugDB::createCheckpoint(bool force_compaction, bool reopen) {
  std::unique_lock<std::mutex> lock(mutex_);
  if (config_.compact_on_close || force_compaction) {
    graph_.Compact(config_.compact_csr, config_.csr_reserve_ratio,
                   MAX_TIMESTAMP);
  }
  graph_.Dump(reopen);
  VLOG(1) << "Finish checkpoint";
}

}  // namespace neug
