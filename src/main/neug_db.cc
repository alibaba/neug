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
#include "neug/main/app_manager.h"
#include "neug/main/connection_manager.h"
#include "neug/main/file_lock.h"
#include "neug/main/neug_db_session.h"
#include "neug/main/query_processor.h"
#include "neug/main/transaction_manager.h"
#include "neug/storages/file_names.h"
#include "neug/storages/graph/schema.h"
#include "neug/transaction/compact_transaction.h"
#include "neug/transaction/version_manager.h"
#include "neug/transaction/wal/wal.h"
#include "neug/utils/allocators.h"
#include "neug/utils/exception/exception.h"
#include "neug/utils/file_utils.h"
#include "neug/utils/result.h"

namespace gs {

class Connection;
static void IngestWalRange(PropertyGraph& graph,
                           std::shared_ptr<TransactionManager> txn_manager,
                           const IWalParser& parser, uint32_t from, uint32_t to,
                           const std::string& work_dir, int thread_num) {
  std::atomic<uint32_t> cur_ts(from);
  std::vector<std::thread> threads(thread_num);
  for (int i = 0; i < thread_num; ++i) {
    threads[i] = std::thread(
        [&](int tid) {
          while (true) {
            uint32_t got_ts = cur_ts.fetch_add(1);
            if (got_ts >= to) {
              break;
            }
            const auto& unit = parser.get_insert_wal(got_ts);
            InsertTransaction::IngestWal(
                graph, got_ts, unit.ptr, unit.size,
                txn_manager->GetSession(tid).allocator());
            if (got_ts % 1000000 == 0) {
              LOG(INFO) << "Ingested " << got_ts << " WALs";
            }
          }
        },
        i);
  }
  for (auto& thrd : threads) {
    thrd.join();
  }
}

NeugDB::NeugDB()
    : last_compaction_ts_(0),
      closed_(true),
      is_pure_memory_(false),
      thread_num_(1),
      monitor_thread_running_(false),
      compact_thread_running_(false) {}

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
                  bool warmup, bool enable_auto_compaction, bool compact_csr,
                  bool compact_on_close, bool checkpoint_on_close) {
  NeugDBConfig config(data_dir, max_num_threads);
  config.mode = mode;
  config.warmup = warmup;
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
  gs::runtime::PlanParser::get().init();
  openGraphAndSchema();
  initVersionManager();      // must before initTransactionManager
  initAppManager();          // must before initTransactionManager
  initTransactionManager();  // must before ingestWals
  ingestWals();
  startMonitorIfNeeded();
  startCompactThreadIfNeeded();
  initPlannerAndQueryProcessor();

  LOG(INFO) << "NeugDB opened successfully";
  closed_.store(false);
  return true;
}

void NeugDB::Close() {
  // atomic flag to avoid double close
  if (closed_.exchange(true)) {
    return;
  }
  closed_.store(true);
  if (monitor_thread_running_) {
    monitor_thread_running_ = false;
    monitor_thread_.join();
  }
  if (compact_thread_running_) {
    compact_thread_running_ = false;
    compact_thread_.join();
  }
  if (connection_manager_) {
    connection_manager_->Close();
    connection_manager_.reset();
  }
  if (app_manager_) {
    app_manager_.reset();
  }
  if (planner_) {
    planner_.reset();
  }
  if (query_processor_) {
    query_processor_.reset();
  }
  // -----------Clear graph_db----------------
  if (config_.checkpoint_on_close) {
    createCheckpoint();
  }
  graph_.Clear();
  if (version_manager_) {
    version_manager_->clear();
    version_manager_.reset();
  }
  if (txn_manager_) {
    txn_manager_->Clear();
    txn_manager_.reset();
  }

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

ReadTransaction NeugDB::GetReadTransaction(int thread_id) {
  return txn_manager_->GetReadTransaction(thread_id);
}

InsertTransaction NeugDB::GetInsertTransaction(int thread_id) {
  return txn_manager_->GetInsertTransaction(thread_id);
}

UpdateTransaction NeugDB::GetUpdateTransaction(int thread_id) {
  return txn_manager_->GetUpdateTransaction(thread_id);
}

CompactTransaction NeugDB::GetCompactTransaction(int thread_id) {
  return txn_manager_->GetCompactTransaction(thread_id);
}

NeugDBSession& NeugDB::GetSession(int thread_id) {
  return txn_manager_->GetSession(thread_id);
}

const NeugDBSession& NeugDB::GetSession(int thread_id) const {
  return txn_manager_->GetSession(thread_id);
}

int NeugDB::SessionNum() const { return thread_num_; }

void NeugDB::SwitchToTPMode() {
  if (closed_) {
    THROW_INTERNAL_EXCEPTION("Cannot switch to TP mode on closed db");
  }
  if (connection_manager_ && connection_manager_->ConnectionNum() > 0) {
    THROW_NOT_SUPPORTED_EXCEPTION(
        "Cannot switch to TP mode when there are active connections");
  }
  createCheckpoint();
  std::unique_lock<std::mutex> lock(mutex_);
  if (dynamic_cast<TPVersionManager*>(version_manager_.get()) != nullptr) {
    LOG(WARNING) << "The version manager is already a TPVersionManager, no "
                    "need to switch.";
    return;
  }

  auto new_vm = std::make_shared<TPVersionManager>();
  new_vm->init_ts(version_manager_->acquire_read_timestamp(), thread_num_);
  version_manager_ = new_vm;
  txn_manager_->SetVersionManager(version_manager_);
  txn_manager_->SwitchToTPMode(thread_num_);
}

void NeugDB::SwitchToAPMode() {
  if (closed_) {
    THROW_INTERNAL_EXCEPTION("Cannot switch to AP mode on closed db");
  }
  if (connection_manager_ && connection_manager_->ConnectionNum() > 0) {
    THROW_NOT_SUPPORTED_EXCEPTION(
        "Cannot switch to AP mode when there are active connections");
  }
  createCheckpoint();
  std::unique_lock<std::mutex> lock(mutex_);
  if (dynamic_cast<APVersionManager*>(version_manager_.get()) != nullptr) {
    LOG(WARNING) << "The version manager is already an APVersionManager, no "
                    "need to switch.";
    return;
  }
  auto new_vm = std::make_shared<APVersionManager>();
  new_vm->init_ts(version_manager_->acquire_read_timestamp(), thread_num_);
  version_manager_ = new_vm;
  txn_manager_->SetVersionManager(version_manager_);
  txn_manager_->SwitchToAPMode(thread_num_);
}

bool NeugDB::IsReadyForServing() const {
  if (closed_) {
    return false;
  }
  if (dynamic_cast<TPVersionManager*>(version_manager_.get()) == nullptr) {
    LOG(WARNING)
        << "The version manager is not a TPVersionManager, cannot serve.";
    return false;
  }
  if (connection_manager_ && connection_manager_->ConnectionNum() > 0) {
    LOG(WARNING)
        << "There are active local connections, cannot switch to serving.";
    return false;
  }
  return true;
}

void NeugDB::initVersionManager() {
  if (version_manager_) {
    THROW_INTERNAL_EXCEPTION("Version manager has already been initialized");
  }
  // By default we use APVersionManager, when starting serving, we switch to
  // TPVersionManager.
  version_manager_ = std::make_shared<APVersionManager>();
}

void NeugDB::initTransactionManager() {
  if (txn_manager_) {
    THROW_INTERNAL_EXCEPTION(
        "Transaction manager has already been initialized");
  }

  txn_manager_ = std::make_shared<TransactionManager>(
      app_manager_, version_manager_, graph_, config_, work_dir_, thread_num_);

  VLOG(1) << "Successfully Created TransactionManager with " << thread_num_
          << " threads";
}

void NeugDB::initAppManager() {
  if (app_manager_) {
    THROW_INTERNAL_EXCEPTION("App manager has already been initialized");
  }
  app_manager_ = std::make_shared<AppManager>(*this);
  app_manager_->initApps();
}

size_t NeugDB::getExecutedQueryNum() const {
  return txn_manager_->getExecutedQueryNum();
}

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
       << std::chrono::duration_cast<std::chrono::milliseconds>(duration)
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
  auto wal_parser = WalParserFactory::CreateWalParser(wal_uri);
  ingestWals(*wal_parser, work_dir_);
  version_manager_->init_ts(wal_parser->last_ts(), thread_num_);
}

void NeugDB::ingestWals(IWalParser& parser, const std::string& work_dir) {
  uint32_t from_ts = 1;
  LOG(INFO) << "Ingesting update wals size: "
            << parser.get_update_wals().size();
  for (auto& update_wal : parser.get_update_wals()) {
    uint32_t to_ts = update_wal.timestamp;
    if (from_ts < to_ts) {
      IngestWalRange(graph_, txn_manager_, parser, from_ts, to_ts, work_dir,
                     thread_num_);
    }
    if (update_wal.size == 0) {
      graph_.Compact(config_.reset_timestamp_before_checkpoint,
                     config_.compact_csr, config_.csr_reserve_ratio,
                     update_wal.timestamp);
      last_compaction_ts_ = update_wal.timestamp;
    } else {
      UpdateTransaction::IngestWal(graph_, work_dir, to_ts, update_wal.ptr,
                                   update_wal.size,
                                   txn_manager_->GetSession(0).allocator());
    }
    from_ts = to_ts + 1;
  }
  if (from_ts <= parser.last_ts()) {
    IngestWalRange(graph_, txn_manager_, parser, from_ts, parser.last_ts() + 1,
                   work_dir, thread_num_);
  }
  LOG(INFO) << "Finish ingesting wals up to timestamp: " << parser.last_ts();
  version_manager_->init_ts(parser.last_ts(), thread_num_);
}
void NeugDB::startMonitorIfNeeded() {
  if (config_.enable_monitoring) {
    if (monitor_thread_running_) {
      monitor_thread_running_ = false;
      monitor_thread_.join();
    }
    monitor_thread_running_ = true;
    monitor_thread_ = std::thread([&]() {
      std::vector<double> last_eval_durations(thread_num_, 0);
      std::vector<int64_t> last_query_nums(thread_num_, 0);
      while (monitor_thread_running_) {
        sleep(10);
        size_t curr_allocated_size = 0;
        double total_eval_durations = 0;
        double min_eval_duration = std::numeric_limits<double>::max();
        double max_eval_duration = 0;
        int64_t total_query_num = 0;
        int64_t min_query_num = std::numeric_limits<int64_t>::max();
        int64_t max_query_num = 0;

        for (int i = 0; i < thread_num_; ++i) {
          curr_allocated_size += txn_manager_->GetAlloctedMemorySize(i);
          if (last_eval_durations[i] == 0) {
            last_eval_durations[i] = txn_manager_->GetEvalDuration(i);
          } else {
            double curr = txn_manager_->GetEvalDuration(i);
            double eval_duration = curr;
            total_eval_durations += eval_duration;
            min_eval_duration = std::min(min_eval_duration, eval_duration);
            max_eval_duration = std::max(max_eval_duration, eval_duration);

            last_eval_durations[i] = curr;
          }
          if (last_query_nums[i] == 0) {
            last_query_nums[i] = txn_manager_->GetQueryNum(i);
          } else {
            int64_t curr = txn_manager_->GetQueryNum(i);
            total_query_num += curr;
            min_query_num = std::min(min_query_num, curr);
            max_query_num = std::max(max_query_num, curr);

            last_query_nums[i] = curr;
          }
        }
        if (max_query_num != 0) {
          double avg_eval_durations =
              total_eval_durations / static_cast<double>(thread_num_);
          double avg_query_num = static_cast<double>(total_query_num) /
                                 static_cast<double>(thread_num_);
          double allocated_size_in_gb =
              static_cast<double>(curr_allocated_size) / 1024.0 / 1024.0 /
              1024.0;
          LOG(INFO) << "allocated: " << allocated_size_in_gb << " GB, eval: ["
                    << min_eval_duration << ", " << avg_eval_durations << ", "
                    << max_eval_duration << "] s, query num: [" << min_query_num
                    << ", " << avg_query_num << ", " << max_query_num << "]";
        }
      }
    });
  }
}

void NeugDB::startCompactThreadIfNeeded() {
  if (config_.enable_auto_compaction) {
    if (compact_thread_running_) {
      compact_thread_running_ = false;
      compact_thread_.join();
    }
    compact_thread_running_ = true;
    compact_thread_ = std::thread([&]() {
      size_t last_compaction_at = 0;
      while (compact_thread_running_) {
        size_t query_num_before = getExecutedQueryNum();
        sleep(30);
        if (!compact_thread_running_) {
          break;
        }
        size_t query_num_after = getExecutedQueryNum();
        if (query_num_before == query_num_after &&
            (query_num_after > (last_compaction_at + 100000))) {
          VLOG(10) << "Trigger auto compaction";
          last_compaction_at = query_num_after;
          auto txn = txn_manager_->GetCompactTransaction(0);
          txn.Commit();
          VLOG(10) << "Finish compaction";
        }
      }
    });
  }
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

  query_processor_ = std::make_shared<QueryProcessor>(
      *this, thread_num_, config_.mode == DBMode::READ_ONLY);

  connection_manager_ = std::make_unique<ConnectionManager>(
      graph_, planner_, query_processor_, config_);
}

void NeugDB::createCheckpoint() {
  std::unique_lock<std::mutex> lock(mutex_);
  if (config_.compact_on_close) {
    graph_.Compact(config_.reset_timestamp_before_checkpoint,
                   config_.compact_csr, config_.csr_reserve_ratio,
                   MAX_TIMESTAMP);
  }
  graph_.Dump();
  VLOG(1) << "Finish checkpoint";
}

}  // namespace gs
