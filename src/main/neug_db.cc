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
#include <stdlib.h>
#include <unistd.h>
#include <algorithm>
#include <atomic>
#include <cstdint>
#include <exception>

#include <filesystem>
#include <limits>
#include <new>
#include <regex>
#include <sstream>
#include <vector>

#include "neug/execution/execute/plan_parser.h"
#include "neug/execution/utils/opr_timer.h"
#include "neug/main/app_manager.h"
#include "neug/main/neug_db_session.h"
#include "neug/storages/file_names.h"
#include "neug/transaction/compact_transaction.h"
#include "neug/utils/allocators.h"
#include "neug/utils/app_utils.h"

namespace gs {

static void IngestWalRange(PropertyGraph& graph, const IWalParser& parser,
                           uint32_t from, uint32_t to,
                           std::vector<Allocator>& allocators,
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
            InsertTransaction::IngestWal(graph, got_ts, unit.ptr, unit.size,
                                         allocators[tid]);
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
    std::filesystem::remove_all(work_dir_);
  }
}

bool NeugDB::Open(const Schema& schema, const NeugDBConfig& config) {
  graph_.mutable_schema() = schema;
  return Open(config);
}

bool NeugDB::Open(const std::string& data_dir, int32_t max_num_threads,
                  const DBMode mode, const std::string& planner_kind,
                  bool warmup, bool enable_auto_compaction,
                  bool checkpoint_on_close) {
  NeugDBConfig config(data_dir, max_num_threads);
  config.mode = mode;
  config.warmup = warmup;
  config.planner_kind = planner_kind;
  config.enable_auto_compaction = enable_auto_compaction;
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
  initVersionManager();  // must before initTransactionManager
  ingestWals();
  initAppManager();  // must before initTransactionManager
  initTransactionManager();
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
  //-----------Clear graph_db----------------
  if (config_.checkpoint_on_close) {
    // TODO(zhanlei,lineng): Currently we dump the graph to a new directory,
    // we should invoke compact and checkpoint after checkpoint is
    // implemented.
    auto latest_ts = get_snapshot_version(work_dir_);
    VLOG(1) << "Dumping graph to " << snapshot_dir(work_dir_, latest_ts + 1)
            << ", this is the temporally workaround for data persistence, will "
               "be deleted in the future";
    graph_.Dump(work_dir_, latest_ts + 1);
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

  // TODO(zhanglei,lineng): Remove this adhoc resolution when checkpoint is
  // ready.
  if (config_.checkpoint_on_close) {
    auto latest_ts = get_snapshot_version(work_dir_);
    if (latest_ts > 0) {
      VLOG(10) << "Remove previous checkpoint at: "
               << snapshot_dir(work_dir_, latest_ts - 1)
               << ", current latest snapshot is at: "
               << snapshot_dir(work_dir_, latest_ts)
               << ", this behaviour is not elegant and will be replaced with "
                  "checkpoint in the near future";
      std::filesystem::remove_all(snapshot_dir(work_dir_, latest_ts - 1));
    }
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
  thread_num_ = 1;  // APVersionManager only support single thread
  txn_manager_->SwitchToAPMode(thread_num_);
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

  txn_manager_ = std::make_unique<TransactionManager>(
      app_manager_, version_manager_, graph_, config_, work_dir_);

  VLOG(1) << "Successfully Created TransactionManager with " << thread_num_
          << " threads";
}

void NeugDB::initAppManager() {
  if (app_manager_) {
    THROW_INTERNAL_EXCEPTION("App manager has already been initialized");
  }
  app_manager_ = std::make_shared<AppManager>(*this);
  app_manager_->initApps(graph_.schema().GetPlugins());
}

size_t NeugDB::getExecutedQueryNum() const {
  return txn_manager_->getExecutedQueryNum();
}

void NeugDB::preprocessConfig() {
  if (config_.thread_num == 0) {
    config_.thread_num = std::thread::hardware_concurrency();
  }
  auto db_dir = config_.data_dir;
  if (db_dir.empty()) {
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
}

void NeugDB::openGraphAndSchema() {
  if (!std::filesystem::exists(work_dir_)) {
    std::filesystem::create_directories(work_dir_);
  }

  // The schema provided by NeugDBConfig could be empty, and if it is empty,
  // we skip using it.
  bool create_empty_graph = false;
  std::string schema_file = schema_path(work_dir_);
  auto schema = graph_.mutable_schema();
  if (!std::filesystem::exists(schema_file) && !schema.Empty()) {
    create_empty_graph = true;
  }

  thread_num_ = config_.thread_num;
  try {
    graph_.Open(work_dir_, config_.memory_level);
  } catch (std::exception& e) {
    LOG(ERROR) << "Exception: " << e.what();
    THROW_INTERNAL_EXCEPTION(e.what());
  }

  if (!schema.Empty() && (!create_empty_graph) &&
      (!graph_.schema().Equals(schema))) {
    LOG(ERROR) << "Schema inconsistent..\n";
    THROW_INTERNAL_EXCEPTION("Schema inconsistent");
  }
  // Set the plugin info from schema to graph_.schema(), since the plugin info
  // is not serialized and deserialized.
  auto& mutable_schema = graph_.mutable_schema();
  mutable_schema.SetPluginDir(schema.GetPluginDir());
  std::vector<std::pair<std::string, std::string>> plugin_name_paths;
  const auto& plugins = schema.GetPlugins();
  for (auto plugin_pair : plugins) {
    plugin_name_paths.emplace_back(
        std::make_pair(plugin_pair.first, plugin_pair.second.first));
  }

  std::sort(plugin_name_paths.begin(), plugin_name_paths.end(),
            [&](const std::pair<std::string, std::string>& a,
                const std::pair<std::string, std::string>& b) {
              return plugins.at(a.first).second < plugins.at(b.first).second;
            });
  mutable_schema.EmplacePlugins(plugin_name_paths);
}

void NeugDB::ingestWals() {
  MemoryStrategy allocator_strategy = MemoryStrategy::kMemoryOnly;
  if (config_.memory_level == 0) {
    allocator_strategy = MemoryStrategy::kSyncToFile;
  } else if (config_.memory_level >= 2) {
    allocator_strategy = MemoryStrategy::kHugepagePrefered;
  }
  auto wal_uri = parse_wal_uri(config_.wal_uri, work_dir_);
  auto wal_parser = WalParserFactory::CreateWalParser(wal_uri);
  ingestWals(*wal_parser, work_dir_, allocator_strategy, thread_num_);
  version_manager_->init_ts(wal_parser->last_ts(), thread_num_);
}

void NeugDB::ingestWals(IWalParser& parser, const std::string& work_dir,
                        MemoryStrategy allocator_strategy, int thread_num) {
  uint32_t from_ts = 1;
  std::vector<Allocator> allocators;
  for (int i = 0; i < thread_num; ++i) {
    allocators.emplace_back(allocator_strategy,
                            allocator_strategy != MemoryStrategy::kSyncToFile
                                ? ""
                                : wal_ingest_allocator_prefix(work_dir, i));
  }
  for (auto& update_wal : parser.get_update_wals()) {
    uint32_t to_ts = update_wal.timestamp;
    if (from_ts < to_ts) {
      IngestWalRange(graph_, parser, from_ts, to_ts, allocators, work_dir,
                     thread_num);
    }
    if (update_wal.size == 0) {
      graph_.Compact(update_wal.timestamp);
      last_compaction_ts_ = update_wal.timestamp;
    } else {
      UpdateTransaction::IngestWal(graph_, work_dir, to_ts, update_wal.ptr,
                                   update_wal.size, allocators[0]);
    }
    from_ts = to_ts + 1;
  }
  if (from_ts <= parser.last_ts()) {
    IngestWalRange(graph_, parser, from_ts, parser.last_ts() + 1, allocators,
                   work_dir, thread_num);
  }
  version_manager_->init_ts(parser.last_ts(), thread_num);
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

}  // namespace gs
