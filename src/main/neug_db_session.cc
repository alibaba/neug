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

#include "neug/main/neug_db_session.h"

#include <glog/logging.h>
#include <stddef.h>
#include <stdint.h>
#include <array>
#include <atomic>
#include <chrono>
#include <memory>
#include <ostream>
#include <string>
#include <string_view>
#include <thread>
#include <tl/expected.hpp>
#include <tuple>
#include <vector>

#include "neug/config.h"
#include "neug/main/app/app_base.h"
#include "neug/main/app_manager.h"
#include "neug/storages/graph/property_graph.h"
#include "neug/storages/graph/schema.h"
#include "neug/transaction/compact_transaction.h"
#include "neug/transaction/insert_transaction.h"
#include "neug/transaction/read_transaction.h"
#include "neug/transaction/update_transaction.h"
#include "neug/transaction/version_manager.h"
#include "neug/utils/app_utils.h"
#include "neug/utils/likely.h"
#include "neug/utils/property/types.h"
#ifdef USE_SYSTEM_PROTOBUF
#include "neug/generated/proto/plan/common.pb.h"
#include "neug/generated/proto/plan/stored_procedure.pb.h"
#else
#include "neug/utils/proto/plan/common.pb.h"
#include "neug/utils/proto/plan/stored_procedure.pb.h"
#endif
#include "neug/utils/result.h"

namespace gs {

class Schema;

ReadTransaction NeugDBSession::GetReadTransaction() const {
  uint32_t ts = version_manager_->acquire_read_timestamp();
  return ReadTransaction(graph_, *version_manager_, ts);
}

InsertTransaction NeugDBSession::GetInsertTransaction() {
  uint32_t ts = version_manager_->acquire_insert_timestamp();
  return InsertTransaction(graph_, alloc_, logger_, *version_manager_, ts);
}

UpdateTransaction NeugDBSession::GetUpdateTransaction() {
  uint32_t ts = version_manager_->acquire_update_timestamp();
  return UpdateTransaction(graph_, alloc_, work_dir_, logger_,
                           *version_manager_, ts);
}

const PropertyGraph& NeugDBSession::graph() const { return graph_; }

PropertyGraph& NeugDBSession::graph() { return graph_; }

const Schema& NeugDBSession::schema() const { return graph_.schema(); }

result<std::vector<char>> NeugDBSession::Eval(const std::string& input) {
  const auto start = std::chrono::high_resolution_clock::now();

  if (input.size() < 2) {
    RETURN_ERROR(
        Status(StatusCode::ERR_INVALID_ARGUMENT,
               "Invalid input, input size: " + std::to_string(input.size())));
  }

  auto type_res = parse_query_type(input);
  if (!type_res) {
    LOG(ERROR) << "Fail to parse query type";
    RETURN_ERROR(type_res.error());
  }

  uint8_t type;
  std::string_view sv;
  std::tie(type, sv) = type_res.value();

  std::vector<char> result_buffer;

  Encoder encoder(result_buffer);
  Decoder decoder(sv.data(), sv.size());

  AppBase* app = GetApp(type);
  if (!app) {
    RETURN_ERROR(Status(
        StatusCode::ERR_NOT_FOUND,
        "Procedure not found, id:" + std::to_string(static_cast<int>(type))));
  }

  for (size_t i = 0; i < MAX_RETRY; ++i) {
    result_buffer.clear();
    if (app->run(*this, decoder, encoder)) {
      const auto end = std::chrono::high_resolution_clock::now();
      app_metrics_[type].add_record(
          std::chrono::duration_cast<std::chrono::microseconds>(end - start)
              .count());
      eval_duration_.fetch_add(
          std::chrono::duration_cast<std::chrono::microseconds>(end - start)
              .count());
      ++query_num_;
      return result_buffer;
    }

    LOG(INFO) << "[Query-" << static_cast<int>(type) << "][Thread-"
              << thread_id_ << "] retry - " << i << " / " << MAX_RETRY;
    if (i + 1 < MAX_RETRY) {
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }

    decoder.reset(sv.data(), sv.size());
  }

  const auto end = std::chrono::high_resolution_clock::now();
  eval_duration_.fetch_add(
      std::chrono::duration_cast<std::chrono::microseconds>(end - start)
          .count());
  ++query_num_;
  // When query failed, we assume the user may put the error message in the
  // output buffer.
  // For example, for adhoc_app.cc, if the query failed, the error info will
  // be put in the output buffer.
  if (result_buffer.size() > 4) {
    RETURN_ERROR(Status(
        StatusCode::ERR_QUERY_EXECUTION,
        std::string(result_buffer.data() + 4, result_buffer.size() - 4)
        // The first 4 bytes are the length of the message.
        ));
  } else {
    RETURN_ERROR(Status(StatusCode::ERR_QUERY_EXECUTION,
                        "Query failed for procedure id:" +
                            std::to_string(static_cast<int>(type))));
  }
}

int NeugDBSession::SessionId() const { return thread_id_; }

CompactTransaction NeugDBSession::GetCompactTransaction() {
  timestamp_t ts = version_manager_->acquire_update_timestamp();
  return CompactTransaction(graph_, logger_, *version_manager_,
                            db_config_.reset_timestamp_before_checkpoint,
                            db_config_.compact_csr,
                            db_config_.csr_reserve_ratio, ts);
}

double NeugDBSession::eval_duration() const {
  return static_cast<double>(eval_duration_.load()) / 1000000.0;
}

int64_t NeugDBSession::query_num() const { return query_num_.load(); }

AppBase* NeugDBSession::GetApp(int type) {
  // create if not exist
  if (type >= MAX_PLUGIN_NUM) {
    LOG(ERROR) << "Query type is out of range: " << type << " > "
               << MAX_PLUGIN_NUM;
    return nullptr;
  }
  AppBase* app = nullptr;
  if (NEUG_LIKELY(apps_[type] != nullptr)) {
    app = apps_[type];
  } else {
    app_wrappers_[type] = app_manager_.CreateApp(type, thread_id_);
    if (app_wrappers_[type].app() == NULL) {
      LOG(ERROR) << "[Query-" + std::to_string(static_cast<int>(type))
                 << "] is not registered...";
      return nullptr;
    } else {
      apps_[type] = app_wrappers_[type].app();
      app = apps_[type];
    }
  }
  return app;
}

const AppMetric& NeugDBSession::GetAppMetric(int idx) const {
  return app_metrics_[idx];
}

}  // namespace gs
