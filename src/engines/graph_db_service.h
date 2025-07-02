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
#ifndef ENGINES_COMMON_GRAPH_DB_SERVICE_H_
#define ENGINES_COMMON_GRAPH_DB_SERVICE_H_

#include <cctype>
#include <memory>
#include <string>

#include "src/engines/graph_db/database/graph_db.h"
#include "src/storages/metadata/graph_meta_store.h"
#include "src/storages/metadata/metadata_store_factory.h"
#include "src/utils/http_handler_manager.h"
#include "src/utils/result.h"
#include "src/utils/service_utils.h"

#include <yaml-cpp/yaml.h>
#include <boost/process.hpp>

namespace server {
/* Stored service configuration, read from interactive_config.yaml
 */

class GraphDBService {
 public:
  static const std::string DEFAULT_GRAPH_NAME;
  static const std::string DEFAULT_INTERACTIVE_HOME;
  static const std::string COMPILER_SERVER_CLASS_NAME;
  static GraphDBService& get();

  gs::GraphDB& graph_db() { return db_; }
  ~GraphDBService();

  void init(const ServiceConfig& config);

  const ServiceConfig& get_service_config() const;

  bool is_initialized() const;

  bool is_running() const;

  uint16_t get_query_port() const;

  uint64_t get_start_time() const;

  void reset_start_time();

  std::shared_ptr<gs::IGraphMetaStore> get_metadata_store() const;

  gs::Result<std::string> service_status();

  void run_and_wait_for_exit();

  // Actually stop the actors, the service is still on, but returns error code
  // for each request.
  void stop_query_actors();

  bool is_actors_running() const;

  // Actually create new actors with a different scope_id,
  // Because we don't know whether the previous scope_id can be reused.
  void start_query_actors();

  bool check_compiler_ready() const;

 private:
  GraphDBService() = default;

  std::string find_interactive_class_path();
  // Insert graph meta into metadata store.
  gs::GraphId insert_default_graph_meta();
  void open_default_graph();
  void clear_running_graph();

 private:
  gs::GraphDB db_;
  std::unique_ptr<IHttpHandlerManager> hdl_mgr_;
  std::atomic<bool> running_{false};
  std::atomic<bool> initialized_{false};
  std::atomic<uint64_t> start_time_{0};
  std::mutex mtx_;

  ServiceConfig service_config_;
  boost::process::child compiler_process_;
  // handler for metadata store
  std::shared_ptr<gs::IGraphMetaStore> metadata_store_;
};

}  // namespace server

#endif  // ENGINES_COMMON_GRAPH_DB_SERVICE_H_
