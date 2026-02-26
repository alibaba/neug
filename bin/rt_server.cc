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

#include "neug/main/file_lock.h"
#include "neug/main/neug_db.h"
#include "neug/server/neug_db_service.h"
#include "neug/utils/service_utils.h"

#include <glog/logging.h>
#include <chrono>
#include <csignal>
#include "cxxopts/cxxopts.hpp"

using namespace neug;

void signal_handler(int signal) {
  LOG(INFO) << "Received signal " << signal << ", exiting...";
  // support SIGKILL, SIGINT, SIGTERM
  if (signal == SIGINT || signal == SIGTERM || signal == SIGABRT) {
    LOG(ERROR) << "Received signal " << signal << ", Remove all filelocks";
    // remove all files in work_dir
    neug::FileLock::CleanupAllLocks();
    exit(signal);
  } else {
    LOG(ERROR) << "Received unexpected signal " << signal << ", exiting...";
    exit(1);
  }
}

void setup_signal_handler() {
  std::signal(SIGINT, signal_handler);
  std::signal(SIGTERM, signal_handler);
  std::signal(SIGABRT, signal_handler);
}

int main(int argc, char** argv) {
  cxxopts::Options options("rt_server", "Real-time graph server for NeuG");
  options.add_options()("h,help", "Display help message")("v,version",
                                                          "Display version")(
      "s,shard-num", "Shard number of actor system",
      cxxopts::value<uint32_t>()->default_value("9"))(
      "p,http-port", "HTTP port of query handler",
      cxxopts::value<uint16_t>()->default_value("10000"))(
      "d,data-path", "Data directory path", cxxopts::value<std::string>())(
      "m,memory-level", "Memory level for graph data",
      cxxopts::value<int>()->default_value("1"))(
      "sharding-mode", "Sharding mode (exclusive or cooperative)",
      cxxopts::value<std::string>()->default_value("cooperative"))(
      "wal-uri", "URI for Write-Ahead Logging storage",
      cxxopts::value<std::string>()->default_value(
          "file://{GRAPH_DATA_DIR}/wal"))("host", "Host address",
                                          cxxopts::value<std::string>());

  google::InitGoogleLogging(argv[0]);
  FLAGS_logtostderr = true;

  cxxopts::ParseResult vm = options.parse(argc, argv);

  setup_signal_handler();

  if (vm.count("help")) {
    std::cout << options.help() << std::endl;
    return 0;
  }
  if (vm.count("version")) {
    std::cout << "NeuG version " << NEUG_VERSION << std::endl;
    return 0;
  }

  int memory_level = vm["memory-level"].as<int>();
  uint32_t shard_num = vm["shard-num"].as<uint32_t>();
  uint16_t http_port = vm["http-port"].as<uint16_t>();

  std::string data_path = "";

  if (!vm.count("data-path")) {
    LOG(ERROR) << "data-path is required";
    return -1;
  }
  data_path = vm["data-path"].as<std::string>();

  setenv("TZ", "Asia/Shanghai", 1);
  tzset();

  auto start = std::chrono::high_resolution_clock::now();
  neug::NeugDB db;
  neug::NeugDBConfig config(data_path, shard_num);
  config.memory_level = memory_level;
  config.wal_uri = vm["wal-uri"].as<std::string>();
  if (config.memory_level >= 2) {
    config.enable_auto_compaction = true;
  }
  db.Open(config);

  auto end = std::chrono::high_resolution_clock::now();
  double elapsed = std::chrono::duration<double>(end - start).count();

  LOG(INFO) << "Finished loading graph, elapsed " << elapsed << " s";

  // start service
  LOG(INFO) << "GraphScope http server start to listen on port " << http_port;

  neug::ServiceConfig service_config;
  service_config.shard_num = shard_num;
  service_config.host_str =
      vm.count("host") ? vm["host"].as<std::string>() : "127.0.0.1";
  service_config.query_port = http_port;
  neug::NeugDBService service(db, service_config);

  service.run_and_wait_for_exit();

  return 0;
}
