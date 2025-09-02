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

#include "cxxopts/cxxopts.hpp"
#include "neug/server/graph_db_service.h"
#include "neug/transaction/graph_db.h"
#include "neug/utils/service_utils.h"

#include <glog/logging.h>

using namespace server;

int main(int argc, char** argv) {
  gs::blockSignal(SIGINT);
  gs::blockSignal(SIGTERM);

  cxxopts::Options options("rt_server", "Real-time graph server for NeuG");
  options.add_options()("h,help", "Display help message")("v,version",
                                                          "Display version")(
      "s,shard-num", "Shard number of actor system",
      cxxopts::value<uint32_t>()->default_value("1"))(
      "p,http-port", "HTTP port of query handler",
      cxxopts::value<uint16_t>()->default_value("10000"))(
      "d,data-path", "Data directory path", cxxopts::value<std::string>())(
      "w,warmup", "Warmup graph data",
      cxxopts::value<bool>()->default_value("false"))(
      "m,memory-level", "Memory level for graph data",
      cxxopts::value<int>()->default_value("1"))(
      "c,compiler-path", "Path to the compiler",
      cxxopts::value<std::string>()->default_value(""))(
      "sharding-mode", "Sharding mode (exclusive or cooperative)",
      cxxopts::value<std::string>()->default_value("cooperative"))(
      "wal-uri", "URI for Write-Ahead Logging storage",
      cxxopts::value<std::string>()->default_value(
          "file://{GRAPH_DATA_DIR}/wal"));

  google::InitGoogleLogging(argv[0]);
  FLAGS_logtostderr = true;

  cxxopts::ParseResult vm = options.parse(argc, argv);

  if (vm.count("help")) {
    std::cout << options.help() << std::endl;
    return 0;
  }
  if (vm.count("version")) {
    std::cout << "NeuG version " << NEUG_VERSION << std::endl;
    return 0;
  }

  bool enable_dpdk = false;
  bool warmup = vm["warmup"].as<bool>();
  int memory_level = vm["memory-level"].as<int>();
  uint32_t shard_num = vm["shard-num"].as<uint32_t>();
  uint16_t http_port = vm["http-port"].as<uint16_t>();

  std::string data_path = "";

  if (!vm.count("data-path")) {
    LOG(ERROR) << "data-path is required";
    return -1;
  }
  data_path = vm["data-path"].as<std::string>();
  std::string compiler_path = vm["compiler-path"].as<std::string>();

  setenv("TZ", "Asia/Shanghai", 1);
  tzset();

  double t0 = -grape::GetCurrentTime();
  gs::GraphDB& db = server::GraphDBService::get().graph_db();
  std::string graph_schema_path = data_path + "/graph.yaml";
  auto schema = gs::Schema::LoadFromYaml(graph_schema_path);
  if (!schema.ok()) {
    LOG(FATAL) << "Failed to load schema: " << schema.status().error_message();
  }
  gs::GraphDBConfig config(schema.value(), data_path, compiler_path, shard_num);
  config.memory_level = memory_level;
  config.wal_uri = vm["wal-uri"].as<std::string>();
  config.warmup = warmup;
  if (config.memory_level >= 2) {
    config.enable_auto_compaction = true;
  }
  db.Open(config);

  t0 += grape::GetCurrentTime();

  LOG(INFO) << "Finished loading graph, elapsed " << t0 << " s";

  // start service
  LOG(INFO) << "GraphScope http server start to listen on port " << http_port;

  server::ServiceConfig service_config;
  service_config.shard_num = shard_num;
  service_config.dpdk_mode = enable_dpdk;
  service_config.query_port = http_port;
  service_config.set_sharding_mode(vm["sharding-mode"].as<std::string>());
  server::GraphDBService::get().init(service_config);

  server::GraphDBService::get().run_and_wait_for_exit();

  return 0;
}
