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

#include <glog/logging.h>
#include <stdlib.h>
#include <time.h>

#include <chrono>
#include <csignal>
#include <cstdint>
#include <filesystem>
#include <iostream>
#include <memory>
#include <string>
#include <system_error>

#include "cxxopts/cxxopts.hpp"
#include "neug/storages/graph/property_graph.h"
#include "neug/storages/graph/schema.h"
#include "neug/storages/loader/i_fragment_loader.h"
#include "neug/storages/loader/loader_factory.h"
#include "neug/storages/loader/loading_config.h"
#include "neug/utils/result.h"

static std::string work_dir;

void signal_handler(int signal) {
  LOG(INFO) << "Received signal " << signal << ", exiting...";
  // support SIGKILL, SIGINT, SIGTERM
  if (signal == SIGINT || signal == SIGTERM || signal == SIGABRT) {
    LOG(ERROR) << "Received signal " << signal
               << ",Clearing directory: " << work_dir << ", exiting...";
    // remove all files in work_dir
    std::filesystem::remove_all(work_dir);
    exit(signal);
  } else {
    LOG(ERROR) << "Received unexpected signal " << signal << ", exiting...";
    exit(1);
  }
}

int main(int argc, char** argv) {
  /**
   * When loading the edges of a graph, there are two stages involved.
   *
   * The first stage involves reading the edges into a temporary vector and
   * acquiring information on the degrees of the vertices,
   * Then constructs the CSR using the degree information.
   *
   * During the first stage, the edges are stored in the form of triplets, which
   * can lead to a certain amount of memory expansion, so the `use-mmap-vector`
   * option is provided, mmap_vector utilizes mmap to map files, supporting
   * runtime memory swapping to disk.
   *
   * Constructing the CSR involves random reads and writes, we offer the
   * `build-csr-in-mem` option, which allows CSR to be built in-memory to
   * avoid extensive disk random read and write operations
   *
   */

  cxxopts::Options options("bulk_loader",
                           "Bulk loader for NeuG graph database");
  options.add_options()("h,help", "Display help message")("v,version",
                                                          "Display version")(
      "p,parallelism", "Parallelism of bulk loader",
      cxxopts::value<uint32_t>()->default_value("1"))(
      "d,data-path", "Data directory path", cxxopts::value<std::string>())(
      "g,graph-config", "Graph schema config file",
      cxxopts::value<std::string>())("l,bulk-load", "Bulk-load config file",
                                     cxxopts::value<std::string>())(
      "build-csr-in-mem", "Build CSR in memory",
      cxxopts::value<bool>()->default_value("false"))(
      "use-mmap-vector", "Use mmap vector",
      cxxopts::value<bool>()->default_value("false"));

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

  std::string data_path = "";
  /**
   * If the data path is an oss path, the data will be uploaded to oss after
   * loading to a temporary directory. To improve the performance of the
   * performance, bulk_loader will zip the data directory before uploading.
   * The data path should be in the format of oss://bucket_name/object_path
   */
  std::string bulk_load_config_path = "";
  std::string graph_schema_path = "";

  if (!vm.count("graph-config")) {
    LOG(ERROR) << "graph-config is required";
    return -1;
  }
  graph_schema_path = vm["graph-config"].as<std::string>();
  if (!vm.count("data-path")) {
    LOG(ERROR) << "data-path is required";
    return -1;
  }
  data_path = vm["data-path"].as<std::string>();
  if (!vm.count("bulk-load")) {
    LOG(ERROR) << "bulk-load-config is required";
    return -1;
  }
  bulk_load_config_path = vm["bulk-load"].as<std::string>();
  if (!std::filesystem::exists(bulk_load_config_path)) {
    LOG(ERROR) << "bulk-load-config file does not exist: "
               << bulk_load_config_path;
    return -1;
  }

  setenv("TZ", "Asia/Shanghai", 1);
  tzset();

  auto start = std::chrono::high_resolution_clock::now();

  auto schema_res = neug::Schema::LoadFromYaml(graph_schema_path);
  if (!schema_res) {
    LOG(ERROR) << "Fail to load graph schema file: "
               << schema_res.error().error_message();
    return -1;
  }
  auto loading_config_res = neug::LoadingConfig::ParseFromYamlFile(
      schema_res.value(), bulk_load_config_path);
  if (!loading_config_res) {
    LOG(ERROR) << "Fail to parse loading config file: "
               << loading_config_res.error().error_message();
    return -1;
  }

  // check whether parallelism, build_csr_in_mem, use_mmap_vector are overridden
  if (vm.count("parallelism")) {
    loading_config_res.value().SetParallelism(vm["parallelism"].as<uint32_t>());
  }
  if (vm.count("build-csr-in-mem")) {
    loading_config_res.value().SetBuildCsrInMem(
        vm["build-csr-in-mem"].as<bool>());
  }
  if (vm.count("use-mmap-vector")) {
    loading_config_res.value().SetUseMmapVector(
        vm["use-mmap-vector"].as<bool>());
  }

  if (data_path.find("oss://") == 0) {
#ifdef BUILD_WITH_OSS
    upload_to_oss = true;
    check_oss_object_not_exist(data_path, object_path, oss_conf);
#else
    LOG(ERROR) << "OSS is not supported in this build";
    return -1;
#endif
  }

  std::filesystem::path data_dir_path(data_path);
  if (!std::filesystem::exists(data_dir_path)) {
    std::filesystem::create_directory(data_dir_path);
  }
  std::filesystem::path serial_path = data_dir_path / "schema";
  if (std::filesystem::exists(serial_path)) {
    LOG(WARNING) << "data directory is not empty: " << data_dir_path.string()
                 << ", please remove the directory and try again.";
    return -1;
  }

  {
    std::error_code ec;
    std::filesystem::copy(graph_schema_path, data_dir_path / "graph.yaml",
                          std::filesystem::copy_options::overwrite_existing,
                          ec);
    if (ec) {
      LOG(FATAL) << "Failed to copy graph schema file: " << ec.message();
    }
  }

  work_dir = data_dir_path.string();

  // Register handlers for SIGKILL, SIGINT, SIGTERM, SIGSEGV, SIGABRT
  // LOG(FATAL) cause SIGABRT
  std::signal(SIGINT, signal_handler);
  std::signal(SIGTERM, signal_handler);
  std::signal(SIGABRT, signal_handler);
  std::signal(SIGFPE, signal_handler);

  auto loader = neug::LoaderFactory::CreateFragmentLoader(
      data_dir_path.string(), schema_res.value(), loading_config_res.value());

  auto result = loader->LoadFragment();
  if (!result) {
    std::filesystem::remove_all(data_dir_path);
    LOG(ERROR) << "Failed to load fragment: " << result.error().error_message();
    return -1;
  }
  auto end = std::chrono::high_resolution_clock::now();
  double elapsed = std::chrono::duration<double>(end - start).count();
  LOG(INFO) << "Finished bulk loading in " << elapsed << " seconds.";

  // Also copy the graph.yaml to the data directory
  std::error_code ec;
  std::filesystem::copy(graph_schema_path, data_dir_path / "graph.yaml",
                        std::filesystem::copy_options::overwrite_existing, ec);
  if (ec) {
    LOG(ERROR) << "Failed to copy graph schema file: " << ec.message();
  }

  return 0;
}
