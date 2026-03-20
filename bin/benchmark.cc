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

#include <fstream>
#include <iostream>
#include <sstream>
#include <string>

#include <glog/logging.h>

#include "neug/execution/common/context.h"
#include "neug/execution/common/operators/retrieve/sink.h"
#include "neug/execution/common/types/value.h"
#include "neug/execution/execute/plan_parser.h"
#include "neug/main/neug_db.h"
#include "neug/main/query_request.h"
#include "neug/server/neug_db_service.h"
#include "neug/storages/graph/graph_interface.h"
#include "neug/utils/encoder.h"
#include "neug/utils/exception/exception.h"

#include <glog/logging.h>
#include "cxxopts/cxxopts.hpp"

std::vector<std::string> split_string(const std::string& s, char delimiter) {
  std::vector<std::string> tokens;
  std::string token;
  std::istringstream tokenStream(s);
  while (std::getline(tokenStream, token, delimiter)) {
    tokens.push_back(token);
  }
  return tokens;
}

neug::execution::ParamsMap deserialize_string_kv_map(
    const neug::execution::ParamsMetaMap& meta_map,
    const std::map<std::string, std::string>& param_str_map) {
  neug::execution::ParamsMap map;
  for (auto iter : param_str_map) {
    if (meta_map.count(iter.first) > 0) {
      auto type = meta_map.at(iter.first);
      switch (type.id()) {
      case neug::DataTypeId::kInt32: {
        map.emplace(iter.first,
                    neug::execution::Value::INT32(std::stoi(iter.second)));
        break;
      }
      case neug::DataTypeId::kInt64: {
        map.emplace(iter.first,
                    neug::execution::Value::INT64(std::stoll(iter.second)));
        break;
      }
      case neug::DataTypeId::kUInt32: {
        map.emplace(iter.first,
                    neug::execution::Value::UINT32(std::stoul(iter.second)));
        break;
      }
      case neug::DataTypeId::kUInt64: {
        map.emplace(iter.first,
                    neug::execution::Value::UINT64(std::stoull(iter.second)));
        break;
      }
      case neug::DataTypeId::kBoolean: {
        map.emplace(iter.first,
                    neug::execution::Value::BOOLEAN(iter.second == "true"));
        break;
      }
      case neug::DataTypeId::kVarchar: {
        map.emplace(iter.first, neug::execution::Value::STRING(iter.second));
        break;
      }
      case neug::DataTypeId::kTimestampMs: {
        map.emplace(iter.first, neug::execution::Value::TIMESTAMPMS(
                                    neug::DateTime(std::stoll(iter.second))));
        break;
      }
      case neug::DataTypeId::kDate: {
        map.emplace(iter.first, neug::execution::Value::DATE(neug::Date(
                                    int64_t(std::stoll(iter.second)))));
        break;
      default:
        LOG(WARNING) << "Unsupported parameter type for key: " << iter.first
                     << ", type id: " << static_cast<int>(type.id());
      }
      }
    } else {
      continue;
    }
  }
  return map;
}

class BenchmarkConfig {
 public:
  struct BenchmarkUnit {
    std::string name;
    std::string query_pb_path;
    std::string query_param_path;
    int repeat;
  };

  BenchmarkConfig(const std::string& config_file) {
    load_from_file(config_file);
  }
  ~BenchmarkConfig() = default;

  const std::vector<BenchmarkUnit>& benchmarks() const { return units_; }

 private:
  void load_from_file(const std::string& file) {
    std::ifstream fin(file);
    if (!fin.is_open()) {
      LOG(ERROR) << "Failed to open benchmark config file: " << file;
      return;
    }
    units_.clear();
    std::string line;
    while (std::getline(fin, line)) {
      if (line.empty() || line[0] == '#') {
        continue;
      }
      std::vector<std::string> tokens = split_string(line, '|');
      if (tokens.size() != 4) {
        LOG(ERROR) << "Invalid benchmark config line: " << line;
        continue;
      }
      BenchmarkUnit unit;
      unit.name = tokens[0];
      unit.query_pb_path = tokens[1];
      unit.query_param_path = tokens[2];
      unit.repeat = std::stoi(tokens[3]);
      units_.emplace_back(unit);
    }
    fin.close();
  }

  std::vector<BenchmarkUnit> units_;
};

std::vector<std::map<std::string, std::string>> parse_query_file(
    const std::string& fname) {
  std::vector<std::map<std::string, std::string>> ret;
  std::ifstream fin(fname);
  std::string line;

  std::vector<std::string> types;

  while (std::getline(fin, line)) {
    std::vector<std::string> tokens = split_string(line, '|');
    if (types.empty()) {
      types = tokens;
    } else {
      CHECK_EQ(tokens.size(), types.size());
      size_t n = tokens.size();
      std::map<std::string, std::string> m;
      for (size_t k = 0; k < n; ++k) {
        m.emplace(types[k], tokens[k]);
      }
      ret.emplace_back(std::move(m));
    }
  }
  return ret;
}

std::string parse_query(const std::string& filename) {
  std::ifstream file(filename, std::ios::binary);

  CHECK(file.is_open());

  file.seekg(0, std::ios::end);
  size_t size = file.tellg();
  file.seekg(0, std::ios::beg);

  std::string buffer;
  buffer.resize(size);

  file.read(&buffer[0], size);

  file.close();

  return buffer;
}

void benchmark_iteration(
    neug::IStorageInterface& graph, neug::execution::Pipeline& pipeline,
    const std::vector<neug::execution::ParamsMap>& parameters, int query_num,
    std::vector<std::vector<char>>& outputs, neug::execution::OprTimer& timer) {
  outputs.resize(query_num);
  for (int i = 0; i < query_num; ++i) {
    neug::execution::Context ctx;
    auto& m = parameters[i % parameters.size()];
    google::protobuf::Arena arena;
    // Create a QueryResponse message on the arena to hold the results.
    neug::QueryResponse* response =
        google::protobuf::Arena::CreateMessage<neug::QueryResponse>(&arena);
    if (i == 0) {
      auto ctx = pipeline.Execute(graph, neug::execution::Context(), m, &timer);
      if (!ctx) {
        LOG(ERROR) << "Failed to execute pipeline: " << ctx.error().ToString();
        return;
      }
      outputs[i].clear();

      neug::Encoder output(outputs[i]);
      neug::execution::Sink::sink_results(
          ctx.value(), dynamic_cast<neug::StorageReadInterface&>(graph),
          response);
    } else {
      neug::execution::OprTimer cur_timer;
      auto ctx =
          pipeline.Execute(graph, neug::execution::Context(), m, &cur_timer);

      outputs[i].clear();
      neug::Encoder output(outputs[i]);
      neug::execution::Sink::sink_results(
          ctx.value(), dynamic_cast<neug::StorageReadInterface&>(graph),
          response);
      timer += cur_timer;
    }
  }
}

int main(int argc, char** argv) {
  cxxopts::Options options("benchmark", "Benchmarking tool for NeuG");
  options.add_options()("help", "Display help message")(
      "data-path,d", "", cxxopts::value<std::string>())(
      "memory-level,m", "", cxxopts::value<int>()->default_value("1"))(
      "benchmark-config,c", "", cxxopts::value<std::string>());
  google::InitGoogleLogging(argv[0]);
  FLAGS_logtostderr = true;
  cxxopts::ParseResult vm = options.parse(argc, argv);

  if (vm.count("help")) {
    std::cout << options.help() << std::endl;
    return 0;
  }
  int memory_level = vm["memory-level"].as<int>();

  std::string data_path = "";
  if (!vm.count("data-path")) {
    LOG(ERROR) << "data-path is required";
    return -1;
  }
  data_path = vm["data-path"].as<std::string>();
  std::string graph_schema_path = data_path + "/graph.yaml";
  int shard_num = 1;

  neug::NeugDB db;
  neug::NeugDBConfig config(data_path, shard_num);
  config.memory_level = memory_level;

  config.enable_auto_compaction = false;
  db.Open(config);
  auto compiler = db.GetPlanner();
  auto svc = std::make_shared<neug::NeugDBService>(db);

  if (!vm.count("benchmark-config")) {
    LOG(ERROR) << "benchmark-config is required";
    return -1;
  }
  std::string benchmark_config_path = vm["benchmark-config"].as<std::string>();
  BenchmarkConfig benchmark_config(benchmark_config_path);

  auto sess = svc->AcquireSession();
  auto txn = sess->GetReadTransaction();
  neug::StorageReadInterface graph(txn.graph(), txn.timestamp());

  for (const auto& unit : benchmark_config.benchmarks()) {
    int query_num = unit.repeat;
    auto parameters = parse_query_file(unit.query_param_path);
    if (query_num == 0) {
      query_num = parameters.size();
    }
    LOG(INFO) << "Running benchmark: " << unit.name
              << ", repeat: " << query_num;
    std::vector<std::vector<char>> outputs(query_num);

    auto query_str = parse_query(unit.query_pb_path);
    const auto res = compiler->compilePlan(query_str);
    if (!res) {
      LOG(ERROR) << "Failed to compile plan: " << res.error().ToString();
      continue;
    }
    auto plan = res.value().first;

    {
      std::ofstream fout("./tmp_physical_plan.pb");
      fout << plan.DebugString();
      fout.close();
    }
    auto param_meta = neug::execution::PlanParser::parse_params_type(plan);
    std::vector<neug::execution::ParamsMap> params_map;
    for (const auto& param : parameters) {
      params_map.emplace_back(deserialize_string_kv_map(param_meta, param));
    }
    auto pipeline = neug::execution::PlanParser::get().parse_execute_pipeline(
        txn.graph().schema(), neug::execution::ContextMeta(), plan);

    std::unique_ptr<neug::execution::OprTimer> best_timer =
        std::make_unique<neug::execution::OprTimer>();
    double best_elapsed = std::numeric_limits<double>::max();
    for (int iter = 0; iter < 3; ++iter) {
      std::unique_ptr<neug::execution::OprTimer> timer =
          std::make_unique<neug::execution::OprTimer>();
      benchmark_iteration(graph, pipeline.value().first, params_map, query_num,
                          outputs, *timer);
      if (timer->elapsed() < best_elapsed) {
        best_timer = std::move(timer);
        best_elapsed = best_timer->elapsed();
      }
    }

    std::cout << "Benchmark " << unit.name
              << " finished, best time: " << best_elapsed
              << " s, avg elapsed: " << best_elapsed / query_num * 1000 * 1000
              << " us\n";
    best_timer->output("", std::cout);
  }

  return 0;
}