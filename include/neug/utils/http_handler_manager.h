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
#pragma once

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <memory>
#include <string>

#include "neug/utils/service_utils.h"

namespace gs {}
namespace server {

struct ServiceConfig {
  enum class ShardingMode { EXCLUSIVE, COOPERATIVE };
  static constexpr const uint32_t DEFAULT_SHARD_NUM = 1;
  static constexpr const uint32_t DEFAULT_QUERY_PORT = 10000;
  static constexpr const uint32_t DEFAULT_VERBOSE_LEVEL = 0;
  static constexpr const uint32_t DEFAULT_LOG_LEVEL =
      0;  // 0 = INFO, 1 = WARNING, 2 = ERROR, 3 = FATAL
  static constexpr const ShardingMode DEFAULT_SHARDING_MODE =
      ShardingMode::EXCLUSIVE;
  static constexpr const uint32_t DEFAULT_MAX_CONTENT_LENGTH =
      1024 * 1024 * 1024;  // 1GB
  static constexpr const char* DEFAULT_WAL_URI =
      "{GRAPH_DATA_DIR}/wal";  // By default we will use the wal directory in
                               // the graph data directory. The {GRAPH_DATA_DIR}
                               // is a placeholder, which will be replaced by
                               // the actual graph data directory.

  // Those has default value
  uint32_t query_port;
  uint32_t shard_num;
  uint32_t memory_level;
  bool enable_adhoc_handler;  // Whether to enable adhoc handler.
  bool dpdk_mode;
  bool enable_thread_resource_pool;
  unsigned external_thread_num;
  // verbose log level. should be a int
  // could also be set from command line: GLOG_v={}.
  // If we found GLOG_v in the environment, we will at the first place.
  int log_level;
  int verbose_level;
  ShardingMode sharding_mode;  // exclusive or cooperative. With exclusive mode,
                               // we will reserve one shard for only processing
                               // admin requests, and the other shards for
                               // processing query requests. With cooperative
                               // mode, all shards will process both admin and
                               // query requests. With only one shard available,
                               // the sharding mode must be cooperative.

  // Those has not default value
  std::string engine_config_path;  // used for codegen.
  std::string wal_uri;             // The uri of the wal storage.
  std::string host_str;

  ServiceConfig()
      : query_port(DEFAULT_QUERY_PORT),
        shard_num(DEFAULT_SHARD_NUM),
        enable_adhoc_handler(false),
        dpdk_mode(false),
        enable_thread_resource_pool(true),
        external_thread_num(2),
        log_level(DEFAULT_LOG_LEVEL),
        verbose_level(DEFAULT_VERBOSE_LEVEL),
        sharding_mode(DEFAULT_SHARDING_MODE),
        wal_uri(DEFAULT_WAL_URI),
        host_str("127.0.0.1") {}

  void set_sharding_mode(const std::string& mode) {
    VLOG(10) << "Set sharding mode: " << mode;
    if (mode == "exclusive") {
      sharding_mode = ShardingMode::EXCLUSIVE;
    } else if (mode == "cooperative") {
      sharding_mode = ShardingMode::COOPERATIVE;
    } else {
      LOG(FATAL) << "Invalid sharding mode: " << mode;
    }
  }

  int32_t get_exclusive_shard_id() const {
    return sharding_mode == ShardingMode::EXCLUSIVE ? shard_num - 1 : -1;
  }

  int32_t get_cooperative_shard_num() const {
    return sharding_mode == ShardingMode::EXCLUSIVE
               ? std::max((int32_t) shard_num - 1, 1)
               : shard_num;  // shard_num >= 1
  }
};

class IHttpHandlerManager {
 public:
  virtual ~IHttpHandlerManager() = default;
  virtual void Init(const ServiceConfig& config) = 0;
  virtual std::string Start() = 0;
  virtual void Stop() = 0;
  virtual void RunAndWaitForExit() = 0;
  virtual bool IsRunning() const = 0;
};
}  // namespace server

namespace YAML {

template <>
struct convert<server::ServiceConfig> {
  // The encode function is not defined, since we don't need to write the config
  static bool decode(const Node& config,
                     server::ServiceConfig& service_config) {
    if (!config.IsMap()) {
      LOG(ERROR) << "ServiceConfig should be a map";
      return false;
    }
    // log level: INFO=0, WARNING=1, ERROR=2, FATAL=3
    if (config["log_level"]) {
      auto level_str = gs::toUpper(config["log_level"].as<std::string>());

      if (level_str == "INFO") {
        service_config.log_level = 0;
      } else if (level_str == "WARNING") {
        service_config.log_level = 1;
      } else if (level_str == "ERROR") {
        service_config.log_level = 2;
      } else if (level_str == "FATAL") {
        service_config.log_level = 3;
      } else {
        LOG(ERROR) << "Unsupported log level: " << level_str;
        return false;
      }
    } else {
      LOG(INFO) << "log_level not found, use default value "
                << service_config.log_level;
    }

    // verbose log level
    if (config["verbose_level"]) {
      service_config.verbose_level = config["verbose_level"].as<int>();
    } else {
      LOG(INFO) << "verbose_level not found, use default value "
                << service_config.verbose_level;
    }

    auto engine_node = config["compute_engine"];
    if (engine_node) {
      auto engine_type = engine_node["type"];
      if (engine_type) {
        auto engine_type_str = engine_type.as<std::string>();
        if (engine_type_str != "brpc" && engine_type_str != "Brpc") {
          LOG(ERROR) << "compute_engine type should be brpc, found: "
                     << engine_type_str;
          return false;
        }
      }
      auto shard_num_node = engine_node["thread_num_per_worker"];
      if (shard_num_node) {
        service_config.shard_num = shard_num_node.as<uint32_t>();
      } else {
        LOG(INFO) << "shard_num not found, use default value "
                  << service_config.shard_num;
      }

      if (engine_node["wal_uri"]) {
        service_config.wal_uri = engine_node["wal_uri"].as<std::string>();
      }
    } else {
      LOG(ERROR) << "Fail to find compute_engine configuration";
      return false;
    }
    auto http_service_node = config["http_service"];
    if (http_service_node) {
      auto query_port_node = http_service_node["query_port"];
      if (query_port_node) {
        service_config.query_port = query_port_node.as<uint32_t>();
      } else {
        LOG(INFO) << "query_port not found, use default value "
                  << service_config.query_port;
      }

      if (http_service_node["sharding_mode"]) {
        auto sharding_mode =
            http_service_node["sharding_mode"].as<std::string>();
        if (sharding_mode != "exclusive" && sharding_mode != "cooperative") {
          LOG(ERROR) << "Unsupported sharding mode: " << sharding_mode;
          return false;
        }
        if (sharding_mode == "exclusive" && service_config.shard_num == 1) {
          LOG(ERROR) << "exclusive sharding mode requires at least 2 shards";
          return false;
        }
        service_config.set_sharding_mode(sharding_mode);
        VLOG(1) << "sharding_mode: " << sharding_mode;
      }
    } else {
      LOG(ERROR) << "Fail to find http_service configuration";
      return false;
    }

    return true;
  }
};
}  // namespace YAML
