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

#include "neug/compiler/planner/graph_planner.h"
#include "neug/execution/common/params_map.h"
#include "neug/execution/execute/pipeline.h"
#include "neug/execution/execute/plan_parser.h"
#include "neug/generated/proto/response/response.pb.h"
#include "neug/utils/access_mode.h"

namespace neug {
namespace execution {

struct CacheValue {
  Pipeline pipeline;
  ParamsMetaMap params_type;
  neug::MetaDatas result_schema;
  physical::ExecutionFlag flags;

  CacheValue(Pipeline pipeline, ParamsMetaMap params_type,
             const neug::MetaDatas& result_schema,  // ← 注意：const&
             physical::ExecutionFlag flags)
      : pipeline(std::move(pipeline)),
        params_type(std::move(params_type)),
        result_schema(result_schema),
        flags(flags) {}
};

/**
 * @brief A global query cache to store compiled physical plans for queries for
 * a NeugDB instance. It could be shared across multiple NeugDBSession, but it
 * is not exactly global, since there could be multiple NeugDB instances in a
 * single process.
 *
 * The methods are all thread-safe.
 */
class GlobalQueryCache {
 public:
  GlobalQueryCache(std::shared_ptr<IGraphPlanner> planner)
      : planner_(planner), version_(0) {
    cache_.clear();
  }

  uint64_t version() const { return version_.load(); }

  result<std::shared_ptr<CacheValue>> Get(const Schema& schema,
                                          const std::string& query) {
    {
      std::shared_lock<std::shared_mutex> read_lock(mutex_);
      auto iter = cache_.find(query);
      if (iter != cache_.end()) {
        return iter->second;
      }
    }
    GS_AUTO(plan_result, planner_->compilePlan(query));
    ContextMeta ctx_meta;
    GS_AUTO(pipeline_result_pair, PlanParser::get().parse_execute_pipeline(
                                      schema, ctx_meta, plan_result.first));
    auto pipeline_result = std::move(pipeline_result_pair.first);

    const auto& rt_names = parse_result_schema_column_names(plan_result.second);

    neug::MetaDatas sch;
    for (size_t i = 0; i < rt_names.size(); ++i) {
      const auto& rt_name = rt_names[i];
      sch.add_name(rt_name);
    }

    auto params_type =
        execution::PlanParser::parse_params_type(plan_result.first);
    {
      std::unique_lock<std::shared_mutex> write_lock(mutex_);
      auto iter = cache_.find(query);
      if (iter != cache_.end()) {
        return iter->second;
      }
      cache_.emplace(query,
                     std::make_shared<CacheValue>(std::move(pipeline_result),
                                                  std::move(params_type), sch,
                                                  plan_result.first.flag()));
      return cache_.at(query);
    }
  }

  void clear(const YAML::Node& schema, const std::string& statistics = "") {
    std::unique_lock<std::shared_mutex> write_lock(mutex_);
    version_.fetch_add(1);
    cache_.clear();
    if (!schema.IsNull()) {
      planner_->update_meta(schema);
    }
    if (!statistics.empty()) {
      planner_->update_statistics(statistics);
    }
  }

 private:
  GlobalQueryCache() : version_(0) {}
  std::shared_ptr<IGraphPlanner> planner_;
  std::atomic<uint64_t> version_;
  std::unordered_map<std::string, std::shared_ptr<CacheValue>> cache_;
  std::shared_mutex mutex_;
};

/**
 * Only used in TP mode, one local query cache for each NeugDBSession.
 */
class LocalQueryCache {
 public:
  LocalQueryCache(std::shared_ptr<GlobalQueryCache> global_cache)
      : global_cache_(global_cache), version_(global_cache_->version()) {}
  ~LocalQueryCache() = default;
  result<std::shared_ptr<CacheValue>> Get(const Schema& schema,
                                          const std::string& query) {
    if (version_ != global_cache_->version()) {
      cache_.clear();
      version_ = global_cache_->version();
    }
    auto iter = cache_.find(query);
    if (iter != cache_.end()) {
      return iter->second;
    }
    GS_AUTO(cache_value_res, global_cache_->Get(schema, query));
    cache_.emplace(query, cache_value_res);
    return cache_.at(query);
  }

  void clearGlobalCache(const YAML::Node& schema,
                        const std::string& statistics = "") {
    global_cache_->clear(schema, statistics);
    version_ = global_cache_->version();
    cache_.clear();
  }

 private:
  std::shared_ptr<GlobalQueryCache> global_cache_;
  uint64_t version_;
  std::unordered_map<std::string, std::shared_ptr<CacheValue>> cache_;
};
}  // namespace execution
}  // namespace neug