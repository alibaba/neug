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

#include <cstdint>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <unordered_map>

#include "neug/compiler/planner/graph_planner.h"
#include "neug/execution/common/params_map.h"
#include "neug/execution/execute/pipeline.h"
#include "neug/execution/execute/plan_parser.h"
#include "neug/generated/proto/response/response.pb.h"
#include "neug/storages/graph/graph_stats.h"
#include "neug/utils/access_mode.h"

namespace neug {
namespace execution {

struct CacheValue {
  Pipeline pipeline;
  ParamsMetaMap params_type;
  neug::MetaDatas result_schema;
  physical::ExecutionFlag flags;

  CacheValue(Pipeline pipeline, ParamsMetaMap params_type,
             const neug::MetaDatas& result_schema,
             physical::ExecutionFlag flags)
      : pipeline(std::move(pipeline)),
        params_type(std::move(params_type)),
        result_schema(result_schema),
        flags(flags) {}
};

/**
 * @brief A global query cache to store compiled physical plans for queries for
 * a NeugDB instance. It can be shared across multiple ExecutionSlot instances,
 * is not exactly global, since there could be multiple NeugDB instances in a
 * single process.
 *
 * The methods are all thread-safe.
 */
class GlobalQueryCache {
 public:
  GlobalQueryCache(std::shared_ptr<IGraphPlanner> planner)
      : planner_(planner), planning_generation_(0) {}

  result<std::shared_ptr<CacheValue>> Get(const GraphStats& stats,
                                          const std::string& query) {
    const auto planning_generation = stats.planning_generation();
    {
      std::shared_lock<std::shared_mutex> read_lock(mutex_);
      if (planning_generation == planning_generation_) {
        auto iter = cache_.find(query);
        if (iter != cache_.end()) {
          return iter->second;
        }
      }
    }

    GS_AUTO(cache_value, CompileUncached(stats, query));

    std::unique_lock<std::shared_mutex> write_lock(mutex_);
    if (planning_generation < planning_generation_) {
      return cache_value;
    }
    // The request's GraphStats carries the planning generation of its pinned
    // snapshot.
    // Advance monotonically so an older pinned snapshot can never roll the
    // global cache back after a newer generation has been observed.
    if (planning_generation > planning_generation_) {
      planning_generation_ = planning_generation;
      cache_.clear();
    }
    return cache_.emplace(query, cache_value).first->second;
  }

  // Compile against a private graph view without reading or populating the
  // shared cache. Explicit write transactions use this after a successful
  // schema, bulk, or transient mutation changes their planning inputs.
  result<std::shared_ptr<CacheValue>> CompileUncached(
      const GraphStats& stats, const std::string& query) {
    const auto& schema = stats.schema();
    GS_AUTO(plan_result, planner_->compilePlan(query, &schema, stats));
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
    return std::make_shared<CacheValue>(std::move(pipeline_result),
                                        std::move(params_type), sch,
                                        plan_result.first.flag());
  }

 private:
  std::shared_ptr<IGraphPlanner> planner_;
  uint64_t planning_generation_;
  std::unordered_map<std::string, std::shared_ptr<CacheValue>> cache_;
  mutable std::shared_mutex mutex_;
};

/**
 * One local query cache for each ExecutionSlot.
 */
class LocalQueryCache {
 public:
  LocalQueryCache(std::shared_ptr<GlobalQueryCache> global_cache)
      : global_cache_(global_cache) {}
  ~LocalQueryCache() = default;
  result<std::shared_ptr<CacheValue>> Get(const GraphStats& stats,
                                          const std::string& query) {
    const auto planning_generation = stats.planning_generation();
    // An ExecutionSlot is leased exclusively, so its local cache only needs
    // plans for one planning generation. Keeping the generation outside the
    // map avoids adding it to every query key.
    if (planning_generation_ != planning_generation) {
      cache_.clear();
      planning_generation_ = planning_generation;
    }
    auto iter = cache_.find(query);
    if (iter != cache_.end()) {
      return iter->second;
    }
    GS_AUTO(cache_value_res, global_cache_->Get(stats, query));
    cache_.emplace(query, cache_value_res);
    return cache_value_res;
  }

  result<std::shared_ptr<CacheValue>> CompileUncached(
      const GraphStats& stats, const std::string& query) {
    return global_cache_->CompileUncached(stats, query);
  }

 private:
  std::shared_ptr<GlobalQueryCache> global_cache_;
  uint64_t planning_generation_{0};
  std::unordered_map<std::string, std::shared_ptr<CacheValue>> cache_;
};
}  // namespace execution
}  // namespace neug
