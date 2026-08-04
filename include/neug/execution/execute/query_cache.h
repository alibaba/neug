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

#include <atomic>
#include <cstdint>
#include <mutex>
#include <optional>
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
  physical::ExplainMode explain_mode = physical::ExplainMode::NONE;

  CacheValue(Pipeline pipeline, ParamsMetaMap params_type,
             const neug::MetaDatas& result_schema,
             physical::ExecutionFlag flags,
             physical::ExplainMode explain_mode = physical::ExplainMode::NONE)
      : pipeline(std::move(pipeline)),
        params_type(std::move(params_type)),
        result_schema(result_schema),
        flags(flags),
        explain_mode(explain_mode) {}
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
      : planner_(planner), schema_generation_(0), cache_epoch_(0) {}

  // Monotonic epoch for invalidations that do not change logical schema, such
  // as a bulk load changing planner cardinalities.
  uint64_t cache_epoch() const { return cache_epoch_.load(); }

  result<std::shared_ptr<CacheValue>> Get(const GraphStats& stats,
                                          uint64_t schema_generation,
                                          const std::string& query) {
    bool eligible_for_global_write_back = false;
    uint64_t observed_cache_epoch = 0;
    {
      std::shared_lock<std::shared_mutex> read_lock(mutex_);
      eligible_for_global_write_back = schema_generation >= schema_generation_;
      if (schema_generation == schema_generation_) {
        auto iter = cache_.find(query);
        if (iter != cache_.end()) {
          return iter->second;
        }
      }
      if (eligible_for_global_write_back) {
        observed_cache_epoch = cache_epoch_.load();
      }
    }

    GS_AUTO(cache_value, CompileUncached(stats, query));
    if (!eligible_for_global_write_back) {
      return cache_value;
    }
    {
      std::unique_lock<std::shared_mutex> write_lock(mutex_);
      if (schema_generation < schema_generation_) {
        return cache_value;
      }
      // A request carries the generation of its pinned, published snapshot.
      // Advance monotonically so an older pinned snapshot can never roll the
      // global cache back after a newer generation has been observed.
      if (schema_generation > schema_generation_) {
        // Local caches observe schema_generation directly. Do not bump the
        // cache epoch here, or this compilation would invalidate itself.
        schema_generation_ = schema_generation;
        cache_.clear();
      }
      // A schema advance or cache-wide clear during compilation makes this
      // plan ineligible for global write-back.
      if (observed_cache_epoch != cache_epoch_.load()) {
        return cache_value;
      }
      return cache_.emplace(query, cache_value).first->second;
    }
  }

  void clear() {
    std::unique_lock<std::shared_mutex> write_lock(mutex_);
    cache_epoch_.fetch_add(1);
    cache_.clear();
  }

 private:
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
    auto explain_mode = plan_result.first.explain_mode();
    return std::make_shared<CacheValue>(std::move(pipeline_result),
                                        std::move(params_type), sch,
                                        plan_result.first.flag(), explain_mode);
  }

  std::shared_ptr<IGraphPlanner> planner_;
  uint64_t schema_generation_;
  std::atomic<uint64_t> cache_epoch_;
  std::unordered_map<std::string, std::shared_ptr<CacheValue>> cache_;
  mutable std::shared_mutex mutex_;
};

/**
 * One local query cache for each ExecutionSlot.
 */
class LocalQueryCache {
 public:
  LocalQueryCache(std::shared_ptr<GlobalQueryCache> global_cache)
      : global_cache_(global_cache),
        observed_cache_epoch_(global_cache_->cache_epoch()) {}
  ~LocalQueryCache() = default;
  result<std::shared_ptr<CacheValue>> Get(const GraphStats& stats,
                                          uint64_t schema_generation,
                                          const std::string& query) {
    const auto global_cache_epoch = global_cache_->cache_epoch();
    if (observed_cache_epoch_ != global_cache_epoch) {
      cache_.clear();
      observed_cache_epoch_ = global_cache_epoch;
      schema_generation_.reset();
    }
    // An ExecutionSlot is leased exclusively, so its local cache only needs
    // plans for the most recently observed schema generation. Keeping the
    // generation outside the map avoids copying query on every cache lookup.
    if (!schema_generation_.has_value() ||
        schema_generation_.value() != schema_generation) {
      cache_.clear();
      schema_generation_ = schema_generation;
    }
    auto iter = cache_.find(query);
    if (iter != cache_.end()) {
      return iter->second;
    }
    GS_AUTO(cache_value_res,
            global_cache_->Get(stats, schema_generation, query));
    cache_.emplace(query, cache_value_res);
    return cache_value_res;
  }

  void clearGlobalCache() {
    global_cache_->clear();
    observed_cache_epoch_ = global_cache_->cache_epoch();
    cache_.clear();
    schema_generation_.reset();
  }

 private:
  std::shared_ptr<GlobalQueryCache> global_cache_;
  uint64_t observed_cache_epoch_;
  std::optional<uint64_t> schema_generation_;
  std::unordered_map<std::string, std::shared_ptr<CacheValue>> cache_;
};
}  // namespace execution
}  // namespace neug
