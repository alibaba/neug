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

#include <glog/logging.h>

#include <map>
#include <shared_mutex>
#include <string>

#include "neug/compiler/planner/graph_planner.h"
#include "neug/execution/common/params_map.h"
#include "neug/execution/common/types/value.h"
#include "neug/execution/execute/query_cache.h"
#include "neug/execution/utils/opr_timer.h"
#include "neug/generated/proto/plan/physical.pb.h"
#include "neug/main/query_result.h"
#include "neug/storages/graph/graph_interface.h"
#include "neug/utils/access_mode.h"
#include "neug/utils/allocators.h"
#include "neug/utils/result.h"

namespace neug {

class QueryProcessor {
 public:
  QueryProcessor(
      PropertyGraph& graph, std::shared_ptr<IGraphPlanner> planner,
      std::shared_ptr<execution::GlobalQueryCache> global_query_cache,
      Allocator& alloc, int32_t max_num_threads, bool is_read_only = false)
      : g_(graph),
        planner_(planner),
        global_query_cache_(global_query_cache),
        allocator_(alloc),
        max_num_threads_(max_num_threads),
        is_read_only_(is_read_only) {}

  result<QueryResult> execute(const std::string& query_string,
                              const std::string& access_mode,
                              const execution::ParamsMap& parameters = {},
                              int32_t num_threads = 0);

  result<QueryResult> execute(const std::string& query_string,
                              const std::string& access_mode,
                              const rapidjson::Value& parameters_json,
                              int32_t num_threads = 0);

 private:
  result<std::pair<AccessMode, std::shared_ptr<execution::CacheValue>>>
  check_and_retrieve_pipeline(const std::string& query_string,
                              const std::string& access_mode,
                              int32_t num_threads);

  result<QueryResult> execute_internal(
      const std::string& query_string,
      std::shared_ptr<execution::CacheValue> cache_value,
      AccessMode access_mode, const execution::ParamsMap& parameters = {},
      int32_t num_threads = 0);

  bool need_exclusive_lock(AccessMode access_mode);

  void update_compiler_meta_if_needed(const physical::ExecutionFlag& flags,
                                      AccessMode mode);

  PropertyGraph& g_;
  std::shared_ptr<IGraphPlanner> planner_;
  std::shared_ptr<execution::GlobalQueryCache> global_query_cache_;
  Allocator& allocator_;
  int32_t max_num_threads_;
  bool is_read_only_ = false;

  std::shared_mutex mutex_;
};

}  // namespace neug
