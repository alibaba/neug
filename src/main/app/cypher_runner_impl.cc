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

#include "neug/main/app/cypher_runner_impl.h"

#include <glog/logging.h>

#include <memory>
#include <ostream>
#include <utility>
#include <vector>

#include "neug/execution/common/context.h"
#include "neug/execution/common/graph_interface.h"
#include "neug/execution/common/operators/retrieve/sink.h"
#include "neug/execution/execute/pipeline.h"
#include "neug/execution/execute/plan_parser.h"
#include "neug/execution/utils/opr_timer.h"
#include "neug/main/app/cypher_app_utils.h"
#include "neug/main/neug_db.h"
#include "neug/main/neug_db_session.h"
#include "neug/storages/graph/schema.h"
#include "neug/transaction/insert_transaction.h"
#include "neug/transaction/read_transaction.h"
#include "neug/transaction/update_transaction.h"
#include "neug/utils/app_utils.h"
#ifdef USE_SYSTEM_PROTOBUF
#include "neug/generated/proto/plan/physical.pb.h"
#else
#include "neug/utils/proto/plan/physical.pb.h"
#endif

namespace gs {
namespace runtime {

bool CypherRunnerImpl::gen_plan(const NeugDB& db, const std::string& query,
                                std::string& plan_str) {
  auto& plan_cache = plan_cache_;
  const std::string statistics = db.work_dir() + "/statistics.json";
  const std::string& compiler_yaml = db.work_dir() + "/graph.yaml";
  const std::string& tmp_dir = db.work_dir() + "/runtime/tmp/";
  const auto& compiler_path = db.schema().get_compiler_path();

  if (plan_cache.get(query, plan_str)) {
    return true;
  }

  physical::PhysicalPlan plan;
  {
    // avoid multiple threads to generate plan for the same query
    std::unique_lock<std::mutex> lock(mutex_);
    if (plan_cache.get(query, plan_str)) {
      return true;
    }
    if (!generate_plan(query, statistics, compiler_path, compiler_yaml, tmp_dir,
                       plan)) {
      LOG(ERROR) << "Generate plan failed for query: " << query;
      return false;
    }
    plan_str = plan.SerializeAsString();
    plan_cache.put(query, plan_str);
  }

  return true;
}

CypherRunnerImpl::CypherRunnerImpl() : plan_cache_() {}

CypherRunnerImpl& CypherRunnerImpl::get() {
  static CypherRunnerImpl runner;
  return runner;
}

const PlanCache& CypherRunnerImpl::get_plan_cache() const {
  return plan_cache_;
}

void CypherRunnerImpl::clear_cache() { plan_cache_.plan_cache.clear(); }

}  // namespace runtime
}  // namespace gs