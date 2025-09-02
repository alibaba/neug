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

#ifndef SRC_MAIN_QUERY_PROCESS_H_
#define SRC_MAIN_QUERY_PROCESS_H_

#include <string>

#include <glog/logging.h>
#include "neug/execution/runtime/common/graph_interface.h"
#include "neug/execution/runtime/utils/opr_timer.h"
#include "neug/main/query_result.h"
#include "neug/transaction/graph_db.h"
#include "neug/utils/leaf_utils.h"
#ifdef USE_SYSTEM_PROTOBUF
#include "neug/generated/proto/plan/physical.pb.h"
#include "neug/generated/proto/plan/results.pb.h"
#else
#include "neug/utils/proto/plan/physical.pb.h"
#include "neug/utils/proto/plan/results.pb.h"
#endif
#include "neug/utils/result.h"

namespace gs {

class QueryProcessor {
 public:
  QueryProcessor(GraphDB& db, int32_t max_num_threads,
                 bool is_read_only = false)
      : db_(db),
        max_num_threads_(max_num_threads),
        is_read_only_(is_read_only) {}

  Result<results::CollectiveResults> execute(const physical::PhysicalPlan& plan,
                                             int32_t num_threads = 0);

 private:
  Result<results::CollectiveResults> execute_read_only(
      const physical::PhysicalPlan& plan, int32_t num_threads);

  Result<results::CollectiveResults> execute_read_write(
      const physical::PhysicalPlan& plan, int32_t num_threads);

  Result<results::CollectiveResults> execute_ddl(
      const physical::DDLPlan& ddl_plan, int32_t num_threads);

  GraphDB& db_;
  int32_t max_num_threads_;
  bool is_read_only_ = false;
};

}  // namespace gs

#endif  // SRC_MAIN_QUERY_PROCESS_H_
