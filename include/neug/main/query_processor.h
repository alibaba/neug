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

#ifndef INCLUDE_NEUG_MAIN_QUERY_PROCESSOR_H_
#define INCLUDE_NEUG_MAIN_QUERY_PROCESSOR_H_

#include <glog/logging.h>

#include <string>

#include "neug/execution/utils/opr_timer.h"
#include "neug/generated/proto/plan/physical.pb.h"
#include "neug/generated/proto/plan/results.pb.h"
#include "neug/main/query_result.h"
#include "neug/storages/graph/graph_interface.h"
#include "neug/utils/allocators.h"
#include "neug/utils/result.h"

namespace gs {

class NeugDB;

class QueryProcessor {
 public:
  QueryProcessor(NeugDB& db, Allocator& alloc, int32_t max_num_threads,
                 bool is_read_only = false)
      : db_(db),
        allocator_(alloc),
        max_num_threads_(max_num_threads),
        is_read_only_(is_read_only) {}

  result<results::CollectiveResults> execute(const physical::PhysicalPlan& plan,
                                             int32_t num_threads = 0);

 private:
  result<results::CollectiveResults> execute_admin(
      const physical::AdminPlan& admin_plan, int32_t num_threads);

  result<results::CollectiveResults> execute_read_only(
      const physical::PhysicalPlan& plan, int32_t num_threads);

  result<results::CollectiveResults> execute_read_write(
      const physical::PhysicalPlan& plan, int32_t num_threads);

  result<results::CollectiveResults> execute_ddl(
      const physical::DDLPlan& ddl_plan, int32_t num_threads);

  NeugDB& db_;
  Allocator& allocator_;
  int32_t max_num_threads_;
  bool is_read_only_ = false;
};

}  // namespace gs

#endif  // INCLUDE_NEUG_MAIN_QUERY_PROCESSOR_H_
