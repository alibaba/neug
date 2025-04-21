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
#include <boost/leaf.hpp>
#include "src/engines/graph_db/database/graph_db.h"
#include "src/engines/graph_db/runtime/common/graph_interface.h"
#include "src/engines/graph_db/runtime/common/leaf_utils.h"
#include "src/engines/graph_db/runtime/utils/opr_timer.h"
#include "src/main/query_result.h"
#include "src/proto_generated_gie/physical.pb.h"
#include "src/proto_generated_gie/results.pb.h"
#include "src/utils/result.h"

namespace gs {

class QueryProcessor {
 public:
  QueryProcessor(GraphDB& db, int32_t max_num_threads)
      : db_(db), max_num_threads_(max_num_threads) {}

  Result<results::CollectiveResults> execute(const physical::PhysicalPlan& plan,
                                             int32_t num_threads = 0);

 private:
  GraphDB& db_;
  int32_t max_num_threads_;
  runtime::OprTimer timer_;
};

}  // namespace gs

#endif  // SRC_MAIN_QUERY_PROCESS_H_
