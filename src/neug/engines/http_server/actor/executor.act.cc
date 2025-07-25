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

#include "neug/engines/http_server/actor/executor.act.h"

#include "neug/engines//graph_db_service.h"
#include "neug/engines/graph_db/database/graph_db.h"
#include "neug/engines/graph_db/database/graph_db_operations.h"
#include "neug/engines/graph_db/database/graph_db_session.h"

// Disable class-memaccess warning to facilitate compilation with gcc>7
// https://github.com/Tencent/rapidjson/issues/1700
#pragma GCC diagnostic push
#if defined(__GNUC__) && __GNUC__ >= 8
#pragma GCC diagnostic ignored "-Wclass-memaccess"
#endif
#include <rapidjson/document.h>
#pragma GCC diagnostic pop
#include <seastar/core/print.hh>

namespace server {

executor::~executor() {
  // finalization
  // ...
}

executor::executor(hiactor::actor_base* exec_ctx, const hiactor::byte_t* addr)
    : hiactor::actor(exec_ctx, addr) {
  set_max_concurrency(1);  // set max concurrency for task reentrancy (stateful)
  // initialization
  // ...
}

seastar::future<query_result> executor::run_graph_db_query(
    query_param&& param) {
  auto ret = GraphDBService::get()
                 .graph_db()
                 .GetSession(hiactor::local_shard_id())
                 .Eval(param.content);
  if (!ret.ok()) {
    LOG(ERROR) << "Eval failed: " << ret.status().error_message();
    return seastar::make_exception_future<query_result>(
        "Query failed: " + ret.status().error_message());
  }

  auto result = ret.value();
  seastar::sstring content(result.data(), result.size());
  return seastar::make_ready_future<query_result>(std::move(content));
}

}  // namespace server
