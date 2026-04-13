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

#include "neug/execution/execute/pipeline.h"

#include <glog/logging.h>
#include <exception>
#include <ostream>

#include "neug/execution/common/context.h"
#include "neug/utils/likely.h"
#include "neug/utils/result.h"

namespace neug {
namespace execution {
class OprTimer;

neug::result<Context> Pipeline::Execute(IStorageInterface& graph, Context&& ctx,
                                        const ParamsMap& params,
                                        OprTimer* timer) {
  neug::Status status = Status::OK();
  TimerUnit tu;
  OprTimer* cur_timer = timer;
  std::unique_ptr<OprTimer> next_timer = nullptr;
  for (size_t i = 0; i < operators_.size(); ++i) {
    if (NEUG_UNLIKELY(timer != nullptr)) {
      tu.start();
    }
    TRY_HANDLE_ALL_WITH_EXCEPTION(
        neug::result<Context>,
        [&]() -> neug::result<Context> {
          auto ret =
              operators_[i]->Eval(graph, params, std::move(ctx), cur_timer);
          if (!ret) {
            return ret;
          }
          if (NEUG_UNLIKELY(timer != nullptr)) {
            cur_timer->set_name(operators_[i]->get_operator_name());
            cur_timer->add_num_tuples(ret.value().row_num());
            cur_timer->record(tu);
            if (i + 1 < operators_.size()) {
              next_timer = std::make_unique<OprTimer>();
              cur_timer->set_next(std::move(next_timer));
              cur_timer = cur_timer->next();
            }
          }
          return ret;
        },
        [&](const neug::Status& err) {
          status = neug::Status(err.error_code(),
                                "Execution failed at operator: [" +
                                    operators_[i]->get_operator_name() + "], " +
                                    err.error_message());
        },
        [&ctx](neug::result<Context>&& res) { ctx = std::move(res.value()); });
    if (!status.ok()) {
      RETURN_ERROR(status);
    }
  }
  return ctx;
}

}  // namespace execution

}  // namespace neug
