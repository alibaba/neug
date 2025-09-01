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

#include "neug/engines/graph_db/runtime/execute/pipeline.h"

#include <glog/logging.h>
#include <exception>
#include <ostream>

#include "neug/engines/graph_db/runtime/common/context.h"
#include "neug/utils/result.h"

namespace gs {
namespace runtime {
class OprTimer;

gs::result<Context> ReadPipeline::Execute(
    const GraphReadInterface& graph, Context&& ctx,
    const std::map<std::string, std::string>& params, OprTimer* timer) {
  gs::Status status = Status::OK();
  TimerUnit tu;
  OprTimer* cur_timer = timer;
  std::unique_ptr<OprTimer> next_timer = nullptr;
  for (size_t i = 0; i < operators_.size(); ++i) {
    if (timer != nullptr) [[unlikely]] {
      tu.start();
    }
    TRY_HANDLE_ALL_WITH_EXCEPTION(
        gs::result<Context>,
        [&]() -> gs::result<Context> {
          auto ret = operators_[i]->Eval(graph, params, std::move(ctx), timer);
          if (!ret) {
            return ret;
          }
          if (timer != nullptr) [[unlikely]] {
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
        [&](const gs::Status& err) {
          status = gs::Status(err.error_code(),
                              "Execution failed at operator: [" +
                                  operators_[i]->get_operator_name() + "], " +
                                  err.error_message());
        },
        [&ctx](gs::result<Context>&& res) { ctx = std::move(res.value()); });
    if (!status.ok()) {
      RETURN_ERROR(status);
    }
  }
  return ctx;
}

template <typename GraphInterface>
gs::result<WriteContext> InsertPipeline::Execute(
    GraphInterface& graph, WriteContext&& ctx,
    const std::map<std::string, std::string>& params, OprTimer* timer) {
  OprTimer* cur_timer = timer;
  std::unique_ptr<OprTimer> next_timer = nullptr;
  TimerUnit tu;
  gs::Status status = Status::OK();
  for (size_t i = 0; i < operators_.size(); ++i) {
    if (timer != nullptr) [[unlikely]] {
      tu.start();
    }
    TRY_HANDLE_ALL_WITH_EXCEPTION(
        gs::result<Context>,
        [&]() -> gs::result<WriteContext> {
          auto ret = operators_[i]->Eval(graph, params, std::move(ctx), timer);
          if (!ret) {
            return ret;
          }
          if (timer != nullptr) [[unlikely]] {
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
        [&](const gs::Status& err) {
          status = gs::Status(err.error_code(),
                              "Execution failed at operator: [" +
                                  operators_[0]->get_operator_name() + "], " +
                                  err.error_message());
        },
        [&ctx](gs::result<WriteContext>&& res) {
          ctx = std::move(res.value());
        });
    if (!status.ok()) {
      RETURN_ERROR(status);
    }
  }
  return ctx;
}

template gs::result<WriteContext> InsertPipeline::Execute(
    GraphInsertInterface& graph, WriteContext&& ctx,
    const std::map<std::string, std::string>& params, OprTimer* timer);

template gs::result<WriteContext> InsertPipeline::Execute(
    GraphUpdateInterface& graph, WriteContext&& ctx,
    const std::map<std::string, std::string>& params, OprTimer* timer);

gs::result<Context> UpdatePipeline::Execute(
    GraphUpdateInterface& graph, Context&& ctx,
    const std::map<std::string, std::string>& params, OprTimer* timer) {
  gs::Status status = Status::OK();
  TimerUnit tu;
  OprTimer* cur_timer = timer;
  std::unique_ptr<OprTimer> next_timer = nullptr;
  for (size_t i = 0; i < operators_.size(); ++i) {
    if (timer != nullptr) [[unlikely]] {
      tu.start();
    }
    TRY_HANDLE_ALL_WITH_EXCEPTION(
        gs::result<Context>,
        [&]() -> gs::result<Context> {
          auto ret = operators_[i]->Eval(graph, params, std::move(ctx), timer);
          if (!ret) {
            return ret;
          }
          if (timer != nullptr) [[unlikely]] {
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
        [&](const gs::Status& err) {
          LOG(INFO) << "Failed at operator index " << i;
          status = gs::Status(err.error_code(),
                              "Execution failed at operator: [" +
                                  operators_[i]->get_operator_name() + "], " +
                                  err.error_message());
        },
        [&ctx](gs::result<Context>&& res) { ctx = std::move(res.value()); });
    if (!status.ok()) {
      RETURN_ERROR(status);
    }
  }

  return ctx;
}

gs::result<WriteContext> UpdatePipeline::Execute(
    GraphUpdateInterface& graph, WriteContext&& ctx,
    const std::map<std::string, std::string>& params, OprTimer* timer) {
  return inserts_->Execute(graph, std::move(ctx), params, timer);
}

}  // namespace runtime

}  // namespace gs
