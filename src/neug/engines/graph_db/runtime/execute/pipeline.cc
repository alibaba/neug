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

bl::result<Context> ReadPipeline::Execute(
    const GraphReadInterface& graph, Context&& ctx,
    const std::map<std::string, std::string>& params, OprTimer* timer) {
  gs::Status status = gs::Status::OK();
  size_t cur_ind = 0;

  TRY_HANDLE_ALL_WITH_EXCEPTION_CATCHING(
      ret,
      [&]() -> bl::result<Context> {
        TimerUnit tu;
        OprTimer* cur_timer = timer;
        std::unique_ptr<OprTimer> next_timer = nullptr;
        for (size_t i = 0; i < operators_.size(); ++i) {
          cur_ind = i;
          auto& opr = operators_[i];
          if (timer != nullptr) [[unlikely]] {
            tu.start();
          }
          BOOST_LEAF_ASSIGN(
              ctx, opr->Eval(graph, params, std::move(ctx), cur_timer));
          if (timer != nullptr) [[unlikely]] {
            cur_timer->set_name(opr->get_operator_name());
            cur_timer->add_num_tuples(ctx.row_num());
            cur_timer->record(tu);
            if (i + 1 < operators_.size()) {
              next_timer = std::make_unique<OprTimer>();
              cur_timer->set_next(std::move(next_timer));
              cur_timer = cur_timer->next();
            }
          }
        }
        return ctx;
      },
      bl::result<Context>,
      [&](const gs::Status& err) {
        status = err;
        return ctx;
      },
      ctx);

  if (!status.ok()) {
    return bl::new_error(gs::Status(
        status.error_code(), "Execution failed at operator: [" +
                                 operators_[cur_ind]->get_operator_name() +
                                 "], " + status.error_message()));
  }
  return std::move(ret);
}

template <typename GraphInterface>
bl::result<WriteContext> InsertPipeline::Execute(
    GraphInterface& graph, WriteContext&& ctx,
    const std::map<std::string, std::string>& params, OprTimer* timer) {
  gs::Status status = gs::Status::OK();
  size_t cur_ind = 0;

  TRY_HANDLE_ALL_WITH_EXCEPTION_CATCHING(
      ret,
      [&]() -> bl::result<WriteContext> {
        OprTimer* cur_timer = timer;
        std::unique_ptr<OprTimer> next_timer = nullptr;
        TimerUnit tu;
        for (size_t i = 0; i < operators_.size(); ++i) {
          cur_ind = i;
          if (timer != nullptr) [[unlikely]] {
            tu.start();
          }
          auto& opr = operators_[i];
          BOOST_LEAF_ASSIGN(
              ctx, opr->Eval(graph, params, std::move(ctx), cur_timer));
          if (timer != nullptr) [[unlikely]] {
            cur_timer->set_name(opr->get_operator_name());
            cur_timer->add_num_tuples(ctx.row_num());
            cur_timer->record(tu);
            if (i + 1 < operators_.size()) {
              next_timer = std::make_unique<OprTimer>();
              cur_timer->set_next(std::move(next_timer));
              cur_timer = cur_timer->next();
            }
          }
        }
        return ctx;
      },
      bl::result<WriteContext>,
      [&](const gs::Status& err) {
        status = err;
        return WriteContext();
      },
      ctx);

  if (!status.ok()) {
    return bl::new_error(gs::Status(
        status.error_code(), "Execution failed at operator: [" +
                                 operators_[cur_ind]->get_operator_name() +
                                 "], " + status.error_message()));
  }
  return std::move(ret);
}

template bl::result<WriteContext> InsertPipeline::Execute(
    GraphInsertInterface& graph, WriteContext&& ctx,
    const std::map<std::string, std::string>& params, OprTimer* timer);

template bl::result<WriteContext> InsertPipeline::Execute(
    GraphUpdateInterface& graph, WriteContext&& ctx,
    const std::map<std::string, std::string>& params, OprTimer* timer);

bl::result<Context> UpdatePipeline::Execute(
    GraphUpdateInterface& graph, Context&& ctx,
    const std::map<std::string, std::string>& params, OprTimer* timer) {
  gs::Status status = gs::Status::OK();
  size_t cur_ind = 0;
  TRY_HANDLE_ALL_WITH_EXCEPTION_CATCHING(
      ret,
      [&]() -> bl::result<Context> {
        TimerUnit tu;
        OprTimer* cur_timer = timer;
        std::unique_ptr<OprTimer> next_timer = nullptr;
        for (size_t i = 0; i < operators_.size(); ++i) {
          cur_ind = i;
          auto& opr = operators_[i];
          if (timer != nullptr) [[unlikely]] {
            tu.start();
          }
          BOOST_LEAF_ASSIGN(
              ctx, opr->Eval(graph, params, std::move(ctx), cur_timer));
          if (timer != nullptr) [[unlikely]] {
            cur_timer->set_name(opr->get_operator_name());
            cur_timer->add_num_tuples(ctx.row_num());
            cur_timer->record(tu);
            if (i + 1 < operators_.size()) {
              next_timer = std::make_unique<OprTimer>();
              cur_timer->set_next(std::move(next_timer));
              cur_timer = cur_timer->next();
            }
          }
        }
        return ctx;
      },
      bl::result<Context>,
      [&](const gs::Status& err) {
        status = err;
        return ctx;
      },
      ctx);

  if (!status.ok()) {
    return bl::new_error(gs::Status(
        status.error_code(), "Execution failed at operator: [" +
                                 operators_[cur_ind]->get_operator_name() +
                                 "], " + status.error_message()));
  }
  return std::move(ret);
}

bl::result<WriteContext> UpdatePipeline::Execute(
    GraphUpdateInterface& graph, WriteContext&& ctx,
    const std::map<std::string, std::string>& params, OprTimer* timer) {
  return inserts_->Execute(graph, std::move(ctx), params, timer);
}

}  // namespace runtime

}  // namespace gs
