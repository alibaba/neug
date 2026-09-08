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
#include <algorithm>
#include <exception>
#include <ostream>
#include <sstream>

#include "neug/execution/common/context.h"
#include "neug/utils/likely.h"
#include "neug/utils/result.h"

namespace neug {
namespace execution {
class OprTimer;

namespace {
Status operator_error(const Status& error, const std::string& name) {
  return Status(error.error_code(), "Execution failed at operator: [" + name +
                                        "], " + error.error_message());
}

// Pulling upstream happens inside downstream Eval/Next. Charge that time only
// to its producer, rather than counting it twice in PROFILE.
class StreamTimerScope {
 public:
  StreamTimerScope(OprTimer* timer, std::shared_ptr<double> charged)
      : timer_(timer), charged_(std::move(charged)), before_(*charged_) {
    clock_.start();
  }
  ~StreamTimerScope() {
    if (timer_) {
      double elapsed = std::max(0.0, clock_.elapsed() - (*charged_ - before_));
      timer_->add_elapsed(elapsed);
      *charged_ += elapsed;
    }
  }

 private:
  OprTimer* timer_;
  std::shared_ptr<double> charged_;
  double before_;
  TimerUnit clock_;
};
}  // namespace

neug::result<Context> Pipeline::Execute(IStorageInterface& graph, Context&& ctx,
                                        const ParamsMap& params,
                                        OprTimer* timer) {
  GS_AUTO(stream, ExecuteStream(graph, stream_from_context(std::move(ctx)),
                                params, timer));
  return materialize(std::move(stream));
}

result<Stream<ContextChunk>> Pipeline::ExecuteStream(
    IStorageInterface& graph, Stream<ContextChunk> stream,
    const ParamsMap& params, OprTimer* timer) {
  auto charged = std::make_shared<double>(0.0);
  auto* current_timer = timer;
  for (size_t i = 0; i < operators_.size(); ++i) {
    const auto name = operators_[i]->get_operator_name();
    if (current_timer) {
      current_timer->set_name(name);
    }
    auto invoke = [&]() -> result<Stream<ContextChunk>> {
      StreamTimerScope scope(current_timer, charged);
      result<Stream<ContextChunk>> output = Stream<ContextChunk>();
      TRY_HANDLE_ALL_WITH_EXCEPTION(
          result<Stream<ContextChunk>>,
          [&]() {
            return operators_[i]->Eval(graph, params, std::move(stream),
                                       current_timer);
          },
          [&](const Status& error) { output = tl::unexpected(error); },
          [&](result<Stream<ContextChunk>>&& result) {
            output = std::move(result);
          });
      return output;
    };
    auto output = invoke();
    if (!output) {
      return tl::unexpected(operator_error(output.error(), name));
    }
    auto tags = std::move(output->tag_ids);
    auto producer = std::make_shared<Stream<ContextChunk>>(std::move(*output));
    stream = Stream<ContextChunk>(
        [producer, current_timer, charged,
         name]() -> Stream<ContextChunk>::NextResult {
          StreamTimerScope scope(current_timer, charged);
          auto next = producer->Next();
          if (!next) {
            return tl::unexpected(operator_error(next.error(), name));
          }
          if (current_timer && *next) {
            const auto& batch = **next;
            auto rows = batch.row_num();
            current_timer->add_num_tuples(rows);
          }
          return next;
        },
        std::move(tags));
    if (current_timer && i + 1 < operators_.size()) {
      current_timer->set_next(std::make_unique<OprTimer>());
      current_timer = current_timer->next();
    }
  }
  return std::move(stream);
}

neug::result<std::unique_ptr<OprTimer>> Pipeline::explain_tree(
    IStorageInterface& graph, const ParamsMap& params) {
  std::unique_ptr<OprTimer> root = nullptr;
  OprTimer* current = nullptr;

  for (size_t i = 0; i < operators_.size(); ++i) {
    auto timer_node = std::make_unique<OprTimer>();
    timer_node->set_name(operators_[i]->get_operator_name());

    // Add current timer_node to the linked list
    if (!root) {
      root = std::move(timer_node);
      current = root.get();
    } else {
      auto next = std::move(timer_node);
      current->set_next(std::move(next));
      current = current->next();
    }
  }

  // process children for each operator
  if (root) {
    OprTimer* op_timer = root.get();
    for (size_t i = 0; i < operators_.size(); ++i) {
      if (op_timer) {
        operators_[i]->build_explain_children(op_timer, params, graph);
        op_timer = op_timer->next();
      }
    }
  }

  return root;
}

}  // namespace execution

}  // namespace neug
