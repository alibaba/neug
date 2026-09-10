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
  StreamTimerScope(OprTimer& timer, double& charged)
      : timer_(timer), charged_(charged), before_(charged) {
    clock_.start();
  }
  ~StreamTimerScope() {
    double elapsed = std::max(0.0, clock_.elapsed() - (charged_ - before_));
    timer_.add_elapsed(elapsed);
    charged_ += elapsed;
  }

 private:
  OprTimer& timer_;
  double& charged_;
  double before_;
  TimerUnit clock_;
};
}  // namespace

neug::result<Context> Pipeline::Execute(IStorageInterface& graph, Context&& ctx,
                                        const ParamsMap& params,
                                        OprTimer* timer) {
  auto stream =
      ExecuteStream(graph, stream_from_context(std::move(ctx)), params, timer);
  return materialize(std::move(stream));
}

Stream<ContextChunk> Pipeline::ExecuteStream(IStorageInterface& graph,
                                             Stream<ContextChunk> stream,
                                             const ParamsMap& params,
                                             OprTimer* timer) {
  auto charged = timer ? std::make_shared<double>(0.0) : nullptr;
  auto* current_timer = timer;
  for (size_t i = 0; i < operators_.size(); ++i) {
    const auto name = operators_[i]->get_operator_name();
    if (current_timer) {
      current_timer->set_name(name);
    }
    auto output =
        operators_[i]->Eval(graph, params, std::move(stream), current_timer);
    auto metadata = output.metadata();
    auto producer = std::make_shared<Stream<ContextChunk>>(std::move(output));
    auto pull = [producer, name]() -> Stream<ContextChunk>::NextResult {
      auto next = producer->Next();
      if (!next) {
        return tl::unexpected(operator_error(next.error(), name));
      }
      return next;
    };
    if (current_timer) {
      stream = Stream<ContextChunk>(
          [pull = std::move(pull), current_timer,
           charged]() -> Stream<ContextChunk>::NextResult {
            StreamTimerScope scope(*current_timer, *charged);
            auto next = pull();
            if (next && *next) {
              current_timer->add_num_tuples((**next).row_num());
            }
            return next;
          },
          std::move(metadata));
    } else {
      stream = Stream<ContextChunk>(std::move(pull), std::move(metadata));
    }
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
