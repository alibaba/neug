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
#pragma once

#include "neug/execution/common/types/graph_types.h"
#include "neug/utils/result.h"

namespace neug {
class IStorageInterface;
namespace execution {
class Context;
struct JoinParams;

class Join {
 public:
  static neug::result<Context> join(Context&& ctx, Context&& ctx2,
                                    const JoinParams& params);

  static neug::result<Context> pk_join(IStorageInterface&, Context&& ctx,
                                       const std::vector<label_t>& labels,
                                       int tag, int alias);
};
}  // namespace execution
}  // namespace neug
