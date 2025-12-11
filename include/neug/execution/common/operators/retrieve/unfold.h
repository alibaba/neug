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

#include "neug/execution/common/context.h"
#include "neug/execution/utils/expr.h"
#include "neug/utils/result.h"
namespace gs {

namespace runtime {
class Context;

class Unfold {
 public:
  static gs::result<Context> unfold(Context&& ctxs, int key, int alias);

  static gs::result<Context> unfold(Context&& ctxs, const Expr& key, int alias);
};

}  // namespace runtime

}  // namespace gs
