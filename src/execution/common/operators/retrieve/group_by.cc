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

#include "neug/execution/common/operators/retrieve/group_by.h"

namespace neug {
namespace execution {

neug::result<Context> GroupBy::group_by(Context&& ctx,
                                        std::unique_ptr<KeyBase>&& key,
                                        std::vector<ReduceOp>&& aggrs) {
  auto [offsets, groups] = key->group(ctx);
  Context ret;
  const auto& tag_alias = key->tag_alias();
  for (size_t i = 0; i < tag_alias.size(); ++i) {
    ret.set(tag_alias[i].second, ctx.get(tag_alias[i].first));
  }
  ret.reshuffle(offsets);
  for (auto& aggr : aggrs) {
    aggr.reduce(ctx, ret, groups);
  }
  return ret;
}
}  // namespace execution
}  // namespace neug