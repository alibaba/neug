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

#include "neug/utils/result.h"

namespace neug {
class StorageInsertInterface;
namespace execution {
class Context;
struct LabelTriplet;
class BindedExprBase;
namespace ops {
class CreateEdge {
 public:
  static neug::result<Context> insert_edge(
      StorageInsertInterface& graph, Context&& ctx,
      std::vector<LabelTriplet> labels,
      const std::vector<std::pair<int32_t, int32_t>>& src_dst_tags,
      std::vector<std::vector<
          std::pair<std::string, std::unique_ptr<BindedExprBase>>>>&& props,
      const std::vector<int>& alias);
};
}  // namespace ops
}  // namespace execution
}  // namespace neug