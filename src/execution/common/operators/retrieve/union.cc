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

#include "neug/execution/common/operators/retrieve/union.h"

#include <glog/logging.h>
#include "neug/execution/common/context_chunk.h"
#include "neug/utils/result.h"

namespace neug {

namespace execution {

neug::result<ContextChunk> Union::union_op(std::vector<ContextChunk>&& chunks) {
  if (chunks.size() != 2) {
    LOG(ERROR) << "Union: only support two chunks";
    RETURN_UNSUPPORTED_ERROR("Union: only support two chunks");
  }
  auto& chunk0 = chunks[0];
  auto& chunk1 = chunks[1];
  if (chunk0.col_num() != chunk1.col_num()) {
    LOG(ERROR) << "Union: column size not match";
    RETURN_INVALID_ARGUMENT_ERROR("Union: column size not match");
  }
  ContextChunk ret = chunk0.union_with(chunk1);
  ret.head().reset();
  return ret;
}

}  // namespace execution

}  // namespace neug
