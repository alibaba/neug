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

#include "neug/execution/common/operators/retrieve/limit.h"

namespace neug {

namespace execution {

neug::result<ContextChunk> Limit::limit(ContextChunk&& chunk, size_t lower,
                                        size_t upper) {
  if (lower == 0 && upper >= chunk.row_num()) {
    return chunk;
  }
  if (upper > chunk.row_num()) {
    upper = chunk.row_num();
  }

  std::vector<size_t> offsets(upper - lower);
  for (size_t i = lower; i < upper; ++i) {
    offsets[i - lower] = i;
  }
  chunk.reshuffle(offsets);

  return chunk;
}

}  // namespace execution

}  // namespace neug
