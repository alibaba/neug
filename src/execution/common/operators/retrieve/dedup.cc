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

#include "neug/execution/common/operators/retrieve/dedup.h"

#include <algorithm>
#include <tuple>

#include "neug/execution/common/columns/i_context_column.h"
#include "neug/execution/common/context.h"
#include "neug/utils/encoder.h"
#include "parallel_hashmap/phmap.h"

namespace neug {

namespace execution {

neug::result<Context> Dedup::dedup(Context&& ctx,
                                   const std::vector<size_t>& cols) {
  size_t row_num = ctx.row_num();
  std::vector<size_t> offsets;
  if (cols.size() == 0) {
    return ctx;
  } else if (cols.size() == 1) {
    ctx.get(cols[0])->generate_dedup_offset(offsets);
  } else {
    phmap::flat_hash_set<std::string> set;
    for (size_t r_i = 0; r_i < row_num; ++r_i) {
      std::vector<char> bytes;
      Encoder encoder(bytes);
      for (size_t c_i = 0; c_i < cols.size(); ++c_i) {
        auto val = ctx.get(cols[c_i])->get_elem(r_i);
        encode_value(val, encoder);
        encoder.put_byte('#');
      }
      std::string cur(bytes.begin(), bytes.end());
      if (set.find(cur) == set.end()) {
        offsets.push_back(r_i);
        set.insert(cur);
      }
    }
  }
  Context ret;
  for (size_t i = 0; i < cols.size(); i++) {
    ret.set(cols[i], ctx.get(cols[i]));
  }
  ret.reshuffle(offsets);
  return ret;
}

}  // namespace execution

}  // namespace neug
