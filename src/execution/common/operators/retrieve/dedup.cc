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

namespace neug {

namespace execution {

neug::result<Context> Dedup::dedup(Context&& ctx,
                                   const vector_t<uint32_t>& cols) {
  sel_t row_num = static_cast<sel_t>(ctx.row_num());
  sel_vec_t offsets;
  if (cols.size() == 0) {
    return ctx;
  }
  if (cols.size() == 1 && ctx.get(cols[0])->generate_dedup_offset(offsets)) {
  } else {
    offsets.clear();
    flat_hash_set_t<string_t> set;
    for (sel_t r_i = 0; r_i < row_num; ++r_i) {
      vector_t<char> bytes;
      Encoder encoder(bytes);
      for (size_t c_i = 0; c_i < cols.size(); ++c_i) {
        auto val = ctx.get(cols[c_i])->get_elem(r_i);
        encode_value(val, encoder);
        encoder.put_byte('#');
      }
      string_t cur(bytes.begin(), bytes.end());
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
