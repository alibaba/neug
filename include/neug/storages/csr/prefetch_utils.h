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

#include <cstddef>
#include <cstdint>

namespace neug {

struct CsrPrefetchPolicy {
  uint16_t metadata_distance = 32;
  uint16_t head_distance = 0;
  uint8_t metadata_locality = 1;  // cache level for metadata
  uint8_t head_locality = 1;      // cache level for head
};

struct CsrDegreeDistributionStats {
  uint64_t edge_count = 0;
  uint64_t nonzero_count = 0;
  int32_t max_degree = 0;
  int32_t nonzero_p99 = 0;
};

CsrPrefetchPolicy create_csr_prefetch_policy(
    const CsrDegreeDistributionStats& stats);

CsrDegreeDistributionStats compute_csr_degree_distribution(const int* degrees,
                                                           size_t vertex_count);

}  // namespace neug
