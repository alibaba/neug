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

#include "neug/storages/csr/prefetch_utils.h"

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <vector>

namespace neug {
namespace {

uint64_t percentile_rank(uint64_t n, double percentile) {
  if (n == 0) {
    return 0;
  }
  uint64_t rank = static_cast<uint64_t>(std::ceil(percentile * n));
  return std::min(rank == 0 ? uint64_t{0} : rank - 1, n - 1);
}

}  // namespace

CsrPrefetchPolicy create_csr_prefetch_policy(
    const CsrDegreeDistributionStats& stats) {
  CsrPrefetchPolicy policy;
  double nonzero_avg_degree =
      stats.nonzero_count == 0
          ? 0.0
          : static_cast<double>(stats.edge_count) / stats.nonzero_count;
  int32_t nonzero_p99 = stats.nonzero_p99;

  // metadata_locality tracks degree: dense lists keep a long gap between
  // consecutive metadata uses while their bodies thrash the cache, so the
  // prefetched degree/pointer lines need higher retention (2) to survive until
  // use; sparse lists are consumed almost immediately and stream densely, so a
  // low/streaming locality (1) avoids polluting the cache they iterate.
  if (stats.max_degree <= 1) {
    policy.metadata_distance = 64;
    policy.head_distance = 32;
    policy.metadata_locality = 2;
    policy.head_locality = 0;
  } else if (nonzero_avg_degree >= 128 || nonzero_p99 >= 1024) {
    policy.metadata_distance = 4;
    policy.head_distance = 0;
    policy.metadata_locality = 2;
    policy.head_locality = 0;
  } else if (nonzero_avg_degree >= 16 || nonzero_p99 >= 128) {
    policy.metadata_distance = 8;
    policy.head_distance = 0;
    policy.metadata_locality = 2;
    policy.head_locality = 0;
  } else if (nonzero_avg_degree >= 4 || nonzero_p99 >= 16) {
    policy.metadata_distance = 24;
    policy.head_distance = 16;
    policy.metadata_locality = 1;
    policy.head_locality = 1;
  } else {
    policy.metadata_distance = 48;
    policy.head_distance = 32;
    policy.metadata_locality = 1;
    policy.head_locality = 1;
  }
  return policy;
}

CsrDegreeDistributionStats compute_csr_degree_distribution(
    const int* degrees, size_t vertex_count) {
  CsrDegreeDistributionStats stats;
  if (degrees == nullptr || vertex_count == 0) {
    return stats;
  }

  for (size_t i = 0; i < vertex_count; ++i) {
    int32_t degree = degrees[i];
    if (degree <= 0) {
      continue;
    }
    stats.edge_count += static_cast<uint64_t>(degree);
    ++stats.nonzero_count;
    stats.max_degree = std::max(stats.max_degree, degree);
  }
  if (stats.nonzero_count == 0) {
    return stats;
  }

  constexpr int32_t kMaxExactHistogramDegree = 1'000'000;
  constexpr size_t kMaxSampleSize = 1'000'000;
  constexpr double kNonzeroP99 = 0.99;

  if (stats.max_degree <= kMaxExactHistogramDegree) {
    std::vector<uint64_t> histogram(static_cast<size_t>(stats.max_degree) + 1,
                                    0);
    for (size_t i = 0; i < vertex_count; ++i) {
      int32_t degree = degrees[i];
      if (degree > 0) {
        histogram[static_cast<size_t>(degree)]++;
      }
    }
    size_t target = percentile_rank(stats.nonzero_count, kNonzeroP99);
    uint64_t cumulative = 0;
    for (size_t degree = 1; degree < histogram.size(); ++degree) {
      cumulative += histogram[degree];
      if (cumulative > target) {
        stats.nonzero_p99 = static_cast<int32_t>(degree);
        break;
      }
    }
    return stats;
  }

  size_t stride =
      std::max<size_t>(1, (vertex_count + kMaxSampleSize - 1) / kMaxSampleSize);
  std::vector<int32_t> sample;
  sample.reserve(std::min(vertex_count, kMaxSampleSize + size_t{1}));
  for (size_t i = 0; i < vertex_count; i += stride) {
    int32_t degree = std::max<int32_t>(degrees[i], 0);
    if (degree > 0) {
      sample.push_back(degree);
    }
  }
  if (sample.empty()) {
    return stats;
  }
  std::sort(sample.begin(), sample.end());
  stats.nonzero_p99 = sample[percentile_rank(sample.size(), kNonzeroP99)];
  return stats;
}

}  // namespace neug
