/**
 * Copyright 2020 Alibaba Group Holding Limited.
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

/**
 * This file is originally from the FaSTest project
 * (https://github.com/SNUCSE-CTA/FaSTest) Licensed under the MIT License.
 * Modified by Yunkai Lou and Shunyang Li in 2025 to support Neug-specific
 * features.
 */

#include "SubgraphCounting/cardinality_estimation.h"
#include <chrono>
#include <cmath>

namespace neug {
namespace pattern_matching {
namespace graphlib {
namespace CardinalityEstimation {

FaSTestCardinalityEstimation::FaSTestCardinalityEstimation(
    const neug::StorageReadInterface& graph, DataGraphMeta& data_meta,
    CardEstOption opt)
    : graph_(graph), data_meta_(data_meta), opt_(opt) {
  CS = new CandidateSpace(graph_, data_meta_, opt);
  TS = new CandidateTreeSampler(graph_, data_meta_, opt);
  GS = new CandidateGraphSampler(graph_, data_meta_, opt);
  result.clear();
}

double FaSTestCardinalityEstimation::EstimateEmbeddings(PatternGraph* query,
                                                        int sample_size) {
  result.clear();
  query_ = query;
  if (!CS->BuildCS(query_))
    return 0;
  for (auto& [key, value] : CS->GetCSInfo()) {
    result[key] = value;
  }
  TS->Preprocess(query, CS);
  TS->ClearSampledResult();

  VLOG(1) << "[FaSTest] Tree sampler preprocess done.";
  auto ts_result = TS->Estimate(sample_size);
  VLOG(1) << "[FaSTest] Tree sampler estimate done.";
  for (auto& [key, value] : TS->GetInfo()) {
    result[key] = value;
  }
  double est = ts_result.first;

  if (ts_result.second <= 10 || est < 0) {
    GS->ClearSampledResult();
    GS->Preprocess(query, CS);
    VLOG(1) << "[FaSTest] Graph sampler preprocess done.";
    // est = GS->Estimate(ceil((double)(opt_.ub_initial *
    // query_->GetNumVertices()) / sqrt(ts_result.second + 1)));
    int ub = std::max(
        sample_size,
        (int) ceil((double) (opt_.ub_initial * query_->GetNumVertices()) /
                   sqrt(ts_result.second + 1)));
    est = GS->Estimate(ub, sample_size);

    for (auto& [key, value] : GS->GetInfo()) {
      result[key] = value;
    }
    sampled_result = GS->GetSampledResult();
  } else {
    sampled_result = TS->GetSampledResult();
  }

  // Ensure estimation is never negative (handles NaN/overflow edge cases)
  if (est < 0 || std::isnan(est) || std::isinf(est)) {
    est = 0.0;
  }
  return est;
}

}  // namespace CardinalityEstimation
}  // namespace graphlib
}  // namespace pattern_matching
}  // namespace neug
