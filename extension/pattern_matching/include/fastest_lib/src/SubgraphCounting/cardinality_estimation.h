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

#pragma once

#include <algorithm>
#include <cmath>

#include <glog/logging.h>

// clang-format off
#include "SubgraphCounting/option.h"
#include "SubgraphMatching/candidate_space.h"
#include "SubgraphCounting/candidate_tree_sampling.h"
#include "SubgraphCounting/candidate_graph_sampling.h"
// clang-format on
#include "pattern_matching_data_graph_meta.h"
// #include "SubgraphCounting/TreeRejectionSampling.h"
/**
 * @brief Subgraph Cardinality Estimation : Given G and P, approximate the
 * number of embeddings of P in G.
 * @date 2023-05-01
 * @author Wonseok Shin
 * @ref
 */

namespace neug {
namespace pattern_matching {
namespace graphlib {
using SubgraphMatching::PatternGraph, SubgraphMatching::CandidateSpace;
namespace CardinalityEstimation {
class FaSTestCardinalityEstimation {
  CandidateSpace* CS;
  const neug::StorageReadInterface& graph_;
  DataGraphMeta& data_meta_;
  PatternGraph* query_;
  CardEstOption opt_;
  dict result;
  std::vector<int> sampled_result;
  CandidateTreeSampler* TS;
  CandidateGraphSampler* GS;

 public:
  FaSTestCardinalityEstimation(const neug::StorageReadInterface& graph,
                               DataGraphMeta& data_meta, CardEstOption opt);
  dict GetResult() { return result; }
  std::vector<int> GetSampledResult() { return sampled_result; }
  double EstimateEmbeddings(PatternGraph* query, int sample_size);
};
}  // namespace CardinalityEstimation
}  // namespace graphlib
}  // namespace pattern_matching
}  // namespace neug
