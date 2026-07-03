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

#include "SubgraphCounting/candidate_tree_sampling.h"
#include "SubgraphCounting/option.h"
#include "SubgraphMatching/candidate_space.h"
#include "pattern_matching_data_graph_meta.h"

namespace neug {
namespace pattern_matching {
namespace graphlib {
namespace CardinalityEstimation {
class CandidateGraphSampler {
  CardEstOption opt;
  CandidateSpace* CS;
  const neug::StorageReadInterface& graph_;
  DataGraphMeta& data_meta_;
  PatternGraph* query_;
  dict info;
  std::vector<int> sampled_result;
  int sampled_result_num;
  int root;
  int min_cand;
  bool* seen;
  int **local_candidates, *local_candidate_size;

  int num_embeddings = 0;

 public:
  dict GetInfo() { return info; }
  std::vector<int> GetSampledResult() { return sampled_result; }
  void ClearSampledResult();
  CandidateGraphSampler(const neug::StorageReadInterface& graph,
                        DataGraphMeta& data_meta, CardEstOption opt_);
  ~CandidateGraphSampler();

  void Preprocess(PatternGraph* query, CandidateSpace* cs);
  std::vector<int> sample;
  std::vector<std::pair<std::vector<int>::iterator, std::vector<int>::iterator>>
      iterators;
  std::vector<int> root_candidates_;

  double Estimate(int ub_initial, int sample_total_size);

  std::pair<double, int> StratifiedSampling(int vertex_id, int ub, double w,
                                            int sample_total_size);

  int ChooseExtendableVertex();

  void BuildExtendableCandidates(int u);

  void Intersection(int index);
};

inline int printcnt = 0;

}  // namespace CardinalityEstimation
}  // namespace graphlib
}  // namespace pattern_matching
}  // namespace neug
