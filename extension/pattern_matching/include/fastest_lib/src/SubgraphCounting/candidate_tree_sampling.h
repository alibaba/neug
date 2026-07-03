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
#include <glog/logging.h>

#include <cmath>

#include "SubgraphCounting/option.h"
#include "SubgraphMatching/candidate_space.h"
#include "pattern_matching_data_graph_meta.h"

namespace neug {
namespace pattern_matching {
namespace sampled_match_stats {
// Regularized incomplete beta function I_x(a,b) — the beta CDF. Header-only
// replacement for gsl_cdf_beta_P so the extension no longer depends on GSL.
// Uses the Lentz continued-fraction evaluation (Numerical Recipes betacf/betai)
// with the standard reflection I_x(a,b) = 1 - I_{1-x}(b,a) for fast
// convergence; accurate to ~1e-15 across the (a,b,x) ranges the Clopper-Pearson
// bounds below use.
double beta_continued_fraction(double a, double b, double x);

double incomplete_beta(double x, double a, double b);

// Clopper-Pearson exact confidence-interval bounds, drop-in replacement for
// boost::math::binomial_distribution::find_{lower,upper}_bound_on_p.
// alpha is the one-sided tail mass (e.g. 0.05/2 for a 95% two-sided CI).
//
// Implemented as bisection on incomplete_beta (the regularized incomplete
// beta CDF), which is monotone in x; bisection trades a handful of evaluations
// (≈60 iters → ~1e-18) for unconditional robustness, avoiding the inverse-CDF
// Newton iteration that fails to converge near a≈b. The surrounding sampling
// loop calls this at most once per 100 trials so the overhead is negligible.
double beta_quantile(double p, double a, double b);
long double clopper_pearson_lower(long trials, long success, double alpha);
long double clopper_pearson_upper(long trials, long success, double alpha);
}  // namespace sampled_match_stats
}  // namespace pattern_matching
}  // namespace neug

namespace neug {
namespace pattern_matching {
namespace graphlib {

static std::random_device rd;
static std::mt19937 gen(rd());
int sample_from_distribution(std::discrete_distribution<int>& weighted_distr);

using std::vector;
using SubgraphMatching::CandidateSpace;
using SubgraphMatching::PatternGraph;
namespace CardinalityEstimation {
struct QueryTree {
  PatternGraph* query_;
  vector<vector<int>> tree_adj_list, tree_children;
  vector<int> tree_sequence;
  vector<int> parent, child_index;
  int root;

  void Initialize(PatternGraph* query, int root_idx);

  void AddEdge(int u, int v);

  void BuildTree();

  vector<int>& GetChildren(int v) { return tree_children[v]; }
  int GetParent(int v) { return parent[v]; }
  int GetChildIndex(int v) { return child_index[v]; }
  int GetKthVertex(int k) { return tree_sequence[k]; }
};
class CandidateTreeSampler {
 protected:
  dict info;
  std::vector<int> sampled_result;
  int sampled_result_num;
  const neug::StorageReadInterface& graph_;
  DataGraphMeta& data_meta_;
  PatternGraph* query_;
  CandidateSpace* CS;
  CardEstOption opt;

  double **num_trees_, total_trees_;
  bool* seen;
  // vector<vector<vector<vector<int>>>> sample_candidates_;
  vector<vector<vector<vector<double>>>> sample_candidate_weights_;
  vector<vector<vector<std::discrete_distribution<int>>>> sample_dist_;
  vector<int> root_candidates_, sample;
  vector<double> root_weights_;
  std::discrete_distribution<int> sample_root_dist_;

  QueryTree Tq;

 public:
  dict GetInfo() { return info; }
  std::vector<int> GetSampledResult() { return sampled_result; }
  void ClearSampledResult();
  CandidateTreeSampler(const neug::StorageReadInterface& graph,
                       DataGraphMeta& data_meta, CardEstOption option);

  void Preprocess(PatternGraph* query, CandidateSpace* cs);

  void BuildSpanningTree();

  void CountCandidateTrees();

  bool GetSampleTree(int sample_size);

  // (Estimate, #Success)
  std::pair<double, int> Estimate(int sample_size);
};
}  // namespace CardinalityEstimation
}  // namespace graphlib
}  // namespace pattern_matching
}  // namespace neug
