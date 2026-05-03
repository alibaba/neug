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
 * (https://github.com/SNUCSE-CTA/FaSTest) Licensed under the MIT License. Modified by
 * Yunkai Lou and Shunyang Li in 2025 to support Neug-specific features.
 */

#pragma once
#include <boost/math/distributions.hpp>
#include "../SubgraphMatching/candidate_space.h"
#include "sampled_match_data_graph_meta.h"

// Use DataGraphMeta from neug namespace
using neug::function::DataGraphMeta;

static std::random_device rd;
static std::mt19937 gen(rd());
inline int sample_from_distribution(std::discrete_distribution<int> &weighted_distr) {
    return weighted_distr(gen);
}

namespace GraphLib {
    using SubgraphMatching::PatternGraph;
    using SubgraphMatching::CandidateSpace;
    using std::vector;
    namespace CardinalityEstimation {
        struct QueryTree {
            PatternGraph *query_;
            vector<vector<int>> tree_adj_list, tree_children;
            vector<int> tree_sequence;
            vector<int> parent, child_index;
            int root;

            void Initialize(PatternGraph *query, int root_idx) {
                query_ = query;
                tree_adj_list.clear();
                tree_children.clear();
                tree_sequence.clear();
                parent.clear();
                child_index.clear();
                root = root_idx;

                tree_adj_list.resize(query_->GetNumVertices());
                tree_children.resize(query_->GetNumVertices());
                tree_sequence.resize(query_->GetNumVertices(), -1);
                parent.resize(query_->GetNumVertices(), -1);
                child_index.resize(query_->GetNumVertices(), -1);
            }

            void AddEdge(int u, int v) {
                tree_adj_list[u].push_back(v);
                tree_adj_list[v].push_back(u);
            }

            void BuildTree() {
                bool *visit = new bool[query_->GetNumVertices()];
                std::fill(visit, visit + query_->GetNumVertices(), false);
                std::queue<int> q;
                int id = 0;
                parent[root] = -1;
                tree_sequence[id++] = root;
                q.push(root);
                visit[root] = true;
                while (!q.empty()) {
                    int v = q.front();
                    q.pop();
                    for (int c : tree_adj_list[v]) {
                        if (!visit[c]) {
                            q.push(c);
                            visit[c] = true;
                            child_index[c] = tree_children[v].size();
                            tree_children[v].push_back(c);
                            parent[c] = v;
                            tree_sequence[id++] = c;
                        }
                    }
                }
                delete[] visit;
            }

            vector<int>& GetChildren(int v) {return tree_children[v];}
            int GetParent(int v) {return parent[v];}
            int GetChildIndex(int v) {return child_index[v];}
            int GetKthVertex(int k) {return tree_sequence[k];}
        };
        class CandidateTreeSampler {
        protected:
            dict info;
            std::vector<int> sampled_result;
            int sampled_result_num;
            const neug::StorageReadInterface& graph_;
            DataGraphMeta& data_meta_;
            PatternGraph *query_;
            CandidateSpace *CS;
            CardEstOption opt;

            double **num_trees_, total_trees_;
            bool *seen;
            // vector<vector<vector<vector<int>>>> sample_candidates_;
            vector<vector<vector<vector<double>>>> sample_candidate_weights_;
            vector<vector<vector<std::discrete_distribution<int>>>> sample_dist_;
            vector<int> root_candidates_, sample;
            vector<double> root_weights_;
            std::discrete_distribution<int> sample_root_dist_;

            QueryTree Tq;
        public:
            dict GetInfo() {return info;}
            std::vector<int> GetSampledResult() {return sampled_result;}
            void ClearSampledResult() {
                sampled_result_num = 0;
                sampled_result.clear();
            }
            CandidateTreeSampler(const neug::StorageReadInterface& graph, DataGraphMeta& data_meta, CardEstOption option)
                : graph_(graph), data_meta_(data_meta) {
                opt = option;
                num_trees_ = new double *[opt.MAX_QUERY_VERTEX];
                for (int i = 0; i < opt.MAX_QUERY_VERTEX; i++) {
                    num_trees_[i] = new double[data_meta_.GetNumVertices()];
                }
                seen = new bool[data_meta_.GetNumVertices()]();
                sampled_result_num = 0;
            }


            void Preprocess(PatternGraph *query, CandidateSpace *cs) {
                info.clear();
                Timer timer; timer.Start();
                query_ = query;
                CS = cs;
                sample_dist_.clear();
                // sample_candidates_.clear();
                sample_candidate_weights_.clear();
                root_candidates_.clear();

                sample.resize(query_->GetNumVertices(), -1);
                std::memset(seen, 0, sizeof(bool) * data_meta_.GetNumVertices());
                BuildSpanningTree();
                CountCandidateTrees();
                timer.Stop();
                info["TreeCountTime"] = timer.GetTime();
                info["#CandTree"] = total_trees_;
            };

            void BuildSpanningTree() {
                if (opt.treegen_strategy == CardinalityEstimation::TREEGEN_RANDOM) {
                    int root_node = rand() % query_->GetNumVertices();
                    Tq.Initialize(query_, root_node);
                    int num_discovered = 1; 
                    std::vector<int> is_discovered(query_->GetNumVertices(), false);
                    is_discovered[root_node] = true;
                    int cur = root_node;
                    while (num_discovered < query_->GetNumVertices()) {
                        // For directed graph: combine out and in neighbors for random walk
                        std::vector<int> neighbors;
                        for (int n : query_->GetOutNeighbors(cur)) neighbors.push_back(n);
                        for (int n : query_->GetInNeighbors(cur)) neighbors.push_back(n);
                        if (neighbors.empty()) break;
                        int di = rand() % neighbors.size();
                        int v = neighbors[di];
                        if (!is_discovered[v]) {
                            Tq.AddEdge(cur, v);
                            is_discovered[v] = true;
                            num_discovered++;
                        }
                        cur = v; 
                    }
                    Tq.BuildTree();
                    return; 
                }
                int root_idx = 0, num_root_cands = CS->GetCandidateSetSize(0);
                std::vector<std::pair<double, std::pair<int, int>>> edges;
                for (int i = 0; i < query_->GetNumVertices(); i++) {
                    // For directed graph: process out-edges (i -> q_neighbor)
                    for (int q_neighbor : query_->GetOutNeighbors(i)) {
                        double density = 0.0;
                        for (int cand_idx = 0; cand_idx < CS->GetCandidateSetSize(i); cand_idx++) {
                            int num_cs_neighbor = CS->GetOutCandidateNeighbors(i, cand_idx, q_neighbor).size();
                            density += num_cs_neighbor;
                        }
                        if (opt.treegen_strategy == CardinalityEstimation::TREEGEN_DENSITY_MST) {
                            density /= ((CS->GetCandidateSetSize(i) * 1.0) * (CS->GetCandidateSetSize(q_neighbor) * 1.0));
                        }
                        if (density > 0) {
                            edges.push_back({density * 1.0, {i, q_neighbor}});
                        }
                    }
                    if (CS->GetCandidateSetSize(i) < num_root_cands) {
                        num_root_cands = CS->GetCandidateSetSize(i);
                        root_idx = i;
                    }
                }

                std::sort(edges.begin(), edges.end());
                Tq.Initialize(query_, root_idx);
                int num_tree_edges = 0;
                int qV = query_->GetNumVertices();
                std::vector<int> deg(qV, 0);
                UnionFind uf(qV);
                while (num_tree_edges + 1 < qV) {
                    double minw = 1e9;
                    std::pair<int, int> me;
                    for (auto &[w, e] : edges) {
                        auto [u, v] = e;
                        if (uf.find(u) == uf.find(v)) continue;
                        if (minw > w + deg[u] * 1e-7) {
                            minw = w + deg[u] * 1e-7;
                            me = e;
                        }
                    }
                    uf.unite(me.first, me.second);
                    deg[me.first]++;
                    deg[me.second]++;
                    Tq.AddEdge(me.first, me.second);
                    num_tree_edges++;
                }
                Tq.BuildTree();
            };


            void CountCandidateTrees() {
                for (int i = 0; i < query_->GetNumVertices(); i++) {
                    memset(num_trees_[i], 0, sizeof(double) * CS->GetCandidateSetSize(i));
                }
                int cnt = 0;
                sample_candidate_weights_.resize(query_->GetNumVertices());
                sample_dist_.resize(query_->GetNumVertices());
                for (int i = 0; i < query_->GetNumVertices(); i++) {
                    int u = Tq.GetKthVertex(query_->GetNumVertices() - i - 1);
                    int num_cands = CS->GetCandidateSetSize(u);
                    auto children = Tq.GetChildren(u);
                    int num_children = children.size();
                    sample_candidate_weights_[u].resize(num_cands);
                    sample_dist_[u].resize(num_cands);

                    std::vector<double> tmp_num_child(num_children);
                    for (int cs_idx = 0; cs_idx < num_cands; cs_idx++) {
                        sample_candidate_weights_[u][cs_idx].resize(num_children);
                        sample_dist_[u][cs_idx].resize(num_children);

                        double num_ = 1.0;
                        std::fill(tmp_num_child.begin(), tmp_num_child.end(), 0.0);
                        for (int uc_idx = 0; uc_idx < num_children; uc_idx++) {
                            int uc = children[uc_idx];
                            auto candidate_neighbors = CS->GetCandidateNeighbors(u, cs_idx, uc);
                            sample_candidate_weights_[u][cs_idx][uc_idx].resize(candidate_neighbors.size());
                            for (int j = 0; j < candidate_neighbors.size(); j++) {
                                int vc_idx = candidate_neighbors[j];
                                tmp_num_child[uc_idx] += num_trees_[uc][vc_idx];
                                sample_candidate_weights_[u][cs_idx][uc_idx][j] = num_trees_[uc][vc_idx];
                            }
                        }
                        for (int j = 0; j < num_children; j++) {
                            num_ *= tmp_num_child[j];
                            sample_dist_[u][cs_idx][j] = std::discrete_distribution<int>(
                                    sample_candidate_weights_[u][cs_idx][j].begin(),
                                    sample_candidate_weights_[u][cs_idx][j].end());
                            cnt++;
                        }
                        num_trees_[u][cs_idx] = num_;
                    }
                }

                total_trees_ = 0.0;
                root_candidates_.clear();
                root_weights_.clear();
                int root = Tq.root;
                int root_candidate_size = CS->GetCandidateSetSize(root);
                for (int root_candidate_idx = 0; root_candidate_idx < root_candidate_size; ++root_candidate_idx) {
                    total_trees_ += num_trees_[root][root_candidate_idx];
                    if (num_trees_[root][root_candidate_idx] > 0) {
                        root_candidates_.emplace_back(root_candidate_idx);
                        root_weights_.emplace_back(num_trees_[root][root_candidate_idx]);
                    }
                }
                sample_root_dist_ = std::discrete_distribution<int>(root_weights_.begin(), root_weights_.end());
            };

            bool GetSampleTree(int sample_size) {
                std::fill(sample.begin(), sample.end(), -1);
                bool valid = true;
                if (root_candidates_.empty()) return false;
                sample[Tq.root] = root_candidates_[sample_from_distribution(sample_root_dist_)];
                seen[CS->GetCandidate(Tq.root, sample[Tq.root])] = true;
                for (int i = 0; i < query_->GetNumVertices(); ++i) {
                    int u = Tq.GetKthVertex(i);
                    int v_idx = sample[u];
                    auto &children = Tq.GetChildren(u);
                    for (int uc_idx = 0; uc_idx < children.size(); ++uc_idx) {
                        int uc = children[uc_idx];
                        int vc_idx = sample_from_distribution(sample_dist_[u][v_idx][uc_idx]);
                        sample[uc] = CS->GetCandidateNeighbor(u, v_idx, uc, vc_idx);
                        int cand = CS->GetCandidate(uc, sample[uc]);
                        if (seen[cand]) {
                            valid = false;
                            goto INJECTIVE_VIOLATED;
                        }
                        seen[cand] = true;
                    }
                }
                INJECTIVE_VIOLATED:
                for (int i = 0; i < query_->GetNumVertices(); i++) {
                    if (sample[i] >= 0) {
                        int cand = CS->GetCandidate(i, sample[i]);
                        seen[cand] = false;
                    }
                }
                if (!valid) return false;
                for (int i = 0; i < query_->GetNumVertices(); i++) {
                    if (sample[i] == -1) return false;
                    sample[i] = CS->GetCandidate(i, sample[i]);
                }
                // For directed graphs: verify all out-edges exist in the data graph
                for (int i = 0; i < query_->GetNumVertices(); i++) {
                    for (int qe : query_->GetAllOutIncidentEdges(i)) {
                        int j = query_->GetOppositePoint(qe);
                        int label = query_->GetEdgeLabel(qe);

                        // Check directed edge: i -> j in query must map to sample[i] -> sample[j] in data
                        // Use Graph's GetEdgeIndex when graph_storage_ptr is not available
                        
                        if (data_meta_.GetEdgeIndex(sample[i], sample[j], label) == -1) {
                            return false;
                        }
                        
                    }
                }

                for (int i = 0; i < query_->GetNumVertices(); i++) {
                    for (auto& edge_pair : query_->no_edge_pairs[i]) {
                        int j = edge_pair.first;
                        int edge_label = edge_pair.second;
                        // if (i > j) continue;
                        // 检查数据图中是否存在这种不应该存在的指定label的边
                        if (data_meta_.GetEdgeIndex(sample[i], sample[j], edge_label) != -1)
                            return false;
                    }
                }

                if (sampled_result_num < sample_size) {
                    for (int i = 0; i < query_->GetNumVertices(); i++) {
                        sampled_result.push_back(sample[i]);
                    }
                    sampled_result_num++;
                }
                
                return true;
            }

            // (Estimate, #Success)
            std::pair<double, int> Estimate(int sample_size) {
                Timer timer; timer.Start();
                int success = 0, trials = 0;
                int max_trials = std::max(4000000, sample_size * 50);
                while (++trials) {
                    auto result = GetSampleTree(sample_size);
                    if (result) success++;
                    double rhohat = (success * 1.0 / trials);
                    if ((trials == 50000 and success <= 10) || (trials >= max_trials  && success < sample_size)) {
                        timer.Stop();
                        info["#TreeTrials"] = trials;
                        info["#TreeSuccess"] = success;
                        info["TreeSampleTime"] = timer.GetTime();
                        return {-1, success};
                    }

                    if (trials >= 1000 and trials % 100 == 0 && success >= sample_size) {
                        std::cout << "trials: " << trials << ", success: " << success << " " << sample_size <<std::endl;
                        long double wplus = boost::math::binomial_distribution<>::find_upper_bound_on_p(trials, success, 0.05/2);
                        long double wminus = boost::math::binomial_distribution<>::find_lower_bound_on_p(trials, success, 0.05/2);
                        if (rhohat * 0.8 < wminus && wplus < rhohat * 1.25) {
                            timer.Stop();
                            break;
                        }
                    }
                }
                
                auto est = std::make_pair((success * 1.0 / (trials * 1.0)) * total_trees_, success);
                info["#TreeTrials"] = trials;
                info["#TreeSuccess"] = success;
                info["TreeSampleTime"] = timer.GetTime();
                return est;
            }
        };
    }
}