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

#include <algorithm>
#include <cmath>

#include "../SubgraphCounting/option.h"
#include "../SubgraphMatching/candidate_space.h"
#include "../SubgraphCounting/candidate_tree_sampling.h"
#include "../SubgraphCounting/candidate_graph_sampling.h"
#include "sampled_match_data_graph_meta.h"
// #include "SubgraphCounting/TreeRejectionSampling.h"
/**
 * @brief Subgraph Cardinality Estimation : Given G and P, approximate the number of embeddings of P in G.
 * @date 2023-05-01
 * @author Wonseok Shin
 * @ref
 */

// Use DataGraphMeta from neug namespace
using neug::function::DataGraphMeta;

namespace GraphLib{
using SubgraphMatching::PatternGraph, SubgraphMatching::CandidateSpace;
namespace CardinalityEstimation {
    class FaSTestCardinalityEstimation {
        CandidateSpace *CS;
        const neug::StorageReadInterface& graph_;
        DataGraphMeta& data_meta_;
        PatternGraph *query_;
        CardEstOption opt_;
        dict result;
        std::vector<int> sampled_result;
        CandidateTreeSampler *TS;
        CandidateGraphSampler *GS;
    public:
        FaSTestCardinalityEstimation(const neug::StorageReadInterface& graph, DataGraphMeta& data_meta, CardEstOption opt) 
            : graph_(graph), data_meta_(data_meta), opt_(opt) {
            CS = new CandidateSpace(graph_, data_meta_, opt);
            TS = new CandidateTreeSampler(graph_, data_meta_, opt);
            GS = new CandidateGraphSampler(graph_, data_meta_, opt);
            result.clear();
        };
        dict GetResult() {return result;}
        std::vector<int> GetSampledResult() {return sampled_result;}
        double EstimateEmbeddings(PatternGraph *query, int sample_size) {
            result.clear();
            double query_time = 0.0;
            query_ = query;
            if (!CS->BuildCS(query_))
                return 0;
            for (auto &[key, value] : CS->GetCSInfo()) {
                result[key] = value;
            }
            TS->Preprocess(query, CS);
            TS->ClearSampledResult();

            std::cout << "TS preprocess done" << std::endl;
            auto ts_result = TS->Estimate(sample_size);
            std::cout << "TS estimate done" << std::endl;
            for (auto &[key, value] : TS->GetInfo()) {
                result[key] = value;
            }
            result["GraphSampleTime"] = 0.00;
            double est = ts_result.first;  

            if (ts_result.second <= 10 || est < 0) {
                GS->ClearSampledResult();
                GS->Preprocess(query, CS);
                std::cout << "GS preprocess done" << std::endl;
                // est = GS->Estimate(ceil((double)(opt_.ub_initial * query_->GetNumVertices()) / sqrt(ts_result.second + 1)));
                int ub = std::max(sample_size, (int)ceil((double)(opt_.ub_initial * query_->GetNumVertices()) / sqrt(ts_result.second + 1)));
                est = GS->Estimate(ub, sample_size);

                for (auto &[key, value] : GS->GetInfo()) {
                    result[key] = value;
                }
                sampled_result = GS->GetSampledResult();
            } else {
                sampled_result = TS->GetSampledResult();
            }
            
            query_time = std::any_cast<double>(result["CSBuildTime"])
                    + std::any_cast<double>(result["TreeCountTime"])
                    + std::any_cast<double>(result["TreeSampleTime"])
                    + std::any_cast<double>(result["GraphSampleTime"]);
            result["QueryTime"] = query_time;
            
            // Ensure estimation is never negative (handles NaN/overflow edge cases)
            if (est < 0 || std::isnan(est) || std::isinf(est)) {
                est = 0.0;
            }
            return est;
        };
    };
}
}