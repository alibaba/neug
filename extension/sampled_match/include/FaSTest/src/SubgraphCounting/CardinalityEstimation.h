#pragma once

#include <algorithm>
#include <cmath>

#include "../SubgraphCounting/Option.h"
#include "../SubgraphMatching/CandidateSpace.h"
#include "../SubgraphCounting/CandidateTreeSampling.h"
#include "../SubgraphCounting/CandidateGraphSampling.h"
#include "data_graph_meta.h"
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