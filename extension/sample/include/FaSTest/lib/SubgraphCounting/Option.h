#pragma once
#include "../SubgraphMatching/CandidateSpace.h"
namespace GraphLib {
    namespace CardinalityEstimation {
        enum TreegenerationStrategy {
            TREEGEN_EDGE_MST, 
            TREEGEN_DENSITY_MST,
            TREEGEN_RANDOM
        };
        class CardEstOption : public SubgraphMatching::SubgraphMatchingOption {
        public:
            int ub_initial = 100000;
            double strata_ratio = 0.5;
            int treegen_strategy = TREEGEN_DENSITY_MST;
        };
    }
}