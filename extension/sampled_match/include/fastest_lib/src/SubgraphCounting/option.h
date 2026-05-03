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
#include "../SubgraphMatching/candidate_space.h"
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