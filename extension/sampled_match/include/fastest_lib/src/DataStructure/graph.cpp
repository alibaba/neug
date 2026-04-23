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

#include "graph.h"

namespace GraphLib {

void Graph::BuildNoEdgePairsFromSchema(std::shared_ptr<std::unordered_map<label_t, std::unordered_map<label_t, std::vector<label_t>>>> schema_graph) {
    // 基于schema构建no_edge_pairs，只检查schema中定义的边类型

    edge_index_map.resize(GetNumVertices());
    for (int i = 0; i < edge_list.size(); i++) {
        edge_index_map[edge_list[i].first][edge_list[i].second][edge_label[i]] = i;
    }

    if (schema_graph) {
        auto& schema = *schema_graph;
        
        for (int i = 0; i < GetNumVertices(); i++) {
            int i_label = GetVertexLabel(i);
            for (int j = 0; j < GetNumVertices(); j++) {
                if (i == j) continue;
                int j_label = GetVertexLabel(j);
                
                // 遍历schema中定义的所有边类型
                for (const auto& edge_type : schema[i_label][j_label]) {
                    if (GetEdgeIndex(i, j, edge_type) == -1 && GetEdgeIndex(i, j) == -1) // to refine
                    {
                        no_edge_pairs[i].push_back(std::make_pair(j, edge_type));
                    }
                }
            }
        }

        for (int i = 0; i < GetNumVertices(); i++) {
            for (const auto& [v, label] : no_edge_pairs[i]) {
                std::cout << "no_edge_pairs[" << i << "][" << v << "][" << label << "]" << std::endl;
            }
        }
    } else {
        // 如果没有schema信息，退回到遍历所有label的方式
        for (int i = 0; i < GetNumVertices(); i++) {
            for (int j = 0; j < GetNumVertices(); j++) {
                if (i == j) continue;
                for (int label = 0; label < GetNumEdgeLabels(); label++) {
                    if (GetEdgeIndex(i, j, label) == -1 && GetEdgeIndex(i, j) == -1) {
                        no_edge_pairs[i].push_back(std::make_pair(j, label));
                    }
                }
            }
        }

        // for (int i = 0; i < GetNumVertices(); i++) {
        //     for (const auto& [v, label] : no_edge_pairs[i]) {
        //         std::cout << "no_edge_pairs[" << i << "][" << v << "][" << label << "]" << std::endl;
        //     }
        // }
    }
}

}  // namespace GraphLib