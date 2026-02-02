#pragma once
#include "../SubgraphMatching/DataGraph.h"
#include "../SubgraphMatching/PatternGraph.h"
#include "../DataStructure/Graph.h"
#include "../Base/Base.h"
#include "../Base/BasicAlgorithms.h"
#include "../Base/Timer.h"
#include "../../../../data_graph_meta.h"
#include <algorithm>
#include <vector>

// Use DataGraphMeta from neug namespace
using neug::function::DataGraphMeta;

/**
 * @brief The Candidate Space structure
 * @date 2023-05
 */

namespace GraphLib {
namespace SubgraphMatching {
    BipartiteMaximumMatching BPSolver;
    enum STRUCTURE_FILTER {
        NO_STRUCTURE_FILTER,
        TRIANGLE_SAFETY,
        FOURCYCLE_SAFETY
    };
    enum NEIGHBOR_FILTER {
        NEIGHBOR_SAFETY,
        NEIGHBOR_BIPARTITE_SAFETY,
        EDGE_BIPARTITE_SAFETY
    };
    class SubgraphMatchingOption {
    public:
        // For directed graphs: disable cycle filters by default
        STRUCTURE_FILTER structure_filter = NO_STRUCTURE_FILTER;
        NEIGHBOR_FILTER neighborhood_filter = NEIGHBOR_SAFETY;
        int MAX_QUERY_VERTEX = 50, MAX_QUERY_EDGE = 250;
        long long max_num_matches = -1;
    };

    class CandidateSpace {
    public:
        SubgraphMatchingOption opt;
        CandidateSpace(const neug::StorageReadInterface& graph, DataGraphMeta& data_meta, SubgraphMatchingOption filter_option);

        ~CandidateSpace();

        CandidateSpace &operator=(const CandidateSpace &) = delete;

        CandidateSpace(const CandidateSpace &) = delete;

        inline int GetCandidateSetSize(const int u) const {return candidate_set_[u].size();};

        inline int GetCandidate(const int u, const int v_idx) const {return candidate_set_[u][v_idx];};

        bool BuildCS(PatternGraph *query);

        std::vector<int>& GetCandidates(int u) {
            return candidate_set_[u];
        }

        // For directed graph: separate out/in candidate neighbors
        // GetOutCandidateNeighbors: for query edge cur -> nxt (out-edge from cur)
        std::vector<int>& GetOutCandidateNeighbors(int cur, int cand_idx, int nxt) {
            return out_candidate_neighbors[cur][cand_idx][query_->GetOutAdjIdx(cur, nxt)];
        }
        // GetInCandidateNeighbors: for query edge nxt -> cur (in-edge to cur)
        std::vector<int>& GetInCandidateNeighbors(int cur, int cand_idx, int nxt) {
            return in_candidate_neighbors[cur][cand_idx][query_->GetInAdjIdx(cur, nxt)];
        }
        
        int GetOutCandidateNeighbor(int cur, int cand_idx, int nxt, int nxt_idx) {
            return out_candidate_neighbors[cur][cand_idx][query_->GetOutAdjIdx(cur, nxt)][nxt_idx];
        }
        int GetInCandidateNeighbor(int cur, int cand_idx, int nxt, int nxt_idx) {
            return in_candidate_neighbors[cur][cand_idx][query_->GetInAdjIdx(cur, nxt)][nxt_idx];
        }
        
        // Auto-detect direction version (for compatibility, handles single direction only)
        std::vector<int>& GetCandidateNeighbors(int cur, int cand_idx, int nxt) {
            if (query_->GetEdgeIndex(cur, nxt) != -1) {
                return GetOutCandidateNeighbors(cur, cand_idx, nxt);
            }
            return GetInCandidateNeighbors(cur, cand_idx, nxt);
        }
        int GetCandidateNeighbor(int cur, int cand_idx, int nxt, int nxt_idx) {
            if (query_->GetEdgeIndex(cur, nxt) != -1) {
                return GetOutCandidateNeighbor(cur, cand_idx, nxt, nxt_idx);
            }
            return GetInCandidateNeighbor(cur, cand_idx, nxt, nxt_idx);
        }

        dict GetCSInfo() {return CSInfo;};

        int GetNumCSVertex() {return num_candidate_vertex;};
        int GetNumCSEdge() {return num_candidate_edge;};
    private:
        dict CSInfo;
        const neug::StorageReadInterface& graph_;
        DataGraphMeta& data_meta_;
        PatternGraph *query_;
        // Separate storage for out/in candidate neighbors
        // out_candidate_neighbors[u][cand_idx][out_neighbor_idx] = list of uc's candidate indices
        std::vector<std::vector<std::vector<std::vector<int>>>> out_candidate_neighbors;
        // in_candidate_neighbors[u][cand_idx][in_neighbor_idx] = list of uc's candidate indices
        std::vector<std::vector<std::vector<std::vector<int>>>> in_candidate_neighbors;

        std::vector<std::vector<int>> candidate_set_;
        // For directed graph: separate label frequencies for out/in neighbors
        std::vector<int> out_neighbor_label_frequency;
        std::vector<int> in_neighbor_label_frequency;
        int num_candidate_vertex = 0, num_candidate_edge = 0;
        bool* out_neighbor_cs;
        bool* in_neighbor_cs;
        bool** BitsetCS;
        bool** BitsetEdgeCS;
        int* num_visit_cs_;

        bool BuildInitialCS();

        void ConstructCS();

        bool InitRootCandidates(int root);

        bool RefineCS();

        void PrepareNeighborSafety(int cur);

        bool CheckNeighborSafety(int cur, int cand);

        bool NeighborBipartiteSafety(int cur, int cand);

        bool EdgeBipartiteSafety(int cur, int cand);

        inline bool EdgeCandidacy(int query_edge_id, int data_edge_id);

        bool TriangleSafety(int query_edge_id, int data_edge_id);

        bool FourCycleSafety(int query_edge_id, int data_edge_id);

        bool CheckSubStructures(int cur, int cand);

        bool CheckVertexPropertyConstraints(int query_vertex, int data_vertex);
        
        bool CheckEdgePropertyConstraints(int query_edge_id, int data_edge_id);

        bool CheckValueConstraint(const gbi::Value& data_value, gbi::CompType comp_type, const gbi::Value& constraint_value);

        bool NeighborFilter(int cur, int cand) {
            switch (opt.neighborhood_filter) {
                case NEIGHBOR_SAFETY:
                    return CheckNeighborSafety(cur, cand);
                case NEIGHBOR_BIPARTITE_SAFETY:
                    return NeighborBipartiteSafety(cur, cand);
                case EDGE_BIPARTITE_SAFETY:
                    return EdgeBipartiteSafety(cur, cand);
            }
        };

        bool StructureFilter(int query_edge_id, int data_edge_id) {
            switch (opt.structure_filter) {
                case NO_STRUCTURE_FILTER:
                    return true;
                case TRIANGLE_SAFETY:
                    return TriangleSafety(query_edge_id, data_edge_id);
                case FOURCYCLE_SAFETY:
                    return TriangleSafety(query_edge_id, data_edge_id) and FourCycleSafety(query_edge_id, data_edge_id);
            }
            return true;
        };

    };

    CandidateSpace::CandidateSpace(const neug::StorageReadInterface& graph, DataGraphMeta& data_meta, SubgraphMatchingOption filter_option)
        : graph_(graph), data_meta_(data_meta) {
        opt = filter_option;
        BitsetCS = new bool*[opt.MAX_QUERY_VERTEX];
        for (int i = 0; i < opt.MAX_QUERY_VERTEX; i++) {
            BitsetCS[i] = new bool[data_meta_.GetNumVertices()]();
        }
        BitsetEdgeCS = new bool*[opt.MAX_QUERY_EDGE];
        for (int i = 0; i < opt.MAX_QUERY_EDGE; i++) {
            BitsetEdgeCS[i] = new bool[data_meta_.GetMaxEdgeIndex()]();
        }
        out_neighbor_cs = new bool[data_meta_.GetNumVertices()]();
        in_neighbor_cs = new bool[data_meta_.GetNumVertices()]();
        out_neighbor_label_frequency.resize(data_meta_.GetNumLabels());
        in_neighbor_label_frequency.resize(data_meta_.GetNumLabels());
        num_visit_cs_ = new int[data_meta_.GetNumVertices()]();
        candidate_set_.resize(opt.MAX_QUERY_VERTEX);
        BPSolver.Initialize(50, data_meta_.GetMaxDegree(), 50);
        fprintf(stderr, "Constructing Candidate Space: %d %d\n", opt.MAX_QUERY_VERTEX, opt.MAX_QUERY_EDGE);
    }

    CandidateSpace::~CandidateSpace() {
        for (int i = 0; i < opt.MAX_QUERY_VERTEX; i++) {
            delete[] BitsetCS[i];
        }
        delete[] BitsetCS;
        for (int i = 0; i < opt.MAX_QUERY_EDGE; i++) {
            delete[] BitsetEdgeCS[i];
        }
        delete[] BitsetEdgeCS;
        delete[] num_visit_cs_;
        delete[] out_neighbor_cs;
        delete[] in_neighbor_cs;
    }


    inline bool CandidateSpace::BuildCS(PatternGraph *query) {
        CSInfo.clear();
        query_ = query;
        num_candidate_vertex = num_candidate_edge = 0;
        for (int i = 0; i < query_->GetNumVertices(); i++) {
            memset(BitsetCS[i], false, data_meta_.GetNumVertices());
        }
        for (int i = 0; i < query_->GetNumEdges(); i++) {
            memset(BitsetEdgeCS[i], false, data_meta_.GetMaxEdgeIndex());
        }
        memset(num_visit_cs_, 0, data_meta_.GetNumVertices());
        BPSolver.Reset();
        for (int i = 0; i < query_->GetNumVertices(); i++) {
            candidate_set_[i].clear();
        }
        Timer CStimer; CStimer.Start();
        if (!BuildInitialCS()) return false;
        if (!RefineCS()) return false;
        ConstructCS();
        CStimer.Stop();
        CSInfo["CSBuildTime"] = CStimer.GetTime();
        CSInfo["#CSVertex"] = num_candidate_vertex;
        CSInfo["#CSEdge"] = num_candidate_edge;
        return true;
    }

    inline void CandidateSpace::ConstructCS() {
        // Initialize separate out/in candidate neighbors
        out_candidate_neighbors.clear();
        out_candidate_neighbors.resize(query_->GetNumVertices());
        in_candidate_neighbors.clear();
        in_candidate_neighbors.resize(query_->GetNumVertices());
        
        // candidate_index: maps data vertex -> index in candidate_set_[u]
        // Initialized to -1 to indicate "not a candidate"
        std::vector<int> candidate_index(data_meta_.GetNumVertices(), -1);
        
        for (int i = 0; i < query_->GetNumVertices(); ++i) {
            out_candidate_neighbors[i].resize(GetCandidateSetSize(i));
            in_candidate_neighbors[i].resize(GetCandidateSetSize(i));
        }
        
        for (int u = 0; u < query_->GetNumVertices(); u++) {
            int u_label = query_->GetVertexLabel(u);
            int u_in_degree = query_->GetInDegree(u);
            int u_out_degree = query_->GetOutDegree(u);
            num_candidate_vertex += GetCandidateSetSize(u);
            
            // Build candidate_index mapping and initialize neighbor arrays
            for (int idx = 0; idx < GetCandidateSetSize(u); idx++) {
                candidate_index[candidate_set_[u][idx]] = idx;
                out_candidate_neighbors[u][idx].resize(u_out_degree);
                in_candidate_neighbors[u][idx].resize(u_in_degree);
            }

            // Case 1: Process in-edges to u (query edges uc -> u)
            // For each in-neighbor uc of u, find data edges vc -> v
            for (int uc : query_->GetInNeighbors(u)) {
                // Query edge: uc -> u
                int query_edge_idx = query_->GetEdgeIndex(uc, u);
                for (size_t vc_idx = 0; vc_idx < candidate_set_[uc].size(); ++vc_idx) {
                    int vc = candidate_set_[uc][vc_idx];
                    // Data edges: out-edges from vc to vertices with u_label
                    for (const auto& data_edge : data_meta_.GetOutIncidentEdges(vc, u_label)) {
                        int v = data_meta_.GetDestPoint(data_edge);  // v is the destination of edge vc -> v
                        int data_edge_idx = data_meta_.EdgeToIndex(data_edge);
                        // Directed graph: check in-degree for in-edges
                        if (data_meta_.GetInDegree(v) < u_in_degree) break;
                        if (data_meta_.GetOutDegree(v) < u_out_degree) break;
                        if (!BitsetEdgeCS[query_edge_idx][data_edge_idx]) continue;
                        int v_idx = candidate_index[v];
                        if (v_idx == -1) continue;  // v is not a candidate for u
                        num_candidate_edge++;
                        // Store in in_candidate_neighbors for in-edge uc -> u
                        in_candidate_neighbors[u][v_idx][query_->GetInAdjIdx(u, uc)].emplace_back(vc_idx);
                    }
                }
            }
            
            // Case 2: Process out-edges from u (query edges u -> uc)
            // For each out-neighbor uc of u, find data edges v -> vc
            for (int uc : query_->GetOutNeighbors(u)) {
                // Query edge: u -> uc
                int query_edge_idx = query_->GetEdgeIndex(u, uc);
                // Iterate over uc's candidates and look for in-edges from u's candidates
                for (size_t vc_idx = 0; vc_idx < candidate_set_[uc].size(); ++vc_idx) {
                    int vc = candidate_set_[uc][vc_idx];
                    // Data edges: in-edges to vc from vertices with u_label
                    for (const auto& data_edge : data_meta_.GetInIncidentEdges(vc, u_label)) {
                        // Get source vertex v (the edge is v -> vc)
                        int v = data_meta_.GetSourcePoint(data_edge);
                        int data_edge_idx = data_meta_.EdgeToIndex(data_edge);
                        // Directed graph: check out-degree for out-edges
                        if (data_meta_.GetOutDegree(v) < u_out_degree) continue;
                        if (!BitsetEdgeCS[query_edge_idx][data_edge_idx]) continue;
                        int v_idx = candidate_index[v];
                        if (v_idx == -1) continue;  // v is not a candidate for u
                        num_candidate_edge++;
                        // Store in out_candidate_neighbors for out-edge u -> uc
                        out_candidate_neighbors[u][v_idx][query_->GetOutAdjIdx(u, uc)].emplace_back(vc_idx);
                    }
                }
            }
            
            // Reset candidate_index for next query vertex
            for (int idx = 0; idx < GetCandidateSetSize(u); idx++) {
                candidate_index[candidate_set_[u][idx]] = -1;
            }
        }
    }

    inline bool CandidateSpace::BuildInitialCS() {
        std::memset(num_visit_cs_, 0, data_meta_.GetNumVertices() * sizeof(int));
        std::vector<int> initial_cs_size(query_->GetNumVertices(), 0);
        // Separate built neighbors for in/out edges
        // built_out_neighbors[cur]: neighbors where edge is cur -> neighbor (already built)
        // built_in_neighbors[cur]: neighbors where edge is neighbor -> cur (already built)
        std::vector<std::vector<int>> built_out_neighbors(query_->GetNumVertices());
        std::vector<std::vector<int>> built_in_neighbors(query_->GetNumVertices());
        int root = 0;
        for (int i = 0; i < query_->GetNumVertices(); i++) {
            int l = query_->GetVertexLabel(i);
            initial_cs_size[i] = data_meta_.GetVerticesByLabel(l).size();
            if (initial_cs_size[i] <= initial_cs_size[0]) {
                root = i;
            }
        }
        InitRootCandidates(root);
        // For directed graphs: separate out and in neighbors
        for (int uc : query_->GetOutNeighbors(root)) {
            // root -> uc, so uc has root as in-neighbor
            built_in_neighbors[uc].push_back(root);
        }
        for (int uc : query_->GetInNeighbors(root)) {
            // uc -> root, so uc has root as out-neighbor
            built_out_neighbors[uc].push_back(root);
        }
        for (int i = 1; i < query_->GetNumVertices(); i++) {
            int cur = -1;
            for (int j = 0; j < query_->GetNumVertices(); j++) {
                if (!candidate_set_[j].empty()) continue;
                if (cur == -1) { cur = j; continue; }
                int j_total = built_out_neighbors[j].size() + built_in_neighbors[j].size();
                int cur_total = built_out_neighbors[cur].size() + built_in_neighbors[cur].size();
                if (j_total > cur_total) { cur = j; continue; }
                if (j_total == cur_total && initial_cs_size[j] < initial_cs_size[cur]) { cur = j; continue; }
            }
            int cur_label = query_->GetVertexLabel(cur);
            int num_parent = 0;
            
            // Process in-neighbors (parent -> cur edges)
            for (int parent : built_in_neighbors[cur]) {
                int query_edge_idx = query_->GetEdgeIndex(parent, cur);
                for (int parent_cand : candidate_set_[parent]) {
                    // parent -> cur, so get out-edges from parent_cand
                    for (const auto& data_edge : data_meta_.GetOutIncidentEdges(parent_cand, cur_label)) {
                        int cand = data_meta_.GetDestPoint(data_edge);
                        int data_edge_idx = data_meta_.EdgeToIndex(data_edge);
                        if (data_meta_.GetInDegree(cand) < query_->GetInDegree(cur) ||
                            data_meta_.GetOutDegree(cand) < query_->GetOutDegree(cur)) continue;
                        if (data_meta_.GetEdgeLabel(data_edge) != query_->GetEdgeLabel(query_edge_idx)) continue;
                        if (num_visit_cs_[cand] < num_parent) continue;
                        if (data_meta_.GetCoreNum(cand) < query_->GetCoreNum(cur)) continue;
                        if (!CheckVertexPropertyConstraints(cur, cand)) continue;
                        if (!CheckEdgePropertyConstraints(query_edge_idx, data_edge_idx)) continue;
                        if (num_visit_cs_[cand] == num_parent) {
                            num_visit_cs_[cand] += 1;
                            if (num_visit_cs_[cand] == 1) {
                                candidate_set_[cur].emplace_back(cand);
                                BitsetCS[cur][cand] = true;
                            }
                        }
                    }
                }
                num_parent++;
            }
            
            // Process out-neighbors (cur -> parent edges)
            for (int parent : built_out_neighbors[cur]) {
                int query_edge_idx = query_->GetEdgeIndex(cur, parent);
                for (int parent_cand : candidate_set_[parent]) {
                    // cur -> parent, so get in-edges to parent_cand
                    for (const auto& data_edge : data_meta_.GetInIncidentEdges(parent_cand, cur_label)) {
                        int cand = data_meta_.GetSourcePoint(data_edge);
                        int data_edge_idx = data_meta_.EdgeToIndex(data_edge);
                        if (data_meta_.GetInDegree(cand) < query_->GetInDegree(cur) ||
                            data_meta_.GetOutDegree(cand) < query_->GetOutDegree(cur)) continue;
                        if (data_meta_.GetEdgeLabel(data_edge) != query_->GetEdgeLabel(query_edge_idx)) continue;
                        if (num_visit_cs_[cand] < num_parent) continue;
                        if (data_meta_.GetCoreNum(cand) < query_->GetCoreNum(cur)) continue;
                        if (!CheckVertexPropertyConstraints(cur, cand)) continue;
                        if (!CheckEdgePropertyConstraints(query_edge_idx, data_edge_idx)) continue;
                        if (num_visit_cs_[cand] == num_parent) {
                            num_visit_cs_[cand] += 1;
                            if (num_visit_cs_[cand] == 1) {
                                candidate_set_[cur].emplace_back(cand);
                                BitsetCS[cur][cand] = true;
                            }
                        }
                    }
                }
                num_parent++;
            }
            
            for (size_t j = 0; j < candidate_set_[cur].size(); j++) {
                int cand = candidate_set_[cur][j];
                BitsetCS[cur][cand] = false;
                if (num_visit_cs_[cand] == num_parent) {
                    BitsetCS[cur][cand] = true;
                }
                else {
                    candidate_set_[cur][j] = candidate_set_[cur].back();
                    candidate_set_[cur].pop_back();
                    j--;
                }
                num_visit_cs_[cand] = 0;
            }
            if (candidate_set_[cur].empty()) {
                std::cout << "Empty Candidate Set during Initial CS Construction!" << std::endl;
                return false;
            }
            // Update built neighbors for unvisited neighbors
            for (int uc : query_->GetOutNeighbors(cur)) {
                if (candidate_set_[uc].empty()) {
                    // cur -> uc, so uc has cur as in-neighbor
                    built_in_neighbors[uc].push_back(cur);
                }
            }
            for (int uc : query_->GetInNeighbors(cur)) {
                if (candidate_set_[uc].empty()) {
                    // uc -> cur, so uc has cur as out-neighbor
                    built_out_neighbors[uc].push_back(cur);
                }
            }
        }

        int cs_edge = 0, cs_vertex = 0;
        for (int i = 0; i < query_->GetNumVertices(); i++) {
            // For directed graphs: use out-edges for edge candidacy
            for (int q_edge_idx : query_->GetAllOutIncidentEdges(i)) {
                int q_nxt = query_->GetOppositePoint(q_edge_idx);
                for (int j : candidate_set_[i]) {
                    for (const auto& d_edge : data_meta_.GetOutIncidentEdges(j, query_->GetVertexLabel(q_nxt))) {
                        int d_edge_idx = data_meta_.EdgeToIndex(d_edge);
                        if (data_meta_.GetEdgeLabel(d_edge) != query_->GetEdgeLabel(q_edge_idx)) continue;
                        
                        // 添加边属性约束检查
                        if (!CheckEdgePropertyConstraints(q_edge_idx, d_edge_idx)) continue;
                        
                        int d_nxt = data_meta_.GetDestPoint(d_edge);
                        if (BitsetCS[q_nxt][d_nxt]) {
                            BitsetEdgeCS[q_edge_idx][d_edge_idx] = true;
                            cs_edge++;
                        }
                    }
                }
            }
            cs_vertex += candidate_set_[i].size();
        }

        // for (int i = 0; i < query_->GetNumVertices(); i++) {
        //     fprintf(stderr, "Init CS %d: %lu\n", i, candidate_set_[i].size());
        // }
        return true;
    }

    inline bool CandidateSpace::InitRootCandidates(int root) {
        int root_label = query_->GetVertexLabel(root);
        for (int cand : data_meta_.GetVerticesByLabel(root_label)) {
            // Directed graph: check both in-degree and out-degree
            if (data_meta_.GetInDegree(cand) < query_->GetInDegree(root) ||
                data_meta_.GetOutDegree(cand) < query_->GetOutDegree(root)) continue;
            if (data_meta_.GetCoreNum(cand) < query_->GetCoreNum(root)) continue;
            
            // 添加属性约束检查
            if (!CheckVertexPropertyConstraints(root, cand)) continue;
            
            candidate_set_[root].emplace_back(cand);
            BitsetCS[root][cand] = true;
        }
        return !candidate_set_[root].empty();
    }

    inline bool CandidateSpace::RefineCS(){
        std::vector<int> local_stage(query_->GetNumVertices(), 0);
        std::vector<double> priority(query_->GetNumVertices(), 0.50);
        int queue_pop_count = 0;
        int maximum_queue_cnt = 5 * query_->GetNumEdges();
        int current_stage = 0;
        while (queue_pop_count < maximum_queue_cnt) {
            int cur = 0;
            double cur_priority = priority[cur];
            for (int i = 1; i < query_->GetNumVertices(); i++) {
                if (priority[i] > cur_priority) {
                    cur_priority = priority[i];
                    cur = i;
                }
                else if (priority[i] == cur_priority) {
                    if (GetCandidateSetSize(i) > GetCandidateSetSize(cur)) {
                        cur = i;
                    }
                }
            }
            if (cur_priority < 0.1) break;
            current_stage++;
            queue_pop_count+=query_->GetDegree(cur);
            int bef_cand_size = candidate_set_[cur].size();
            if (opt.neighborhood_filter == NEIGHBOR_SAFETY) {
                std::fill(out_neighbor_label_frequency.begin(), out_neighbor_label_frequency.end(), 0);
                std::fill(in_neighbor_label_frequency.begin(), in_neighbor_label_frequency.end(), 0);
                memset(out_neighbor_cs, false, data_meta_.GetNumVertices());
                memset(in_neighbor_cs, false, data_meta_.GetNumVertices());
                PrepareNeighborSafety(cur);
            }
            for (int i = 0; i < candidate_set_[cur].size(); i++) {
                int cand = candidate_set_[cur][i];
                bool valid = true;
                if (valid and opt.structure_filter > NO_STRUCTURE_FILTER) valid = CheckSubStructures(cur, cand);
                if (valid) valid = NeighborFilter(cur, cand);
                // if (cur == 11 and cand == 56230) {
                //     fprintf(stderr, "Cur, Cand: %d, %d\n", cur, cand);
                //     fprintf(stderr, "NBR: %d\n", NeighborFilter(cur, cand));
                //     fprintf(stderr, "Struct: %d\n", CheckSubStructures(cur, cand));
                // }
                if (!valid) {
                    int removed = candidate_set_[cur][i];
                    // For directed graphs: iterate over out-edges from cur
                    for (int query_edge_idx : query_->GetAllOutIncidentEdges(cur)) {
                        int nxt = query_->GetOppositePoint(query_edge_idx);
                        int nxt_label = query_->GetVertexLabel(nxt);
                        for (const auto& data_edge : data_meta_.GetOutIncidentEdges(removed, nxt_label)) {
                            int nxt_cand = data_meta_.GetDestPoint(data_edge);
                            int data_edge_idx = data_meta_.EdgeToIndex(data_edge);
                            // Directed graph: check both in-degree and out-degree for nxt
                            if (data_meta_.GetInDegree(nxt_cand) < query_->GetInDegree(nxt) ||
                                data_meta_.GetOutDegree(nxt_cand) < query_->GetOutDegree(nxt)) continue;
                            if (BitsetEdgeCS[query_edge_idx][data_edge_idx]) {
                                BitsetEdgeCS[query_edge_idx][data_edge_idx] = false;
                                // No opposite edge in directed graph
                            }
                        }
                    }
                    // if (cur == 11 and cand == 56230) {
                    //     fprintf(stderr, "Remove %d from CS[%d]! swap with %d\n",
                    //         candidate_set_[cur][i], cur, candidate_set_[cur].back());
                    // }
                    candidate_set_[cur][i] = candidate_set_[cur].back();
                    candidate_set_[cur].pop_back();
                    --i;
                    BitsetCS[cur][cand] = false;
                }
            }
            if (candidate_set_[cur].empty()) {
                std::cout << "Empty Candidate Set during Initial CS Construction!" << std::endl;
                return false;
            }
            int aft_cand_size = candidate_set_[cur].size();
            // fprintf(stderr, "Refine CS %d: %d -> %d\n", cur, bef_cand_size, aft_cand_size);

            if (aft_cand_size == bef_cand_size) {
                priority[cur] = 0;
                continue;
            }
            double out_prob = 1 - aft_cand_size * 1.0 / bef_cand_size;
            priority[cur] = 0;
            for (int nxt : query_->GetNeighbors(cur)) {
                priority[nxt] = 1 - (1 - out_prob) * (1 - priority[nxt]);
                local_stage[nxt] = current_stage;
            }
        }
        return true;
    }

    inline void CandidateSpace::PrepareNeighborSafety(int cur) {
        // For directed graphs: separately count out-neighbors and in-neighbors
        // out_neighbor_label_frequency: labels of cur's out-neighbors (cur -> neighbor)
        // in_neighbor_label_frequency: labels of cur's in-neighbors (neighbor -> cur)
        for (int q_neighbor : query_->GetOutNeighbors(cur)) {
            out_neighbor_label_frequency[query_->GetVertexLabel(q_neighbor)]++;
            for (int d_neighbor : candidate_set_[q_neighbor]) {
                out_neighbor_cs[d_neighbor] = true;
            }
        }
        for (int q_neighbor : query_->GetInNeighbors(cur)) {
            in_neighbor_label_frequency[query_->GetVertexLabel(q_neighbor)]++;
            for (int d_neighbor : candidate_set_[q_neighbor]) {
                in_neighbor_cs[d_neighbor] = true;
            }
        }
    }

    inline bool CandidateSpace::CheckNeighborSafety(int cur, int cand) {
        // For directed graphs: check out-neighbors and in-neighbors separately
        // Check out-neighbors (cand -> d_neighbor must cover cur -> q_neighbor)
        for (int d_neighbor : data_meta_.GetOutNeighbors(cand)) {
            if (out_neighbor_cs[d_neighbor]) {
                out_neighbor_label_frequency[data_meta_.GetVertexLabel(d_neighbor)]--;
            }
        }
        // Check in-neighbors (d_neighbor -> cand must cover q_neighbor -> cur)
        for (int d_neighbor : data_meta_.GetInNeighbors(cand)) {
            if (in_neighbor_cs[d_neighbor]) {
                in_neighbor_label_frequency[data_meta_.GetVertexLabel(d_neighbor)]--;
            }
        }
        
        bool valid = true;
        for (int l = 0; l < data_meta_.GetNumLabels(); ++l) {
            if (out_neighbor_label_frequency[l] > 0 || in_neighbor_label_frequency[l] > 0) {
                valid = false;
                break;
            }
        }
        
        // Restore frequencies
        for (int d_neighbor : data_meta_.GetOutNeighbors(cand)) {
            if (out_neighbor_cs[d_neighbor]) {
                out_neighbor_label_frequency[data_meta_.GetVertexLabel(d_neighbor)]++;
            }
        }
        for (int d_neighbor : data_meta_.GetInNeighbors(cand)) {
            if (in_neighbor_cs[d_neighbor]) {
                in_neighbor_label_frequency[data_meta_.GetVertexLabel(d_neighbor)]++;
            }
        }
        return valid;
    }

    inline bool CandidateSpace::EdgeCandidacy(int query_edge_id, int data_edge_id) {
        if (query_edge_id == -1 || data_edge_id == -1) {
            return false;
        }
        return BitsetEdgeCS[query_edge_id][data_edge_id];
    }

    inline bool CandidateSpace::TriangleSafety(int query_edge_id, int data_edge_id) {
        auto &query_triangles = query_->GetLocalTriangles(query_edge_id);
        if (query_triangles.empty()) return true;
        auto &candidate_triangles = data_meta_.GetLocalTriangles(data_edge_id);
        if (query_triangles.size() > candidate_triangles.size()) return false;
        for (auto qtv: query_triangles) {
            bool found = std::any_of(candidate_triangles.begin(),
                                     candidate_triangles.end(),
                                     [&](auto tv) {
                                         return BitsetEdgeCS[std::get<1>(qtv)][std::get<1>(tv)] and  BitsetEdgeCS[std::get<2>(qtv)][std::get<2>(tv)];
                                     });
            if (!found) return false;
        }
        return true;
    };

    inline bool CandidateSpace::FourCycleSafety(int query_edge_id, int data_edge_id) {
        auto &query_cycles = query_->GetLocalFourCycles(query_edge_id);
        auto &data_cycles = data_meta_.GetLocalFourCycles(data_edge_id);
        if (query_cycles.size() > data_cycles.size()) return false;
        for (int i = 0; i < query_cycles.size(); i++) {
            auto &q_info = query_cycles[i];
            for (int j = 0; j < data_cycles.size(); j++) {
                auto &d_info =  data_cycles[j];
                bool validity = true;
                validity &= BitsetEdgeCS[std::get<1>(q_info.edges)][std::get<1>(d_info.edges)];
                if (!validity) continue;
                validity &= BitsetEdgeCS[std::get<2>(q_info.edges)][std::get<2>(d_info.edges)];
                if (!validity) continue;
                validity &= BitsetEdgeCS[std::get<3>(q_info.edges)][std::get<3>(d_info.edges)];
                if (validity and std::get<0>(q_info.diags) != -1)
                    validity &= EdgeCandidacy(std::get<0>(q_info.diags),std::get<0>(d_info.diags));
                if (validity and std::get<1>(q_info.diags) != -1)
                    validity &= EdgeCandidacy(std::get<1>(q_info.diags), std::get<1>(d_info.diags));
                if (validity) {
                    goto nxt_cycle;
                }
            }
            return false;
            nxt_cycle:
            continue;
        }
        return true;
    };

    inline bool CandidateSpace::CheckSubStructures(int cur, int cand) {
        // For directed graphs: check out-edges from cur
        for (int query_edge_idx : query_->GetAllOutIncidentEdges(cur)) {
            int nxt = query_->GetOppositePoint(query_edge_idx);
            int nxt_label = query_->GetVertexLabel(nxt);
            bool found = false;
            for (const auto& data_edge : data_meta_.GetOutIncidentEdges(cand, nxt_label)) {
                int nxt_cand = data_meta_.GetDestPoint(data_edge);
                int data_edge_idx = data_meta_.EdgeToIndex(data_edge);
                // Directed graph: check both in-degree and out-degree for nxt
                if (data_meta_.GetInDegree(nxt_cand) < query_->GetInDegree(nxt) ||
                    data_meta_.GetOutDegree(nxt_cand) < query_->GetOutDegree(nxt)) continue;
                if (!BitsetEdgeCS[query_edge_idx][data_edge_idx]) continue;
                if (!StructureFilter(query_edge_idx, data_edge_idx)) {
                    BitsetEdgeCS[query_edge_idx][data_edge_idx] = false;
                    // No opposite edge in directed graph
                    continue;
                }
                found = true;
            }
            if (!found) {
                return false;
            }
        }
        return true;
    };

    inline bool CandidateSpace::NeighborBipartiteSafety(int cur, int cand){
        // For directed graphs: check out-edges
        if (query_->GetOutDegree(cur) == 1) {
            int uc = query_->GetOutNeighbors(cur)[0];
            int query_edge_index = query_->GetEdgeIndex(cur, uc);
            int label = query_->GetVertexLabel(uc);
            auto edges = data_meta_.GetOutIncidentEdges(cand, label);
            for (const auto& data_edge : edges) {
                int data_edge_index = data_meta_.EdgeToIndex(data_edge);
                if (BitsetEdgeCS[query_edge_index][data_edge_index]) {
                    return true;
                }
            }
            return false;
        }
        BPSolver.Reset();
        int i = 0, j = 0;
        for (int query_edge_index : query_->GetAllOutIncidentEdges(cur)) {
            j = 0;
            for (const auto& data_edge : data_meta_.GetAllOutIncidentEdges(cand)) {
                int edge_id = data_meta_.EdgeToIndex(data_edge);
                if (BitsetEdgeCS[query_edge_index][edge_id]) {
                    BPSolver.AddEdge(i, j);
                }
                j++;
            }
            i++;
        }
        return BPSolver.Solve() == query_->GetOutDegree(cur);
    };

    inline bool CandidateSpace::EdgeBipartiteSafety(int cur, int cand) {
        // For directed graphs: use out-edges
        auto query_edges = query_->GetAllOutIncidentEdges(cur);
        auto data_edges_vec = data_meta_.GetAllOutIncidentEdges(cand);
        // Convert data edges to indices for BitsetEdgeCS access
        std::vector<int> data_edge_indices;
        for (const auto& e : data_edges_vec) {
            data_edge_indices.push_back(data_meta_.EdgeToIndex(e));
        }
        
        if (query_edges.size() == 1) {
            int q_edge_id = query_edges[0];
            for (int d_edge_id : data_edge_indices) {
                if (BitsetEdgeCS[q_edge_id][d_edge_id])
                    return true;
            }
            return false;
        }
        std::vector<std::pair<int, int>> edge_pairs;
        int ii = 0, jj = 0;
        BPSolver.Reset();
        for (int query_edge_index : query_edges) {
            int uc = query_->GetOppositePoint(query_edge_index);
            jj = 0;
            for (size_t idx = 0; idx < data_edges_vec.size(); ++idx) {
                const auto& data_edge = data_edges_vec[idx];
                int edge_id = data_edge_indices[idx];
                int vc = data_meta_.GetDestPoint(data_edge);
                // Directed graph: check both in-degree and out-degree for uc
                if (data_meta_.GetInDegree(vc) < query_->GetInDegree(uc) ||
                    data_meta_.GetOutDegree(vc) < query_->GetOutDegree(uc)) {
                    jj++;
                    continue;
                }
                if (BitsetEdgeCS[query_edge_index][edge_id]) {
                    BPSolver.AddEdge(ii, jj);
                    edge_pairs.emplace_back(ii, jj);
                }
                jj++;
            }
            ii++;
        }
        bool b = BPSolver.FindUnmatchableEdges(query_edges.size());
        if (!b) {
            return false;
        }
        for (auto &[i, j] : edge_pairs) {
            if (!BPSolver.matchable[i][j]) {
                int left_unmatch = query_edges[i];
                int right_unmatch = data_edge_indices[j];
                BitsetEdgeCS[left_unmatch][right_unmatch] = false;
                // No opposite edge in directed graph
            }
        }
        return true;
    }

    inline bool CandidateSpace::CheckVertexPropertyConstraints(int query_vertex, int data_vertex) {
        // 如果没有属性约束，直接返回true
        if (query_->vertex_property_constraints.empty()) {
            return true;
        }
        
        // 检查该顶点索引是否在约束范围内
        if (query_vertex >= query_->vertex_property_constraints.size()) {
            return true; // 没有约束则认为满足
        }
        
        const std::vector<gbi::PropCons>& constraints = query_->vertex_property_constraints[query_vertex];
        
        // 如果该顶点没有约束，返回true
        if (constraints.empty()) {
            return true;
        }
        
        // 获取数据顶点的标签，用于查找属性名
        int data_label = data_meta_.GetVertexLabel(data_vertex);
        
        // 检查所有约束，所有约束都必须满足
        for (const auto& constraint : constraints) {
            // 如果约束的属性名为空，跳过该约束
            if (constraint._prop_name.empty()) {
                continue;
            }
            
            // 查找属性名在该标签属性名列表中的索引
            const std::vector<std::string>& prop_names = data_meta_.vertex_property_names[data_label];
            
            auto it = std::find(prop_names.begin(), prop_names.end(), constraint._prop_name);
            if (it == prop_names.end()) {
                std::cout << "property name not found: " << constraint._prop_name << std::endl;
                continue;
                // return false; // 数据顶点没有该属性
            }
            
            size_t prop_idx = std::distance(prop_names.begin(), it);
            if (prop_idx >= data_meta_.vertex_properties[data_vertex].size()) {
                return false; // 属性值数组越界
            }
            
            const gbi::Value& data_value = data_meta_.vertex_properties[data_vertex][prop_idx];
            
            // 比较属性值是否满足约束
            if (!CheckValueConstraint(data_value, constraint._comp_type, constraint._value)) {
                return false; // 有任何一个约束不满足就返回false
            }
        }
        
        return true; // 所有约束都满足
    }

    inline bool CandidateSpace::CheckEdgePropertyConstraints(int query_edge_id, int data_edge_id) {
        // 如果没有边属性约束，直接返回true
        if (query_->edge_property_constraints.empty()) {
            return true;
        }
        
        // 检查该边索引是否在约束范围内
        if (query_edge_id >= query_->edge_property_constraints.size()) {
            return true; // 没有约束则认为满足
        }
        
        const std::vector<gbi::PropCons>& constraints = query_->edge_property_constraints[query_edge_id];
        
        // 如果该边没有约束，返回true
        if (constraints.empty()) {
            return true;
        }
        
        // 检查所有约束，所有约束都必须满足
        for (const auto& constraint : constraints) {
            // 如果约束的属性名为空，跳过该约束
            if (constraint._prop_name.empty()) {
                continue;
            }
            
            // 查找属性名在边属性名列表中的索引
            const std::vector<std::string>& prop_names = data_meta_.edge_property_names[data_meta_.GetEdgeLabel(data_edge_id)]; 
            
            auto it = std::find(prop_names.begin(), prop_names.end(), constraint._prop_name);
            if (it == prop_names.end()) {
                std::cout << "property name not found: " << constraint._prop_name << std::endl;
                continue;
                // return false; // 数据边没有该属性
            }
            
            size_t prop_idx = std::distance(prop_names.begin(), it);
            if (prop_idx >= data_meta_.edge_properties[data_edge_id].size()) {
                return false; // 属性值数组越界
            }
            
            const gbi::Value& data_value = data_meta_.edge_properties[data_edge_id][prop_idx];
            
            // 比较属性值是否满足约束
            if (!CheckValueConstraint(data_value, constraint._comp_type, constraint._value)) {
                return false; // 有任何一个约束不满足就返回false
            }
        }
        
        return true; // 所有约束都满足
    }

    // 辅助函数：检查值是否满足约束条件
    inline bool CandidateSpace::CheckValueConstraint(const gbi::Value& data_value, gbi::CompType comp_type, const gbi::Value& constraint_value) {
        switch (comp_type) {
            case gbi::CompType::COMP_EQUAL:
                return data_value == constraint_value;
            case gbi::CompType::COMP_GREATER:
                return data_value > constraint_value;
            case gbi::CompType::COMP_LESS:
                return data_value < constraint_value;
            case gbi::CompType::COMP_GREATER_EQUAL:
                return data_value >= constraint_value;
            case gbi::CompType::COMP_LESS_EQUAL:
                return data_value <= constraint_value;
            // TODO: 实现 COMP_IN 和 COMP_NOT_IN
            default:
                return true; // 不支持的比较类型，默认返回true
        }
    }

} }
