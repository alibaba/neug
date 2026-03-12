#include "../DataStructure/Graph.h"

namespace GraphLib {

    const int MAX_NUM_TRIANGLES = 1000000000;
    const int MAX_NUM_FOUR_CYCLES = 1000000000;

    bool Graph::EnumerateLocalTriangles() {
        std::cout << GetNumVertices() << " " << GetNumEdges() << std::endl;
        local_triangles.resize(GetNumEdges());
        std::vector<int> vertices_sorted_by_degree(GetNumVertices(), -1);
        std::iota(vertices_sorted_by_degree.begin(), vertices_sorted_by_degree.end(), 0);
        std::sort(vertices_sorted_by_degree.begin(), vertices_sorted_by_degree.end(),
                  [&](int a, int b) { return GetDegree(a) > GetDegree(b); });
        std::vector<std::vector<int>> inc_edge_list(GetNumVertices(), std::vector<int>());
        std::vector<int> edge_inv_idx_from(GetNumEdges(), -1);
        std::vector<int> nbr_edge_id(GetNumVertices(), -1);
        for (int i = 0; i < GetNumVertices(); i++) {
            for (int e : GetAllIncidentEdges(i)) {
                edge_inv_idx_from[e] = inc_edge_list[i].size();
                inc_edge_list[i].emplace_back(e);
            }
        }
        unsigned long long num_triangles = 0;
        for (int i = 0; i < GetNumVertices() - 2; i++) {
            int u = vertices_sorted_by_degree[i];
            for (int e : inc_edge_list[u]) {
                nbr_edge_id[GetOppositePoint(e)] = e;
            }
            for (int e : inc_edge_list[u]) {
                int v = GetOppositePoint(e), oe = GetOppositeEdge(e);
                for (int eprime : GetAllIncidentEdges(v)) {
                    int w = GetOppositePoint(eprime);
                    if (nbr_edge_id[w] != -1) {
                        local_triangles[e].emplace_back(std::tuple(w, nbr_edge_id[w], eprime));
                        local_triangles[oe].emplace_back(std::tuple(w, eprime, nbr_edge_id[w]));
                        num_triangles += 2;
                        if (e > nbr_edge_id[w]) {
                            local_triangles[eprime].emplace_back(std::tuple(u, oe, GetOppositeEdge(nbr_edge_id[w])));
                            local_triangles[GetOppositeEdge(eprime)].emplace_back(std::tuple(u, GetOppositeEdge(nbr_edge_id[w]), oe));
                            num_triangles += 2;
                        }
                        if (num_triangles >= MAX_NUM_TRIANGLES) {
                            local_triangles.clear();
                            std::cout << "too many triangles" << std::endl;
                            return false;
                        }
                    }
                }
            }
            for (int e : inc_edge_list[u]) {
                int v = GetOppositePoint(e), oe = GetOppositeEdge(e);
                nbr_edge_id[v] = -1;
                int oe_idx_from_v = edge_inv_idx_from[oe];
                if (inc_edge_list[v].size() == 1) {
                    inc_edge_list[v].pop_back();
                }
                else {
                    int v_inc_last = inc_edge_list[v].back();
                    std::swap(inc_edge_list[v][inc_edge_list[v].size()-1], inc_edge_list[v][oe_idx_from_v]);
                    edge_inv_idx_from[v_inc_last] = oe_idx_from_v;
                    edge_inv_idx_from[oe] = -1;
                    inc_edge_list[v].pop_back();
                }
            }
        }

        return true;
    }



    bool Graph::EnumerateLocalFourCycles() {
        Timer timer; timer.Start();
        local_four_cycles.resize(GetNumEdges());
        double total_required = 0, done = 0;
        for (int i = 0; i < GetNumEdges(); i++) {
            auto &[u, v] = edge_list[i];
            total_required += GetDegree(u) * GetDegree(v);
        }
        long long num_four_cycles = 0;
        for (int i = 0; i < GetNumEdges(); i++) {
            auto &[u, v] = edge_list[i];
            if (GetDegree(u) < GetDegree(v)) {
                for (int &fourth_edge_opp : GetAllIncidentEdges(u)) {
                    int fourth_vertex = GetOppositePoint(fourth_edge_opp);
                    if (fourth_vertex == v) continue;
                    if (GetDegree(fourth_vertex) < GetDegree(v)) {
                        for (int &third_edge_opp : GetAllIncidentEdges(fourth_vertex)) {
                            int third_vertex = GetOppositePoint(third_edge_opp);
                            if (third_vertex == u) continue;
                            int snd_edge = GetEdgeIndex(v, third_vertex);
                            if (snd_edge != -1) {
                                int fourth_edge = fourth_edge_opp ^ 1;
                                int third_edge = third_edge_opp ^ 1;
                                int fst_diag = GetEdgeIndex(u, third_vertex);
                                int snd_diag = GetEdgeIndex(v, fourth_vertex);
                                local_four_cycles[i].emplace_back(FourMotif(
                                        {i, snd_edge, third_edge, fourth_edge},
                                        {fst_diag, snd_diag})
                                );
                                num_four_cycles++;
                                if (num_four_cycles >= MAX_NUM_FOUR_CYCLES) {
                                    local_four_cycles.clear();
                                    std::cout << "too many four cycles" << std::endl;
                                    return false;
                                }
                            }
                        }
                    }
                    else {
                        for (int &snd_edge : GetAllIncidentEdges(v)) {
                            int third_vertex = GetOppositePoint(snd_edge);
                            if (third_vertex == u) continue;
                            int third_edge = GetEdgeIndex(third_vertex, fourth_vertex);
                            if (third_edge != -1) {
                                int fourth_edge = fourth_edge_opp ^ 1;
                                int fst_diag = GetEdgeIndex(u, third_vertex);
                                int snd_diag = GetEdgeIndex(v, fourth_vertex);
                                local_four_cycles[i].emplace_back(FourMotif(
                                        {i, snd_edge, third_edge, fourth_edge},
                                        {fst_diag, snd_diag})
                                );
                                num_four_cycles++;
                                if (num_four_cycles >= MAX_NUM_FOUR_CYCLES) {
                                    local_four_cycles.clear();
                                    std::cout << "too many four cycles" << std::endl;
                                    return false;
                                }
                            }
                        }
                    }
                }
            }
            else {
                for (int &snd_edge : GetAllIncidentEdges(v)) {
                    int third_vertex = GetOppositePoint(snd_edge);
                    if (third_vertex == u) continue;
                    if (GetDegree(third_vertex) < GetDegree(u)) {
                        for (int &third_edge : GetAllIncidentEdges(third_vertex)) {
                            int fourth_vertex = GetOppositePoint(third_edge);
                            if (fourth_vertex == v) continue;
                            int fourth_edge = GetEdgeIndex(fourth_vertex, u);
                            if (fourth_edge != -1) {
                                int fst_diag = GetEdgeIndex(u, third_vertex);
                                int snd_diag = GetEdgeIndex(v, fourth_vertex);
                                local_four_cycles[i].emplace_back(FourMotif(
                                        {i, snd_edge, third_edge, fourth_edge},
                                        {fst_diag, snd_diag})
                                );
                                num_four_cycles++;
                                if (num_four_cycles >= MAX_NUM_FOUR_CYCLES) {
                                    local_four_cycles.clear();
                                    std::cout << "too many four cycles" << std::endl;
                                    return false;
                                }
                            }
                        }
                    }
                    else {
                        for (int &fourth_edge_opp : GetAllIncidentEdges(u)) {
                            int fourth_vertex = GetOppositePoint(fourth_edge_opp);
                            if (fourth_vertex == v) continue;
                            int third_edge = GetEdgeIndex(third_vertex, fourth_vertex);
                            if (third_edge != -1) {
                                int fourth_edge = fourth_edge_opp ^ 1;
                                int fst_diag = GetEdgeIndex(u, third_vertex);
                                int snd_diag = GetEdgeIndex(v, fourth_vertex);
                                local_four_cycles[i].emplace_back(FourMotif(
                                        {i, snd_edge, third_edge, fourth_edge},
                                        {fst_diag, snd_diag})
                                );
                                num_four_cycles++;
                                if (num_four_cycles >= MAX_NUM_FOUR_CYCLES) {
                                    local_four_cycles.clear();
                                    std::cout << "too many four cycles" << std::endl;
                                    return false;
                                }
                            }
                        }
                    }
                }
            }
            done += GetDegree(u) * GetDegree(v);
        }
        timer.Stop();
        return true;
    }


    void Graph::ChibaNishizeki() {
        std::vector<int> vertices_by_degree(GetNumVertices(), 0);
        for (int i = 0; i < GetNumVertices(); i++) vertices_by_degree[i] = i;
        std::sort(vertices_by_degree.begin(), vertices_by_degree.end(),[this](int a, int b)->bool{
            return GetDegree(a) > GetDegree(b);
        });

    }
}