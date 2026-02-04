#pragma once
/**
* @brief Class for subgraph pattern
*/
#include "../DataStructure/Graph.h"
#include "../../../data_graph_meta.h"

// Use types from neug namespace
using neug::function::DataGraphMeta;
using neug::function::PropCons;

//#include "ortools/linear_solver/linear_solver.h"
//
//namespace OR = operations_research;
//using OR::MPSolver, OR::MPVariable, OR::MPConstraint;
//std::unique_ptr<MPSolver> solver(MPSolver::CreateSolver("PDLP"));
//const double infinity = solver->infinity();

namespace GraphLib::SubgraphMatching {
    class PatternGraph : public Graph {
    public:
        PatternGraph(){};
        PatternGraph(const Graph& g) : Graph(g) {};
        ~PatternGraph(){};

        PatternGraph &operator=(const PatternGraph &) = delete;
        PatternGraph(const PatternGraph &) = delete;

        void ProcessPattern(DataGraphMeta &data_meta, std::shared_ptr<std::unordered_map<label_t, std::unordered_map<label_t, std::vector<label_t>>>> schema_graph = nullptr);
        
        // For directed graph: separate indices for out-neighbors and in-neighbors
        std::vector<std::vector<int>> out_adj_idx;  // out_adj_idx[u][uc] = index of uc in out_adj_list[u]
        std::vector<std::vector<int>> in_adj_idx;   // in_adj_idx[u][uc] = index of uc in in_adj_list[u]
        
        // GetOutAdjIdx: for query edge u -> uc, get index in out_candidate_neighbors
        inline int GetOutAdjIdx(int u, int uc) const { return out_adj_idx[u][uc]; }
        // GetInAdjIdx: for query edge uc -> u, get index in in_candidate_neighbors  
        inline int GetInAdjIdx(int u, int uc) const { return in_adj_idx[u][uc]; }
        
        // Legacy interface (deprecated, use GetOutAdjIdx/GetInAdjIdx instead)
        std::vector<std::vector<int>> adj_idx;
        inline int GetAdjIdx(int u, int uc) const { return adj_idx[u][uc]; }

        std::vector<std::vector<PropCons>> vertex_property_constraints;
        std::vector<std::vector<PropCons>> edge_property_constraints;

//        void FindFractionalEdgeCover(std::vector<double> &weights);
//
//        std::vector<double> fractional_edge_cover;
    };

    inline void PatternGraph::ProcessPattern(DataGraphMeta &data_meta, std::shared_ptr<std::unordered_map<label_t, std::unordered_map<label_t, std::vector<label_t>>>> schema_graph) {
        // Construct adj list and label frequency
        // Labels are already consecutive integers starting from 0, no transfer needed
        // For directed graph: update max_out_degree, max_in_degree, and max_degree
        max_degree = 0;
        max_out_degree = 0;
        max_in_degree = 0;
        for (int v = 0; v < GetNumVertices(); v++) {
            // vertex_label[v] is already set correctly, no need to transfer
            // For directed graph: track both out and in degrees
            max_out_degree = std::max(max_out_degree, (int)(out_adj_list[v].size()));
            max_in_degree = std::max(max_in_degree, (int)(in_adj_list[v].size()));
            max_degree = std::max(max_degree, (int)(adj_list[v].size()));
        }
        num_vertex_labels = data_meta.GetNumLabels();
        num_edge_labels = data_meta.GetNumEdgeLabels();
        std::cout << "build incidence list" << std::endl;
        BuildIncidenceList(true, schema_graph);
        std::cout << "build incidence list done" << std::endl;
        ComputeCoreNum();
        std::cout << "compute core num" << std::endl;
        // For directed graph: build separate indices for out-neighbors and in-neighbors
        // out_candidate_neighbors[u][v_idx][out_adj_idx] stores out-edge candidates
        // in_candidate_neighbors[u][v_idx][in_adj_idx] stores in-edge candidates
        out_adj_idx.resize(GetNumVertices(), std::vector<int>(GetNumVertices(), -1));
        in_adj_idx.resize(GetNumVertices(), std::vector<int>(GetNumVertices(), -1));
        for (int u = 0; u < GetNumVertices(); u++) {
            // out_adj_idx: index in out_candidate_neighbors
            for (size_t i = 0; i < out_adj_list[u].size(); i++) {
                out_adj_idx[u][out_adj_list[u][i]] = i;
            }
            // in_adj_idx: index in in_candidate_neighbors (no offset needed)
            for (size_t i = 0; i < in_adj_list[u].size(); i++) {
                in_adj_idx[u][in_adj_list[u][i]] = i;
            }
        }
        
        // Legacy adj_idx (for backward compatibility, may have issues with bidirectional edges)
        adj_idx.resize(GetNumVertices(), std::vector<int>(GetNumVertices(), -1));
        for (int u = 0; u < GetNumVertices(); u++) {
            for (size_t i = 0; i < adj_list[u].size(); i++) {
                adj_idx[u][adj_list[u][i]] = i;
            }
        }
    }

    /*void PatternGraph::FindFractionalEdgeCover(std::vector<double> &weights) {
        std::vector<OR::MPVariable*> edgevariables(GetNumEdges()/2);
        std::vector<OR::MPConstraint*> vertexconstraints(GetNumVertices());
        OR::MPObjective* objective = solver->MutableObjective();
        std::vector<int> X[50];
        for (int i = 0; i < GetNumVertices(); i++) {
            vertexconstraints[i] = solver->MakeRowConstraint(1.0, infinity);
        }
        for (int i = 0; i < GetNumEdges(); i+=2) {
            edgevariables[i/2] = solver->MakeNumVar(0.0, 1.0, "e" + std::to_string(i/2));
            auto &[u, v] = edge_list[i];
            vertexconstraints[u]->SetCoefficient(edgevariables[i/2], 1.0);
            vertexconstraints[v]->SetCoefficient(edgevariables[i/2], 1.0);
            X[u].push_back(i/2);
            X[v].push_back(i/2);
//            std::cout << "weight " << i/2 << " : " << weights[i] << std::endl;
            objective->SetCoefficient(edgevariables[i/2], weights[i]);
        }
//        for (int i = 0; i < GetNumVertices(); i++) {
//            printf("Constraint [%d]: ", i);
//            for (auto &it : X[i]) {
//                printf(" %d ", it);
//            }
//            printf("\n");
//        }
        objective->SetMinimization();
        const MPSolver::ResultStatus result_status = solver->Solve();
        // Check that the problem has an optimal solution.
//        if (result_status != MPSolver::OPTIMAL) {
//            LOG(FATAL) << "The problem does not have an optimal solution!";
//        }

        fractional_edge_cover.resize(GetNumEdges()/2);
//        LOG(INFO) << "Solution:";
//        LOG(INFO) << "Optimal objective value = " << objective->Value();
        for (int i = 0; i < GetNumEdges(); i+=2) {
//            LOG(INFO) << edgevariables[i/2]->name() << " = " << edgevariables[i/2]->solution_value();
            fractional_edge_cover[i/2] = edgevariables[i/2]->solution_value();
        }
    }*/
}
