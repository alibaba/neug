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

#include <iostream>
#include <set>
#include "Base/metrics.h"
#include "Base/timer.h"
#include "DataStructure/graph.h"
#include "SpecialSubgraphs/small_cycle.h"
#include "SubgraphMatching/data_graph.h"
#include "SubgraphMatching/pattern_graph.h"
#include "SubgraphMatching/candidate_space.h"
#include "SubgraphMatching/candidate_filter.h"
#include "SubgraphCounting/option.h"
#include "SubgraphCounting/cardinality_estimation.h"

using namespace std;
using namespace GraphLib;

std::set<std::string> scientific_type_results = {"#CandTree"};
std::set<std::string> double_type_results = {
    "Truth", "Est", "logQError", "CSBuildTime", "TreeCountTime", "TreeSampleTime", "GraphSampleTime", "QueryTime"
};
std::set<std::string> longlong_type_results = {};
std::vector<std::string> print_order = {
    "#CSVertex", "#CSEdge", "#CandTree", "#TreeTrials", "#TreeSuccess","Truth", "Est", "logQError",
    "CSBuildTime", "TreeCountTime", "TreeSampleTime", "GraphSampleTime", "QueryTime"
};
std::vector<dict> results;
std::string query_path;
Timer timer;
std::vector<PatternGraph*> pattern_graphs;
std::deque<std::string> query_names;
std::unordered_map<std::string, double> true_cnt;
double total_time = 0.0;

void read_ans(const std::string& dataset) {
    std::string ans_file_name = query_path;
    cout << ans_file_name << endl;
    std::ifstream ans_in(ans_file_name);
    while (!ans_in.eof()) {
        std::string name, t, c;
        ans_in >> name >> t >> c;
        if (name.empty() || c.empty()) continue;
        name = "../dataset/"+dataset+"/query_graph/"+name;
        true_cnt[name] = stod(c);
        query_names.push_back(name);
    }
}


void read_filter_option(const std::string& opt, const std::string &filter, CardinalityEstimation::CardEstOption& option) {
    if (opt.substr(2) == "STRUCTURE") {
        if (filter == "X")
            option.structure_filter = SubgraphMatching::NO_STRUCTURE_FILTER;
        else if (filter == "3")
            option.structure_filter = SubgraphMatching::TRIANGLE_SAFETY;
        else if (filter == "4")
            option.structure_filter = SubgraphMatching::FOURCYCLE_SAFETY;
    }
}

int32_t main(int argc, char *argv[]) {
    std::string dataset = "wordnet";
    CardinalityEstimation::CardEstOption opt;

    for (int i = 1; i < argc; ++i) {
        if (argv[i][0] == '-') {
            switch (argv[i][1]) {
                case 'd':
                    dataset = argv[i + 1];
                    break;
                case 'q':
                    query_path = argv[i + 1];
                    break;
                case 'K':
                    opt.ub_initial = atoi(argv[i + 1]);
                    break;
                case '-':
                    read_filter_option(std::string(argv[i]), std::string(argv[i+1]), opt);
                    break;
                default:
                    break;
            }
        }
    }

    if (query_path.empty()) {
        query_path = "../dataset/"+dataset+"/"+dataset+"_ans.txt";
    }

    std::string data_path = "../dataset/"+dataset+"/"+dataset+".graph";
    read_ans(dataset);
    DataGraph D;
    D.LoadLabeledGraph(data_path);
    D.Preprocess();
    opt.MAX_QUERY_VERTEX = 12;
    opt.MAX_QUERY_EDGE = 4;
    pattern_graphs.resize(query_names.size());
    for (int i = 0; i < query_names.size(); i++) {
        std::string query_name = query_names[i];
        pattern_graphs[i] = new PatternGraph();
        pattern_graphs[i]->LoadLabeledGraph(query_name);
        pattern_graphs[i]->ProcessPattern(D);
        pattern_graphs[i]->EnumerateLocalTriangles();
        pattern_graphs[i]->EnumerateLocalFourCycles();
        opt.MAX_QUERY_VERTEX = std::max(opt.MAX_QUERY_VERTEX, pattern_graphs[i]->GetNumVertices());
        opt.MAX_QUERY_EDGE = std::max(opt.MAX_QUERY_EDGE, pattern_graphs[i]->GetNumEdges());
    }

    if (opt.structure_filter >= SubgraphMatching::FOURCYCLE_SAFETY) {
        D.EnumerateLocalFourCycles();
    }
    if (opt.structure_filter >= SubgraphMatching::TRIANGLE_SAFETY) {
        D.EnumerateLocalTriangles();
    }
    CardinalityEstimation::FaSTestCardinalityEstimation estimator(&D, opt);

    for (int i = 0; i < pattern_graphs.size(); i++) {
        PatternGraph* P = pattern_graphs[i];
        std::string query_name = query_names[i];
        std::cout << "Start Processing " << query_name << std::endl;
        double est = estimator.EstimateEmbeddings(P, 0);
        dict query_result = estimator.GetResult();
        query_result["Est"] = est;
        if (true_cnt.find(query_name)!= true_cnt.end()) {
            query_result["Truth"] = std::any(true_cnt[query_name]*1.0);
            query_result["logQError"] = std::any(logQError(true_cnt[query_name]*1.0, est));
        }
        for (auto &key : print_order) {
            if (query_result.find(key) == query_result.end()) continue;
            std::any value = query_result[key];
            if (double_type_results.find(key) != double_type_results.end())
                fprintf(stdout, "  [Result] %-20s: %.04lf\n", key.c_str(), std::any_cast<double>(value));
            else if (scientific_type_results.find(key)!= scientific_type_results.end())
                fprintf(stdout, "  [Result] %-20s: %.04g\n", key.c_str(), std::any_cast<double>(value));
            else if (longlong_type_results.find(key)!= longlong_type_results.end())
                fprintf(stdout, "  [Result] %-20s: %lld\n", key.c_str(), std::any_cast<long long>(value));
            else
                fprintf(stdout, "  [Result] %-20s: %d\n", key.c_str(), std::any_cast<int>(value));
        }
        cout << query_name << " Finished!\n";
        fflush(stdout);
        results.push_back(query_result);
    }

    std::function<double(double)> absolute_value = [](double x) {return std::abs(x);};
    cout << std::fixed << std::setprecision(2) << "Total Time: " << Total(results, "QueryTime") << "ms\n";
    cout << std::fixed << std::setprecision(2) <<
        "Average Abs log Q-Error: " << Average(results, "logQError", absolute_value) << endl;
}