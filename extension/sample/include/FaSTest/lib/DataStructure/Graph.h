#pragma once
#include <unordered_map>
#include <map>
#include <set>
#include <iostream>
#include <fstream>
#include <sstream>
#include "../Base/Base.h"
#include "../../../../storage/graph_element.hpp"
#include "json.hpp"

// 前向声明
namespace gbi {
    class GraphStorage;
    class SchemaGraph;
}

// #define HUGE_GRAPH
namespace GraphLib {
    class Graph {
    public:
        // Adjacency Lists - for directed graphs
        std::vector<std::vector<int>> adj_list;       // Combined adjacency (for compatibility)
        std::vector<std::vector<int>> out_adj_list;   // Out-edge adjacency list
        std::vector<std::vector<int>> in_adj_list;    // In-edge adjacency list
        std::vector<int> core_num, vertex_color;
        std::vector<int> degeneracy_order;
        int num_vertex = 0, num_edge = 0, max_degree = 0, degeneracy = 0, num_color = 0;
        int max_out_degree = 0, max_in_degree = 0;
        int num_vertex_labels = 0;
        int num_edge_labels = 0;

        /**
         * Basic data structures for directed graph
         * @attribute (vertex/edge)_label : array of labels
         * @attribute edge_list : list of edges as pair<int, int> form (src, dst)
         * @attribute out_incident_edges[v][l] : list of indices of out-edges from v to endpoint label l
         * @attribute in_incident_edges[v][l] : list of indices of in-edges to v from source label l
         * @attribute all_out_incident_edges[v] : list of indices of all out-edges from v
         * @attribute all_in_incident_edges[v] : list of indices of all in-edges to v
         * @attribute edge_index_map : Queryable as map[{u, v, label}] -> edge_id.
         */
        std::vector<int> vertex_label, edge_label, edge_to;
        std::vector<std::pair<int, int>> edge_list;
        std::vector<std::vector<int>> all_incident_edges;      // Combined (for compatibility)
        std::vector<std::vector<int>> all_out_incident_edges;  // All out-edges from v
        std::vector<std::vector<int>> all_in_incident_edges;   // All in-edges to v
        std::vector<std::vector<std::string>> vertex_property_names, edge_property_names;
        std::vector<std::vector<gbi::Value>> vertex_properties, edge_properties;
#ifdef HUGE_GRAPH
        std::vector<std::map<int, std::vector<int>>> incident_edges;
        std::vector<std::map<int, std::vector<int>>> out_incident_edges;  // [v][label] -> out-edges
        std::vector<std::map<int, std::vector<int>>> in_incident_edges;   // [v][label] -> in-edges
#else
        std::vector<std::vector<std::vector<int>>> incident_edges;
        std::vector<std::vector<std::vector<int>>> out_incident_edges;  // [v][label] -> out-edges to label
        std::vector<std::vector<std::vector<int>>> in_incident_edges;   // [v][label] -> in-edges from label
#endif
        std::vector<std::unordered_map<int, std::unordered_map<int, int>>> edge_index_map;  // 起点->终点->label->边id
        std::vector<std::vector<std::pair<int, int>>> no_edge_pairs;  // 记录不存在的（终点，边类型）对


        /*
         * Enumeration of Small Cycles for Cyclic Substructure Filter
         * For each edge e, store local triangles and four-cycles
         */
        struct FourMotif {
            std::tuple<int, int, int, int> edges;
            std::tuple<int, int> diags;
            FourMotif(std::tuple<int, int, int, int> edges, std::tuple<int, int> diags) : edges(edges), diags(diags) {}
        };
        std::vector<std::vector<std::tuple<int, int, int>>> local_triangles;
        std::vector<std::vector<FourMotif>> local_four_cycles;
    public:
        // 指向GraphStorage的指针，用于访问更多图信息
        gbi::GraphStorage* graph_storage_ptr = nullptr;
        gbi::PatternGraph* pattern_graph_ptr = nullptr;

        Graph() {}
        ~Graph() {}
        Graph &operator=(const Graph &) = delete;

        std::vector<int>& GetNeighbors(int v) {
            return adj_list[v];
        }

        // Directed graph neighbor accessors
        std::vector<int>& GetOutNeighbors(int v) {
            return out_adj_list[v];
        }
        
        std::vector<int>& GetInNeighbors(int v) {
            return in_adj_list[v];
        }

        inline int GetDegree(int v) const {
            return adj_list[v].size();
        }
        
        inline int GetOutDegree(int v) const {
            return out_adj_list[v].size();
        }
        
        inline int GetInDegree(int v) const {
            return in_adj_list[v].size();
        }

        inline int GetNumVertices() const {
            return num_vertex;
        }

        inline int GetNumEdges() const {
            return num_edge;
        }

        inline int GetMaxDegree() const {
            return max_degree;
        }
        
        inline int GetMaxOutDegree() const {
            return max_out_degree;
        }
        
        inline int GetMaxInDegree() const {
            return max_in_degree;
        }

        void ComputeCoreNum();

        inline int GetCoreNum(int v) const {
            return core_num[v];
        }

        inline int GetDegeneracy() const {return degeneracy;}

        void AssignVertexColor();

        inline int GetNumColors() const {return num_color;}

        inline int GetVertexColor(int v) const {return vertex_color[v];}


        void LoadLabeledGraph(const std::string &filename, bool directed = true);
        void LoadLabeledGraph(const std::string &filename, std::unordered_map<std::string, int> &label2id_mapping, std::unordered_map<int, std::string> &id2label_mapping, bool directed = true);
        void LoadProperty(const std::string vertex_property_filename, const std::string edge_property_filename, std::unordered_map<std::string, int> &label2id_mapping, std::shared_ptr<gbi::SchemaGraph> schema_graph);

        inline std::vector<int>& GetAllIncidentEdges(int v) {return all_incident_edges[v];}
        inline std::vector<int>& GetIncidentEdges(int v, int label) {return incident_edges[v][label];}
        
        // Directed graph incident edge accessors
        inline std::vector<int>& GetAllOutIncidentEdges(int v) {return all_out_incident_edges[v];}
        inline std::vector<int>& GetAllInIncidentEdges(int v) {return all_in_incident_edges[v];}
        inline std::vector<int>& GetOutIncidentEdges(int v, int label) {return out_incident_edges[v][label];}
        inline std::vector<int>& GetInIncidentEdges(int v, int label) {return in_incident_edges[v][label];}
        inline int GetVertexLabel(int v) const {return vertex_label[v];}
        inline int GetEdgeLabel(int edge_id) const {return edge_label[edge_id];}
        inline int GetNumLabels() const {return num_vertex_labels;}
        inline int GetNumEdgeLabels() const {return num_edge_labels;}
        inline int GetOppositeEdge(int edge_id) const {return edge_id^1;}
        inline int GetOppositePoint(int edge_id) const {return edge_to[edge_id];}
        // For directed graph: get edge source and destination
        inline int GetSourcePoint(int edge_id) const {return edge_list[edge_id].first;}
        inline int GetDestPoint(int edge_id) const {return edge_list[edge_id].second;}
        inline int GetEdgeIndex(int u, int v) {
            auto it_v = edge_index_map[u].find(v);
            if (it_v == edge_index_map[u].end() || it_v->second.empty()) {
                return -1;
            }
            // 返回第一个找到的边（任意label）
            return it_v->second.begin()->second;
        }
        
        inline int GetEdgeIndex(int u, int v, int label) {
            auto it_v = edge_index_map[u].find(v);
            if (it_v == edge_index_map[u].end()) {
                return -1;
            }
            auto it_label = it_v->second.find(label);
            return (it_label == it_v->second.end() ? -1 : it_label->second);
        }
        inline std::pair<int, int>& GetEdge(int edge_id) {
            return edge_list[edge_id];
        }

        inline std::vector<std::tuple<int, int, int>>& GetLocalTriangles(int edge_id) {return local_triangles[edge_id];}
        inline std::vector<FourMotif>& GetLocalFourCycles(int edge_id) {return local_four_cycles[edge_id];}

        bool EnumerateLocalTriangles();
        bool EnumerateLocalFourCycles();
        void ChibaNishizeki();
        bool build_four_cycle = false;
        bool build_triangle = false;

        /**
         * @brief Build the incidence list structure
         */
        void BuildIncidenceList(bool load_no_edge_pairs=false, std::shared_ptr<std::unordered_map<label_t, std::unordered_map<label_t, std::vector<label_t>>>> schema_graph = nullptr);
        
    protected:
        /**
         * @brief 构建no_edge_pairs，可以被子类重写以使用schema
         */
        virtual void BuildNoEdgePairsFromSchema(std::shared_ptr<std::unordered_map<label_t, std::unordered_map<label_t, std::vector<label_t>>>> schema_graph);
    
    public:

        void LoadGraph(std::vector<int> &vertex_labels, std::vector<std::pair<int, int>> &edges,
                       std::vector<int> &edge_labels,   bool directed = false);

        void WriteToFile(string filename);
    };

    /**
     * @brief Compute the core number of each vertex
     * @date Oct 21, 2022
     */
    inline void Graph::ComputeCoreNum() {
        core_num.resize(num_vertex, 0);
        int *bin = new int[GetMaxDegree() + 1];
        int *pos = new int[GetNumVertices()];
        int *vert = new int[GetNumVertices()];

        std::fill(bin, bin + (GetMaxDegree() + 1), 0);

        for (int v = 0; v < GetNumVertices(); v++) {
            core_num[v] = adj_list[v].size();
            bin[core_num[v]] += 1;
        }

        int start = 0;
        int num;

        for (int d = 0; d <= GetMaxDegree(); d++) {
            num = bin[d];
            bin[d] = start;
            start += num;
        }

        for (int v = 0; v < GetNumVertices(); v++) {
            pos[v] = bin[core_num[v]];
            vert[pos[v]] = v;
            bin[core_num[v]] += 1;
        }

        for (int d = GetMaxDegree(); d--;)
            bin[d + 1] = bin[d];
        bin[0] = 0;

        for (int i = 0; i < GetNumVertices(); i++) {
            int v = vert[i];

            for (int u : GetNeighbors(v)) {
                if (core_num[u] > core_num[v]) {
                    int du = core_num[u];
                    int pu = pos[u];
                    int pw = bin[du];
                    int w = vert[pw];

                    if (u != w) {
                        pos[u] = pw;
                        pos[w] = pu;
                        vert[pu] = w;
                        vert[pw] = u;
                    }

                    bin[du]++;
                    core_num[u]--;
                }
            }
        }
        degeneracy_order.resize(GetNumVertices());
        for (int i = 0; i < GetNumVertices(); i++) {
            degeneracy_order[i] = vert[i];
        }
        std::reverse(degeneracy_order.begin(),degeneracy_order.end());

        degeneracy = 0;
        for (int i = 0; i < GetNumVertices(); i++) {
            degeneracy = std::max(core_num[i], degeneracy);
        }

        delete[] bin;
        delete[] pos;
        delete[] vert;
    }

    /**
     * @brief Greedy coloring of the graph, following the given initial order of vertices.
     * @date Sep 16, 2022
     */
    inline void Graph::AssignVertexColor() {
        vertex_color.resize(GetNumVertices(), -1);
        num_color = 0;
        bool *used = new bool[GetNumVertices()];
        for (int vertexID : degeneracy_order) {
            for (int neighbor : adj_list[vertexID]) {
                if (vertex_color[neighbor] == -1) continue;
                used[vertex_color[neighbor]] = true;
            }
            int c = 0; while (used[c]) c++;
            vertex_color[vertexID] = c;
            num_color = std::max(num_color, c+1);
            for (int neighbor : adj_list[vertexID]) {
                if (vertex_color[neighbor] == -1) continue;
                used[vertex_color[neighbor]] = false;
            }
        }
    }



    inline void Graph::LoadLabeledGraph(const std::string &filename, bool directed) {
        std::ifstream fin(filename);
        int v, e;
        std::string ignore, type, line;
        fin >> ignore >> v >> e;
        num_vertex = v;
        num_edge = directed ? e : e * 2;  // Directed: single edge, Undirected: both directions
        adj_list.resize(num_vertex);
        out_adj_list.resize(num_vertex);
        in_adj_list.resize(num_vertex);
        vertex_label.resize(num_vertex);
        edge_label.resize(num_edge);
        int num_lines = 0;
        while (getline(fin, line)) {
            if (line.empty())
                continue;
            auto tok = parse(line, " ");
            type = tok[0];
            tok.pop_front();
            if (type[0] == 'v') {
                int id = std::stoi(tok.front());
                tok.pop_front();
                int l;
                if (tok.empty()) l = 0;
                else {
                    l = std::stoi(tok.front());
                    tok.pop_front();
                }
                vertex_label[id] = l;
            }
            else if (type[0] == 'e') {
                int v1, v2;
                v1 = std::stoi(tok.front()); tok.pop_front();
                v2 = std::stoi(tok.front()); tok.pop_front();
                int el = tok.empty() ? 0 : std::stoi(tok.front());
                
                if (directed) {
                    // Directed graph: v1 -> v2
                    out_adj_list[v1].push_back(v2);
                    in_adj_list[v2].push_back(v1);
                    adj_list[v1].push_back(v2);
                    adj_list[v2].push_back(v1);
                    edge_to.push_back(v2);
                    edge_list.push_back({v1, v2});
                    edge_label[edge_list.size()-1] = el;
                    max_out_degree = std::max(max_out_degree, (int)out_adj_list[v1].size());
                    max_in_degree = std::max(max_in_degree, (int)in_adj_list[v2].size());
                } else {
                    // Undirected graph: add both directions
                    out_adj_list[v1].push_back(v2);
                    out_adj_list[v2].push_back(v1);
                    in_adj_list[v2].push_back(v1);
                    in_adj_list[v1].push_back(v2);
                    adj_list[v1].push_back(v2);
                    adj_list[v2].push_back(v1);
                    edge_to.push_back(v2);
                    edge_to.push_back(v1);
                    edge_list.push_back({v1, v2});
                    edge_list.push_back({v2, v1});
                    edge_label[edge_list.size()-2] = edge_label[edge_list.size()-1] = el;
                    max_out_degree = std::max(max_out_degree, (int)std::max(out_adj_list[v1].size(), out_adj_list[v2].size()));
                    max_in_degree = std::max(max_in_degree, (int)std::max(in_adj_list[v1].size(), in_adj_list[v2].size()));
                }
                max_degree = std::max(max_degree, (int)std::max(adj_list[v1].size(), adj_list[v2].size()));
            }
            num_lines++;
        }
    }

    inline void Graph::LoadLabeledGraph(const std::string &filename, std::unordered_map<std::string, int> &label2id_mapping, std::unordered_map<int, std::string> & /*id2label_mapping*/, bool directed) {
        std::ifstream fin(filename);
        int v, e;
        std::string ignore, type, line;
        fin >> ignore >> v >> e;
        num_vertex = v;
        num_edge = directed ? e : e * 2;  // Directed: single edge, Undirected: both directions
        adj_list.resize(num_vertex);
        out_adj_list.resize(num_vertex);
        in_adj_list.resize(num_vertex);
        vertex_label.resize(num_vertex);
        edge_label.resize(num_edge);
        int num_lines = 0;
        while (getline(fin, line)) {
            if (line.empty())
                continue;
            auto tok = parse(line, " ");
            type = tok[0];
            tok.pop_front();
            if (type[0] == 'v') {
                int id = std::stoi(tok.front());
                tok.pop_front();
                int l;
                if (tok.empty()) l = label2id_mapping[""];
                else {
                    l = label2id_mapping[tok.front()];
                    tok.pop_front();
                }
                vertex_label[id] = l;
            }
            else if (type[0] == 'e') {
                int v1, v2;
                v1 = std::stoi(tok.front()); tok.pop_front();
                v2 = std::stoi(tok.front()); tok.pop_front();
                int el = tok.empty() ? label2id_mapping[""] : label2id_mapping[tok.front()];
                
                if (directed) {
                    // Directed graph: v1 -> v2
                    out_adj_list[v1].push_back(v2);
                    in_adj_list[v2].push_back(v1);
                    adj_list[v1].push_back(v2);
                    adj_list[v2].push_back(v1);
                    edge_to.push_back(v2);
                    edge_list.push_back({v1, v2});
                    edge_label[edge_list.size()-1] = el;
                    max_out_degree = std::max(max_out_degree, (int)out_adj_list[v1].size());
                    max_in_degree = std::max(max_in_degree, (int)in_adj_list[v2].size());
                } else {
                    // Undirected graph: add both directions
                    out_adj_list[v1].push_back(v2);
                    out_adj_list[v2].push_back(v1);
                    in_adj_list[v2].push_back(v1);
                    in_adj_list[v1].push_back(v2);
                    adj_list[v1].push_back(v2);
                    adj_list[v2].push_back(v1);
                    edge_to.push_back(v2);
                    edge_to.push_back(v1);
                    edge_list.push_back({v1, v2});
                    edge_list.push_back({v2, v1});
                    edge_label[edge_list.size()-2] = edge_label[edge_list.size()-1] = el;
                    max_out_degree = std::max(max_out_degree, (int)std::max(out_adj_list[v1].size(), out_adj_list[v2].size()));
                    max_in_degree = std::max(max_in_degree, (int)std::max(in_adj_list[v1].size(), in_adj_list[v2].size()));
                }
                max_degree = std::max(max_degree, (int)std::max(adj_list[v1].size(), adj_list[v2].size()));
            }
            num_lines++;
        }
    }

    inline void Graph::LoadProperty(const std::string vertex_property_filename, const std::string edge_property_filename, std::unordered_map<std::string, int> &label2id_mapping, std::shared_ptr<gbi::SchemaGraph> schema_graph) {
        using json = nlohmann::json;
        
        // 清空现有属性
        vertex_property_names.clear();
        edge_property_names.clear();
        vertex_properties.clear();
        edge_properties.clear();

        // 初始化数据结构
        vertex_property_names.resize(num_vertex_labels);
        vertex_properties.resize(num_vertex);
        edge_property_names.resize(num_edge_labels);
        edge_properties.resize(num_edge);
        
        // 优化版本：只读取一次文件，同时设置schema和存储值
        std::ifstream vertex_file(vertex_property_filename);
        if (vertex_file.is_open()) {
            std::string line;
            while (std::getline(vertex_file, line)) {
                if (line.empty()) continue;
                try {
                    json j = json::parse(line);
                    int vertex_id = j["id"];
                    int label = label2id_mapping[j["label"]];
                    
                    if (vertex_id >= num_vertex || label >= num_vertex_labels) {
                        continue; // 跳过无效的顶点
                    }
                    
                    // 检查该label是否已经设置过schema
                    bool is_first_for_this_label = vertex_property_names[label].empty();
                    
                    if (is_first_for_this_label) {
                        // 第一次遇到这个label，设置schema
                        for (auto& [key, value] : j.items()) {
                            if (key != "label") {
                                vertex_property_names[label].push_back(key);
                            }
                        }
                    }
                    
                    // 存储属性值
                    vertex_properties[vertex_id].resize(vertex_property_names[label].size());
                    
                    for (size_t i = 0; i < vertex_property_names[label].size(); i++) {
                        const std::string& prop_name = vertex_property_names[label][i];
                        if (j.contains(prop_name)) {
                            auto& val = j[prop_name];
                            if (val.is_number_integer()) {
                                vertex_properties[vertex_id][i] = gbi::Value((int32_t)val);
                            } else if (val.is_number_float()) {
                                vertex_properties[vertex_id][i] = gbi::Value((double)val);
                            } else if (val.is_string()) {
                                vertex_properties[vertex_id][i] = gbi::Value((std::string)val);
                            } else if (val.is_boolean()) {
                                vertex_properties[vertex_id][i] = gbi::Value((int32_t)(val ? 1 : 0));
                            } else if(val.is_null()) {
                                gbi::EIdType schema_vertex_id = schema_graph->label_vertex[label];
                                gbi::ValueType value_type = schema_graph->vertex_value_type_map[schema_vertex_id][prop_name];
                                vertex_properties[vertex_id][i] = gbi::GetDefaultValue(value_type);
                            }
                        }
                    }
                } catch (const std::exception& e) {
                    std::cerr << "Error parsing vertex property line: " << line << std::endl;
                }
            }
            vertex_file.close();
        }
        
        // 读取边属性文件，同时设置schema和存储值
        std::ifstream edge_file(edge_property_filename);
        if (edge_file.is_open()) {
            std::string line;
            int edge_idx = 0;
            
            while (std::getline(edge_file, line) && edge_idx < num_edge/2) {
                if (line.empty()) continue;
                try {
                    json j = json::parse(line);
                    int label = label2id_mapping[j["type"]];
                    int src_id = j["src"];
                    int dst_id = j["dst"];
                    int src_label_id = vertex_label[src_id];
                    int dst_label_id = vertex_label[dst_id];

                    bool is_first_for_this_label = edge_property_names[label].empty();

                    if (is_first_for_this_label) {
                        for (auto& [key, value] : j.items()) {
                            if (key != "src" && key != "dst" && key != "type") {
                                edge_property_names[label].push_back(key);
                            }
                        }
                    }

                    edge_properties[edge_idx * 2].resize(edge_property_names[label].size());
                    edge_properties[edge_idx * 2 + 1].resize(edge_property_names[label].size());
                    
                    // 处理当前边数据
                    if (edge_idx * 2 < num_edge) {
                        for (size_t i = 0; i < edge_property_names[label].size(); i++) {
                            const std::string& prop_name = edge_property_names[label][i];
                            if (j.contains(prop_name)) {
                                auto& val = j[prop_name];
                                gbi::Value prop_value;
                                if (val.is_number_integer()) {
                                    prop_value = gbi::Value((int64_t)val);
                                } else if (val.is_number_float()) {
                                    prop_value = gbi::Value((double)val);
                                } else if (val.is_string()) {
                                    prop_value = gbi::Value((std::string)val);
                                } else if (val.is_boolean()) {
                                    prop_value = gbi::Value::BOOLEAN(val ? 1 : 0);
                                } else if (val.is_null()) {
                                    gbi::EIdType schema_edge_id = schema_graph->label_edge[label][{src_label_id, dst_label_id}];
                                    gbi::ValueType value_type = schema_graph->edge_value_type_map[schema_edge_id][prop_name];
                                    prop_value = gbi::GetDefaultValue(value_type);
                                }
                                
                                // 对于无向图，两个方向的边具有相同的属性
                                edge_properties[edge_idx * 2][i] = prop_value;
                                edge_properties[edge_idx * 2 + 1][i] = prop_value;
                            }
                        }
                    }
                    edge_idx++;
                } catch (const std::exception& e) {
                    std::cerr << "Error parsing edge property line: " << line << std::endl;
                }
            }
            edge_file.close();
        }
        
        std::cout << "Loaded properties for " << num_vertex << " vertices and " << num_edge/2 << " edges." << std::endl;
    }

    inline void Graph::LoadGraph(std::vector<int> &vertex_labels,
                          std::vector<std::pair<int, int>> &edges,
                          std::vector<int> &edge_labels,
                          bool directed) {
        num_vertex = vertex_labels.size();
        num_edge = directed ? edges.size() : edges.size() * 2;  // Directed: single edge, Undirected: both directions
        
        // Initialize adjacency lists
        adj_list.resize(num_vertex);
        out_adj_list.resize(num_vertex);
        in_adj_list.resize(num_vertex);
        vertex_label.resize(num_vertex);
        edge_label.resize(num_edge);
        
        for (int i = 0; i < num_vertex; i++) vertex_label[i] = vertex_labels[i];
        
        for (size_t i = 0; i < edges.size(); i++) {
            auto &[v1, v2] = edges[i];
            int el = (edge_labels.size() > i) ? edge_labels[i] : 0;
            
            if (directed) {
                // Directed graph: v1 -> v2
                out_adj_list[v1].push_back(v2);
                in_adj_list[v2].push_back(v1);
                adj_list[v1].push_back(v2);
                adj_list[v2].push_back(v1);
                edge_to.push_back(v2);
                edge_list.push_back({v1, v2});
                edge_label[edge_list.size()-1] = el;
                max_out_degree = std::max(max_out_degree, (int)out_adj_list[v1].size());
                max_in_degree = std::max(max_in_degree, (int)in_adj_list[v2].size());
            } else {
                // Undirected graph: add both directions
                out_adj_list[v1].push_back(v2);
                out_adj_list[v2].push_back(v1);
                in_adj_list[v2].push_back(v1);
                in_adj_list[v1].push_back(v2);
                adj_list[v1].push_back(v2);
                adj_list[v2].push_back(v1);
                edge_to.push_back(v2);
                edge_to.push_back(v1);
                edge_list.push_back({v1, v2});
                edge_list.push_back({v2, v1});
                edge_label[edge_list.size()-2] = edge_label[edge_list.size()-1] = el;
                max_out_degree = std::max(max_out_degree, (int)std::max(out_adj_list[v1].size(), out_adj_list[v2].size()));
                max_in_degree = std::max(max_in_degree, (int)std::max(in_adj_list[v1].size(), in_adj_list[v2].size()));
            }
            max_degree = std::max(max_degree, (int)std::max(adj_list[v1].size(), adj_list[v2].size()));
        }
    }



    inline void Graph::BuildIncidenceList(bool load_no_edge_pairs, std::shared_ptr<std::unordered_map<label_t, std::unordered_map<label_t, std::vector<label_t>>>> schema_graph) {
        // Initialize structures for combined (undirected) view
        all_incident_edges.resize(num_vertex);
        incident_edges.resize(num_vertex);
        edge_index_map.resize(num_vertex);
        
        // Initialize structures for directed edges
        all_out_incident_edges.resize(num_vertex);
        all_in_incident_edges.resize(num_vertex);
        out_incident_edges.resize(num_vertex);
        in_incident_edges.resize(num_vertex);
        
        for (int i = 0; i < GetNumVertices(); i++) {
#ifndef HUGE_GRAPH
            incident_edges[i].resize(GetNumLabels());
            out_incident_edges[i].resize(GetNumLabels());
            in_incident_edges[i].resize(GetNumLabels());
#endif
        }
        
        int edge_id = 0;
        for (auto& [u, v] : edge_list) {
            // For directed graph: edge goes from u to v
            int dst_label = GetVertexLabel(v);
            int src_label = GetVertexLabel(u);
            int edge_label_val = GetEdgeLabel(edge_id);
            
            // Build out-edge structures (edges going OUT from u)
            all_out_incident_edges[u].push_back(edge_id);
            out_incident_edges[u][dst_label].push_back(edge_id);
            
            // Build in-edge structures (edges coming IN to v)
            all_in_incident_edges[v].push_back(edge_id);
            in_incident_edges[v][src_label].push_back(edge_id);
            
            // Combined structures for compatibility
            all_incident_edges[u].push_back(edge_id);
            incident_edges[u][dst_label].push_back(edge_id);
            
            // Edge index map: src -> dst -> label -> edge_id
            edge_index_map[u][v][edge_label_val] = edge_id;
            edge_id++;
        }

        if (load_no_edge_pairs) {
            no_edge_pairs.resize(GetNumVertices());
            BuildNoEdgePairsFromSchema(schema_graph);
        }

        // Sort edges by degree of endpoint (using total degree for sorting)
        for (int i = 0; i < GetNumVertices(); i++) {
#ifdef HUGE_GRAPH
            // Sort combined incident edges
            for (auto &[l, vec] : incident_edges[i]) {
                std::stable_sort(vec.begin(), vec.end(),[this](auto &a, auto &b) -> bool {
                    int opp_a = edge_list[a].second;
                    int opp_b = edge_list[b].second;
                    return adj_list[opp_a].size() > adj_list[opp_b].size();
                });
            }
            // Sort out-incident edges
            for (auto &[l, vec] : out_incident_edges[i]) {
                std::stable_sort(vec.begin(), vec.end(),[this](auto &a, auto &b) -> bool {
                    int opp_a = edge_list[a].second;
                    int opp_b = edge_list[b].second;
                    return adj_list[opp_a].size() > adj_list[opp_b].size();
                });
            }
            // Sort in-incident edges
            for (auto &[l, vec] : in_incident_edges[i]) {
                std::stable_sort(vec.begin(), vec.end(),[this](auto &a, auto &b) -> bool {
                    int opp_a = edge_list[a].first;  // Source vertex for in-edges
                    int opp_b = edge_list[b].first;
                    return adj_list[opp_a].size() > adj_list[opp_b].size();
                });
            }
#else
            // Sort combined incident edges
            for (auto &vec : incident_edges[i]) {
                std::stable_sort(vec.begin(), vec.end(),[this](auto &a, auto &b) -> bool {
                    int opp_a = edge_list[a].second;
                    int opp_b = edge_list[b].second;
                    return adj_list[opp_a].size() > adj_list[opp_b].size();
                });
            }
            // Sort out-incident edges
            for (auto &vec : out_incident_edges[i]) {
                std::stable_sort(vec.begin(), vec.end(),[this](auto &a, auto &b) -> bool {
                    int opp_a = edge_list[a].second;
                    int opp_b = edge_list[b].second;
                    return adj_list[opp_a].size() > adj_list[opp_b].size();
                });
            }
            // Sort in-incident edges
            for (auto &vec : in_incident_edges[i]) {
                std::stable_sort(vec.begin(), vec.end(),[this](auto &a, auto &b) -> bool {
                    int opp_a = edge_list[a].first;  // Source vertex for in-edges
                    int opp_b = edge_list[b].first;
                    return adj_list[opp_a].size() > adj_list[opp_b].size();
                });
            }
#endif
            // Sort all incident edges
            std::stable_sort(all_incident_edges[i].begin(), all_incident_edges[i].end(), [this](auto &a, auto &b) -> bool {
                return adj_list[edge_list[a].second].size() > adj_list[edge_list[b].second].size();
            });
            std::stable_sort(all_out_incident_edges[i].begin(), all_out_incident_edges[i].end(), [this](auto &a, auto &b) -> bool {
                return adj_list[edge_list[a].second].size() > adj_list[edge_list[b].second].size();
            });
            std::stable_sort(all_in_incident_edges[i].begin(), all_in_incident_edges[i].end(), [this](auto &a, auto &b) -> bool {
                return adj_list[edge_list[a].first].size() > adj_list[edge_list[b].first].size();
            });
        }
    }

    inline void Graph::WriteToFile(std::string filename) {
        std::filesystem::path filepath = filename;
        std::filesystem::create_directories(filepath.parent_path());
        std::ofstream out(filename);
        out << "t " << GetNumVertices() << ' ' << GetNumEdges()/2 << '\n';
        for (int i = 0; i < GetNumVertices(); i++) {
            out << "v " << i << ' ' << GetVertexLabel(i) << ' ' << GetDegree(i) << '\n';
        }
        int idx = 0;
        for (auto &e : edge_list) {
            if (e.first < e.second) {
                out << "e " << e.first << ' ' << e.second << ' ' << GetEdgeLabel(idx) << '\n';
            }
            idx++;
        }
    }
}

// 实现在Graph.cpp中，因为需要访问GraphStorage的完整定义
