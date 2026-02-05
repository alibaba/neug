/**
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <algorithm>
#include <fstream>
#include <memory>
#include <sstream>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "glog/logging.h"
#include "rapidjson/document.h"
#include "rapidjson/error/en.h"
#include "rapidjson/filereadstream.h"

#include "neug/compiler/common/types/types.h"
#include "neug/compiler/function/function.h"
#include "neug/compiler/function/neug_call_function.h"
#include "neug/execution/common/context.h"
#include "neug/execution/common/columns/value_columns.h"
#include "neug/storages/graph/graph_interface.h"
#include "neug/storages/graph/property_graph.h"

#include "data_graph_meta.h"
#include "FaSTest/lib/SubgraphMatching/PatternGraph.h"
#include "FaSTest/lib/SubgraphCounting/CardinalityEstimation.h"

namespace neug {
namespace function {

// ============================================================================
// Helper functions for parsing pattern JSON
// ============================================================================

// Helper function to parse comparison operator
inline CompType ParseOperator(const std::string& op) {
    if (op == "=" || op == "==") return CompType::COMP_EQUAL;
    if (op == ">") return CompType::COMP_GREATER;
    if (op == "<") return CompType::COMP_LESS;
    if (op == ">=") return CompType::COMP_GREATER_EQUAL;
    if (op == "<=") return CompType::COMP_LESS_EQUAL;
    if (op == "in") return CompType::COMP_IN;
    if (op == "not_in") return CompType::COMP_NOT_IN;
    return CompType::COMP_EQUAL; // default
}

// Helper function to create Value from rapidjson
inline Value CreateValueFromRapidjson(const rapidjson::Value& val) {
    if (val.IsInt()) {
        return Value::INT32(val.GetInt());
    } else if (val.IsInt64()) {
        return Value::INT64(val.GetInt64());
    } else if (val.IsDouble()) {
        return Value::DOUBLE(val.GetDouble());
    } else if (val.IsString()) {
        return Value::STRING(val.GetString());
    } else if (val.IsBool()) {
        return Value::BOOLEAN(val.GetBool());
    }
    return Value::INT32(0);
}

// Helper function to parse constraints from rapidjson
inline std::vector<PropCons> ParseConstraints(const rapidjson::Value& constraints_json) {
    std::vector<PropCons> constraints;
    if (!constraints_json.IsArray()) return constraints;
    
    for (rapidjson::SizeType i = 0; i < constraints_json.Size(); i++) {
        const auto& c = constraints_json[i];
        std::string prop_name = c.HasMember("property") ? c["property"].GetString() : "";
        std::string op_str = c.HasMember("operator") ? c["operator"].GetString() : "=";
        CompType op = ParseOperator(op_str);
        Value value = c.HasMember("value") ? CreateValueFromRapidjson(c["value"]) : Value::INT32(0);
        
        constraints.emplace_back(prop_name, op, std::move(value));
    }
    return constraints;
}

// ============================================================================
// SampledSubgraphMatcher: Subgraph matching using FaSTest algorithm
// ============================================================================

class SampledSubgraphMatcher {
 public:
  SampledSubgraphMatcher(const StorageReadInterface& graph,
                            const std::string& pattern_file)
    : graph_(graph), pattern_file_(pattern_file), data_meta_(graph), schema_graph_(std::make_shared<std::unordered_map<label_t, std::unordered_map<label_t, std::vector<label_t>>>>()) {
    }
  
  /**
   * @brief Execute subgraph matching
   * @return Estimated count of embeddings
   */
  double match() {
    // Step 0: Build label mappings from schema
    std::cout << "[0] Building label mappings..." << std::endl;
    BuildLabelMappings();
    std::cout << "  Found " << schema_graph_->size() << " source labels in schema" << std::endl;
    std::cout << std::endl;

    // Step 1: Preprocess data graph (build degrees, k-core, etc.)
    std::cout << "[1] Preprocessing data graph..." << std::endl;
    data_meta_.Preprocess();
    std::cout << "  Vertices: " << data_meta_.GetNumVertices() << std::endl;
    std::cout << "  Edges: " << data_meta_.GetNumEdges() << std::endl;
    std::cout << "  Max degree: " << data_meta_.GetMaxDegree() << std::endl;
    std::cout << "  Degeneracy: " << data_meta_.GetDegeneracy() << std::endl;
    std::cout << std::endl;

    // Step 2: Load pattern from JSON file
    std::cout << "[2] Loading pattern graph from: " << pattern_file_ << std::endl;
    pattern_graph_ = CreatePatternFromJson(pattern_file_);
    if (!pattern_graph_ || pattern_graph_->GetNumVertices() == 0) {
        std::cerr << "  ERROR: Failed to load pattern!" << std::endl;
        return -1;
    }
    std::cout << "  Pattern: " << pattern_graph_->GetNumVertices() << " vertices, " 
              << pattern_graph_->GetNumEdges() << " edges" << std::endl;
    
    // Print pattern details
    std::cout << "  Pattern vertices:" << std::endl;
    for (int i = 0; i < pattern_graph_->GetNumVertices(); i++) {
        int label = pattern_graph_->vertex_label[i];
        std::cout << "    v" << i << ": label=" << label 
                  << " (out_deg=" << pattern_graph_->GetOutDegree(i) 
                  << ", in_deg=" << pattern_graph_->GetInDegree(i) << ")" << std::endl;
    }
    std::cout << "  Pattern edges:" << std::endl;
    for (int i = 0; i < pattern_graph_->GetNumEdges(); i++) {
        auto& [src, dst] = pattern_graph_->edge_list[i];
        int label = pattern_graph_->edge_label[i];
        std::cout << "    e" << i << ": " << src << " -[label=" << label << "]-> " << dst << std::endl;
    }
    std::cout << std::endl;

    // Step 3: Process pattern (compute core numbers, build incidence list, etc.)
    std::cout << "[3] Processing pattern..." << std::endl;
    pattern_graph_->ProcessPattern(data_meta_, schema_graph_);
    std::cout << "  Done." << std::endl;
    std::cout << std::endl;
    
    // Step 4: Setup cardinality estimation options
    std::cout << "[4] Setting up cardinality estimation..." << std::endl;
    GraphLib::CardinalityEstimation::CardEstOption opt;
    opt.MAX_QUERY_VERTEX = std::max(12, pattern_graph_->GetNumVertices());
    opt.MAX_QUERY_EDGE = std::max(24, pattern_graph_->GetNumEdges());
    opt.structure_filter = GraphLib::SubgraphMatching::NO_STRUCTURE_FILTER;
    std::cout << "  MAX_QUERY_VERTEX: " << opt.MAX_QUERY_VERTEX << std::endl;
    std::cout << "  MAX_QUERY_EDGE: " << opt.MAX_QUERY_EDGE << std::endl;
    std::cout << std::endl;
    
    // Step 5: Run cardinality estimation
    std::cout << "[5] Running cardinality estimation..." << std::endl;
    int sample_size = 1000;
    std::cout << "  Sample size: " << sample_size << std::endl;
    
    GraphLib::CardinalityEstimation::FaSTestCardinalityEstimation estimator(graph_, data_meta_, opt);
    double est = estimator.EstimateEmbeddings(pattern_graph_.get(), sample_size);
    
    std::cout << std::endl;
    std::cout << "=== Results ===" << std::endl;
    std::cout << "  Estimated embedding count: " << (long long)est << std::endl;
    
    // Get and store sampled results
    sampled_results_ = estimator.GetSampledResult();
    int num_samples = sampled_results_.size() / pattern_graph_->GetNumVertices();
    std::cout << "  Number of sampled embeddings: " << num_samples << std::endl;
    
    // Print first few samples
    if (num_samples > 0) {
        std::cout << "  First 5 sampled embeddings:" << std::endl;
        int show_count = std::min(5, num_samples);
        for (int i = 0; i < show_count; i++) {
            std::cout << "    [" << i << "]: ";
            for (int j = 0; j < pattern_graph_->GetNumVertices(); j++) {
                if (j > 0) std::cout << " -> ";
                std::cout << sampled_results_[i * pattern_graph_->GetNumVertices() + j];
            }
            std::cout << std::endl;
        }
    }
    
    // Print estimation info
    auto result_info = estimator.GetResult();
    std::cout << std::endl;
    std::cout << "  Estimation details:" << std::endl;
    for (const auto& [key, value] : result_info) {
        std::cout << "    " << key << ": ";
        if (value.type() == typeid(double)) {
            std::cout << std::any_cast<double>(value);
        } else if (value.type() == typeid(int)) {
            std::cout << std::any_cast<int>(value);
        } else if (value.type() == typeid(std::string)) {
            std::cout << std::any_cast<std::string>(value);
        }
        std::cout << std::endl;
    }
    
    estimated_count_ = est;
    std::cout << std::endl;
    std::cout << "=== Matching completed ===" << std::endl;
    
    return est;
  }
  
  // Get sampled results after matching
  const std::vector<int>& GetSampledResults() const { return sampled_results_; }
  double GetEstimatedCount() const { return estimated_count_; }
  int GetPatternVertexCount() const { return pattern_graph_ ? pattern_graph_->GetNumVertices() : 0; }

 private:
    /**
     * @brief Build label mappings from the graph storage interface
     * 
     * This function reads vertex and edge labels from the graph schema
     * and builds the schema_graph mapping needed for pattern matching.
     */
    void BuildLabelMappings() {
        const auto& schema = graph_.schema();
        
        // Build schema_graph: mapping (src_label, dst_label) -> [edge_labels]
        // Iterate directly over e_schemas_ instead of triple loop
        for (const auto& [key, edge_schema] : schema.get_all_edge_schemas()) {
            auto [src_label, dst_label, e_label] = schema.parse_edge_label(key);
            
            const std::string& src_name = schema.get_vertex_label_name(src_label);
            const std::string& dst_name = schema.get_vertex_label_name(dst_label);
            const std::string& edge_name = schema.get_edge_label_name(e_label);
            
            (*schema_graph_)[src_label][dst_label].push_back(e_label);
            std::cout << "  Edge triplet: " << src_name << " -[" << edge_name 
                      << "]-> " << dst_name << std::endl;
        }
    }

    // Create pattern graph from JSON file using rapidjson
    std::unique_ptr<GraphLib::SubgraphMatching::PatternGraph> CreatePatternFromJson(
        const std::string& pattern_file) {
        
        // Use const_cast because vertex_label_to_index/edge_label_to_index are not const
        // but they should be (they don't modify schema state)
        auto& schema = const_cast<Schema&>(graph_.schema());

        // Read file content
        std::ifstream fin(pattern_file);
        if (!fin.is_open()) {
            std::cerr << "Error: Cannot open pattern file: " << pattern_file << std::endl;
            return nullptr;
        }
        
        std::stringstream buffer;
        buffer << fin.rdbuf();
        std::string json_content = buffer.str();
        fin.close();
        
        // Parse JSON
        rapidjson::Document doc;
        if (doc.Parse(json_content.c_str()).HasParseError()) {
            std::cerr << "Error parsing JSON: " << rapidjson::GetParseError_En(doc.GetParseError()) 
                      << " at offset " << doc.GetErrorOffset() << std::endl;
            return nullptr;
        }
        
        auto pattern = std::make_unique<GraphLib::SubgraphMatching::PatternGraph>();
        
        // Parse vertices
        if (!doc.HasMember("vertices") || !doc["vertices"].IsArray()) {
            std::cerr << "Error: JSON must have 'vertices' array" << std::endl;
            return nullptr;
        }
        if (!doc.HasMember("edges") || !doc["edges"].IsArray()) {
            std::cerr << "Error: JSON must have 'edges' array" << std::endl;
            return nullptr;
        }
        
        const auto& vertices = doc["vertices"];
        const auto& edges = doc["edges"];
        int v = vertices.Size();
        int e = edges.Size();
        
        pattern->num_vertex = v;
        pattern->num_edge = e;
        pattern->vertex_label.resize(v);
        pattern->edge_label.resize(e);
        pattern->adj_list.resize(v);
        pattern->out_adj_list.resize(v);
        pattern->in_adj_list.resize(v);
        pattern->edge_to.resize(e);
        pattern->edge_list.resize(e);
        pattern->vertex_property_constraints.resize(v);
        pattern->edge_property_constraints.resize(e);
        
        for (rapidjson::SizeType i = 0; i < vertices.Size(); i++) {
            const auto& vertex = vertices[i];
            int id = vertex["id"].GetInt();
            std::string label = vertex["label"].GetString();
            
            pattern->vertex_label[id] = schema.vertex_label_to_index(label);
            
            // Parse vertex property constraints
            if (vertex.HasMember("constraints") && vertex["constraints"].IsArray()) {
                pattern->vertex_property_constraints[id] = ParseConstraints(vertex["constraints"]);
            }
        }
        
        // Parse edges
        int edge_idx = 0;
        for (rapidjson::SizeType i = 0; i < edges.Size(); i++) {
            const auto& edge = edges[i];
            int src = edge["source"].GetInt();
            int dst = edge["target"].GetInt();
            
            std::string edge_type = "";
            if (edge.HasMember("label") && edge["label"].IsString()) {
                edge_type = edge["label"].GetString();
            }
            
            if (!edge_type.empty()) {
                pattern->edge_label[edge_idx] = schema.edge_label_to_index(edge_type);
            } else {
                pattern->edge_label[edge_idx] = 0;
            }
            
            // Directed graph: src -> dst
            pattern->out_adj_list[src].push_back(dst);
            pattern->in_adj_list[dst].push_back(src);
            pattern->adj_list[src].push_back(dst);
            pattern->adj_list[dst].push_back(src);
            
            pattern->edge_to[edge_idx] = dst;
            pattern->edge_list[edge_idx] = {src, dst};
            
            pattern->max_out_degree = std::max(pattern->max_out_degree, (int)pattern->out_adj_list[src].size());
            pattern->max_in_degree = std::max(pattern->max_in_degree, (int)pattern->in_adj_list[dst].size());
            pattern->max_degree = std::max(pattern->max_degree, (int)std::max(pattern->adj_list[src].size(), pattern->adj_list[dst].size()));
            
            // Parse edge property constraints
            if (edge.HasMember("constraints") && edge["constraints"].IsArray()) {
                pattern->edge_property_constraints[edge_idx] = ParseConstraints(edge["constraints"]);
            }
            
            edge_idx++;
        }
        
        return pattern;
    }

    // Member variables
    const StorageReadInterface& graph_;
    std::string pattern_file_;
    DataGraphMeta data_meta_;
    std::unique_ptr<GraphLib::SubgraphMatching::PatternGraph> pattern_graph_;
    
    // Schema graph: schema_graph_[src_label_id][dst_label_id] = [edge_label_ids]
    std::shared_ptr<std::unordered_map<label_t, std::unordered_map<label_t, std::vector<label_t>>>> schema_graph_;
    
    // Results
    std::vector<int> sampled_results_;
    double estimated_count_ = 0.0;
};

// ============================================================================
// NeugCallFunction 实现：子图匹配函数
// ============================================================================

// 函数输入数据结构
struct SampledMatchInput : public CallFuncInputBase {
  std::string patternFilePath;
  
  SampledMatchInput(std::string path) : patternFilePath(std::move(path)) {}
  ~SampledMatchInput() override = default;
};

// 主函数结构
struct SampledMatchFunction {
  static constexpr const char* name = "SAMPLED_MATCH";

  // 支持的最大模式顶点数
  static constexpr int MAX_PATTERN_VERTICES = 20;
  
  static function_set getFunctionSet() {
    function_set functionSet;
    
    // 构建输出列定义
    // 前两列是 estimated_count 和 sample_count
    // 后续列是 v0, v1, v2, ... 用于存储每个模式顶点对应的数据图顶点ID
    call_output_columns outputCols{
        {"estimated_count", common::LogicalTypeID::DOUBLE},
        {"sample_count", common::LogicalTypeID::INT64}
    };
    
    // 添加最多 MAX_PATTERN_VERTICES 个顶点列
    for (int i = 0; i < MAX_PATTERN_VERTICES; i++) {
      outputCols.push_back({"v" + std::to_string(i), common::LogicalTypeID::INT64});
    }
    
    // 创建 NeugCallFunction
    auto func = std::make_unique<NeugCallFunction>(
        name,
        std::vector<common::LogicalTypeID>{common::LogicalTypeID::STRING},
        std::move(outputCols));
    
    // 设置 bindFunc
    func->bindFunc = [](const Schema& schema, const runtime::ContextMeta& ctx_meta,
                        const ::physical::PhysicalPlan& plan, int op_idx) 
        -> std::unique_ptr<CallFuncInputBase> {
      // 从 plan 中提取 pattern 文件路径参数
      auto& procedure = plan.plan(op_idx).opr().procedure_call();
      std::string patternPath;
      
      // 从参数中获取路径
      if (procedure.query().arguments_size() > 0) {
        const auto& arg = procedure.query().arguments(0);
        if (arg.has_const_()) {
          patternPath = arg.const_().str();
        }
      }
      
      LOG(INFO) << "[SAMPLED_MATCH] Bind: pattern file = " << patternPath;
      return std::make_unique<SampledMatchInput>(patternPath);
    };
    

    func->execFunc = [](const CallFuncInputBase& input, IStorageInterface& graph) 
        -> runtime::Context {
      auto& matchInput = static_cast<const SampledMatchInput&>(input);
      
      LOG(INFO) << "[SAMPLED_MATCH] Executing with graph access";
      LOG(INFO) << "[SAMPLED_MATCH] Pattern file: " << matchInput.patternFilePath;
      
      // 将 IStorageInterface 转换为 StorageReadInterface
      // IStorageInterface 是基类，StorageReadInterface 是派生类
      auto* readInterface = dynamic_cast<StorageReadInterface*>(&graph);
      if (!readInterface) {
        LOG(ERROR) << "[SAMPLED_MATCH] ERROR: graph is not a StorageReadInterface!";
        return runtime::Context();
      }
      
      LOG(INFO) << "[SAMPLED_MATCH] Starting subgraph matching...";
      
      // 执行子图匹配
      SampledSubgraphMatcher matcher(*readInterface, matchInput.patternFilePath);
      double estimatedCount = matcher.match();
      
      const auto& sampledResults = matcher.GetSampledResults();
      int patternVertexCount = matcher.GetPatternVertexCount();
      int sampleCount = patternVertexCount > 0 ? 
          sampledResults.size() / patternVertexCount : 0;
      
      LOG(INFO) << "[SAMPLED_MATCH] Estimated count: " << (long long)estimatedCount;
      LOG(INFO) << "[SAMPLED_MATCH] Sampled embeddings: " << sampleCount;
      
      // 创建返回结果 Context
      runtime::Context ctx;
      
      if (sampleCount == 0) {
        // 没有采样结果，返回空 context
        LOG(INFO) << "[SAMPLED_MATCH] No samples found, returning empty context";
        return ctx;
      }
      
      // 每一行代表一个采样结果
      // 列布局:
      //   col 0: estimated_count (double) - 估计的嵌入总数
      //   col 1: sample_count (int64) - 采样数量
      //   col 2 ~ col (2+patternVertexCount-1): v0, v1, v2, ... - 每个模式顶点对应的数据图顶点ID
      
      // 创建 estimated_count 列 (所有行都是相同的值)
      runtime::ValueColumnBuilder<double> estimatedCountBuilder;
      estimatedCountBuilder.reserve(sampleCount);
      for (int i = 0; i < sampleCount; i++) {
        estimatedCountBuilder.push_back_opt(estimatedCount);
      }
      ctx.set(0, estimatedCountBuilder.finish());
      
      // 创建 sample_count 列 (所有行都是相同的值)
      runtime::ValueColumnBuilder<int64_t> sampleCountBuilder;
      sampleCountBuilder.reserve(sampleCount);
      for (int i = 0; i < sampleCount; i++) {
        sampleCountBuilder.push_back_opt(static_cast<int64_t>(sampleCount));
      }
      ctx.set(1, sampleCountBuilder.finish());
      
      // 为每个模式顶点创建一列，包含匹配的数据图顶点ID
      for (int v = 0; v < patternVertexCount; v++) {
        runtime::ValueColumnBuilder<int64_t> vertexColBuilder;
        vertexColBuilder.reserve(sampleCount);
        
        for (int s = 0; s < sampleCount; s++) {
          // sampledResults 是扁平化的数组: [s0_v0, s0_v1, ..., s1_v0, s1_v1, ...]
          int64_t vertexId = static_cast<int64_t>(sampledResults[s * patternVertexCount + v]);
          vertexColBuilder.push_back_opt(vertexId);
        }
        
        // 列索引为 2 + v (前两列是 estimated_count 和 sample_count)
        ctx.set(2 + v, vertexColBuilder.finish());
      }
      
    //   // 设置 tag_ids，告诉系统哪些列应该输出
    //   // tag_ids 包含所有需要输出的列索引: 0 (estimated_count), 1 (sample_count), 2 (v0), 3 (v1), ...
    //   ctx.tag_ids.clear();
    //   for (int i = 0; i < 2 + patternVertexCount; i++) {
    //     ctx.tag_ids.push_back(i);
    //   }
      
      LOG(INFO) << "[SAMPLED_MATCH] Created context with " << sampleCount << " rows and " 
                << (2 + patternVertexCount) << " columns";
      
      // 调试输出：验证 context 状态
      LOG(INFO) << "[SAMPLED_MATCH] tag_ids size: " << ctx.tag_ids.size();
      LOG(INFO) << "[SAMPLED_MATCH] columns size: " << ctx.columns.size();
      LOG(INFO) << "[SAMPLED_MATCH] row_num: " << ctx.row_num();
      for (int i = 0; i < static_cast<int>(ctx.tag_ids.size()); i++) {
        int tag = ctx.tag_ids[i];
        auto col = ctx.get(tag);
        if (col) {
          LOG(INFO) << "[SAMPLED_MATCH] col[" << tag << "]: " << col->column_info()
                    << ", first elem: " << col->get_elem(0).to_string();
        } else {
          LOG(INFO) << "[SAMPLED_MATCH] col[" << tag << "]: nullptr";
        }
      }
      
      return ctx;
    };
    
    functionSet.push_back(std::move(func));
    return functionSet;
  }
};

}  // namespace function
}  // namespace neug
