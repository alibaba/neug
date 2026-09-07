/** Copyright 2020 Alibaba Group Holding Limited.
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

#include "neug/execution/expression/exprs/path_expr.h"

#include "neug/common/types/i_context_column.h"
#include "neug/execution/common/context.h"

namespace neug {
namespace execution {
class BindedPathNodesExpr : public RecordExprBase {
 public:
  BindedPathNodesExpr(std::unique_ptr<BindedExprBase>&& path_expr)
      : path_expr_(std::move(path_expr)) {}
  const DataType& type() const override {
    static DataType list_type = DataType::List(DataType::VERTEX);
    return list_type;
  }

  Value eval_record(const DataChunk& chunk, size_t idx) const override {
    Value path_val = path_expr_->Cast<RecordExprBase>().eval_record(chunk, idx);
    const Path& path = PathValue::Get(path_val);
    const auto& nodes = path.nodes();
    std::vector<Value> node_values;
    for (const auto& node : nodes) {
      node_values.push_back(Value::VERTEX(node));
    }
    return Value::LIST(DataType::List(DataType::VERTEX),
                       std::move(node_values));
  }

 private:
  std::unique_ptr<BindedExprBase> path_expr_;
};

std::unique_ptr<BindedExprBase> PathNodesExpr::bind(
    const IStorageInterface* storage, const ParamsMap& params) const {
  return std::make_unique<BindedPathNodesExpr>(
      path_expr_->bind(storage, params));
}

class BindedPathRelationsExpr : public RecordExprBase {
 public:
  BindedPathRelationsExpr(std::unique_ptr<BindedExprBase>&& path_expr)
      : path_expr_(std::move(path_expr)) {}
  const DataType& type() const override {
    static DataType list_type = DataType::List(DataType::EDGE);
    return list_type;
  }

  Value eval_record(const DataChunk& chunk, size_t idx) const override {
    Value path_val = path_expr_->Cast<RecordExprBase>().eval_record(chunk, idx);
    const Path& path = PathValue::Get(path_val);
    const auto& edges = path.relationships();
    std::vector<Value> edge_values;
    for (const auto& edge : edges) {
      edge_values.push_back(Value::EDGE(edge));
    }
    return Value::LIST(DataType::List(DataType::EDGE), std::move(edge_values));
  }

 private:
  std::unique_ptr<BindedExprBase> path_expr_;
};

std::unique_ptr<BindedExprBase> PathRelationsExpr::bind(
    const IStorageInterface* storage, const ParamsMap& params) const {
  return std::make_unique<BindedPathRelationsExpr>(
      path_expr_->bind(storage, params));
}

class BindedSingleRelationshipPathExpr : public RecordExprBase {
 public:
  BindedSingleRelationshipPathExpr(std::unique_ptr<BindedExprBase>&& start_expr,
                                   std::unique_ptr<BindedExprBase>&& rel_expr,
                                   std::unique_ptr<BindedExprBase>&& end_expr)
      : start_expr_(std::move(start_expr)),
        rel_expr_(std::move(rel_expr)),
        end_expr_(std::move(end_expr)),
        type_(DataType::PATH) {}

  const DataType& type() const override { return type_; }

  Value eval_record(const DataChunk& chunk, size_t idx) const override {
    auto start_val =
        start_expr_->Cast<RecordExprBase>().eval_record(chunk, idx);
    auto rel_val = rel_expr_->Cast<RecordExprBase>().eval_record(chunk, idx);
    auto end_val = end_expr_->Cast<RecordExprBase>().eval_record(chunk, idx);
    if (start_val.IsNull() || rel_val.IsNull() || end_val.IsNull()) {
      return Value(type_);
    }
    const auto& start = start_val.GetValue<vertex_t>();
    const auto& rel = rel_val.GetValue<edge_t>();
    const auto& end = end_val.GetValue<vertex_t>();
    std::vector<std::tuple<label_t, Direction, const void*>> edge_data{
        {rel.label.edge_label, rel.dir, rel.prop}};
    std::vector<VertexRecord> vertices{start, end};
    return Value::PATH(Path(edge_data, vertices));
  }

 private:
  std::unique_ptr<BindedExprBase> start_expr_;
  std::unique_ptr<BindedExprBase> rel_expr_;
  std::unique_ptr<BindedExprBase> end_expr_;
  DataType type_;
};

std::unique_ptr<BindedExprBase> SingleRelationshipPathExpr::bind(
    const IStorageInterface* storage, const ParamsMap& params) const {
  return std::make_unique<BindedSingleRelationshipPathExpr>(
      start_expr_->bind(storage, params), rel_expr_->bind(storage, params),
      end_expr_->bind(storage, params));
}

class BindedPathConcatExpr : public RecordExprBase {
 public:
  BindedPathConcatExpr(std::unique_ptr<BindedExprBase>&& left_expr,
                       std::unique_ptr<BindedExprBase>&& right_expr)
      : left_expr_(std::move(left_expr)),
        right_expr_(std::move(right_expr)),
        type_(DataType::PATH) {}

  const DataType& type() const override { return type_; }

  Value eval_record(const DataChunk& chunk, size_t idx) const override {
    auto left_val = left_expr_->Cast<RecordExprBase>().eval_record(chunk, idx);
    auto right_val =
        right_expr_->Cast<RecordExprBase>().eval_record(chunk, idx);
    if (left_val.IsNull() || right_val.IsNull()) {
      return Value(type_);
    }

    const auto& left_path = PathValue::Get(left_val);
    const auto& right_path = PathValue::Get(right_val);
    auto right_vertices = right_path.nodes();
    if (right_vertices.empty() ||
        !(left_path.end_node() == right_vertices.front())) {
      THROW_INVALID_ARGUMENT_EXCEPTION(
          "Cannot concatenate paths with different boundary vertices");
    }
    auto right_edges = right_path.relationships();
    auto result = left_path;
    for (size_t i = 0; i < right_edges.size(); ++i) {
      const auto& edge = right_edges[i];
      const auto& vertex = right_vertices[i + 1];
      result = result.expand(edge.label.edge_label, vertex.label(),
                             vertex.vid(), edge.dir, edge.prop);
    }
    return Value::PATH(result);
  }

 private:
  std::unique_ptr<BindedExprBase> left_expr_;
  std::unique_ptr<BindedExprBase> right_expr_;
  DataType type_;
};

std::unique_ptr<BindedExprBase> PathConcatExpr::bind(
    const IStorageInterface* storage, const ParamsMap& params) const {
  return std::make_unique<BindedPathConcatExpr>(
      left_expr_->bind(storage, params), right_expr_->bind(storage, params));
}

class BindedPathVerticesPropsExpr : public RecordExprBase {
 public:
  BindedPathVerticesPropsExpr(const StorageReadInterface& graph,
                              std::unique_ptr<BindedExprBase>&& path_expr,
                              const std::string& prop, const DataType& type)
      : graph_(graph),
        path_expr_(std::move(path_expr)),
        prop_(prop),
        elem_type_(ListType::GetChildType(type)),
        type_(type) {}

  const DataType& type() const override { return type_; }

  Value eval_record(const DataChunk& chunk, size_t idx) const override {
    auto path_val = path_expr_->Cast<RecordExprBase>().eval_record(chunk, idx);
    if (path_val.IsNull()) {
      return Value(type_);
    }
    const auto& vertices = PathValue::Get(path_val).nodes();
    std::vector<Value> prop_values;
    for (const auto& vertex : vertices) {
      const auto& prop_names = graph_.schema().get_vertex_property_names(
          static_cast<label_t>(vertex.label()));
      auto it = std::find(prop_names.begin(), prop_names.end(), prop_);
      if (it == prop_names.end()) {
        prop_values.push_back(Value(elem_type_));
      } else {
        auto prop_id = static_cast<int>(std::distance(prop_names.begin(), it));
        prop_values.emplace_back(
            graph_.GetVertexProperty(vertex.label(), vertex.vid(), prop_id));
      }
    }
    return Value::LIST(elem_type_, std::move(prop_values));
  }

 private:
  const StorageReadInterface& graph_;
  std::unique_ptr<BindedExprBase> path_expr_;
  std::string prop_;
  DataType elem_type_;
  DataType type_;
};

class BindedPathEdgesPropsExpr : public RecordExprBase {
 public:
  BindedPathEdgesPropsExpr(const StorageReadInterface& graph,
                           std::unique_ptr<BindedExprBase>&& path_expr,
                           const std::string& prop, const DataType& type)
      : graph_(graph),
        path_expr_(std::move(path_expr)),
        prop_(prop),
        elem_type_(ListType::GetChildType(type)),
        type_(type) {}

  const DataType& type() const override { return type_; }

  Value eval_record(const DataChunk& chunk, size_t idx) const override {
    auto path_val = path_expr_->Cast<RecordExprBase>().eval_record(chunk, idx);
    if (path_val.IsNull()) {
      return Value(type_);
    }
    const auto& edges = PathValue::Get(path_val).relationships();
    std::vector<Value> prop_values;
    for (const auto& edge : edges) {
      const auto& prop_names = graph_.schema().get_edge_property_names(
          edge.label.src_label, edge.label.dst_label, edge.label.edge_label);
      auto it = std::find(prop_names.begin(), prop_names.end(), prop_);
      if (it == prop_names.end()) {
        prop_values.push_back(Value(elem_type_));
      } else {
        auto prop_id = static_cast<int>(std::distance(prop_names.begin(), it));
        const auto& accessor = graph_.GetEdgeDataAccessor(
            edge.label.src_label, edge.label.dst_label, edge.label.edge_label,
            prop_id);
        prop_values.emplace_back(accessor.get_data_from_ptr(edge.prop));
      }
    }
    return Value::LIST(elem_type_, std::move(prop_values));
  }

 private:
  const StorageReadInterface& graph_;
  std::unique_ptr<BindedExprBase> path_expr_;
  std::string prop_;
  DataType elem_type_;
  DataType type_;
};

std::unique_ptr<BindedExprBase> PathPropsExpr::bind(
    const IStorageInterface* storage, const ParamsMap& params) const {
  const auto* graph = dynamic_cast<const StorageReadInterface*>(storage);
  if (extract_vertex_prop_) {
    return std::make_unique<BindedPathVerticesPropsExpr>(
        *graph, path_expr_->bind(storage, params), prop_, type_);
  }
  return std::make_unique<BindedPathEdgesPropsExpr>(
      *graph, path_expr_->bind(storage, params), prop_, type_);
}

class BindedStartEndNodeExpr : public RecordExprBase {
 public:
  BindedStartEndNodeExpr(std::unique_ptr<BindedExprBase>&& edge_expr,
                         bool is_start)
      : edge_expr_(std::move(edge_expr)),
        is_start_(is_start),
        type_(DataType::VERTEX) {}
  const DataType& type() const override { return type_; }

  Value eval_record(const DataChunk& chunk, size_t idx) const override {
    Value edge_val = edge_expr_->Cast<RecordExprBase>().eval_record(chunk, idx);
    const auto& edge = edge_val.GetValue<edge_t>();
    return Value::VERTEX(is_start_ ? edge.start_node() : edge.end_node());
  }

 private:
  std::unique_ptr<BindedExprBase> edge_expr_;
  bool is_start_;
  DataType type_;
};

std::unique_ptr<BindedExprBase> StartEndNodeExpr::bind(
    const IStorageInterface* storage, const ParamsMap& params) const {
  return std::make_unique<BindedStartEndNodeExpr>(
      path_expr_->bind(storage, params), is_start_);
}
}  // namespace execution
}  // namespace neug
