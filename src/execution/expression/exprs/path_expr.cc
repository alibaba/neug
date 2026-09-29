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

class BindedSingleNodePathExpr : public RecordExprBase {
 public:
  explicit BindedSingleNodePathExpr(std::unique_ptr<BindedExprBase>&& node_expr)
      : node_expr_(std::move(node_expr)), type_(DataType::PATH) {}

  const DataType& type() const override { return type_; }

  Value eval_record(const DataChunk& chunk, size_t idx) const override {
    auto node_val = node_expr_->Cast<RecordExprBase>().eval_record(chunk, idx);
    if (node_val.IsNull()) {
      return Value(type_);
    }
    const auto& node = node_val.GetValue<vertex_t>();
    return Value::PATH(Path(node.label(), node.vid()));
  }

 private:
  std::unique_ptr<BindedExprBase> node_expr_;
  DataType type_;
};

std::unique_ptr<BindedExprBase> SingleNodePathExpr::bind(
    const IStorageInterface* storage, const ParamsMap& params) const {
  return std::make_unique<BindedSingleNodePathExpr>(
      node_expr_->bind(storage, params));
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
    // The query pattern already tells us the path orientation: start -> end.
    // Derive the edge direction from the stored src/dst so the resulting
    // Path does not depend on which way the optimizer chose to scan the
    // edge (which can differ across platforms).
    Direction path_dir = rel.dir;
    if (start.vid() == rel.src && end.vid() == rel.dst) {
      path_dir = Direction::kOut;
    } else if (start.vid() == rel.dst && end.vid() == rel.src) {
      path_dir = Direction::kIn;
    }
    std::vector<std::tuple<label_t, Direction, const void*>> edge_data;
    edge_data.emplace_back(rel.label.edge_label, path_dir, rel.prop);
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

// Rebuild `path` in the opposite traversal direction. Edge directions are
// flipped so Path::relationships() keeps reporting storage-order src/dst
// for every edge after the reversal.
static Path reverse_path(const Path& path) {
  auto vertices = path.nodes();
  auto edges = path.relationships();
  if (vertices.empty() || vertices.size() != edges.size() + 1) {
    return path;
  }
  std::reverse(vertices.begin(), vertices.end());
  std::vector<std::tuple<label_t, Direction, const void*>> edge_datas;
  edge_datas.reserve(edges.size());
  for (const auto& edge : edges) {
    Direction flipped = edge.dir;
    if (edge.dir == Direction::kOut) {
      flipped = Direction::kIn;
    } else if (edge.dir == Direction::kIn) {
      flipped = Direction::kOut;
    }
    edge_datas.emplace_back(edge.label.edge_label, flipped, edge.prop);
  }
  return Path(edge_datas, vertices);
}

class BindedPathConcatExpr : public RecordExprBase {
 public:
  explicit BindedPathConcatExpr(
      std::vector<std::unique_ptr<BindedExprBase>>&& path_exprs)
      : path_exprs_(std::move(path_exprs)), type_(DataType::PATH) {}

  const DataType& type() const override { return type_; }

  Value eval_record(const DataChunk& chunk, size_t idx) const override {
    auto first_val =
        path_exprs_.front()->Cast<RecordExprBase>().eval_record(chunk, idx);
    if (first_val.IsNull()) {
      return Value(type_);
    }

    auto result = PathValue::Get(first_val);
    for (size_t path_idx = 1; path_idx < path_exprs_.size(); ++path_idx) {
      auto path_val =
          path_exprs_[path_idx]->Cast<RecordExprBase>().eval_record(chunk, idx);
      if (path_val.IsNull()) {
        return Value(type_);
      }

      auto next = PathValue::Get(path_val);
      auto vertices = next.nodes();
      if (vertices.empty()) {
        THROW_INVALID_ARGUMENT_EXCEPTION(
            "Cannot concatenate paths with different boundary vertices");
      }
      if (!(result.end_node() == vertices.front())) {
        // The engine may expand a segment from the pattern's other endpoint
        // (the planner can bind either end of an undirected var-length
        // relationship), which yields that segment in reverse traversal
        // order. Re-align the boundaries so the concatenated path always
        // follows the query's written direction.
        auto result_nodes = result.nodes();
        if (result.end_node() == vertices.back()) {
          // `next` is reversed; flip it back.
          next = reverse_path(next);
          vertices = next.nodes();
        } else if (path_idx == 1 && !result_nodes.empty() &&
                   result_nodes.front() == vertices.front()) {
          // The first segment is reversed; flip it back.
          result = reverse_path(result);
        } else if (path_idx == 1 && !result_nodes.empty() &&
                   result_nodes.front() == vertices.back()) {
          // Both the first segment and `next` are reversed.
          result = reverse_path(result);
          next = reverse_path(next);
          vertices = next.nodes();
        } else {
          THROW_INVALID_ARGUMENT_EXCEPTION(
              "Cannot concatenate paths with different boundary vertices");
        }
      }
      auto edges = next.relationships();
      for (size_t i = 0; i < edges.size(); ++i) {
        const auto& edge = edges[i];
        const auto& vertex = vertices[i + 1];
        result = result.expand(edge.label.edge_label, vertex.label(),
                               vertex.vid(), edge.dir, edge.prop);
      }
    }
    return Value::PATH(result);
  }

 private:
  std::vector<std::unique_ptr<BindedExprBase>> path_exprs_;
  DataType type_;
};

std::unique_ptr<BindedExprBase> PathConcatExpr::bind(
    const IStorageInterface* storage, const ParamsMap& params) const {
  std::vector<std::unique_ptr<BindedExprBase>> bound_path_exprs;
  bound_path_exprs.reserve(path_exprs_.size());
  for (const auto& path_expr : path_exprs_) {
    bound_path_exprs.emplace_back(path_expr->bind(storage, params));
  }
  return std::make_unique<BindedPathConcatExpr>(std::move(bound_path_exprs));
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
