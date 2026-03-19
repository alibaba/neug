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
#pragma once

#include "neug/execution/common/columns/columns_utils.h"
#include "neug/execution/common/columns/i_context_column.h"
#include "neug/execution/common/types/graph_types.h"
#include "neug/utils/property/column.h"
#include "neug/utils/property/types.h"

namespace neug {

namespace execution {

enum class EdgeColumnType { kSDSL, kSDML, kBDSL, kBDML, kMS, kUnKnown };

class IEdgeColumn : public IContextColumn {
 public:
  IEdgeColumn() : type_(DataType(DataTypeId::kEdge)) {}
  virtual ~IEdgeColumn() = default;

  ContextColumnType column_type() const override {
    return ContextColumnType::kEdge;
  }

  virtual EdgeRecord get_edge(size_t idx) const = 0;

  inline Value get_elem(size_t idx) const override {
    if (is_optional() && !has_value(idx)) {
      return Value(DataType(DataTypeId::kEdge));
    } else {
      auto er = get_edge(idx);
      return Value::EDGE(er);
    }
  }

  virtual Direction dir() const { return Direction::kBoth; }

  inline const DataType& elem_type() const override { return type_; }
  virtual std::vector<LabelTriplet> get_labels() const = 0;
  virtual EdgeColumnType edge_column_type() const = 0;

 private:
  DataType type_;
};

class SDSLEdgeColumnBuilder;
class MSEdgeColumnBuilder;

class SDSLEdgeColumn : public IEdgeColumn {
 public:
  SDSLEdgeColumn(Direction dir, const LabelTriplet& label)
      : dir_(dir), label_(label), is_optional_(false) {}

  inline EdgeRecord get_edge(size_t idx) const override {
    EdgeRecord ret;
    ret.label = label_;
    ret.dir = dir_;
    ret.src = std::get<0>(edges_[idx]);
    ret.dst = std::get<1>(edges_[idx]);
    ret.prop = std::get<2>(edges_[idx]);
    return ret;
  }

  inline size_t size() const override { return edges_.size(); }

  inline Direction dir() const override { return dir_; }

  bool generate_dedup_offset(std::vector<size_t>& offsets) const override {
    ColumnsUtils::generate_dedup_offset(edges_, offsets);
    return true;
  }

  std::string column_info() const override {
    std::string is_optional_str = is_optional_ ? "Optional " : "";
    return is_optional_str + "SDSLEdgeColumn: label = " + label_.to_string() +
           ", dir = " + std::to_string(static_cast<int>(dir_)) +
           ", size = " + std::to_string(edges_.size());
  }

  std::shared_ptr<IContextColumn> shuffle(
      const std::vector<size_t>& offsets) const override;

  std::shared_ptr<IContextColumn> optional_shuffle(
      const std::vector<size_t>& offsets) const override;

  template <typename FUNC_T>
  void foreach_edge_opt(const FUNC_T& func) const {
    for (size_t i = 0; i < edges_.size(); ++i) {
      auto& tup = edges_[i];
      func(i, std::get<0>(tup), std::get<1>(tup), std::get<2>(tup));
    }
  }

  std::vector<LabelTriplet> get_labels() const override { return {label_}; }

  inline EdgeColumnType edge_column_type() const override {
    return EdgeColumnType::kSDSL;
  }

  bool has_value(size_t idx) const override {
    auto& tup = edges_[idx];
    return std::get<0>(tup) != std::numeric_limits<vid_t>::max();
  }

  inline bool is_optional() const override { return is_optional_; }

 private:
  friend class SDSLEdgeColumnBuilder;
  friend class MSEdgeColumnBuilder;

  Direction dir_;
  LabelTriplet label_;
  bool is_optional_;

  std::vector<std::tuple<vid_t, vid_t, const void*>> edges_;
};

class SDSLEdgeColumnBuilder : public IContextColumnBuilder {
 public:
  SDSLEdgeColumnBuilder(Direction dir, const LabelTriplet& label)
      : dir_(dir), label_(label), is_optional_(false) {}
  ~SDSLEdgeColumnBuilder() = default;

  void reserve(size_t size) override { edges_.reserve(size); }
  inline void push_back_elem(const Value& val) override {
    const auto& e = val.GetValue<edge_t>();
    push_back_opt(e.src, e.dst, e.prop);
  }
  inline void push_back_opt(vid_t src, vid_t dst, const void* prop) {
    assert(src != std::numeric_limits<vid_t>::max());
    assert(dst != std::numeric_limits<vid_t>::max());
    edges_.emplace_back(src, dst, prop);
  }

  inline void push_back_null() override {
    is_optional_ = true;
    edges_.emplace_back(std::numeric_limits<vid_t>::max(),
                        std::numeric_limits<vid_t>::max(), nullptr);
  }

  std::shared_ptr<IContextColumn> finish() override;

 private:
  Direction dir_;
  LabelTriplet label_;
  std::vector<std::tuple<vid_t, vid_t, const void*>> edges_;
  bool is_optional_;
};

class MSEdgeColumn : public IEdgeColumn {
 public:
  MSEdgeColumn() {}
  ~MSEdgeColumn() = default;

  inline EdgeRecord get_edge(size_t idx) const override {
    for (auto& seg_tuple : edges_) {
      auto& seg = std::get<2>(seg_tuple);
      if (idx < seg.size()) {
        EdgeRecord ret;
        ret.label = labels_[std::get<0>(seg_tuple)];
        ret.dir = std::get<1>(seg_tuple);
        auto& e = seg[idx];
        ret.src = std::get<0>(e);
        ret.dst = std::get<1>(e);
        ret.prop = std::get<2>(e);
        return ret;
      }
      idx -= seg.size();
    }
    return EdgeRecord();
  }

  inline size_t size() const override { return total_size_; }

  bool generate_dedup_offset(std::vector<size_t>& offsets) const override {
    LOG(ERROR) << "not implemented for " << this->column_info();
    return false;
  }

  std::string column_info() const override {
    std::string is_optional_str = is_optional_ ? "Optional" : "";
    return is_optional_str +
           "MSEdgeColumn: num_labels = " + std::to_string(labels_.size()) +
           ", size = " + std::to_string(total_size_);
  }

  std::shared_ptr<IContextColumn> shuffle(
      const std::vector<size_t>& offsets) const override;

  std::shared_ptr<IContextColumn> optional_shuffle(
      const std::vector<size_t>& offsets) const override;

  inline EdgeColumnType edge_column_type() const override {
    return EdgeColumnType::kMS;
  }

  inline bool is_optional() const override { return is_optional_; }

  std::vector<LabelTriplet> get_labels() const override { return labels_; }

  template <typename FUNC_T>
  void foreach_edge_opt(const FUNC_T& func) const {
    size_t idx = 0;
    for (auto& seg_tuple : edges_) {
      auto& seg = std::get<2>(seg_tuple);
      for (auto& e : seg) {
        func(idx, labels_[std::get<0>(seg_tuple)], std::get<1>(seg_tuple),
             std::get<0>(e), std::get<1>(e), std::get<2>(e));
        ++idx;
      }
    }
  }

  bool has_value(size_t idx) const override {
    const auto& tup = get_edge(idx);
    return tup.src != std::numeric_limits<vid_t>::max();
  }

  size_t seg_num() const { return edges_.size(); }
  const LabelTriplet& seg_label(size_t idx) const {
    return labels_[std::get<0>(edges_[idx])];
  }
  Direction seg_dir(size_t idx) const { return std::get<1>(edges_[idx]); }
  const std::vector<std::tuple<vid_t, vid_t, const void*>>& seg_edges(
      size_t idx) const {
    return std::get<2>(edges_[idx]);
  }

 private:
  friend class MSEdgeColumnBuilder;

  bool is_optional_;
  std::vector<LabelTriplet> labels_;
  std::vector<std::tuple<int, Direction,
                         std::vector<std::tuple<vid_t, vid_t, const void*>>>>
      edges_;
  size_t total_size_;
};

class MSEdgeColumnBuilder : public IContextColumnBuilder {
 public:
  MSEdgeColumnBuilder() : is_optional_(false) {}
  ~MSEdgeColumnBuilder() = default;

  void reserve(size_t size) override { cur_edges_.reserve(size); }

  inline void push_back_elem(const Value& val) override {
    LOG(FATAL) << "not implemented for MSEdgeColumnBuilder";
  }

  inline void start_label_dir(const LabelTriplet& label, Direction dir) {
    if (cur_label_idx_ != -1 &&
        (labels_[cur_label_idx_] != label || dir != cur_dir_)) {
      if (!cur_edges_.empty()) {
        edges_.emplace_back(cur_label_idx_, cur_dir_, std::move(cur_edges_));
      }
    }
    auto it = label_idx_map_.find(label);
    if (it == label_idx_map_.end()) {
      cur_label_idx_ = labels_.size();
      label_idx_map_[label] = cur_label_idx_;
      labels_.emplace_back(label);
    } else {
      cur_label_idx_ = it->second;
    }
    cur_dir_ = dir;
    cur_edges_.clear();
  }

  inline void push_back_opt(vid_t src, vid_t dst, const void* prop) {
    assert(src != std::numeric_limits<vid_t>::max());
    assert(dst != std::numeric_limits<vid_t>::max());
    cur_edges_.emplace_back(src, dst, prop);
  }

  inline void push_back_null() override {
    is_optional_ = true;
    cur_edges_.emplace_back(std::numeric_limits<vid_t>::max(),
                            std::numeric_limits<vid_t>::max(), nullptr);
  }

  inline std::shared_ptr<IContextColumn> finish() override {
    if (!cur_edges_.empty()) {
      edges_.emplace_back(cur_label_idx_, cur_dir_, std::move(cur_edges_));
    }
    if (edges_.empty()) {
      auto col =
          std::make_shared<SDSLEdgeColumn>(cur_dir_, labels_[cur_label_idx_]);
      return col;
    } else if (edges_.size() == 1) {
      auto col = std::make_shared<SDSLEdgeColumn>(
          std::get<1>(edges_[0]), labels_[std::get<0>(edges_[0])]);
      col->edges_ = std::move(std::get<2>(edges_[0]));
      col->is_optional_ = is_optional_;
      return col;
    } else {
      size_t total_size_ = 0;
      for (auto& t : edges_) {
        total_size_ += std::get<2>(t).size();
      }
      auto col = std::make_shared<MSEdgeColumn>();
      col->edges_ = std::move(edges_);
      col->total_size_ = total_size_;
      col->is_optional_ = is_optional_;
      col->labels_ = std::move(labels_);
      return col;
    }
  }

 private:
  std::vector<std::tuple<int, Direction,
                         std::vector<std::tuple<vid_t, vid_t, const void*>>>>
      edges_;
  std::vector<LabelTriplet> labels_;
  bool is_optional_;
  std::map<LabelTriplet, int> label_idx_map_;

  int cur_label_idx_ = -1;
  Direction cur_dir_;
  std::vector<std::tuple<vid_t, vid_t, const void*>> cur_edges_;
};

class BDSLEdgeColumnBuilder;

class BDSLEdgeColumn : public IEdgeColumn {
 public:
  explicit BDSLEdgeColumn(const LabelTriplet& label)
      : label_(label), is_optional_(false) {}

  inline EdgeRecord get_edge(size_t idx) const override {
    EdgeRecord ret;
    auto& e = edges_[idx];
    ret.label = label_;
    ret.dir = std::get<3>(e);
    ret.src = std::get<0>(e);
    ret.dst = std::get<1>(e);
    ret.prop = std::get<2>(e);
    return ret;
  }

  inline size_t size() const override { return edges_.size(); }

  bool generate_dedup_offset(std::vector<size_t>& offsets) const override {
    ColumnsUtils::generate_dedup_offset(edges_, offsets);
    return true;
  }

  std::string column_info() const override {
    std::string is_optional_str = is_optional_ ? "Optional" : "";
    return is_optional_str + "BDSLEdgeColumn: label = " + label_.to_string() +
           ", size = " + std::to_string(edges_.size());
  }

  std::shared_ptr<IContextColumn> shuffle(
      const std::vector<size_t>& offsets) const override;

  std::shared_ptr<IContextColumn> optional_shuffle(
      const std::vector<size_t>& offsets) const override;

  inline EdgeColumnType edge_column_type() const override {
    return EdgeColumnType::kBDSL;
  }

  inline bool is_optional() const override { return is_optional_; }

  std::vector<LabelTriplet> get_labels() const override {
    return std::vector<LabelTriplet>{label_};
  }

  template <typename FUNC_T>
  void foreach_edge_opt(const FUNC_T& func) const {
    for (size_t i = 0; i < edges_.size(); ++i) {
      auto& tup = edges_[i];
      func(i, std::get<0>(tup), std::get<1>(tup), std::get<2>(tup),
           std::get<3>(tup));
    }
  }

  bool has_value(size_t idx) const override {
    const auto& tup = edges_[idx];
    return std::get<0>(tup) != std::numeric_limits<vid_t>::max();
  }

 private:
  friend class BDSLEdgeColumnBuilder;
  LabelTriplet label_;
  std::vector<std::tuple<vid_t, vid_t, const void*, Direction>> edges_;
  bool is_optional_;
};

class BDSLEdgeColumnBuilder : public IContextColumnBuilder {
 public:
  explicit BDSLEdgeColumnBuilder(const LabelTriplet& label)
      : label_(label), edges_(), is_optional_(false) {}

  ~BDSLEdgeColumnBuilder() = default;

  void reserve(size_t size) override { edges_.reserve(size); }

  inline void push_back_elem(const Value& val) override {
    LOG(FATAL) << "not implemented for BDSLEdgeColumnBuilder";
  }

  inline void push_back_opt(vid_t src, vid_t dst, const void* prop,
                            Direction dir) {
    assert(src != std::numeric_limits<vid_t>::max());
    assert(dst != std::numeric_limits<vid_t>::max());
    edges_.emplace_back(src, dst, prop, dir);
  }

  inline void push_back_null() override {
    is_optional_ = true;
    edges_.emplace_back(std::numeric_limits<vid_t>::max(),
                        std::numeric_limits<vid_t>::max(), nullptr,
                        Direction::kOut);
  }

  inline std::shared_ptr<IContextColumn> finish() override {
    auto col = std::make_shared<BDSLEdgeColumn>(label_);
    col->edges_ = std::move(edges_);
    col->is_optional_ = is_optional_;
    return col;
  }

 private:
  LabelTriplet label_;
  std::vector<std::tuple<vid_t, vid_t, const void*, Direction>> edges_;
  bool is_optional_;
};

class SDMLEdgeColumnBuilder;

class SDMLEdgeColumn : public IEdgeColumn {
 public:
  explicit SDMLEdgeColumn(Direction dir) : dir_(dir) {}
  ~SDMLEdgeColumn() = default;

  inline EdgeRecord get_edge(size_t idx) const override {
    EdgeRecord ret;
    auto& e = edges_[idx];
    ret.label = labels_[std::get<0>(e)];
    ret.dir = dir_;
    ret.src = std::get<1>(e);
    ret.dst = std::get<2>(e);
    ret.prop = std::get<3>(e);
    return ret;
  }

  inline size_t size() const override { return edges_.size(); }

  bool generate_dedup_offset(std::vector<size_t>& offsets) const override {
    ColumnsUtils::generate_dedup_offset(edges_, offsets);
    return true;
  }

  std::string column_info() const override {
    std::string is_optional_str = is_optional_ ? "Optional" : "";
    return is_optional_str +
           "SDMLEdgeColumn: num_labels = " + std::to_string(labels_.size()) +
           ", size = " + std::to_string(edges_.size());
  }

  std::shared_ptr<IContextColumn> shuffle(
      const std::vector<size_t>& offsets) const override;

  std::shared_ptr<IContextColumn> optional_shuffle(
      const std::vector<size_t>& offsets) const override;

  inline EdgeColumnType edge_column_type() const override {
    return EdgeColumnType::kSDML;
  }

  template <typename FUNC_T>
  void foreach_edge_opt(const FUNC_T& func) const {
    for (size_t i = 0; i < edges_.size(); ++i) {
      auto& tup = edges_[i];
      func(i, labels_[std::get<0>(tup)], std::get<1>(tup), std::get<2>(tup),
           std::get<3>(tup));
    }
  }

  std::vector<LabelTriplet> get_labels() const override { return labels_; }

  Direction dir() const override { return dir_; }

  bool has_value(size_t idx) const override {
    const auto& tup = edges_[idx];
    return std::get<1>(tup) != std::numeric_limits<vid_t>::max();
  }

 private:
  friend class SDMLEdgeColumnBuilder;
  Direction dir_;
  std::map<LabelTriplet, label_t> index_;
  std::vector<LabelTriplet> labels_;
  std::vector<std::tuple<int, vid_t, vid_t, const void*>> edges_;
  bool is_optional_;
};

class SDMLEdgeColumnBuilder : public IContextColumnBuilder {
 public:
  SDMLEdgeColumnBuilder(Direction dir, const std::vector<LabelTriplet>& labels)
      : dir_(dir), labels_(labels), is_optional_(false) {
    for (size_t i = 0; i < labels.size(); ++i) {
      index_.emplace(labels[i], i);
    }
  }
  ~SDMLEdgeColumnBuilder() = default;

  void reserve(size_t size) override { edges_.reserve(size); }

  inline void push_back_elem(const Value& val) override {
    LOG(FATAL) << "not implemented for SDMLEdgeColumnBuilder";
  }

  inline void push_back_opt(const LabelTriplet& label, vid_t src, vid_t dst,
                            const void* prop) {
    assert(src != std::numeric_limits<vid_t>::max());
    assert(dst != std::numeric_limits<vid_t>::max());
    edges_.emplace_back(index_.at(label), src, dst, prop);
  }

  inline void push_back_opt(int label_idx, vid_t src, vid_t dst,
                            const void* prop) {
    assert(src != std::numeric_limits<vid_t>::max());
    assert(dst != std::numeric_limits<vid_t>::max());
    edges_.emplace_back(label_idx, src, dst, prop);
  }

  inline void push_back_null() override {
    is_optional_ = true;
    edges_.emplace_back(-1, std::numeric_limits<vid_t>::max(),
                        std::numeric_limits<vid_t>::max(), nullptr);
  }

  inline std::shared_ptr<IContextColumn> finish() override {
    auto col = std::make_shared<SDMLEdgeColumn>(dir_);
    col->edges_ = std::move(edges_);
    col->index_ = std::move(index_);
    col->labels_ = std::move(labels_);
    col->is_optional_ = is_optional_;
    return col;
  }

 private:
  Direction dir_;
  std::map<LabelTriplet, label_t> index_;
  std::vector<LabelTriplet> labels_;
  std::vector<std::tuple<int, vid_t, vid_t, const void*>> edges_;
  bool is_optional_;
};

class BDMLEdgeColumnBuilder;

class BDMLEdgeColumn : public IEdgeColumn {
 public:
  BDMLEdgeColumn() {}
  ~BDMLEdgeColumn() = default;

  inline EdgeRecord get_edge(size_t idx) const override {
    EdgeRecord ret;
    auto& e = edges_[idx];
    ret.label = labels_[std::get<0>(e)];
    ret.dir = std::get<4>(e);
    ret.src = std::get<1>(e);
    ret.dst = std::get<2>(e);
    ret.prop = std::get<3>(e);
    return ret;
  }

  inline size_t size() const override { return edges_.size(); }

  bool generate_dedup_offset(std::vector<size_t>& offsets) const override {
    ColumnsUtils::generate_dedup_offset(edges_, offsets);
    return true;
  }

  std::string column_info() const override {
    std::string is_optional_str = is_optional_ ? "Optional" : "";
    return is_optional_str +
           "BDMLEdgeColumn: num_labels = " + std::to_string(labels_.size()) +
           ", size = " + std::to_string(edges_.size());
  }

  std::shared_ptr<IContextColumn> shuffle(
      const std::vector<size_t>& offsets) const override;

  std::shared_ptr<IContextColumn> optional_shuffle(
      const std::vector<size_t>& offsets) const override;

  inline EdgeColumnType edge_column_type() const override {
    return EdgeColumnType::kBDML;
  }

  template <typename FUNC_T>
  void foreach_edge_opt(const FUNC_T& func) const {
    for (size_t i = 0; i < edges_.size(); ++i) {
      auto& tup = edges_[i];
      func(i, labels_[std::get<0>(tup)], std::get<4>(tup), std::get<1>(tup),
           std::get<2>(tup), std::get<3>(tup));
    }
  }

  std::vector<LabelTriplet> get_labels() const override { return labels_; }

  bool has_value(size_t idx) const override {
    const auto& tup = edges_[idx];
    return std::get<1>(tup) != std::numeric_limits<vid_t>::max();
  }

 private:
  friend class BDMLEdgeColumnBuilder;
  std::map<LabelTriplet, int> index_;
  std::vector<LabelTriplet> labels_;
  std::vector<std::tuple<int, vid_t, vid_t, const void*, Direction>> edges_;
  bool is_optional_;
};

class BDMLEdgeColumnBuilder : public IContextColumnBuilder {
 public:
  explicit BDMLEdgeColumnBuilder(const std::vector<LabelTriplet>& labels)
      : labels_(labels), edges_(), is_optional_(false) {
    for (size_t i = 0; i < labels.size(); ++i) {
      index_.emplace(labels[i], i);
    }
  }
  ~BDMLEdgeColumnBuilder() = default;

  void reserve(size_t size) override { edges_.reserve(size); }

  inline void push_back_elem(const Value& val) override {
    const auto& edge = val.GetValue<edge_t>();
    insert_label(edge.label);
    push_back_opt(edge.label, edge.src, edge.dst, edge.prop, edge.dir);
  }

  inline void push_back_opt(const LabelTriplet& label, vid_t src, vid_t dst,
                            const void* prop, Direction dir) {
    assert(src != std::numeric_limits<vid_t>::max());
    assert(dst != std::numeric_limits<vid_t>::max());
    edges_.emplace_back(index_.at(label), src, dst, prop, dir);
  }

  inline void push_back_opt(int label_idx, vid_t src, vid_t dst,
                            const void* prop, Direction dir) {
    assert(src != std::numeric_limits<vid_t>::max());
    assert(dst != std::numeric_limits<vid_t>::max());
    edges_.emplace_back(label_idx, src, dst, prop, dir);
  }

  inline void push_back_null() override {
    is_optional_ = true;
    edges_.emplace_back(-1, std::numeric_limits<vid_t>::max(),
                        std::numeric_limits<vid_t>::max(), nullptr,
                        Direction::kOut);
  }

  inline std::shared_ptr<IContextColumn> finish() override {
    auto col = std::make_shared<BDMLEdgeColumn>();
    col->edges_ = std::move(edges_);
    col->index_ = std::move(index_);
    col->labels_ = std::move(labels_);
    col->is_optional_ = is_optional_;
    return col;
  }

  inline int get_label_index(const LabelTriplet& label) const {
    return index_.at(label);
  }

  inline int insert_label(const LabelTriplet& label) {
    auto it = index_.find(label);
    if (it != index_.end()) {
      return it->second;
    }
    int idx = labels_.size();
    labels_.push_back(label);
    index_.emplace(label, idx);
    return idx;
  }

 private:
  std::map<LabelTriplet, int> index_;
  std::vector<LabelTriplet> labels_;
  std::vector<std::tuple<int, vid_t, vid_t, const void*, Direction>> edges_;
  bool is_optional_;
};

template <typename FUNC_T>
void foreach_edge(
    const IEdgeColumn& col, const FUNC_T& func,
    const std::vector<std::pair<LabelTriplet, Direction>>& to_filter = {}) {
  auto col_type = col.edge_column_type();
  if (to_filter.empty()) {
    if (col_type == EdgeColumnType::kSDSL) {
      auto& c = dynamic_cast<const SDSLEdgeColumn&>(col);
      LabelTriplet label = c.get_labels()[0];
      Direction dir = c.dir();
      c.foreach_edge_opt(
          [&](size_t idx, vid_t src, vid_t dst, const void* prop) {
            func(idx, label, dir, src, dst, prop);
          });
    } else if (col_type == EdgeColumnType::kBDSL) {
      auto& c = dynamic_cast<const BDSLEdgeColumn&>(col);
      LabelTriplet label = c.get_labels()[0];
      c.foreach_edge_opt(
          [&](size_t idx, vid_t src, vid_t dst, const void* prop,
              Direction dir) { func(idx, label, dir, src, dst, prop); });
    } else if (col_type == EdgeColumnType::kMS) {
      auto& c = dynamic_cast<const MSEdgeColumn&>(col);
      c.foreach_edge_opt(func);
    } else if (col_type == EdgeColumnType::kSDML) {
      auto& c = dynamic_cast<const SDMLEdgeColumn&>(col);
      Direction dir = c.dir();
      c.foreach_edge_opt(
          [&](size_t idx, const LabelTriplet& label, vid_t src, vid_t dst,
              const void* prop) { func(idx, label, dir, src, dst, prop); });
    } else if (col_type == EdgeColumnType::kBDML) {
      auto& c = dynamic_cast<const BDMLEdgeColumn&>(col);
      c.foreach_edge_opt(func);
    } else {
      LOG(FATAL) << "Not support foreach_edge for "
                 << static_cast<int>(col.edge_column_type());
    }
  } else {
    if (col_type == EdgeColumnType::kSDSL) {
      auto& c = dynamic_cast<const SDSLEdgeColumn&>(col);
      bool hit = false;
      auto self_label = c.get_labels()[0];
      auto self_dir = c.dir();
      for (auto& p : to_filter) {
        if (p.first == self_label && p.second == self_dir) {
          hit = true;
          break;
        }
      }
      if (!hit) {
        foreach_edge(col, func);
      }
    } else if (col_type == EdgeColumnType::kBDSL) {
      auto& c = dynamic_cast<const BDSLEdgeColumn&>(col);
      bool hit_oe = false, hit_ie = false;
      auto self_label = c.get_labels()[0];
      for (auto& p : to_filter) {
        if (p.first == self_label) {
          if (p.second == Direction::kOut) {
            hit_oe = true;
          } else if (p.second == Direction::kIn) {
            hit_ie = true;
          }
        }
      }
      if (hit_oe && hit_ie) {
        // do nothing
      } else if (hit_oe && !hit_ie) {
        c.foreach_edge_opt([&](size_t idx, vid_t src, vid_t dst,
                               const void* prop, Direction dir) {
          if (dir != Direction::kIn) {
            func(idx, self_label, dir, src, dst, prop);
          }
        });
      } else if (!hit_oe && hit_ie) {
        c.foreach_edge_opt([&](size_t idx, vid_t src, vid_t dst,
                               const void* prop, Direction dir) {
          if (dir != Direction::kOut) {
            func(idx, self_label, dir, src, dst, prop);
          }
        });
      } else {
        foreach_edge(col, func);
      }
    } else if (col_type == EdgeColumnType::kMS) {
      auto& c = dynamic_cast<const MSEdgeColumn&>(col);
      auto labels = c.get_labels();
      std::set<LabelTriplet> labels_set(labels.begin(), labels.end());
      std::set<LabelTriplet> oe_filter_label_set, ie_filter_label_set;
      for (auto& p : to_filter) {
        if (p.second == Direction::kOut &&
            labels_set.find(p.first) != labels_set.end()) {
          oe_filter_label_set.insert(p.first);
        } else if (p.second == Direction::kIn &&
                   labels_set.find(p.first) != labels_set.end()) {
          ie_filter_label_set.insert(p.first);
        }
      }
      if (oe_filter_label_set.empty() && ie_filter_label_set.empty()) {
        foreach_edge(col, func);
      } else {
        auto seg_num = c.seg_num();
        size_t idx = 0;
        for (size_t i = 0; i < seg_num; ++i) {
          auto seg_label = c.seg_label(i);
          auto seg_dir = c.seg_dir(i);
          if ((seg_dir == Direction::kOut &&
               oe_filter_label_set.find(seg_label) ==
                   oe_filter_label_set.end()) ||
              (seg_dir == Direction::kIn &&
               ie_filter_label_set.find(seg_label) ==
                   ie_filter_label_set.end())) {
            auto& seg_edges = c.seg_edges(i);
            for (auto& e : seg_edges) {
              func(idx, seg_label, seg_dir, std::get<0>(e), std::get<1>(e),
                   std::get<2>(e));
              ++idx;
            }
          } else {
            idx += c.seg_edges(i).size();
          }
        }
      }
    } else if (col_type == EdgeColumnType::kSDML) {
      auto& c = dynamic_cast<const SDMLEdgeColumn&>(col);
      auto dir = c.dir();
      auto labels = c.get_labels();
      std::set<LabelTriplet> labels_set(labels.begin(), labels.end());
      std::set<LabelTriplet> filter_label_set;
      for (auto& p : to_filter) {
        if (p.second == dir && labels_set.find(p.first) != labels_set.end()) {
          filter_label_set.insert(p.first);
        }
      }
      if (filter_label_set.empty()) {
        foreach_edge(col, func);
      } else {
        c.foreach_edge_opt([&](size_t idx, const LabelTriplet& label, vid_t src,
                               vid_t dst, const void* prop) {
          if (filter_label_set.find(label) == filter_label_set.end()) {
            func(idx, label, dir, src, dst, prop);
          }
        });
      }
    } else if (col_type == EdgeColumnType::kBDML) {
      auto& c = dynamic_cast<const BDMLEdgeColumn&>(col);
      auto labels = c.get_labels();
      std::set<LabelTriplet> labels_set(labels.begin(), labels.end());
      std::set<LabelTriplet> oe_filter_label_set, ie_filter_label_set;
      for (auto& p : to_filter) {
        if (p.second == Direction::kOut &&
            labels_set.find(p.first) != labels_set.end()) {
          oe_filter_label_set.insert(p.first);
        } else if (p.second == Direction::kIn &&
                   labels_set.find(p.first) != labels_set.end()) {
          ie_filter_label_set.insert(p.first);
        }
      }
      if (oe_filter_label_set.empty() && ie_filter_label_set.empty()) {
        foreach_edge(col, func);
      } else {
        c.foreach_edge_opt([&](size_t idx, const LabelTriplet& label,
                               Direction dir, vid_t src, vid_t dst,
                               const void* prop) {
          if ((dir == Direction::kOut &&
               oe_filter_label_set.find(label) == oe_filter_label_set.end()) ||
              (dir == Direction::kIn &&
               ie_filter_label_set.find(label) == ie_filter_label_set.end())) {
            func(idx, label, dir, src, dst, prop);
          }
        });
      }
    } else {
      LOG(FATAL) << "Not support foreach_edge for "
                 << static_cast<int>(col.edge_column_type());
    }
  }
}

}  // namespace execution

}  // namespace neug
