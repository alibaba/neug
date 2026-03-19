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

#include "neug/execution/common/columns/i_context_column.h"

namespace neug {

namespace execution {

enum class VertexColumnType {
  kSingle,
  kMultiSegment,
  kMultiple,
};

class IVertexColumn : public IContextColumn {
 public:
  IVertexColumn() : type_(DataType(DataTypeId::kVertex)) {}
  virtual ~IVertexColumn() = default;

  __attribute__((always_inline)) ContextColumnType column_type()
      const override {
    return ContextColumnType::kVertex;
  }

  virtual VertexColumnType vertex_column_type() const = 0;
  virtual VertexRecord get_vertex(size_t idx) const = 0;

  Value get_elem(size_t idx) const override {
    if (is_optional() && !has_value(idx)) {
      return Value(DataType::VERTEX);
    }
    return Value::VERTEX(this->get_vertex(idx));
  }

  __attribute__((always_inline)) const DataType& elem_type() const override {
    return type_;
  }

  virtual std::set<label_t> get_labels_set() const = 0;

 private:
  DataType type_;
};

class IVertexColumnBuilder : public IContextColumnBuilder {
 public:
  IVertexColumnBuilder() = default;
  virtual ~IVertexColumnBuilder() = default;

  virtual void push_back_vertex(VertexRecord v) = 0;

  void push_back_elem(const Value& val) override {
    if (val.IsNull()) {
      this->push_back_null();
      return;
    }
    this->push_back_vertex(val.GetValue<VertexRecord>());
  }
};

class MSVertexColumnBuilder;

class SLVertexColumn : public IVertexColumn {
 public:
  explicit SLVertexColumn(label_t label) : label_(label) {}
  ~SLVertexColumn() = default;

  __attribute__((always_inline)) inline size_t size() const override {
    return vertices_.size();
  }

  std::string column_info() const override {
    std::string is_optional_str = is_optional_ ? "Optional " : "";
    return is_optional_str + "SLVertexColumn(" + std::to_string(label_) + ")[" +
           std::to_string(size()) + "]";
  }

  __attribute__((always_inline)) VertexColumnType vertex_column_type()
      const override {
    return VertexColumnType::kSingle;
  }

  std::shared_ptr<IContextColumn> shuffle(
      const std::vector<size_t>& offsets) const override;

  std::shared_ptr<IContextColumn> optional_shuffle(
      const std::vector<size_t>& offset) const override;

  __attribute__((always_inline)) VertexRecord get_vertex(
      size_t idx) const override {
    return {label_, vertices_[idx]};
  }

  __attribute__((always_inline)) bool is_optional() const override {
    return is_optional_;
  }

  __attribute__((always_inline)) bool has_value(size_t idx) const override {
    return vertices_[idx] != std::numeric_limits<vid_t>::max();
  }

  std::shared_ptr<IContextColumn> union_col(
      std::shared_ptr<IContextColumn> other) const override;

  void generate_dedup_offset(std::vector<size_t>& offsets) const override;

  std::pair<std::shared_ptr<IContextColumn>, std::vector<std::vector<size_t>>>
  generate_aggregate_offset() const override;

  template <typename FUNC_T>
  void foreach_vertex(const FUNC_T& func) const {
    size_t num = vertices_.size();
    for (size_t k = 0; k < num; ++k) {
      func(k, label_, vertices_[k]);
    }
  }

  std::set<label_t> get_labels_set() const override {
    std::set<label_t> ret;
    ret.insert(label_);
    return ret;
  }

  __attribute__((always_inline)) label_t label() const { return label_; }

  __attribute__((always_inline)) const std::vector<vid_t>& vertices() const {
    return vertices_;
  }

 private:
  friend class MSVertexColumnBuilder;
  std::vector<vid_t> vertices_;
  label_t label_;
  bool is_optional_ = false;
};

class MSVertexColumn : public IVertexColumn {
 public:
  MSVertexColumn() = default;
  ~MSVertexColumn() = default;

  __attribute__((always_inline)) size_t size() const override {
    size_t ret = 0;
    for (auto& pair : vertices_) {
      ret += pair.second.size();
    }
    return ret;
  }

  std::string column_info() const override {
    std::string is_optional_str = is_optional_ ? "Optional " : "";
    std::string labels;
    for (auto label : labels_) {
      labels += std::to_string(label);
      labels += ", ";
    }
    if (!labels.empty()) {
      labels.resize(labels.size() - 2);
    }
    return is_optional_str + "MSVertexColumn(" + labels + ")[" +
           std::to_string(size()) + "]";
  }

  __attribute__((always_inline)) VertexColumnType vertex_column_type()
      const override {
    return VertexColumnType::kMultiSegment;
  }

  std::shared_ptr<IContextColumn> shuffle(
      const std::vector<size_t>& offsets) const override;

  std::shared_ptr<IContextColumn> optional_shuffle(
      const std::vector<size_t>& offsets) const override;

  __attribute__((always_inline)) VertexRecord get_vertex(
      size_t idx) const override {
    for (auto& pair : vertices_) {
      if (idx < pair.second.size()) {
        return {pair.first, pair.second[idx]};
      }
      idx -= pair.second.size();
    }
    LOG(FATAL) << "not found...";
    return {std::numeric_limits<label_t>::max(),
            std::numeric_limits<vid_t>::max()};
  }

  __attribute__((always_inline)) bool is_optional() const override {
    return is_optional_;
  }

  __attribute__((always_inline)) bool has_value(size_t idx) const override {
    auto v = get_vertex(idx);
    return v.vid_ != std::numeric_limits<vid_t>::max();
  }

  template <typename FUNC_T>
  void foreach_vertex(const FUNC_T& func) const {
    size_t index = 0;
    for (auto& pair : vertices_) {
      label_t label = pair.first;
      for (auto v : pair.second) {
        func(index++, label, v);
      }
    }
  }

  std::set<label_t> get_labels_set() const override { return labels_; }

  __attribute__((always_inline)) size_t seg_num() const {
    return vertices_.size();
  }

  __attribute__((always_inline)) label_t seg_label(size_t seg_id) const {
    return vertices_[seg_id].first;
  }

  __attribute__((always_inline)) const std::vector<vid_t>& seg_vertices(
      size_t seg_id) const {
    return vertices_[seg_id].second;
  }

 private:
  friend class MSVertexColumnBuilder;
  std::vector<std::pair<label_t, std::vector<vid_t>>> vertices_;
  std::set<label_t> labels_;

  bool is_optional_ = false;
};

class MSVertexColumnBuilder : public IVertexColumnBuilder {
 public:
  explicit MSVertexColumnBuilder(label_t label)
      : cur_label_(label), is_optional_(false) {
    CHECK(label != std::numeric_limits<label_t>::max());
  }
  ~MSVertexColumnBuilder() = default;
  void reserve(size_t size) override { cur_list_.reserve(size); }

  // v should not be null
  __attribute__((always_inline)) void push_back_vertex(
      VertexRecord v) override {
    if (v.label_ != cur_label_) {
      start_label(v.label_);
    }
    push_back_opt(v.vid_);
  }

  __attribute__((always_inline)) void start_label(label_t label) {
    if (!cur_list_.empty() && cur_label_ != label &&
        cur_label_ != std::numeric_limits<label_t>::max()) {
      vertices_.emplace_back(cur_label_, std::move(cur_list_));
      cur_list_.clear();
    }
    cur_label_ = label;
  }

  __attribute__((always_inline)) void push_back_opt(vid_t v) {
    assert(v != std::numeric_limits<vid_t>::max());
    cur_list_.push_back(v);
  }

  inline void push_back_null() override {
    is_optional_ = true;
    cur_list_.emplace_back(std::numeric_limits<vid_t>::max());
  }

  std::shared_ptr<IContextColumn> finish() override;

  __attribute__((always_inline)) size_t cur_size() const {
    return cur_list_.size();
  }

 private:
  label_t cur_label_;
  std::vector<vid_t> cur_list_;

  std::vector<std::pair<label_t, std::vector<vid_t>>> vertices_;

  bool is_optional_ = false;
};

class MLVertexColumnBuilder;
class MLVertexColumnBuilderOpt;

class MLVertexColumn : public IVertexColumn {
 public:
  MLVertexColumn() = default;
  ~MLVertexColumn() = default;

  __attribute__((always_inline)) size_t size() const override {
    return vertices_.size();
  }

  std::string column_info() const override {
    std::string is_optional_str = is_optional_ ? "Optional " : "";
    std::string labels;
    for (auto label : labels_) {
      labels += std::to_string(label);
      labels += ", ";
    }
    if (!labels.empty()) {
      labels.resize(labels.size() - 2);
    }
    return is_optional_str + "MLVertexColumn(" + labels + ")[" +
           std::to_string(size()) + "]";
  }

  __attribute__((always_inline)) VertexColumnType vertex_column_type()
      const override {
    return VertexColumnType::kMultiple;
  }

  std::shared_ptr<IContextColumn> shuffle(
      const std::vector<size_t>& offsets) const override;
  std::shared_ptr<IContextColumn> optional_shuffle(
      const std::vector<size_t>& offsets) const override;

  __attribute__((always_inline)) VertexRecord get_vertex(
      size_t idx) const override {
    return vertices_[idx];
  }

  __attribute__((always_inline)) bool is_optional() const override {
    return is_optional_;
  }

  __attribute__((always_inline)) bool has_value(size_t idx) const override {
    return vertices_[idx].vid_ != std::numeric_limits<vid_t>::max();
  }

  template <typename FUNC_T>
  void foreach_vertex(const FUNC_T& func) const {
    size_t index = 0;
    for (auto& pair : vertices_) {
      func(index++, pair.label_, pair.vid_);
    }
  }

  std::set<label_t> get_labels_set() const override { return labels_; }

  void generate_dedup_offset(std::vector<size_t>& offsets) const override;

 private:
  friend class MLVertexColumnBuilder;
  friend class MLVertexColumnBuilderOpt;
  std::vector<VertexRecord> vertices_;
  std::set<label_t> labels_;
  bool is_optional_ = false;
};

class MLVertexColumnBuilder : public IVertexColumnBuilder {
 public:
  MLVertexColumnBuilder() {}
  explicit MLVertexColumnBuilder(const std::set<label_t>& labels)
      : labels_(labels) {}
  ~MLVertexColumnBuilder() = default;

  void reserve(size_t size) override { vertices_.reserve(size); }

  // v should not be null
  __attribute__((always_inline)) void push_back_opt(VertexRecord v) {
    labels_.insert(v.label_);
    assert(v.vid_ != std::numeric_limits<vid_t>::max());
    vertices_.push_back(v);
  }

  inline void push_back_vertex(VertexRecord v) override { push_back_opt(v); }

  inline void push_back_null() override {
    is_optional_ = true;
    vertices_.emplace_back(VertexRecord{std::numeric_limits<label_t>::max(),
                                        std::numeric_limits<vid_t>::max()});
  }

  std::shared_ptr<IContextColumn> finish() override;

 private:
  std::vector<VertexRecord> vertices_;
  std::set<label_t> labels_;
  bool is_optional_ = false;
};

class MLVertexColumnBuilderOpt : public IVertexColumnBuilder {
 public:
  explicit MLVertexColumnBuilderOpt(const std::set<label_t>& labels) {
    size_t max_label = labels.empty() ? 0 : *labels.rbegin();
    labels_bitmap_.resize(max_label + 1, false);
  }
  ~MLVertexColumnBuilderOpt() = default;

  void reserve(size_t size) override { vertices_.reserve(size); }

  // v should not be null
  __attribute__((always_inline)) void push_back_opt(VertexRecord v) {
    labels_bitmap_[v.label_] = true;
    assert(v.vid_ != std::numeric_limits<vid_t>::max());
    vertices_.push_back(v);
  }

  // v should not be null
  __attribute__((always_inline)) void push_back_vertex(
      VertexRecord v) override {
    push_back_opt(v);
  }

  __attribute__((always_inline)) void push_back_null() override {
    is_optional_ = true;
    vertices_.emplace_back(std::numeric_limits<label_t>::max(),
                           std::numeric_limits<vid_t>::max());
  }

  __attribute__((always_inline)) std::shared_ptr<IContextColumn> finish()
      override {
    auto ret = std::make_shared<MLVertexColumn>();
    for (size_t i = 0; i < labels_bitmap_.size(); ++i) {
      if (labels_bitmap_[i]) {
        ret->labels_.insert(static_cast<label_t>(i));
      }
    }
    ret->vertices_.swap(vertices_);
    ret->is_optional_ = is_optional_;
    return ret;
  }

  __attribute__((always_inline)) size_t size() { return vertices_.size(); }

  __attribute__((always_inline)) size_t cur_size() { return vertices_.size(); }

 private:
  std::vector<VertexRecord> vertices_;
  std::vector<bool> labels_bitmap_;
  bool is_optional_ = false;
};

template <typename FUNC_T>
void foreach_vertex(const IVertexColumn& col, const FUNC_T& func) {
  if (col.vertex_column_type() == VertexColumnType::kSingle) {
    const SLVertexColumn& ref = dynamic_cast<const SLVertexColumn&>(col);
    ref.foreach_vertex(func);
  } else if (col.vertex_column_type() == VertexColumnType::kMultiple) {
    const MLVertexColumn& ref = dynamic_cast<const MLVertexColumn&>(col);
    ref.foreach_vertex(func);
  } else {
    const MSVertexColumn& ref = dynamic_cast<const MSVertexColumn&>(col);
    ref.foreach_vertex(func);
  }
}

}  // namespace execution

}  // namespace neug
