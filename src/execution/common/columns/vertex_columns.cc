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

#include "neug/execution/common/columns/vertex_columns.h"

#include <parallel_hashmap/phmap_base.h>

#include "parallel_hashmap/phmap.h"

namespace neug {
namespace execution {

std::shared_ptr<IContextColumn> SLVertexColumn::shuffle(
    const std::vector<size_t>& offsets) const {
  MSVertexColumnBuilder builder(label_);
  builder.reserve(offsets.size());
  if (is_optional_) {
    for (auto offset : offsets) {
      auto v = vertices_[offset];
      if (v == std::numeric_limits<vid_t>::max()) {
        builder.push_back_null();
      } else {
        builder.push_back_opt(v);
      }
    }
  } else {
    for (auto offset : offsets) {
      builder.push_back_opt(vertices_[offset]);
    }
  }
  return builder.finish();
}

std::shared_ptr<IContextColumn> SLVertexColumn::optional_shuffle(
    const std::vector<size_t>& offsets) const {
  MSVertexColumnBuilder builder(label_);
  builder.reserve(offsets.size());
  for (auto offset : offsets) {
    if (offset == std::numeric_limits<size_t>::max()) {
      builder.push_back_null();
    } else if (vertices_[offset] == std::numeric_limits<vid_t>::max()) {
      builder.push_back_null();
    } else {
      builder.push_back_opt(vertices_[offset]);
    }
  }
  return builder.finish();
}

void SLVertexColumn::generate_dedup_offset(std::vector<size_t>& offsets) const {
  offsets.clear();

  std::vector<bool> bitset;
  size_t vnum = vertices_.size();
  bitset.resize(vnum);

  bool flag = false;
  size_t idx = 0;
  for (size_t i = 0; i < vnum; ++i) {
    vid_t v = vertices_[i];
    if (v == std::numeric_limits<vid_t>::max()) {
      if (!flag) {
        flag = true;
        idx = i;
      }
    } else {
      if (bitset.size() <= v) {
        bitset.resize(v + 1);
        bitset[v] = true;

        offsets.push_back(i);
      } else {
        if (!bitset[v]) {
          offsets.push_back(i);
          bitset[v] = true;
        }
      }
    }
  }
  if (flag) {
    offsets.push_back(idx);
  }
}

std::pair<std::shared_ptr<IContextColumn>, std::vector<std::vector<size_t>>>
SLVertexColumn::generate_aggregate_offset() const {
  std::vector<std::vector<size_t>> offsets;
  MSVertexColumnBuilder builder(label_);
  phmap::flat_hash_map<vid_t, size_t> vertex_to_offset;
  size_t idx = 0;
  for (auto v : vertices_) {
    auto iter = vertex_to_offset.find(v);
    if (iter == vertex_to_offset.end()) {
      builder.push_back_opt(v);
      vertex_to_offset.emplace(v, offsets.size());
      std::vector<size_t> tmp;
      tmp.push_back(idx);
      offsets.emplace_back(std::move(tmp));
    } else {
      offsets[iter->second].push_back(idx);
    }
    ++idx;
  }

  return std::make_pair(builder.finish(), std::move(offsets));
}

std::shared_ptr<IContextColumn> SLVertexColumn::union_col(
    std::shared_ptr<IContextColumn> other) const {
  CHECK(other->column_type() == ContextColumnType::kVertex);
  const IVertexColumn& vertex_column =
      *std::dynamic_pointer_cast<IVertexColumn>(other);
  if (vertex_column.vertex_column_type() == VertexColumnType::kSingle) {
    const SLVertexColumn& col =
        dynamic_cast<const SLVertexColumn&>(vertex_column);
    if (label() == col.label()) {
      MSVertexColumnBuilder builder(label());
      if (is_optional_ || other->is_optional()) {
        for (auto v : vertices_) {
          if (v == std::numeric_limits<vid_t>::max()) {
            builder.push_back_opt(v);
          } else {
            builder.push_back_null();
          }
        }
        for (auto v : col.vertices_) {
          if (v != std::numeric_limits<vid_t>::max()) {
            builder.push_back_opt(v);
          } else {
            builder.push_back_null();
          }
        }
      } else {
        for (auto v : vertices_) {
          builder.push_back_opt(v);
        }
        for (auto v : col.vertices_) {
          builder.push_back_opt(v);
        }
      }
      return builder.finish();
    }
  }
  auto col = dynamic_cast<const IVertexColumn*>(other.get());
  std::set<label_t> labels_set = col->get_labels_set();
  labels_set.insert(label_);
  MLVertexColumnBuilderOpt builder(labels_set);
  for (auto v : vertices_) {
    builder.push_back_vertex({label_, v});
  }
  for (size_t i = 0; i < col->size(); ++i) {
    builder.push_back_vertex(col->get_vertex(i));
  }
  return builder.finish();
}

std::shared_ptr<IContextColumn> MSVertexColumn::shuffle(
    const std::vector<size_t>& offsets) const {
  MLVertexColumnBuilderOpt builder(this->get_labels_set());
  builder.reserve(offsets.size());
  for (auto offset : offsets) {
    auto v = get_vertex(offset);
    if (v.vid_ != std::numeric_limits<vid_t>::max()) {
      builder.push_back_vertex(v);
    } else {
      builder.push_back_null();
    }
  }
  return builder.finish();
}

std::shared_ptr<IContextColumn> MSVertexColumn::optional_shuffle(
    const std::vector<size_t>& offsets) const {
  MLVertexColumnBuilderOpt builder(this->get_labels_set());
  builder.reserve(offsets.size());
  for (auto offset : offsets) {
    if (offset == std::numeric_limits<size_t>::max()) {
      builder.push_back_null();
    } else {
      auto v = get_vertex(offset);
      if (v.vid_ != std::numeric_limits<vid_t>::max()) {
        builder.push_back_vertex(v);
      } else {
        builder.push_back_null();
      }
    }
  }
  return builder.finish();
}

std::shared_ptr<IContextColumn> MSVertexColumnBuilder::finish() {
  if (!cur_list_.empty()) {
    vertices_.emplace_back(cur_label_, std::move(cur_list_));
    cur_list_.clear();
  }
  if (vertices_.empty()) {
    auto ret = std::make_shared<SLVertexColumn>(cur_label_);
    return ret;
  } else if (vertices_.size() == 1) {
    auto ret = std::make_shared<SLVertexColumn>(vertices_[0].first);
    ret->vertices_.swap(vertices_[0].second);
    ret->is_optional_ = is_optional_;
    return ret;
  } else {
    auto ret = std::make_shared<MSVertexColumn>();
    auto& label_set = ret->labels_;
    for (auto& pair : vertices_) {
      label_set.insert(pair.first);
    }
    ret->vertices_.swap(vertices_);
    ret->is_optional_ = is_optional_;
    return ret;
  }
}

std::shared_ptr<IContextColumn> MLVertexColumn::shuffle(
    const std::vector<size_t>& offsets) const {
  MLVertexColumnBuilderOpt builder(this->get_labels_set());
  builder.reserve(offsets.size());
  for (auto offset : offsets) {
    auto& v = vertices_[offset];
    if (v.vid_ != std::numeric_limits<vid_t>::max()) {
      builder.push_back_vertex(v);
    } else {
      builder.push_back_null();
    }
  }
  return builder.finish();
}

std::shared_ptr<IContextColumn> MLVertexColumn::optional_shuffle(
    const std::vector<size_t>& offsets) const {
  MLVertexColumnBuilderOpt builder(this->get_labels_set());
  builder.reserve(offsets.size());
  for (auto offset : offsets) {
    if (offset == std::numeric_limits<size_t>::max()) {
      builder.push_back_null();
    } else {
      auto& v = vertices_[offset];
      if (v.vid_ != std::numeric_limits<vid_t>::max()) {
        builder.push_back_vertex(v);
      } else {
        builder.push_back_null();
      }
    }
  }
  return builder.finish();
}

void MLVertexColumn::generate_dedup_offset(std::vector<size_t>& offsets) const {
  offsets.clear();
  std::set<VertexRecord> vset;
  size_t n = vertices_.size();
  for (size_t i = 0; i != n; ++i) {
    auto cur = vertices_[i];
    if (vset.find(cur) == vset.end()) {
      offsets.push_back(i);
      vset.insert(cur);
    }
  }
}

std::shared_ptr<IContextColumn> MLVertexColumnBuilder::finish() {
  auto ret = std::make_shared<MLVertexColumn>();
  ret->vertices_.swap(vertices_);
  ret->labels_.swap(labels_);
  ret->is_optional_ = is_optional_;
  return ret;
}

}  // namespace execution

}  // namespace neug
