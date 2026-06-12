
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

#include "neug/execution/common/types/graph_types.h"

#include "neug/utils/property/types.h"

namespace neug {
namespace execution {
int64_t encode_unique_vertex_id(label_t label_id, vid_t vid) {
  // encode label_id and vid to a unique vid
  GlobalId global_id(label_id, vid);
  return global_id.global_id;
}

std::pair<label_t, vid_t> decode_unique_vertex_id(uint64_t unique_id) {
  return std::pair{GlobalId::get_label_id(unique_id),
                   GlobalId::get_vid(unique_id)};
}

uint32_t generate_edge_label_id(label_t src_label_id, label_t dst_label_id,
                                label_t edge_label_id) {
  uint32_t unique_edge_label_id = src_label_id;
  static constexpr int num_bits = sizeof(label_t) * 8;
  static_assert(num_bits * 3 <= sizeof(uint32_t) * 8,
                "label_t is too large to be encoded in 32 bits");
  unique_edge_label_id = unique_edge_label_id << num_bits;
  unique_edge_label_id = unique_edge_label_id | dst_label_id;
  unique_edge_label_id = unique_edge_label_id << num_bits;
  unique_edge_label_id = unique_edge_label_id | edge_label_id;
  return unique_edge_label_id;
}

std::tuple<label_t, label_t, label_t> decode_edge_label_id(
    uint32_t edge_label_id) {
  static constexpr int num_bits = sizeof(label_t) * 8;
  static_assert(num_bits * 3 <= sizeof(uint32_t) * 8,
                "label_t is too large to be encoded in 32 bits");
  auto mask = (1 << num_bits) - 1;
  label_t edge_label = edge_label_id & mask;
  edge_label_id = edge_label_id >> num_bits;
  label_t dst_label = edge_label_id & mask;
  edge_label_id = edge_label_id >> num_bits;
  label_t src_label = edge_label_id & mask;
  return std::make_tuple(src_label, dst_label, edge_label);
}

int64_t encode_unique_edge_id(uint32_t label_id, vid_t src, vid_t dst) {
  // We assume label_id is only used by 24 bits.
  int64_t unique_edge_id = label_id;
  static constexpr int num_bits = sizeof(int64_t) * 8 - sizeof(uint32_t) * 8;
  unique_edge_id = unique_edge_id << num_bits;
  // bitmask for top 44 bits set to 1
  int64_t bitmask = 0xFFFFFFFFFF000000;
  // 24 bit | 20 bit | 20 bit
  if (bitmask & (int64_t) src || bitmask & (int64_t) dst) {
    LOG(ERROR) << "src or dst is too large to be encoded in 20 bits: " << src
               << " " << dst;
  }
  unique_edge_id = unique_edge_id | (src << 20);
  unique_edge_id = unique_edge_id | dst;
  return unique_edge_id;
}

std::string VertexRecord::to_string() const {
  return "(" + std::to_string(static_cast<int>(label_)) + ", " +
         std::to_string(vid_) + ")";
}

std::string EdgeRecord::to_string() const {
  return "(" + std::to_string(static_cast<int>(label.src_label)) + ", " +
         std::to_string(src) + ") -[" +
         std::to_string(static_cast<int>(label.edge_label)) + "]-> (" +
         std::to_string(static_cast<int>(label.dst_label)) + ", " +
         std::to_string(dst) + ")";
}
struct PathImpl {
  bool operator==(const PathImpl& p) const {
    if ((p.prev_ == nullptr && prev_) || (p.prev_ && prev_ == nullptr)) {
      return false;
    }
    if (v_label_ != p.v_label_ || e_label_ != p.e_label_ ||
        direction_ != p.direction_ || vid_ != p.vid_) {
      return false;
    }
    if (prev_ && p.prev_) {
      return *prev_ == *(p.prev_);
    }
    return true;
  }
  label_t v_label_;
  label_t e_label_;
  Direction direction_;
  vid_t vid_;
  const void* payload_;
  std::shared_ptr<PathImpl> prev_;
  double weight_ = 0.0;
};

Path::Path(label_t v_label, vid_t vid) : impl_(std::make_shared<PathImpl>()) {
  impl_->v_label_ = v_label;
  impl_->vid_ = vid;
  impl_->prev_ = nullptr;
}

Path::Path(label_t label, label_t e_label, const std::vector<vid_t>& vids,
           const std::vector<std::pair<Direction, const void*>>& edge_datas) {
  auto cur = std::make_shared<PathImpl>();
  cur->v_label_ = label;
  cur->e_label_ = std::numeric_limits<label_t>::max();
  cur->vid_ = vids[0];
  cur->prev_ = nullptr;
  impl_ = cur;
  for (size_t i = 1; i < vids.size(); ++i) {
    auto next = std::make_shared<PathImpl>();
    next->v_label_ = label;
    next->e_label_ = e_label;
    next->direction_ = edge_datas[i - 1].first;
    next->payload_ = edge_datas[i - 1].second;
    next->vid_ = vids[i];
    next->prev_ = cur;
    cur = next;
    impl_ = next;
  }
}

Path::Path(
    const std::vector<std::tuple<label_t, Direction, const void*>>& edge_datas,
    const std::vector<VertexRecord>& path) {
  auto cur = std::make_shared<PathImpl>();
  cur->v_label_ = path[0].label_;
  cur->e_label_ = std::numeric_limits<label_t>::max();
  cur->vid_ = path[0].vid_;
  cur->prev_ = nullptr;
  impl_ = cur;
  for (size_t i = 1; i < path.size(); ++i) {
    auto next = std::make_shared<PathImpl>();
    next->v_label_ = path[i].label_;
    next->e_label_ = std::get<0>(edge_datas[i - 1]);
    next->direction_ = std::get<1>(edge_datas[i - 1]);
    next->payload_ = std::get<2>(edge_datas[i - 1]);
    next->vid_ = path[i].vid_;
    next->prev_ = cur;
    cur = next;
    impl_ = next;
  }
}

Path Path::expand(label_t edge_label, label_t label, vid_t v, Direction dir,
                  const void* payload) const {
  auto new_path = std::make_shared<PathImpl>();
  new_path->v_label_ = label;
  new_path->e_label_ = edge_label;
  new_path->vid_ = v;
  new_path->prev_ = impl_;
  new_path->direction_ = dir;
  new_path->payload_ = payload;
  return Path(new_path);
}

int32_t Path::length() const {
  int len = 0;
  auto cur = impl_;
  while (cur != nullptr) {
    ++len;
    cur = cur->prev_;
  }
  return len - 1;
}

std::vector<VertexRecord> Path::nodes() const {
  std::vector<VertexRecord> vertices;
  auto cur = impl_;
  while (cur != nullptr) {
    vertices.emplace_back(cur->v_label_, cur->vid_);
    cur = cur->prev_;
  }
  std::reverse(vertices.begin(), vertices.end());
  return vertices;
}

std::vector<EdgeRecord> Path::relationships() const {
  std::vector<EdgeRecord> relations;
  auto cur = impl_;
  std::vector<std::shared_ptr<PathImpl>> nodes;
  while (cur != nullptr) {
    nodes.push_back(cur);
    cur = cur->prev_;
  }
  std::reverse(nodes.begin(), nodes.end());
  for (size_t i = 0; i + 1 < nodes.size(); ++i) {
    EdgeRecord r;
    r.dir = nodes[i + 1]->direction_;
    if (r.dir == Direction::kOut) {
      r.label = {nodes[i]->v_label_, nodes[i + 1]->v_label_,
                 nodes[i + 1]->e_label_};
      r.src = nodes[i]->vid_;
      r.dst = nodes[i + 1]->vid_;

    } else {
      r.label = {nodes[i + 1]->v_label_, nodes[i]->v_label_,
                 nodes[i + 1]->e_label_};
      r.src = nodes[i + 1]->vid_;
      r.dst = nodes[i]->vid_;
    }
    r.prop = nodes[i + 1]->payload_;
    relations.push_back(r);
  }
  return relations;
}

bool Path::operator<(const Path& p) const {
  const auto& nodes = this->nodes();
  const auto& p_nodes = p.nodes();
  size_t min_len = std::min(nodes.size(), p_nodes.size());
  for (size_t i = 0; i < min_len; ++i) {
    if (nodes[i] < p_nodes[i]) {
      return true;
    } else if (p_nodes[i] < nodes[i]) {
      return false;
    }
  }
  return nodes.size() < p_nodes.size();
}

bool Path::operator==(const Path& p) const {
  return *(this->impl_) == *(p.impl_);
}

double Path::get_weight() const { return impl_->weight_; }

void Path::set_weight(double weight) { impl_->weight_ = weight; }

VertexRecord Path::end_node() const {
  return VertexRecord{impl_->v_label_, impl_->vid_};
}

}  // namespace execution
}  // namespace neug
