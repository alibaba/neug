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

#include <stdint.h>
#include <functional>
#include <ostream>
#include <string>
#include <tuple>
#include <utility>

#include "neug/utils/property/types.h"

namespace neug {

namespace execution {

int64_t encode_unique_vertex_id(label_t label_id, vid_t vid);
std::pair<label_t, vid_t> decode_unique_vertex_id(uint64_t unique_id);
uint32_t generate_edge_label_id(label_t src_label_id, label_t dst_label_id,
                                label_t edge_label_id);
int64_t encode_unique_edge_id(uint32_t label_id, vid_t src, vid_t dst);

std::tuple<label_t, label_t, label_t> decode_edge_label_id(
    uint32_t edge_label_id);
enum class Direction {
  kOut,
  kIn,
  kBoth,
};

enum class VOpt {
  kStart,
  kEnd,
  kOther,
  kBoth,
  kItself,
};

enum class JoinKind {
  kSemiJoin,
  kInnerJoin,
  kAntiJoin,
  kLeftOuterJoin,
  kTimesJoin,
};

enum class PathOpt {
  kArbitrary,
  kAnyShortest,
  kAllShortest,
  kTrail,
  kSimple,
  kAnyWeightedShortest,
};

enum class AggrKind {
  kSum,
  kMin,
  kMax,
  kCount,
  kCountDistinct,
  kToSet,
  kFirst,
  kToList,
  kAvg,
};

struct LabelTriplet {
  LabelTriplet() = default;
  LabelTriplet(label_t src, label_t dst, label_t edge)
      : src_label(src), dst_label(dst), edge_label(edge) {}

  std::string to_string() const {
    return "(" + std::to_string(static_cast<int>(src_label)) + "-" +
           std::to_string(static_cast<int>(edge_label)) + "-" +
           std::to_string(static_cast<int>(dst_label)) + ")";
  }

  bool operator==(const LabelTriplet& rhs) const {
    return src_label == rhs.src_label && dst_label == rhs.dst_label &&
           edge_label == rhs.edge_label;
  }

  bool operator<(const LabelTriplet& rhs) const {
    if (src_label != rhs.src_label) {
      return src_label < rhs.src_label;
    }
    if (dst_label != rhs.dst_label) {
      return dst_label < rhs.dst_label;
    }
    return edge_label < rhs.edge_label;
  }

  label_t src_label;
  label_t dst_label;
  label_t edge_label;
};

class VertexRecord {
 public:
  VertexRecord() = default;
  VertexRecord(label_t label, vid_t vid) : label_(label), vid_(vid) {}

  bool operator<(const VertexRecord& v) const {
    if (label_ == v.label_) {
      return vid_ < v.vid_;
    } else {
      return label_ < v.label_;
    }
  }
  bool operator==(const VertexRecord& v) const {
    return label_ == v.label_ && vid_ == v.vid_;
  }
  std::string to_string() const;

  label_t label() const { return label_; }
  vid_t vid() const { return vid_; }
  label_t label_;
  vid_t vid_;
};

class EdgeRecord {
 public:
  bool operator<(const EdgeRecord& e) const {
    return std::tie(src, dst, label) < std::tie(e.src, e.dst, e.label);
  }
  bool operator==(const EdgeRecord& e) const {
    return std::tie(src, dst, label) == std::tie(e.src, e.dst, e.label);
  }

  VertexRecord start_node() const { return VertexRecord(label.src_label, src); }
  VertexRecord end_node() const { return VertexRecord(label.dst_label, dst); }

  std::string to_string() const;

  LabelTriplet label;
  vid_t src, dst;
  const void* prop;
  Direction dir;
};

struct PathImpl;
struct Path {
 public:
  Path() : impl_(nullptr) {}

  Path(std::shared_ptr<PathImpl> impl) : impl_(impl) {}

  explicit Path(label_t v_label, vid_t vid);

  explicit Path(
      label_t label, label_t e_label, const std::vector<vid_t>& vids,
      const std::vector<std::pair<Direction, const void*>>& edge_datas);

  explicit Path(const std::vector<std::tuple<label_t, Direction, const void*>>&
                    edge_datas,
                const std::vector<VertexRecord>& path);

  Path expand(label_t edge_label, label_t label, vid_t v, Direction dir,
              const void* payload) const;

  int32_t length() const;

  std::vector<VertexRecord> nodes() const;

  std::vector<EdgeRecord> relationships() const;

  bool operator<(const Path& p) const;

  bool operator==(const Path& p) const;

  double get_weight() const;
  void set_weight(double weight);

  VertexRecord end_node() const;

  bool is_null() const { return impl_ == nullptr; }

 private:
  std::shared_ptr<PathImpl> impl_;
};

}  // namespace execution

}  // namespace neug

namespace std {

template <typename T>
static inline void hash_combine(std::size_t& seed, const T& val) {
  std::hash<T> hasher;
  seed ^= hasher(val) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
}
template <>
struct hash<neug::execution::VertexRecord> {
  // Hash combine functions copied from Boost.ContainerHash
  // https://github.com/boostorg/container_hash/blob/171c012d4723c5e93cc7cffe42919afdf8b27dfa/include/boost/container_hash/hash.hpp#L311
  // that is based on Peter Dimov's proposal
  // http://www.open-std.org/JTC1/SC22/WG21/docs/papers/2005/n1756.pdf
  // issue 6.18.

  size_t operator()(const neug::execution::VertexRecord& record) const {
    std::size_t seed = 0;
    hash_combine(seed, record.vid_);
    hash_combine(seed, record.label_);
    return seed;
  }

  std::size_t operator()(
      const std::pair<neug::execution::VertexRecord,
                      neug::execution::VertexRecord>& p) const {
    std::size_t seed = 0;
    hash_combine(seed, p.first.vid_);
    hash_combine(seed, p.first.label_);
    hash_combine(seed, p.second.vid_);
    hash_combine(seed, p.second.label_);
    return seed;
  }
};

template <>
struct hash<neug::DateTime> {
  size_t operator()(const neug::DateTime& date) const {
    return std::hash<int64_t>()(date.milli_second);
  }
};

template <>
struct hash<neug::execution::LabelTriplet> {
  size_t operator()(const neug::execution::LabelTriplet& lt) const {
    size_t seed = 0;
    hash_combine(seed, lt.src_label);
    hash_combine(seed, lt.dst_label);
    hash_combine(seed, lt.edge_label);
    return seed;
  }
};

};  // namespace std
