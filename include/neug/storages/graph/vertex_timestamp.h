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

#include <algorithm>
#include <atomic>
#include <cassert>
#include <memory>
#include <set>

#include "neug/utils/likely.h"
#include "neug/utils/property/types.h"

namespace neug {

class VertexTimestamp {
 public:
  static constexpr timestamp_t DELETED_TIMESTAMP =
      std::numeric_limits<timestamp_t>::max();
  VertexTimestamp() : init_vertex_num_(0), max_vertex_num_(0) {}
  ~VertexTimestamp() { Reset(); }
  VertexTimestamp(VertexTimestamp&& other)
      : init_vertex_num_(other.init_vertex_num_),
        inserted_vertices_(std::move(other.inserted_vertices_)),
        max_vertex_num_(other.max_vertex_num_),
        removed_vertices_(std::move(other.removed_vertices_)) {}

  // TODO(zhanglei): VertexTimestamp doesn't necessarily need open from file.
  // Implement the compaction logic
  void Open(const std::string& tracker_file_prefix);

  void Dump(const std::string& tracker_file_prefix);

  void Init(vid_t init_vertex_num, vid_t max_vertex_num);

  void Reserve(size_t new_size);

  void Swap(VertexTimestamp& other);

  void Reset();

  void Clear();

  inline void InsertVertex(vid_t v, timestamp_t ts) {
    if (v < init_vertex_num_) {
      if (ts == 0) {
        return;
      }
      assert(ts != DELETED_TIMESTAMP);
      auto new_cap = max_vertex_num_ - v;
      resize_inserted_vertices(new_cap, false);
      init_vertex_num_ = v;
    }

    assert(v >= init_vertex_num_ && v < max_vertex_num_);
    timestamp_t expected = DELETED_TIMESTAMP;
    if (!inserted_vertices_[v - init_vertex_num_].compare_exchange_weak(
            expected, ts)) {
      THROW_INTERNAL_EXCEPTION("Vertex " + std::to_string(v) +
                               " already exists: " + std::to_string(expected));
    }
  }

  inline bool IsVertexValid(vid_t v, timestamp_t ts) const {
    if (NEUG_UNLIKELY(v > max_vertex_num_)) {
      return false;
    } else if (v >= init_vertex_num_) {
      return inserted_vertices_[v - init_vertex_num_].load() <= ts;
    } else if (NEUG_UNLIKELY(removed_vertices_)) {
      return removed_vertices_->find(v) == removed_vertices_->end();
    }
    return true;
  }

  inline size_t ValidVertexNum(timestamp_t ts, size_t limit) const {
    limit = std::min(limit, (size_t) max_vertex_num_);
    size_t valid_num = std::min((size_t) init_vertex_num_, limit);
    if (removed_vertices_) {
      for (auto v : *removed_vertices_) {
        if (v < limit) {
          assert(valid_num > 0);
          valid_num--;
        }
      }
    }
    if (limit <= init_vertex_num_) {
      return valid_num;
    }
    assert(inserted_vertices_);
    limit -= init_vertex_num_;
    for (size_t i = 0; i < limit; ++i) {
      if (inserted_vertices_[i].load() <= ts) {
        valid_num++;
      }
    }
    return valid_num;
  }

  timestamp_t RemoveVertex(vid_t v);

  void RevertRemoveVertex(vid_t v, timestamp_t old_ts);

  bool IsRemoved(vid_t v) const {
    if (v >= max_vertex_num_) {
      return false;
    }
    if (v >= init_vertex_num_) {
      return inserted_vertices_[v - init_vertex_num_].load() ==
             DELETED_TIMESTAMP;
    } else if (removed_vertices_) {
      return removed_vertices_->find(v) != removed_vertices_->end();
    }
    return false;
  }

  template <typename FUNC_T>
  void foreach_vertex(const FUNC_T& func, vid_t vnum, timestamp_t ts) const {
    vid_t limit = std::min(vnum, max_vertex_num_);
    if (removed_vertices_) {
      for (vid_t i = 0; i < init_vertex_num_; ++i) {
        if (removed_vertices_->count(i) <= 0) {
          func(i);
        }
      }
    } else {
      for (vid_t i = 0; i < init_vertex_num_; ++i) {
        func(i);
      }
    }
    if (limit <= init_vertex_num_) {
      return;
    }
    limit -= init_vertex_num_;
    for (vid_t i = 0; i < limit; ++i) {
      if (inserted_vertices_[i].load() != DELETED_TIMESTAMP &&
          inserted_vertices_[i].load() <= ts) {
        func(i + init_vertex_num_);
      }
    }
  }

  size_t Capacity() const { return max_vertex_num_; }

  // Compact the vertex timestamp storage
  void Compact();

  // Reset timestamps of all valid vertices to zero
  void ResetTimestamps();

  const vid_t InitVertexNum() const { return init_vertex_num_; }

 private:
  void load_meta(const std::string& meta_filename);
  void dump_meta(const std::string& meta_filename);
  void load_ts(const std::string& ts_filename);
  void dump_ts(const std::string& ts_filename);
  void resize_inserted_vertices(size_t new_size, bool keep_front = true);
  vid_t init_vertex_num_;

  std::unique_ptr<std::atomic<timestamp_t>[]> inserted_vertices_;

  vid_t max_vertex_num_;
  std::unique_ptr<std::set<vid_t>> removed_vertices_;
};

}  // namespace neug
