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

#ifndef INCLUDE_NEUG_TRANSACTION_TRANSACTION_UTILS_H_
#define INCLUDE_NEUG_TRANSACTION_TRANSACTION_UTILS_H_

#include "glog/logging.h"
#include "libgrape-lite/grape/serialization/in_archive.h"
#include "libgrape-lite/grape/serialization/out_archive.h"
#include "neug/storages/graph/property_graph.h"
#include "neug/utils/likely.h"
#include "neug/utils/property/types.h"

namespace gs {

enum class OpType : uint8_t {
  kCreateVertexType = 0,
  kCreateEdgeType = 1,
  kInsertVertex = 2,
  kInsertEdge = 3,
  kUpdateVertexProp = 4,
  kUpdateEdgeProp = 5,
  kRemoveVertex = 6,
  kRemoveEdge = 7,
  kAddVertexProp = 8,
  kAddEdgeProp = 9,
  kRenameVertexProp = 10,
  kRenameEdgeProp = 11,
  kDeleteVertexProp = 12,
  kDeleteEdgeProp = 13,
  kDeleteVertexType = 14,
  kDeleteEdgeType = 15
};

class VertexSet {
 public:
  VertexSet(vid_t size, const mmap_array<timestamp_t>& vertex_ts,
            timestamp_t ts, bool vertex_table_modified)
      : size_(size),
        vertex_ts_(vertex_ts),
        ts_(ts),
        vertex_table_modifed_(vertex_table_modified) {}
  ~VertexSet() {}

  class iterator {
   public:
    iterator(vid_t v, const mmap_array<timestamp_t>& vertex_ts, timestamp_t ts,
             bool vertex_table_modified)
        : v_(v),
          vertex_ts_(vertex_ts),
          ts_(ts),
          vertex_table_modifed_(vertex_table_modified) {
      if (NEUG_UNLIKELY(vertex_table_modifed_)) {
        while (v_ < vertex_ts_.size() && vertex_ts_.get(v_) > ts_) {
          ++v_;
        }
      }
    }
    ~iterator() {}

    inline vid_t operator*() const { return v_; }

    inline iterator& operator++() {
      if (NEUG_UNLIKELY(vertex_table_modifed_)) {
        do {
          ++v_;
        } while (v_ < vertex_ts_.size() && vertex_ts_.get(v_) > ts_);
      } else {
        ++v_;
      }
      return *this;
    }

    inline bool operator==(const iterator& rhs) const { return v_ == rhs.v_; }

    inline bool operator!=(const iterator& rhs) const { return v_ != rhs.v_; }

   private:
    vid_t v_;
    const mmap_array<timestamp_t>& vertex_ts_;
    timestamp_t ts_;
    bool vertex_table_modifed_;
  };

  inline iterator begin() const {
    return iterator(0, vertex_ts_, ts_, vertex_table_modifed_);
  }
  inline iterator end() const {
    return iterator(size_, vertex_ts_, ts_, vertex_table_modifed_);
  }
  inline size_t size() const { return size_; }

 private:
  vid_t size_;
  const mmap_array<timestamp_t>& vertex_ts_;
  timestamp_t ts_;
  bool vertex_table_modifed_;
};

inline grape::InArchive& operator<<(grape::InArchive& in_archive,
                                    OpType& value) {
  in_archive << static_cast<uint8_t>(value);
  return in_archive;
}
inline grape::OutArchive& operator>>(grape::OutArchive& out_archive,
                                     OpType& value) {
  uint8_t op_type;
  out_archive >> op_type;
  value = static_cast<OpType>(op_type);
  return out_archive;
}

}  // namespace gs

#endif  // INCLUDE_NEUG_TRANSACTION_TRANSACTION_UTILS_H_
