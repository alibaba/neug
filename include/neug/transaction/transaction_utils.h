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
#include "neug/transaction/wal/wal.h"
#include "neug/utils/likely.h"
#include "neug/utils/property/types.h"

namespace gs {

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

inline void serialize_field(grape::InArchive& arc, const Any& prop) {
  if (prop.type == PropertyType::Bool()) {
    arc << prop.value.b;
  } else if (prop.type == PropertyType::Int32()) {
    arc << prop.value.i;
  } else if (prop.type == PropertyType::UInt32()) {
    arc << prop.value.ui;
  } else if (prop.type == PropertyType::Date()) {
    arc << prop.value.d.to_u32();
  } else if (prop.type == PropertyType::Timestamp()) {
    arc << prop.value.ts.milli_second;
  } else if (prop.type == PropertyType::DateTime()) {
    arc << prop.value.dt.milli_second;
  } else if (prop.type.type_enum == impl::PropertyTypeImpl::kString) {
    std::string_view s = *prop.value.s_ptr;
    arc << s;
  } else if (prop.type == PropertyType::StringView()) {
    arc << prop.value.s;
  } else if (prop.type == PropertyType::Int64()) {
    arc << prop.value.l;
  } else if (prop.type == PropertyType::UInt64()) {
    arc << prop.value.ul;
  } else if (prop.type == PropertyType::Double()) {
    arc << prop.value.db;
  } else if (prop.type == PropertyType::Float()) {
    arc << prop.value.f;
  } else if (prop.type == PropertyType::Empty()) {
  } else if (prop.type == PropertyType::Record()) {
    arc << prop.value.record.size();
    for (auto& field : prop.value.record) {
      serialize_field(arc, field);
    }
  } else {
    THROW_NOT_SUPPORTED_EXCEPTION(
        "Unexpected property type: " + prop.type.ToString() + ", " +
        std::to_string((int32_t) prop.type.type_enum));
  }
}

inline void deserialize_field(grape::OutArchive& arc, Any& prop) {
  if (prop.type == PropertyType::Bool()) {
    arc >> prop.value.b;
  } else if (prop.type == PropertyType::Int32()) {
    arc >> prop.value.i;
  } else if (prop.type == PropertyType::UInt32()) {
    arc >> prop.value.ui;
  } else if (prop.type == PropertyType::Date()) {
    uint32_t date_val;
    arc >> date_val;
    prop.value.d.from_u32(date_val);
  } else if (prop.type == PropertyType::Timestamp()) {
    int64_t ts_val;
    arc >> ts_val;
    prop.value.ts.milli_second = ts_val;
  } else if (prop.type == PropertyType::DateTime()) {
    int64_t dt_val;
    arc >> dt_val;
    prop.value.dt.milli_second = dt_val;
  } else if (prop.type == PropertyType::StringView()) {
    arc >> prop.value.s;
  } else if (prop.type == PropertyType::Int64()) {
    arc >> prop.value.l;
  } else if (prop.type == PropertyType::UInt64()) {
    arc >> prop.value.ul;
  } else if (prop.type == PropertyType::Double()) {
    arc >> prop.value.db;
  } else if (prop.type == PropertyType::Float()) {
    arc >> prop.value.f;
  } else if (prop.type == PropertyType::Empty()) {
  } else if (prop.type == PropertyType::Record()) {
    size_t len;
    arc >> len;
    Record r(len);
    for (size_t i = 0; i < r.len; ++i) {
      deserialize_field(arc, r.props[i]);
    }
    prop.set_record(r);

  } else {
    LOG(FATAL) << "Unexpected property type: "
               << static_cast<int>(prop.type.type_enum);
  }
}

inline label_t deserialize_oid(const PropertyGraph& graph,
                               grape::OutArchive& arc, Any& oid) {
  label_t label;
  arc >> label;
  oid.type = std::get<0>(graph.schema().get_vertex_primary_key(label).at(0));
  deserialize_field(arc, oid);
  return label;
}

}  // namespace gs

#endif  // INCLUDE_NEUG_TRANSACTION_TRANSACTION_UTILS_H_
