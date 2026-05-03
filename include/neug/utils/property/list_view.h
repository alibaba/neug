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

#include <cassert>
#include <cstdint>
#include <cstring>
#include <string>
#include <string_view>
#include <type_traits>
#include <vector>

#include "neug/common/extra_type_info.h"
#include "neug/common/types.h"

namespace neug {

// ---------------------------------------------------------------------------
// Binary encoding
// ---------------------------------------------------------------------------
//
//  POD list  (child type is fixed-size)
//  ┌─────────────────────────┬──────────────────────────────────────────┐
//  │ count : uint32_t        │ T[0]   T[1]  …  T[count-1]              │
//  └─────────────────────────┴──────────────────────────────────────────┘
//  Total bytes: 4 + count * sizeof(T)
//
//  Non-POD list  (varchar or nested list — variable-width elements)
//  ┌──────────┬────────────────────────────────────────┬──────────────────────┐
//  │ count    │ off[0]  off[1]  …  off[count-1]  off[count] (sentinel)        │
//  │ uint32_t │ uint32_t × (count+1)                   │ data[0] data[1] … │
//  └──────────┴────────────────────────────────────────┴──────────────────────┘
//  off[] are relative to the start of the data region (first byte after the
//  last offset entry).  The sentinel off[count] equals the total data size.
//  Length of element i = off[i+1] - off[i].

// ---------------------------------------------------------------------------
// ListView
// ---------------------------------------------------------------------------
// Zero-copy, read-only view over a serialized list stored in a contiguous
// string_view buffer.  The caller must ensure that the underlying memory
// outlives all ListView instances derived from it.
//
// type_ holds the *enclosing* List<X> DataType; the child element type is
// obtained via  ListType::GetChildType(type_).
struct ListView {
  using length_t = uint32_t;
  using offset_t = uint32_t;

  ListView(const DataType& type, std::string_view data)
      : type_(type), data_(data) {}
  ~ListView() = default;

  // Number of elements stored in this list.
  size_t size() const {
    if (data_.size() < sizeof(uint32_t)) {
      return 0;
    }
    return static_cast<size_t>(
        *reinterpret_cast<const uint32_t*>(data_.data()));
  }

  // Direct element access for POD child types.
  // The template argument T must match the actual element C++ type.
  template <typename T>
  const T& GetElem(size_t idx) const {
    assert(is_pod_type(ListType::GetChildType(type_).id()));
    assert(idx < size());
    return reinterpret_cast<const T*>(data_.data() + sizeof(uint32_t))[idx];
  }

  // Element access for varchar (string) child type.
  std::string_view GetChildStringView(size_t idx) const {
    return child_span(idx);
  }

  // Element access for a nested List child type.
  // The returned ListView borrows from the same underlying buffer.
  ListView GetChildListView(size_t idx) const {
    const DataType& child = ListType::GetChildType(type_);
    return ListView(child, child_span(idx));
  }

  const DataType& type_;
  std::string_view data_;

 private:
  // Returns the raw byte span for the variable-length element at index idx.
  // Must only be called for non-POD encodings.
  std::string_view child_span(size_t idx) const {
    assert(!data_.empty());
    const uint32_t count = *reinterpret_cast<const uint32_t*>(data_.data());
    assert(idx < static_cast<size_t>(count));
    const uint32_t* offsets =
        reinterpret_cast<const uint32_t*>(data_.data() + sizeof(uint32_t));
    // data region starts right after the (count+1) offsets
    const char* data_start =
        data_.data() + sizeof(uint32_t) + (count + 1) * sizeof(uint32_t);
    return std::string_view(data_start + offsets[idx],
                            offsets[idx + 1] - offsets[idx]);
  }
};

// ---------------------------------------------------------------------------
// ListViewBuilder
// ---------------------------------------------------------------------------
// Assembles a binary blob for a single list value to be stored in a
// ListColumn.  The builder accumulates elements and produces an owned
// std::string via finish_pod<T>() or finish_varlen().
//
// The two finish methods correspond to the two encodings above.  Callers are
// expected to choose the right one based on is_pod_type(child_type.id()):
//
//   if (is_pod_type(child_type.id())) {
//     ListViewBuilder b;
//     for (auto v : values) b.append_pod(v);
//     std::string blob = b.finish_pod<int32_t>();
//   } else {
//     ListViewBuilder b;
//     for (auto sv : strings) b.append_blob(sv);
//     std::string blob = b.finish_varlen();
//   }
class ListViewBuilder {
 public:
  ListViewBuilder() = default;

  // Append a single POD element (trivially copyable, fixed size).
  template <typename T>
  void append_pod(const T& val) {
    static_assert(std::is_trivially_copyable_v<T>,
                  "append_pod requires a trivially copyable type");
    const char* bytes = reinterpret_cast<const char*>(&val);
    pod_data_.insert(pod_data_.end(), bytes, bytes + sizeof(T));
    count_++;
  }

  // Append a variable-length element: either a raw string or a nested list
  // blob (the result of another ListViewBuilder::finish_*() call).
  void append_blob(std::string_view sv) {
    var_offsets_.push_back(static_cast<uint32_t>(var_data_.size()));
    var_data_.insert(var_data_.end(), sv.data(), sv.data() + sv.size());
    count_++;
  }

  // Produce the final blob for POD lists.
  // Layout: [count: uint32][T[0]]...[T[count-1]]
  template <typename T>
  std::string finish_pod() const {
    std::string result(sizeof(uint32_t) + pod_data_.size(), '\0');
    char* p = result.data();
    *reinterpret_cast<uint32_t*>(p) = count_;
    p += sizeof(uint32_t);
    if (!pod_data_.empty()) {
      std::memcpy(p, pod_data_.data(), pod_data_.size());
    }
    return result;
  }

  // Produce the final blob for non-POD lists.
  // Layout: [count: uint32][off[0]: uint32]...[off[count]: uint32][data...]
  std::string finish_varlen() const {
    const uint32_t sentinel = static_cast<uint32_t>(var_data_.size());
    const size_t offset_bytes = (count_ + 1) * sizeof(uint32_t);
    std::string result(sizeof(uint32_t) + offset_bytes + var_data_.size(),
                       '\0');
    char* p = result.data();
    *reinterpret_cast<uint32_t*>(p) = count_;
    p += sizeof(uint32_t);
    if (count_ > 0) {
      std::memcpy(p, var_offsets_.data(), count_ * sizeof(uint32_t));
    }
    p += count_ * sizeof(uint32_t);
    *reinterpret_cast<uint32_t*>(p) = sentinel;
    p += sizeof(uint32_t);
    if (!var_data_.empty()) {
      std::memcpy(p, var_data_.data(), var_data_.size());
    }
    return result;
  }

  // Reset for reuse.
  void reset() {
    count_ = 0;
    pod_data_.clear();
    var_offsets_.clear();
    var_data_.clear();
  }

  uint32_t count() const { return count_; }

 private:
  uint32_t count_ = 0;

  // Accumulates raw bytes for POD lists.
  std::vector<char> pod_data_;

  // Accumulate offset + data for non-POD lists.
  std::vector<uint32_t> var_offsets_;
  std::vector<char> var_data_;
};

}  // namespace neug
