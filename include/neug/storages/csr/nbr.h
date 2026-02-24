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

#include "neug/utils/property/column.h"
#include "neug/utils/property/table.h"
#include "neug/utils/property/types.h"

namespace neug {

template <typename EDATA_T>
struct ImmutableNbr {
  ImmutableNbr() = default;
  ImmutableNbr(const ImmutableNbr& rhs)
      : neighbor(rhs.neighbor), data(rhs.data) {}
  ~ImmutableNbr() = default;

  ImmutableNbr& operator=(const ImmutableNbr& rhs) {
    neighbor = rhs.neighbor;
    data = rhs.data;
    return *this;
  }

  const EDATA_T& get_data() const { return data; }
  vid_t get_neighbor() const { return neighbor; }

  void set_data(const EDATA_T& val) { data = val; }
  void set_neighbor(vid_t neighbor) { this->neighbor = neighbor; }

  bool exists() const { return neighbor != std::numeric_limits<vid_t>::max(); }

  vid_t neighbor;
  EDATA_T data;
};

template <>
struct ImmutableNbr<EmptyType> {
  ImmutableNbr() = default;
  ImmutableNbr(const ImmutableNbr& rhs) : neighbor(rhs.neighbor) {}
  ~ImmutableNbr() = default;

  ImmutableNbr& operator=(const ImmutableNbr& rhs) {
    neighbor = rhs.neighbor;
    return *this;
  }

  void set_data(const EmptyType&) {}
  void set_neighbor(vid_t neighbor) { this->neighbor = neighbor; }
  const EmptyType& get_data() const { return data; }
  vid_t get_neighbor() const { return neighbor; }

  union {
    vid_t neighbor;
    EmptyType data;
  };
};

template <typename EDATA_T>
struct MutableNbr {
  MutableNbr() = default;
  MutableNbr(const MutableNbr& rhs)
      : neighbor(rhs.neighbor),
        timestamp(rhs.timestamp.load()),
        data(rhs.data) {}
  ~MutableNbr() = default;

  MutableNbr& operator=(const MutableNbr& rhs) {
    neighbor = rhs.neighbor;
    timestamp.store(rhs.timestamp.load());
    data = rhs.data;
    return *this;
  }

  const EDATA_T& get_data() const { return data; }
  vid_t get_neighbor() const { return neighbor; }
  timestamp_t get_timestamp() const { return timestamp.load(); }

  void set_data(const EDATA_T& val, timestamp_t ts) {
    data = val;
    timestamp.store(ts);
  }
  void set_neighbor(vid_t neighbor) { this->neighbor = neighbor; }
  void set_timestamp(timestamp_t ts) { timestamp.store(ts); }

  vid_t neighbor;
  std::atomic<timestamp_t> timestamp;
  EDATA_T data;
};

template <>
struct MutableNbr<EmptyType> {
  MutableNbr() : timestamp(std::numeric_limits<timestamp_t>::max()) {}
  MutableNbr(const MutableNbr& rhs)
      : neighbor(rhs.neighbor), timestamp(rhs.timestamp.load()) {}
  ~MutableNbr() = default;

  MutableNbr& operator=(const MutableNbr& rhs) {
    neighbor = rhs.neighbor;
    timestamp.store(rhs.timestamp.load());
    return *this;
  }

  void set_data(const EmptyType&, timestamp_t ts) { timestamp.store(ts); }
  void set_neighbor(vid_t neighbor) { this->neighbor = neighbor; }
  void set_timestamp(timestamp_t ts) { timestamp.store(ts); }
  const EmptyType& get_data() const { return data; }
  vid_t get_neighbor() const { return neighbor; }
  timestamp_t get_timestamp() const { return timestamp.load(); }
  vid_t neighbor;
  union {
    std::atomic<timestamp_t> timestamp;
    EmptyType data;
  };
};

}  // namespace neug
