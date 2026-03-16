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

#include <glog/logging.h>

#include <stddef.h>
#include <stdio.h>
#include <string.h>

#include <algorithm>
#include <atomic>
#include <limits>
#include <memory>
#include <ostream>
#include <set>
#include <string>
#include <vector>

#include "neug/storages/csr/csr_base.h"
#include "neug/storages/csr/generic_view.h"
#include "neug/storages/csr/nbr.h"
#include "neug/utils/allocators.h"
#include "neug/utils/mmap_array.h"
#include "neug/utils/property/types.h"
#include "neug/utils/spinlock.h"

namespace neug {

void read_file(const std::string& filename, void* buffer, size_t size,
               size_t num);

void write_file(const std::string& filename, const void* buffer, size_t size,
                size_t num);

template <typename EDATA_T>
class MutableCsr : public TypedCsrBase<EDATA_T> {
 public:
  using data_t = EDATA_T;
  using nbr_t = MutableNbr<EDATA_T>;

  MutableCsr() : locks_(nullptr) {}
  ~MutableCsr() {
    if (locks_ != nullptr) {
      delete[] locks_;
    }
  }

  CsrType csr_type() const override { return CsrType::kMutable; }

  GenericView get_generic_view(timestamp_t ts) const override {
    NbrIterConfig cfg;
    cfg.stride = sizeof(nbr_t);
    cfg.ts_offset = offsetof(nbr_t, timestamp);
    cfg.data_offset = offsetof(nbr_t, data);
    return GenericView(reinterpret_cast<const char*>(adj_list_buffer_.data()),
                       reinterpret_cast<const int*>(adj_list_size_.data()), cfg,
                       ts, unsorted_since_);
  }

  timestamp_t unsorted_since() const override { return unsorted_since_; }

  size_t size() const override { return adj_list_size_.size(); }

  size_t edge_num() const override {
    size_t res = 0;
    for (size_t i = 0; i < adj_list_size_.size(); ++i) {
      auto begin = adj_list_buffer_[i];
      for (size_t j = 0; j < adj_list_size_[i].load(); ++j) {
        if (begin[j].timestamp.load() !=
            std::numeric_limits<timestamp_t>::max()) {
          res++;
        }
      }
    }
    return res;
  }

  void open(const std::string& name, const std::string& snapshot_dir,
            const std::string& work_dir) override;

  void open_in_memory(const std::string& prefix) override;

  void open_with_hugepages(const std::string& prefix) override;

  void dump(const std::string& name,
            const std::string& new_snapshot_dir) override;

  void reset_timestamp() override;

  void compact() override;

  void resize(vid_t vnum) override;

  size_t capacity() const override;

  void close() override;

  void batch_sort_by_edge_data(timestamp_t ts) override;

  void batch_delete_vertices(const std::set<vid_t>& src_set,
                             const std::set<vid_t>& dst_set) override;

  void batch_delete_edges(const std::vector<vid_t>& src_list,
                          const std::vector<vid_t>& dst_list) override;

  void batch_delete_edges(
      const std::vector<std::pair<vid_t, int32_t>>& edges) override;

  void delete_edge(vid_t src, int32_t offset, timestamp_t ts) override;

  void revert_delete_edge(vid_t src, vid_t nbr, int32_t offset,
                          timestamp_t ts) override;

  void batch_put_edges(const std::vector<vid_t>& src_list,
                       const std::vector<vid_t>& dst_list,
                       const std::vector<EDATA_T>& data_list,
                       timestamp_t ts = 0) override;

  int32_t put_edge(vid_t src, vid_t dst, const EDATA_T& data, timestamp_t ts,
                   Allocator& alloc) override {
    if (src >= adj_list_size_.size()) {
      THROW_INVALID_ARGUMENT_EXCEPTION(
          "Source vertex id out of range: " + std::to_string(src) +
          " >= " + std::to_string(adj_list_size_.size()));
    }
    locks_[src].lock();
    int sz = adj_list_size_[src];
    int cap = adj_list_capacity_[src];
    if (sz == cap) {
      cap += (cap >> 1);
      cap = std::max(cap, 8);
      nbr_t* new_buffer =
          static_cast<nbr_t*>(alloc.allocate(cap * sizeof(nbr_t)));
      if (sz > 0) {
        memcpy(new_buffer, adj_list_buffer_[src], sz * sizeof(nbr_t));
      }
      adj_list_buffer_[src] = new_buffer;
      adj_list_capacity_[src] = cap;
    }
    int32_t prev_size = adj_list_size_[src].fetch_add(1);
    auto& nbr = adj_list_buffer_[src][prev_size];
    nbr.neighbor = dst;
    nbr.data = data;
    nbr.timestamp.store(ts);
    locks_[src].unlock();
    return prev_size;
  }

  std::tuple<std::vector<vid_t>, std::vector<vid_t>> batch_export(
      std::shared_ptr<ColumnBase> prev_data_col) const override {
    std::vector<vid_t> src_list, dst_list;
    std::vector<EDATA_T> data_list;
    for (vid_t src = 0; src < adj_list_size_.size(); ++src) {
      for (int i = 0; i < adj_list_size_[src]; ++i) {
        if (adj_list_buffer_[src][i].timestamp.load() !=
            std::numeric_limits<timestamp_t>::max()) {
          src_list.push_back(src);
          dst_list.push_back(adj_list_buffer_[src][i].neighbor);
          data_list.push_back(adj_list_buffer_[src][i].data);
        }
      }
    }
    if (prev_data_col) {
      auto casted =
          std::dynamic_pointer_cast<TypedColumn<EDATA_T>>(prev_data_col);
      if (!casted) {
        THROW_INTERNAL_EXCEPTION(
            "prev_data_col cannot be casted to TypedColumn<EDATA_T>");
      }
      casted->resize(data_list.size());
      for (size_t i = 0; i < data_list.size(); ++i) {
        casted->set_value(i, data_list[i]);
      }
    }
    return std::make_tuple(std::move(src_list), std::move(dst_list));
  }

 private:
  void load_meta(const std::string& prefix);

  void dump_meta(const std::string& prefix) const;

  SpinLock* locks_;
  mmap_array<nbr_t*> adj_list_buffer_;
  mmap_array<std::atomic<int>> adj_list_size_;
  mmap_array<int> adj_list_capacity_;
  mmap_array<nbr_t> nbr_list_;
  timestamp_t unsorted_since_;
};

template <typename EDATA_T>
class SingleMutableCsr : public TypedCsrBase<EDATA_T> {
 public:
  using data_t = EDATA_T;
  using nbr_t = MutableNbr<EDATA_T>;

  SingleMutableCsr() {}
  ~SingleMutableCsr() {}

  CsrType csr_type() const override { return CsrType::kSingleMutable; }

  GenericView get_generic_view(timestamp_t ts) const override {
    NbrIterConfig cfg;
    cfg.stride = sizeof(nbr_t);
    cfg.ts_offset = offsetof(nbr_t, timestamp);
    cfg.data_offset = offsetof(nbr_t, data);
    return GenericView(reinterpret_cast<const char*>(nbr_list_.data()), cfg, ts,
                       std::numeric_limits<timestamp_t>::max());
  }

  timestamp_t unsorted_since() const override {
    return std::numeric_limits<timestamp_t>::max();
  }

  size_t size() const override { return nbr_list_.size(); }

  size_t edge_num() const override {
    size_t cnt = 0;
    for (size_t k = 0; k != nbr_list_.size(); ++k) {
      if (nbr_list_[k].timestamp.load() !=
          std::numeric_limits<timestamp_t>::max()) {
        ++cnt;
      }
    }
    return cnt;
  }

  void open(const std::string& name, const std::string& snapshot_dir,
            const std::string& work_dir) override;

  void open_in_memory(const std::string& prefix) override;

  void open_with_hugepages(const std::string& prefix) override;

  void dump(const std::string& name,
            const std::string& new_snapshot_dir) override;

  void reset_timestamp() override;

  void compact() override;

  void resize(vid_t vnum) override;

  size_t capacity() const override;

  void close() override;

  void batch_sort_by_edge_data(timestamp_t ts) override;

  void batch_delete_vertices(const std::set<vid_t>& src_set,
                             const std::set<vid_t>& dst_set) override;

  void batch_delete_edges(const std::vector<vid_t>& src_list,
                          const std::vector<vid_t>& dst_list) override;

  void batch_delete_edges(
      const std::vector<std::pair<vid_t, int32_t>>& edges) override;

  void delete_edge(vid_t src, int32_t offset, timestamp_t ts) override;

  void revert_delete_edge(vid_t src, vid_t nbr, int32_t offset,
                          timestamp_t ts) override;

  void batch_put_edges(const std::vector<vid_t>& src_list,
                       const std::vector<vid_t>& dst_list,
                       const std::vector<EDATA_T>& data_list,
                       timestamp_t ts = 0) override;

  int32_t put_edge(vid_t src, vid_t dst, const EDATA_T& data, timestamp_t ts,
                   Allocator& alloc) override {
    if (src >= nbr_list_.size()) {
      THROW_INVALID_ARGUMENT_EXCEPTION(
          "Source vertex id out of range: " + std::to_string(src) +
          " >= " + std::to_string(nbr_list_.size()));
    }
    nbr_list_[src].neighbor = dst;
    nbr_list_[src].data = data;
    CHECK_EQ(nbr_list_[src].timestamp, std::numeric_limits<timestamp_t>::max());
    nbr_list_[src].timestamp.store(ts);
    return 0;
  }

  std::tuple<std::vector<vid_t>, std::vector<vid_t>> batch_export(
      std::shared_ptr<ColumnBase> prev_data_col) const override {
    LOG(FATAL) << "not implemented...";
    return {};
  }

 private:
  mmap_array<nbr_t> nbr_list_;
};

template <typename EDATA_T>
class EmptyCsr : public TypedCsrBase<EDATA_T> {
 public:
  EmptyCsr() = default;
  ~EmptyCsr() = default;

  CsrType csr_type() const override { return CsrType::kEmpty; }

  GenericView get_generic_view(timestamp_t ts) const override {
    LOG(FATAL) << "Not implemented";
    return GenericView();
  }

  timestamp_t unsorted_since() const override {
    return std::numeric_limits<timestamp_t>::max();
  }

  size_t size() const override { return 0; }

  size_t edge_num() const override { return 0; }

  void open(const std::string& name, const std::string& snapshot_dir,
            const std::string& work_dir) override {}

  void open_in_memory(const std::string& prefix) override {}

  void open_with_hugepages(const std::string& prefix) override {}

  void dump(const std::string& name,
            const std::string& new_snapshot_dir) override {}

  void reset_timestamp() override {}

  void compact() override {}

  void resize(vid_t vnum) override {}

  size_t capacity() const override { return 0; }

  void close() override {}

  void batch_sort_by_edge_data(timestamp_t ts) override {}

  void batch_delete_vertices(const std::set<vid_t>& src_set,
                             const std::set<vid_t>& dst_set) override {}

  void batch_delete_edges(const std::vector<vid_t>& src_list,
                          const std::vector<vid_t>& dst_list) override {}

  void batch_delete_edges(
      const std::vector<std::pair<vid_t, int32_t>>& edges) override {}

  void delete_edge(vid_t src, int32_t offset, timestamp_t ts) override {}

  void revert_delete_edge(vid_t src, vid_t nbr, int32_t offset,
                          timestamp_t ts) override {}

  void batch_put_edges(const std::vector<vid_t>& src_list,
                       const std::vector<vid_t>& dst_list,
                       const std::vector<EDATA_T>& data_list,
                       timestamp_t ts = 0) override {}

  int32_t put_edge(vid_t src, vid_t dst, const EDATA_T& data, timestamp_t ts,
                   Allocator&) override {
    return 0;
  }

  std::tuple<std::vector<vid_t>, std::vector<vid_t>> batch_export(
      std::shared_ptr<ColumnBase> prev_data_col) const override {
    return {};
  }
};

}  // namespace neug
