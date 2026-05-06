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
#include <cassert>
#include <limits>
#include <memory>
#include <ostream>
#include <set>
#include <string>
#include <vector>

#include "neug/storages/allocators.h"
#include "neug/storages/container/i_container.h"
#include "neug/storages/csr/csr_base.h"
#include "neug/storages/csr/generic_view.h"
#include "neug/storages/csr/nbr.h"
#include "neug/utils/file_utils.h"
#include "neug/utils/property/types.h"
#include "neug/utils/spinlock.h"

namespace neug {

template <typename EDATA_T>
class MutableCsr : public TypedCsrBase<EDATA_T> {
 public:
  using data_t = EDATA_T;
  using nbr_t = MutableNbr<EDATA_T>;

  MutableCsr() : locks_(nullptr) {}
  ~MutableCsr() { close(); }

  CsrType csr_type() const override { return CsrType::kMutable; }

  GenericView get_generic_view(timestamp_t ts) const override {
    NbrIterConfig cfg;
    cfg.stride = sizeof(nbr_t);
    cfg.ts_offset = offsetof(nbr_t, timestamp);
    cfg.data_offset = offsetof(nbr_t, data);
    return GenericView(
        reinterpret_cast<const char*>(adj_list_buffer_->GetData()),
        reinterpret_cast<const int*>(degree_list_->GetData()), cfg, ts,
        unsorted_since_);
  }

  timestamp_t unsorted_since() const override { return unsorted_since_; }

  size_t size() const override { return vertex_capacity(); }

  size_t edge_num() const override { return edge_num_.load(); }

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
    if (src >= vertex_capacity()) {
      THROW_INVALID_ARGUMENT_EXCEPTION(
          "Source vertex id out of range: " + std::to_string(src) +
          " >= " + std::to_string(vertex_capacity()));
    }
    auto** buffers = reinterpret_cast<nbr_t**>(adj_list_buffer_->GetData());
    auto* sizes = reinterpret_cast<int*>(degree_list_->GetData());
    auto* caps = reinterpret_cast<int*>(cap_list_->GetData());
    locks_[src].lock();
    int sz = sizes[src];
    int cap = caps[src];
    if (sz == cap) {
      cap += (cap >> 1);
      cap = std::max(cap, 8);
      nbr_t* new_buffer =
          static_cast<nbr_t*>(alloc.allocate(cap * sizeof(nbr_t)));
      if (sz > 0) {
        memcpy(new_buffer, buffers[src], sz * sizeof(nbr_t));
      }
      buffers[src] = new_buffer;
      caps[src] = cap;
    }
    int32_t prev_size = sizes[src]++;
    auto& nbr = buffers[src][prev_size];
    nbr.neighbor = dst;
    nbr.data = data;
    nbr.timestamp.store(ts);
    edge_num_.fetch_add(1);
    locks_[src].unlock();
    return prev_size;
  }

  std::tuple<std::vector<vid_t>, std::vector<vid_t>> batch_export(
      std::shared_ptr<ColumnBase> prev_data_col) const override {
    std::vector<vid_t> src_list, dst_list;
    std::vector<EDATA_T> data_list;
    const nbr_t* const* adjlists =
        reinterpret_cast<const nbr_t* const*>(adj_list_buffer_->GetData());
    const int* degrees = reinterpret_cast<const int*>(degree_list_->GetData());
    for (vid_t src = 0; src < static_cast<vid_t>(vertex_capacity()); ++src) {
      auto deg = degrees[src];
      for (int i = 0; i < deg; ++i) {
        const auto& nbr = adjlists[src][i];
        if (nbr.timestamp.load() != std::numeric_limits<timestamp_t>::max()) {
          src_list.push_back(src);
          dst_list.push_back(nbr.neighbor);
          data_list.push_back(nbr.data);
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

  void open_internal(const std::string& snapshot_prefix,
                     const std::string& tmp_prefix, MemoryLevel mem_level);

  SpinLock* locks_;
  std::unique_ptr<IDataContainer> adj_list_buffer_;
  std::unique_ptr<IDataContainer> degree_list_;
  std::unique_ptr<IDataContainer> cap_list_;
  std::unique_ptr<IDataContainer> nbr_list_;
  timestamp_t unsorted_since_;
  std::atomic<uint64_t> edge_num_{0};

  size_t vertex_capacity() const {
    if (!degree_list_) {
      return 0;
    }
    return degree_list_->GetDataSize() / sizeof(int);
  }
};

template <typename EDATA_T>
class SingleMutableCsr : public TypedCsrBase<EDATA_T> {
 public:
  using data_t = EDATA_T;
  using nbr_t = MutableNbr<EDATA_T>;

  SingleMutableCsr() {}
  ~SingleMutableCsr() { close(); }

  CsrType csr_type() const override { return CsrType::kSingleMutable; }

  GenericView get_generic_view(timestamp_t ts) const override {
    NbrIterConfig cfg;
    cfg.stride = sizeof(nbr_t);
    cfg.ts_offset = offsetof(nbr_t, timestamp);
    cfg.data_offset = offsetof(nbr_t, data);
    return GenericView(reinterpret_cast<const char*>(nbr_list_->GetData()), cfg,
                       ts, std::numeric_limits<timestamp_t>::max());
  }

  timestamp_t unsorted_since() const override {
    return std::numeric_limits<timestamp_t>::max();
  }

  size_t size() const override { return vertex_capacity(); }

  size_t edge_num() const override { return edge_num_.load(); }

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
    if (src >= vertex_capacity()) {
      THROW_INVALID_ARGUMENT_EXCEPTION(
          "Source vertex id out of range: " + std::to_string(src) +
          " >= " + std::to_string(vertex_capacity()));
    }
    auto* nbrs = reinterpret_cast<nbr_t*>(nbr_list_->GetData());
    nbrs[src].neighbor = dst;
    nbrs[src].data = data;
    CHECK_EQ(nbrs[src].timestamp, std::numeric_limits<timestamp_t>::max());
    nbrs[src].timestamp.store(ts);
    edge_num_.fetch_add(1, std::memory_order_relaxed);
    return 0;
  }

  std::tuple<std::vector<vid_t>, std::vector<vid_t>> batch_export(
      std::shared_ptr<ColumnBase> prev_data_col) const override {
    LOG(FATAL) << "not implemented...";
    return {};
  }

 private:
  void load_meta(const std::string& prefix);
  void dump_meta(const std::string& prefix) const;

  std::unique_ptr<IDataContainer> nbr_list_;
  std::atomic<uint64_t> edge_num_{0};

  size_t vertex_capacity() const {
    if (!nbr_list_) {
      return 0;
    }
    return nbr_list_->GetDataSize() / sizeof(nbr_t);
  }
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
