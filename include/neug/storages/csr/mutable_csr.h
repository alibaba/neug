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
#include <mutex>
#include <ostream>
#include <set>
#include <string>
#include <vector>

#include "neug/storages/allocators.h"
#include "neug/storages/container/i_container.h"
#include "neug/storages/csr/csr_base.h"
#include "neug/storages/csr/csr_view.h"
#include "neug/storages/csr/nbr.h"
#include "neug/storages/module/type_name.h"
#include "neug/utils/io/file/file_utils.h"
#include "neug/utils/property/types.h"
#include "neug/utils/spinlock.h"

namespace neug {

// std::atomic<int> must have the same size as int on supported platforms
// so that the degree_list buffer (persisted as int[]) can be safely
// reinterpreted as atomic<int>[] for concurrent access.
static_assert(
    sizeof(std::atomic<int>) == sizeof(int),
    "atomic<int> must have the same size as int on supported platforms");

template <typename EDATA_T>
class MutableCsr : public TypedCsrBase<EDATA_T> {
 public:
  using data_t = EDATA_T;
  using nbr_t = MutableNbr<EDATA_T>;

  MutableCsr() : unsorted_since_(0) {}
  ~MutableCsr() = default;

  CsrType csr_type() const override { return CsrType::kMutable; }

  CsrView get_generic_view(timestamp_t ts) const override {
    NbrIterConfig cfg;
    cfg.stride = sizeof(nbr_t);
    cfg.ts_offset = offsetof(nbr_t, timestamp);
    cfg.data_offset = offsetof(nbr_t, data);
    return CsrView(reinterpret_cast<const char*>(adj_list_buffer_->GetData()),
                   reinterpret_cast<const int*>(degree_list_->GetData()), cfg,
                   ts, unsorted_since_, prefetch_policy_);
  }

  timestamp_t unsorted_since() const override { return unsorted_since_; }

  size_t size() const override { return vertex_capacity(); }

  size_t edge_num() const override { return edge_num_.load(); }

  void Open(Checkpoint& ckp, const ModuleDescriptor& descriptor,
            MemoryLevel level) override;

  void Dump(Checkpoint& ckp, CheckpointManifest& meta,
            const std::string& key) override;

  void reset_timestamp() override;

  void compact() override;

  void resize(vid_t vnum) override;

  size_t capacity() const override;

  void Close();

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

  std::pair<int32_t, const void*> put_edge(vid_t src, vid_t dst,
                                           const EDATA_T& data, timestamp_t ts,
                                           Allocator& alloc) override {
    if (src >= vertex_capacity()) {
      THROW_INVALID_ARGUMENT_EXCEPTION(
          "Source vertex id out of range: " + std::to_string(src) +
          " >= " + std::to_string(vertex_capacity()));
    }
    auto** buffers = reinterpret_cast<nbr_t**>(adj_list_buffer_->GetData());
    auto* sizes = reinterpret_cast<std::atomic<int>*>(degree_list_->GetData());
    auto* caps = reinterpret_cast<int*>(cap_list_->GetData());
    locks_[src].lock();
    int sz = sizes[src].load(std::memory_order_relaxed);
    int cap = caps[src];
    if (sz == cap) {  // including cap == 0
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
    auto& nbr = buffers[src][sz];
    nbr.neighbor = dst;
    nbr.data = data;
    nbr.timestamp.store(ts);
    edge_num_.fetch_add(1);
    // invalidate sort flag
    if (ts < unsorted_since_) {
      unsorted_since_ = 0;
    }
    const void* data_ptr = static_cast<const void*>(&nbr.data);
    sizes[src].store(sz + 1, std::memory_order_release);
    locks_[src].unlock();
    return {sz, data_ptr};
  }

  std::tuple<std::vector<vid_t>, std::vector<vid_t>> batch_export(
      ColumnBase* prev_data_col) const override {
    std::vector<vid_t> src_list, dst_list;
    std::vector<EDATA_T> data_list;
    const nbr_t* const* adjlists =
        reinterpret_cast<const nbr_t* const*>(adj_list_buffer_->GetData());
    const auto* degrees =
        reinterpret_cast<const std::atomic<int>*>(degree_list_->GetData());
    for (vid_t src = 0; src < static_cast<vid_t>(vertex_capacity()); ++src) {
      auto deg = degrees[src].load(std::memory_order_acquire);
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
      auto casted = dynamic_cast<TypedColumn<EDATA_T>*>(prev_data_col);
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

  void DetachVertex(vid_t vid, Allocator& alloc) override {
    auto v_cap = vertex_capacity();
    if (vid >= v_cap) {
      THROW_INVALID_ARGUMENT_EXCEPTION(
          "Vertex id out of range: " + std::to_string(vid) +
          " >= " + std::to_string(v_cap));
    }
    std::lock_guard<SpinLock> guard(locks_[vid]);
    auto* buffers = reinterpret_cast<nbr_t**>(adj_list_buffer_->GetData());
    auto* caps = reinterpret_cast<int*>(cap_list_->GetData());
    const auto* degrees =
        reinterpret_cast<const std::atomic<int>*>(degree_list_->GetData());
    auto cap = caps[vid];
    if (cap == 0) {
      return;
    }
    auto deg = degrees[vid].load(std::memory_order_acquire);
    void* new_adj_list = alloc.allocate(sizeof(nbr_t) * cap);
    memcpy(new_adj_list, buffers[vid], sizeof(nbr_t) * deg);
    buffers[vid] = static_cast<nbr_t*>(new_adj_list);
  }

  std::unique_ptr<Module> Clone() const override {
    auto cow_clone = std::make_unique<MutableCsr<EDATA_T>>();
    auto v_cap = vertex_capacity();
    cow_clone->locks_ = std::make_unique<SpinLock[]>(v_cap);
    cow_clone->adj_list_buffer_ = adj_list_buffer_;
    cow_clone->degree_list_ = degree_list_;
    cow_clone->cap_list_ = cap_list_;
    cow_clone->nbr_list_ = nbr_list_;
    cow_clone->unsorted_since_ = unsorted_since_;
    cow_clone->edge_num_ = edge_num_.load();
    return cow_clone;
  }

  // Detach shared buffers for COW writes.
  void Detach(Checkpoint& ckp, MemoryLevel level) override {
    adj_list_buffer_ = adj_list_buffer_->Fork(ckp, level);
    degree_list_ = degree_list_->Fork(ckp, level);
    cap_list_ = cap_list_->Fork(ckp, level);
    // nbr_list_ need no deep copy
  }

  std::string ModuleTypeName() const override { return type_name(); }

  static std::string type_name() {
    return "mutable_csr<" + type_name_string<EDATA_T>() + ">";
  }

 private:
  std::unique_ptr<SpinLock[]> locks_;
  std::shared_ptr<IDataContainer> adj_list_buffer_;
  std::shared_ptr<IDataContainer> degree_list_;
  std::shared_ptr<IDataContainer> cap_list_;
  std::shared_ptr<IDataContainer> nbr_list_;
  timestamp_t unsorted_since_;
  std::atomic<uint64_t> edge_num_{0};
  CsrPrefetchPolicy prefetch_policy_;

  void refresh_prefetch_policy();

  size_t vertex_capacity() const {
    if (!degree_list_) {
      return 0;
    }
    return degree_list_->GetDataSize() / sizeof(std::atomic<int>);
  }
};

template <typename EDATA_T>
class SingleMutableCsr : public TypedCsrBase<EDATA_T> {
 public:
  using data_t = EDATA_T;
  using nbr_t = MutableNbr<EDATA_T>;

  SingleMutableCsr() {}
  ~SingleMutableCsr() = default;

  CsrType csr_type() const override { return CsrType::kSingleMutable; }

  CsrView get_generic_view(timestamp_t ts) const override {
    NbrIterConfig cfg;
    cfg.stride = sizeof(nbr_t);
    cfg.ts_offset = offsetof(nbr_t, timestamp);
    cfg.data_offset = offsetof(nbr_t, data);
    return CsrView(reinterpret_cast<const char*>(nbr_list_->GetData()), cfg, ts,
                   std::numeric_limits<timestamp_t>::max(), prefetch_policy_);
  }

  timestamp_t unsorted_since() const override {
    return std::numeric_limits<timestamp_t>::max();
  }

  size_t size() const override { return vertex_capacity(); }

  size_t edge_num() const override { return edge_num_.load(); }

  void Open(Checkpoint& ckp, const ModuleDescriptor& descriptor,
            MemoryLevel) override;

  void Dump(Checkpoint& ckp, CheckpointManifest& meta,
            const std::string& key) override;

  void reset_timestamp() override;

  void compact() override;

  void resize(vid_t vnum) override;

  size_t capacity() const override;

  void Close();

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

  std::pair<int32_t, const void*> put_edge(vid_t src, vid_t dst,
                                           const EDATA_T& data, timestamp_t ts,
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
    return {0, static_cast<const void*>(&nbrs[src].data)};
  }

  std::tuple<std::vector<vid_t>, std::vector<vid_t>> batch_export(
      ColumnBase* prev_data_col) const override {
    std::vector<vid_t> src_list, dst_list;
    std::vector<EDATA_T> data_list;
    const nbr_t* nbrs = reinterpret_cast<const nbr_t*>(nbr_list_->GetData());
    for (vid_t src = 0; src < static_cast<vid_t>(vertex_capacity()); ++src) {
      const auto& nbr = nbrs[src];
      if (nbr.timestamp.load() != std::numeric_limits<timestamp_t>::max()) {
        src_list.push_back(src);
        dst_list.push_back(nbr.neighbor);
        data_list.push_back(nbr.data);
      }
    }
    if (prev_data_col) {
      auto casted = dynamic_cast<TypedColumn<EDATA_T>*>(prev_data_col);
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

  void DetachVertex(vid_t /*vid*/, Allocator& /*alloc*/) override {}

  std::unique_ptr<Module> Clone() const override {
    auto cow_clone = std::make_unique<SingleMutableCsr<EDATA_T>>();
    cow_clone->nbr_list_ = nbr_list_;
    cow_clone->edge_num_ = edge_num_.load();
    return cow_clone;
  }

  // Detach shared buffers for COW writes.
  void Detach(Checkpoint& ckp, MemoryLevel level) override {
    nbr_list_ = nbr_list_->Fork(ckp, level);
  }

  std::string ModuleTypeName() const override { return type_name(); }

  static std::string type_name() {
    return "single_mutable_csr<" + type_name_string<EDATA_T>() + ">";
  }

 private:
  std::shared_ptr<IDataContainer> nbr_list_;
  std::atomic<uint64_t> edge_num_{0};
  CsrPrefetchPolicy prefetch_policy_;

  void refresh_prefetch_policy();

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

  CsrView get_generic_view(timestamp_t ts) const override {
    LOG(FATAL) << "Not implemented";
    return CsrView();
  }

  timestamp_t unsorted_since() const override {
    return std::numeric_limits<timestamp_t>::max();
  }

  size_t size() const override { return 0; }

  size_t edge_num() const override { return 0; }

  void Open(Checkpoint& ckp, const ModuleDescriptor& descriptor,
            MemoryLevel /* level */) override {}

  void Dump(Checkpoint& ckp, CheckpointManifest& meta,
            const std::string& key) override {
    ModuleDescriptor desc;
    desc.module_type = type_name();
    meta.set_module(key, desc);
  }

  void reset_timestamp() override {}

  void compact() override {}

  void resize(vid_t vnum) override {}

  size_t capacity() const override { return 0; }

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

  void DetachVertex(vid_t /*vid*/, Allocator& /*alloc*/) override {}

  std::pair<int32_t, const void*> put_edge(vid_t src, vid_t dst,
                                           const EDATA_T& data, timestamp_t ts,
                                           Allocator&) override {
    return {0, nullptr};
  }

  std::tuple<std::vector<vid_t>, std::vector<vid_t>> batch_export(
      ColumnBase* /*prev_data_col*/) const override {
    return {};
  }

  std::unique_ptr<Module> Clone() const override {
    return std::make_unique<EmptyCsr<EDATA_T>>();
  }

  void Detach(Checkpoint&, MemoryLevel) override {}

  std::string ModuleTypeName() const override { return type_name(); }

  static std::string type_name() {
    return "empty_csr<" + type_name_string<EDATA_T>() + ">";
  }
};

}  // namespace neug
