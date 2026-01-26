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

#include <stddef.h>
#include <limits>
#include <set>
#include <string>
#include <vector>

#include "neug/storages/csr/csr_base.h"
#include "neug/storages/csr/generic_view.h"
#include "neug/storages/csr/nbr.h"
#include "neug/utils/mmap_array.h"
#include "neug/utils/property/types.h"

namespace neug {

template <typename EDATA_T>
class ImmutableCsr : public TypedCsrBase<EDATA_T> {
 public:
  using data_t = EDATA_T;
  using nbr_t = ImmutableNbr<EDATA_T>;

  ImmutableCsr() {}
  ~ImmutableCsr() {}

  CsrType csr_type() const override { return CsrType::kImmutable; }

  GenericView get_generic_view(timestamp_t ts) const override {
    NbrIterConfig cfg;
    cfg.stride = sizeof(nbr_t);
    cfg.ts_offset = 0;
    cfg.data_offset = offsetof(nbr_t, data);
    return GenericView(
        reinterpret_cast<const char*>(adj_lists_.data()), degree_list_.data(),
        cfg, std::numeric_limits<timestamp_t>::max() - 1, unsorted_since_);
  }

  timestamp_t unsorted_since() const override { return unsorted_since_; }

  size_t size() const override { return degree_list_.size(); }

  size_t edge_num() const override {
    size_t ret = 0;
    for (size_t i = 0; i < adj_lists_.size(); ++i) {
      auto deg = degree_list_[i];
      auto begin = adj_lists_[i];
      for (size_t j = 0; j < deg; ++j) {
        if (begin[j].neighbor != std::numeric_limits<vid_t>::max()) {
          ret++;
        }
      }
    }
    return ret;
  }

  void open(const std::string& name, const std::string& snapshot_dir,
            const std::string& work_dir) override;

  void open_in_memory(const std::string& prefix, size_t v_cap) override;

  void open_with_hugepages(const std::string& prefix, size_t v_cap) override;

  void dump(const std::string& name,
            const std::string& new_snapshot_dir) override;

  void reset_timestamp() override;

  void compact() override;

  void resize(vid_t vnum) override;

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

  std::tuple<std::vector<vid_t>, std::vector<vid_t>> batch_export(
      std::shared_ptr<ColumnBase> prev_data_col) const override {
    LOG(FATAL) << "not implemented...";
    return {};
  }

 private:
  void load_meta(const std::string& prefix);

  void dump_meta(const std::string& prefix) const;

  mmap_array<nbr_t*> adj_lists_;
  mmap_array<int> degree_list_;
  mmap_array<nbr_t> nbr_list_;
  timestamp_t unsorted_since_;
};

template <typename EDATA_T>
class SingleImmutableCsr : public TypedCsrBase<EDATA_T> {
 public:
  using data_t = EDATA_T;
  using nbr_t = ImmutableNbr<EDATA_T>;

  SingleImmutableCsr() {}
  ~SingleImmutableCsr() {}

  CsrType csr_type() const override { return CsrType::kSingleImmutable; }

  GenericView get_generic_view(timestamp_t ts) const override {
    NbrIterConfig cfg;
    cfg.stride = sizeof(nbr_t);
    cfg.ts_offset = 0;
    cfg.data_offset = offsetof(nbr_t, data);
    return GenericView(reinterpret_cast<const char*>(nbr_list_.data()), cfg,
                       std::numeric_limits<timestamp_t>::max() - 1,
                       std::numeric_limits<timestamp_t>::max());
  }

  timestamp_t unsorted_since() const override {
    return std::numeric_limits<timestamp_t>::max();
  }

  size_t size() const override { return nbr_list_.size(); }

  size_t edge_num() const override {
    size_t ret = 0;
    for (size_t i = 0; i < nbr_list_.size(); ++i) {
      if (nbr_list_[i].neighbor != std::numeric_limits<vid_t>::max()) {
        ++ret;
      }
    }
    return ret;
  }

  void open(const std::string& name, const std::string& snapshot_dir,
            const std::string& work_dir) override;

  void open_in_memory(const std::string& prefix, size_t v_cap) override;

  void open_with_hugepages(const std::string& prefix, size_t v_cap) override;

  void dump(const std::string& name,
            const std::string& new_snapshot_dir) override;

  void reset_timestamp() override;

  void compact() override;

  void resize(vid_t vnum) override;

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

  std::tuple<std::vector<vid_t>, std::vector<vid_t>> batch_export(
      std::shared_ptr<ColumnBase> prev_data_col) const override {
    LOG(FATAL) << "not implemented...";
    return {};
  }

 private:
  mmap_array<nbr_t> nbr_list_;
};

}  // namespace neug
