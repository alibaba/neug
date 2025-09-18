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

#ifndef STORAGES_RT_MUTABLE_GRAPH_DUAL_CSR_H_
#define STORAGES_RT_MUTABLE_GRAPH_DUAL_CSR_H_

#include <stdio.h>

#include "libgrape-lite/grape/serialization/in_archive.h"
#include "neug/storages/csr/immutable_csr.h"
#include "neug/storages/csr/mutable_csr.h"
#include "neug/utils/allocators.h"
#include "neug/utils/property/table.h"

namespace gs {

class DualCsrBase {
 public:
  DualCsrBase() = default;
  virtual ~DualCsrBase() = default;
  virtual void BatchInit(const std::string& oe_name, const std::string& ie_name,
                         const std::string& edata_name,
                         const std::string& work_dir,
                         const std::vector<int>& oe_degree,
                         const std::vector<int>& ie_degree) = 0;

  virtual void BatchInitInMemory(const std::string& edata_name,
                                 const std::string& work_dir,
                                 const std::vector<int>& oe_degree,
                                 const std::vector<int>& ie_degree) = 0;

  virtual void Open(const std::string& oe_name, const std::string& ie_name,
                    const std::string& edata_name,
                    const std::string& snapshot_dir,
                    const std::string& work_dir,
                    const std::vector<std::string>& col_names,
                    const std::vector<PropertyType>& property_types) = 0;

  virtual void OpenInMemory(const std::string& oe_name,
                            const std::string& ie_name,
                            const std::string& edata_name,
                            const std::string& snapshot_dir,
                            const std::vector<std::string>& col_names,
                            const std::vector<PropertyType>& property_types,
                            size_t src_vertex_cap, size_t dst_vertex_cap) = 0;

  virtual void OpenWithHugepages(
      const std::string& oe_name, const std::string& ie_name,
      const std::string& edata_name, const std::string& snapshot_dir,
      const std::vector<std::string>& col_names,
      const std::vector<PropertyType>& property_types, size_t src_vertex_cap,
      size_t dst_vertex_cap) = 0;

  virtual void Dump(const std::string& oe_name, const std::string& ie_name,
                    const std::string& edata_name,
                    const std::string& new_snapshot_dir) = 0;

  virtual void IngestEdge(vid_t src, vid_t dst, grape::OutArchive& oarc,
                          timestamp_t timestamp, Allocator& alloc) = 0;

  virtual void SortByEdgeData(timestamp_t ts) = 0;

  virtual void UpdateEdge(vid_t src, vid_t dst, const Any& oarc,
                          timestamp_t timestamp, Allocator& alloc) = 0;

  virtual void BatchDeleteVertices(bool is_src,
                                   const std::vector<vid_t>& vids) {}

  virtual void BatchDeleteEdge(
      const std::vector<std::tuple<vid_t, vid_t>>& parsed_edges_vec) {}

  void Resize(vid_t src_vertex_num, vid_t dst_vertex_num) {
    GetInCsr()->resize(dst_vertex_num);
    GetOutCsr()->resize(src_vertex_num);
  }

  size_t EdgeNum() const {
    const CsrBase* oe_csr = GetOutCsr();
    const CsrBase* ie_csr = GetInCsr();
    if (oe_csr) {
      return oe_csr->edge_num();
    } else if (ie_csr) {
      return ie_csr->edge_num();
    } else {
      return 0;
    }
  }

  virtual CsrBase* GetInCsr() = 0;
  virtual CsrBase* GetOutCsr() = 0;
  virtual const CsrBase* GetInCsr() const = 0;
  virtual const CsrBase* GetOutCsr() const = 0;
  virtual void Close() = 0;

  virtual void ResetTimestamp() = 0;
  virtual void CompactNbr(const std::string& work_dir, float reserve_ratio) = 0;

  virtual void Drop() {
    // TODO: Implement drop logic.
    LOG(ERROR) << "Drop is not supported for DualCsrBase.";
  }
};

template <typename EDATA_T>
class DualCsr : public DualCsrBase {
 public:
  DualCsr(EdgeStrategy oe_strategy, EdgeStrategy ie_strategy, bool oe_mutable,
          bool ie_mutable)
      : in_csr_(nullptr), out_csr_(nullptr) {
    if (ie_strategy == EdgeStrategy::kNone) {
      in_csr_ = new EmptyCsr<EDATA_T>();
    } else if (ie_strategy == EdgeStrategy::kMultiple) {
      if (ie_mutable) {
        in_csr_ = new MutableCsr<EDATA_T>();
      } else {
        in_csr_ = new ImmutableCsr<EDATA_T>();
      }
    } else if (ie_strategy == EdgeStrategy::kSingle) {
      if (ie_mutable) {
        in_csr_ = new SingleMutableCsr<EDATA_T>();
      } else {
        in_csr_ = new SingleImmutableCsr<EDATA_T>();
      }
    }
    if (oe_strategy == EdgeStrategy::kNone) {
      out_csr_ = new EmptyCsr<EDATA_T>();
    } else if (oe_strategy == EdgeStrategy::kMultiple) {
      if (oe_mutable) {
        out_csr_ = new MutableCsr<EDATA_T>();
      } else {
        out_csr_ = new ImmutableCsr<EDATA_T>();
      }
    } else if (oe_strategy == EdgeStrategy::kSingle) {
      if (oe_mutable) {
        out_csr_ = new SingleMutableCsr<EDATA_T>();
      } else {
        out_csr_ = new SingleImmutableCsr<EDATA_T>();
      }
    }
  }
  ~DualCsr() {
    if (in_csr_ != nullptr) {
      delete in_csr_;
    }
    if (out_csr_ != nullptr) {
      delete out_csr_;
    }
  }
  void BatchInit(const std::string& oe_name, const std::string& ie_name,
                 const std::string& edata_name, const std::string& work_dir,
                 const std::vector<int>& oe_degree,
                 const std::vector<int>& ie_degree) override {
    in_csr_->batch_init(ie_name, work_dir, ie_degree);
    out_csr_->batch_init(oe_name, work_dir, oe_degree);
  }

  void BatchInitInMemory(const std::string& edata_name,
                         const std::string& work_dir,
                         const std::vector<int>& oe_degree,
                         const std::vector<int>& ie_degree) override {
    in_csr_->batch_init_in_memory(ie_degree);
    out_csr_->batch_init_in_memory(oe_degree);
  }

  void Open(const std::string& oe_name, const std::string& ie_name,
            const std::string& edata_name, const std::string& snapshot_dir,
            const std::string& work_dir,
            const std::vector<std::string>& col_names,
            const std::vector<PropertyType>& property_types) override {
    in_csr_->open(ie_name, snapshot_dir, work_dir);
    out_csr_->open(oe_name, snapshot_dir, work_dir);
  }

  void OpenInMemory(const std::string& oe_name, const std::string& ie_name,
                    const std::string& edata_name,
                    const std::string& snapshot_dir,
                    const std::vector<std::string>& col_names,
                    const std::vector<PropertyType>& property_types,
                    size_t src_vertex_cap, size_t dst_vertex_cap) override {
    in_csr_->open_in_memory(snapshot_dir + "/" + ie_name, dst_vertex_cap);
    out_csr_->open_in_memory(snapshot_dir + "/" + oe_name, src_vertex_cap);
  }

  void OpenWithHugepages(const std::string& oe_name, const std::string& ie_name,
                         const std::string& edata_name,
                         const std::string& snapshot_dir,
                         const std::vector<std::string>& col_names,
                         const std::vector<PropertyType>& property_types,
                         size_t src_vertex_cap,
                         size_t dst_vertex_cap) override {
    in_csr_->open_with_hugepages(snapshot_dir + "/" + ie_name, dst_vertex_cap);
    out_csr_->open_with_hugepages(snapshot_dir + "/" + oe_name, src_vertex_cap);
  }

  void Dump(const std::string& oe_name, const std::string& ie_name,
            const std::string& edata_name,
            const std::string& new_snapshot_dir) override {
    in_csr_->dump(ie_name, new_snapshot_dir);
    out_csr_->dump(oe_name, new_snapshot_dir);
  }

  CsrBase* GetInCsr() override { return in_csr_; }
  CsrBase* GetOutCsr() override { return out_csr_; }
  const CsrBase* GetInCsr() const override { return in_csr_; }
  const CsrBase* GetOutCsr() const override { return out_csr_; }

  void IngestEdge(vid_t src, vid_t dst, grape::OutArchive& oarc, timestamp_t ts,
                  Allocator& alloc) override {
    EDATA_T data;
    oarc >> data;
    in_csr_->put_edge(dst, src, data, ts, alloc);
    out_csr_->put_edge(src, dst, data, ts, alloc);
  }

  void SortByEdgeData(timestamp_t ts) override {
    in_csr_->batch_sort_by_edge_data(ts);
    out_csr_->batch_sort_by_edge_data(ts);
  }

  void UpdateEdge(vid_t src, vid_t dst, const Any& data, timestamp_t ts,
                  Allocator& alloc) override {
    auto oe = out_csr_->edge_iter_mut(src);
    EDATA_T prop;
    ConvertAny<EDATA_T>::to(data, prop);
    bool src_flag = false, dst_flag = false;
    while (oe != nullptr && oe->is_valid()) {
      if (oe->get_neighbor() == dst) {
        oe->set_data(prop, ts);
        src_flag = true;
        break;
      }
      oe->next();
    }
    auto ie = in_csr_->edge_iter_mut(dst);
    while (ie != nullptr && ie->is_valid()) {
      if (ie->get_neighbor() == src) {
        dst_flag = true;
        ie->set_data(prop, ts);
        break;
      }
      ie->next();
    }
    if (!(src_flag || dst_flag)) {
      in_csr_->put_edge(dst, src, prop, ts, alloc);
      out_csr_->put_edge(src, dst, prop, ts, alloc);
    }
  }

  void BatchPutEdge(vid_t src, vid_t dst, const EDATA_T& data) {
    in_csr_->batch_put_edge(dst, src, data);
    out_csr_->batch_put_edge(src, dst, data);
  }

  void BatchDeleteVertices(bool is_src,
                           const std::vector<vid_t>& vids) override {
    MutableCsr<EDATA_T>* casted_in_csr =
        dynamic_cast<MutableCsr<EDATA_T>*>(in_csr_);
    MutableCsr<EDATA_T>* casted_out_csr =
        dynamic_cast<MutableCsr<EDATA_T>*>(out_csr_);
    if (is_src) {
      if (casted_in_csr) {
        casted_in_csr->batch_delete_vertices(false, vids);
      }
      if (casted_out_csr) {
        casted_out_csr->batch_delete_vertices(true, vids);
      }
    } else {
      if (casted_in_csr) {
        casted_in_csr->batch_delete_vertices(true, vids);
      }
      if (casted_out_csr) {
        casted_out_csr->batch_delete_vertices(false, vids);
      }
    }
  }

  void BatchDeleteEdge(
      const std::vector<std::tuple<vid_t, vid_t>>& parsed_edges_vec) override {
    MutableCsr<EDATA_T>* casted_in_csr =
        dynamic_cast<MutableCsr<EDATA_T>*>(in_csr_);
    MutableCsr<EDATA_T>* casted_out_csr =
        dynamic_cast<MutableCsr<EDATA_T>*>(out_csr_);
    if (casted_in_csr) {
      casted_in_csr->batch_delete_edges(false, parsed_edges_vec);
    }
    if (casted_out_csr) {
      casted_out_csr->batch_delete_edges(true, parsed_edges_vec);
    }
  }

  void ResetTimestamp() override {
    in_csr_->reset_timestamp();
    out_csr_->reset_timestamp();
  }

  void CompactNbr(const std::string& work_dir, float reserve_ratio) override {
    in_csr_->compact_nbr(work_dir, reserve_ratio);
    out_csr_->compact_nbr(work_dir, reserve_ratio);
  }

  void Drop() override {
    Close();
    // TODO: delete file in work_dir
  }

  void Close() override {
    in_csr_->close();
    out_csr_->close();
  }

 private:
  TypedCsrBase<EDATA_T>* in_csr_;
  TypedCsrBase<EDATA_T>* out_csr_;
};

template <>
class DualCsr<std::string_view> : public DualCsrBase {
 public:
  DualCsr(EdgeStrategy oe_strategy, EdgeStrategy ie_strategy, Table& table,
          std::atomic<size_t>& column_idx, bool oe_mutable, bool ie_mutable)
      : in_csr_(nullptr),
        out_csr_(nullptr),
        column_idx_(column_idx),
        table_(table) {
    if (ie_strategy == EdgeStrategy::kNone) {
      in_csr_ = new EmptyCsr<std::string_view>(table_);
    } else if (ie_strategy == EdgeStrategy::kMultiple) {
      if (ie_mutable) {
        in_csr_ = new MutableCsr<std::string_view>(table_);
      } else {
        in_csr_ = new ImmutableCsr<std::string_view>(table_);
      }
    } else if (ie_strategy == EdgeStrategy::kSingle) {
      if (ie_mutable) {
        in_csr_ = new SingleMutableCsr<std::string_view>(table_);
      } else {
        in_csr_ = new SingleImmutableCsr<std::string_view>(table_);
      }
    }
    if (oe_strategy == EdgeStrategy::kNone) {
      out_csr_ = new EmptyCsr<std::string_view>(table_);
    } else if (oe_strategy == EdgeStrategy::kMultiple) {
      if (oe_mutable) {
        out_csr_ = new MutableCsr<std::string_view>(table_);
      } else {
        out_csr_ = new ImmutableCsr<std::string_view>(table_);
      }
    } else if (oe_strategy == EdgeStrategy::kSingle) {
      if (oe_mutable) {
        out_csr_ = new SingleMutableCsr<std::string_view>(table_);
      } else {
        out_csr_ = new SingleImmutableCsr<std::string_view>(table_);
      }
    }
  }

  ~DualCsr() {
    if (in_csr_ != nullptr) {
      delete in_csr_;
    }
    if (out_csr_ != nullptr) {
      delete out_csr_;
    }
  }

  void BatchInit(const std::string& oe_name, const std::string& ie_name,
                 const std::string& edata_name, const std::string& work_dir,
                 const std::vector<int>& oe_degree,
                 const std::vector<int>& ie_degree) override {
    size_t ie_num = in_csr_->batch_init(ie_name, work_dir, ie_degree);
    size_t oe_num = out_csr_->batch_init(oe_name, work_dir, oe_degree);
    table_.resize(std::max(ie_num, oe_num));
    column_idx_.store(std::max(ie_num, oe_num));
  }

  void BatchInitInMemory(const std::string& edata_name,
                         const std::string& work_dir,
                         const std::vector<int>& oe_degree,
                         const std::vector<int>& ie_degree) override {
    size_t ie_num = in_csr_->batch_init_in_memory(ie_degree);
    size_t oe_num = out_csr_->batch_init_in_memory(oe_degree);
    table_.resize(std::max(ie_num, oe_num));
    column_idx_.store(std::max(ie_num, oe_num));
  }

  void Open(const std::string& oe_name, const std::string& ie_name,
            const std::string& edata_name, const std::string& snapshot_dir,
            const std::string& work_dir,
            const std::vector<std::string>& col_names,
            const std::vector<PropertyType>& property_types) override {
    in_csr_->open(ie_name, snapshot_dir, work_dir);
    out_csr_->open(oe_name, snapshot_dir, work_dir);
    table_.open(edata_name, snapshot_dir, work_dir, {col_names[0]},
                {PropertyType::StringView()}, {});
    column_idx_.store(table_.row_num());
    table_.resize(
        std::max(table_.row_num() + (table_.row_num() + 4) / 5, 4096ul));
  }

  void OpenInMemory(const std::string& oe_name, const std::string& ie_name,
                    const std::string& edata_name,
                    const std::string& snapshot_dir,
                    const std::vector<std::string>& col_names,
                    const std::vector<PropertyType>& property_types,
                    size_t src_vertex_cap, size_t dst_vertex_cap) override {
    in_csr_->open_in_memory(snapshot_dir + "/" + ie_name, dst_vertex_cap);
    out_csr_->open_in_memory(snapshot_dir + "/" + oe_name, src_vertex_cap);
    table_.open_in_memory(edata_name, snapshot_dir, {col_names[0]},
                          {PropertyType::StringView()}, {});
    column_idx_.store(table_.row_num());
    table_.resize(
        std::max(table_.row_num() + (table_.row_num() + 4) / 5, 4096ul));
  }

  void OpenWithHugepages(const std::string& oe_name, const std::string& ie_name,
                         const std::string& edata_name,
                         const std::string& snapshot_dir,
                         const std::vector<std::string>& col_names,
                         const std::vector<PropertyType>& property_types,
                         size_t src_vertex_cap,
                         size_t dst_vertex_cap) override {
    LOG(FATAL) << "not supported...";
  }

  void Dump(const std::string& oe_name, const std::string& ie_name,
            const std::string& edata_name,
            const std::string& new_snapshot_dir) override {
    in_csr_->dump(ie_name, new_snapshot_dir);
    out_csr_->dump(oe_name, new_snapshot_dir);
    table_.resize(column_idx_.load());
  }

  CsrBase* GetInCsr() override { return in_csr_; }
  CsrBase* GetOutCsr() override { return out_csr_; }
  const CsrBase* GetInCsr() const override { return in_csr_; }
  const CsrBase* GetOutCsr() const override { return out_csr_; }

  void IngestEdge(vid_t src, vid_t dst, grape::OutArchive& oarc, timestamp_t ts,
                  Allocator& alloc) override {
    std::string_view prop;
    oarc >> prop;
    size_t row_id = column_idx_.fetch_add(1);
    auto& column = dynamic_cast<StringColumn&>(*table_.get_column_by_id(0));
    column.set_value(row_id, prop);
    in_csr_->put_edge_with_index(dst, src, row_id, ts, alloc);
    out_csr_->put_edge_with_index(src, dst, row_id, ts, alloc);
  }

  void SortByEdgeData(timestamp_t ts) override {
    LOG(FATAL) << "Not implemented";
  }

  void UpdateEdge(vid_t src, vid_t dst, const Any& data, timestamp_t ts,
                  Allocator& alloc) override {
    auto oe_ptr = out_csr_->edge_iter_mut(src);
    std::string_view prop = data.AsStringView();
    auto oe = dynamic_cast<MutableCsrEdgeIter<std::string_view>*>(oe_ptr.get());
    size_t index = std::numeric_limits<size_t>::max();
    while (oe != nullptr && oe->is_valid()) {
      if (oe->get_neighbor() == dst) {
        oe->set_timestamp(ts);
        index = oe->get_index();
        break;
      }
      oe->next();
    }
    auto ie_ptr = in_csr_->edge_iter_mut(dst);
    auto ie = dynamic_cast<MutableCsrEdgeIter<std::string_view>*>(ie_ptr.get());
    while (ie != nullptr && ie->is_valid()) {
      if (ie->get_neighbor() == src) {
        ie->set_timestamp(ts);
        index = ie->get_index();
        break;
      }
      ie->next();
    }
    auto& column = dynamic_cast<StringColumn&>(*table_.get_column_by_id(0));
    if (index != std::numeric_limits<size_t>::max()) {
      column.set_value(index, prop);
    } else {
      size_t row_id = column_idx_.fetch_add(1);
      column.set_value(row_id, prop);
      in_csr_->put_edge_with_index(dst, src, row_id, ts, alloc);
      out_csr_->put_edge_with_index(src, dst, row_id, ts, alloc);
    }
  }

  void BatchPutEdge(vid_t src, vid_t dst, size_t index) {
    in_csr_->batch_put_edge_with_index(dst, src, index);
    out_csr_->batch_put_edge_with_index(src, dst, index);
  }

  void BatchDeleteVertices(bool is_src,
                           const std::vector<vid_t>& vids) override {
    MutableCsr<std::string_view>* casted_in_csr =
        dynamic_cast<MutableCsr<std::string_view>*>(in_csr_);
    MutableCsr<std::string_view>* casted_out_csr =
        dynamic_cast<MutableCsr<std::string_view>*>(out_csr_);
    if (is_src) {
      if (casted_in_csr) {
        casted_in_csr->batch_delete_vertices(false, vids);
      }
      if (casted_out_csr) {
        casted_out_csr->batch_delete_vertices(true, vids);
      }
    } else {
      if (casted_in_csr) {
        casted_in_csr->batch_delete_vertices(true, vids);
      }
      if (casted_out_csr) {
        casted_out_csr->batch_delete_vertices(false, vids);
      }
    }
  }

  void BatchDeleteEdge(
      const std::vector<std::tuple<vid_t, vid_t>>& parsed_edges_vec) override {
    MutableCsr<std::string_view>* casted_in_csr =
        dynamic_cast<MutableCsr<std::string_view>*>(in_csr_);
    MutableCsr<std::string_view>* casted_out_csr =
        dynamic_cast<MutableCsr<std::string_view>*>(out_csr_);
    if (casted_in_csr) {
      casted_in_csr->batch_delete_edges(false, parsed_edges_vec);
    }
    if (casted_out_csr) {
      casted_out_csr->batch_delete_edges(true, parsed_edges_vec);
    }
  }

  void ResetTimestamp() override {
    in_csr_->reset_timestamp();
    out_csr_->reset_timestamp();
  }

  void CompactNbr(const std::string& work_dir, float reserve_ratio) override {
    in_csr_->compact_nbr(work_dir, reserve_ratio);
    out_csr_->compact_nbr(work_dir, reserve_ratio);
  }

  void Drop() override {
    Close();
    // TODO: delete file in work_dir
  }

  void Close() override {
    in_csr_->close();
    out_csr_->close();
  }

 private:
  TypedCsrBase<std::string_view>* in_csr_;
  TypedCsrBase<std::string_view>* out_csr_;
  std::atomic<size_t>& column_idx_;
  Table& table_;
};

template <>
class DualCsr<RecordView> : public DualCsrBase {
 public:
  DualCsr(EdgeStrategy oe_strategy, EdgeStrategy ie_strategy, Table& table,
          std::atomic<size_t>& table_idx, bool oe_mutable, bool ie_mutable)
      : in_csr_(nullptr),
        out_csr_(nullptr),
        table_idx_(table_idx),
        table_(table) {
    if (ie_strategy == EdgeStrategy::kNone) {
      in_csr_ = new EmptyCsr<RecordView>(table_);
    } else if (ie_strategy == EdgeStrategy::kMultiple) {
      if (ie_mutable) {
        in_csr_ = new MutableCsr<RecordView>(table_);
      } else {
        in_csr_ = new ImmutableCsr<RecordView>(table_);
      }
    } else {
      if (ie_mutable) {
        in_csr_ = new SingleMutableCsr<RecordView>(table_);
      } else {
        in_csr_ = new SingleImmutableCsr<RecordView>(table_);
      }
    }
    if (oe_strategy == EdgeStrategy::kNone) {
      out_csr_ = new EmptyCsr<RecordView>(table_);
    } else if (oe_strategy == EdgeStrategy::kMultiple) {
      if (oe_mutable) {
        out_csr_ = new MutableCsr<RecordView>(table_);
      } else {
        out_csr_ = new ImmutableCsr<RecordView>(table_);
      }
    } else {
      if (oe_mutable) {
        out_csr_ = new SingleMutableCsr<RecordView>(table_);
      } else {
        out_csr_ = new SingleImmutableCsr<RecordView>(table_);
      }
    }
  }

  ~DualCsr() {
    if (in_csr_ != nullptr) {
      delete in_csr_;
    }
    if (out_csr_ != nullptr) {
      delete out_csr_;
    }
  }

  void BatchInit(const std::string& oe_name, const std::string& ie_name,
                 const std::string& edata_name, const std::string& work_dir,
                 const std::vector<int>& oe_degree,
                 const std::vector<int>& ie_degree) override {
    size_t ie_num = in_csr_->batch_init(ie_name, work_dir, ie_degree);
    size_t oe_num = out_csr_->batch_init(oe_name, work_dir, oe_degree);
    table_.resize(std::max(ie_num, oe_num));
    table_idx_.store(std::max(ie_num, oe_num));
  }

  void BatchInitInMemory(const std::string& edata_name,
                         const std::string& work_dir,
                         const std::vector<int>& oe_degree,
                         const std::vector<int>& ie_degree) override {
    size_t ie_num = in_csr_->batch_init_in_memory(ie_degree);
    size_t oe_num = out_csr_->batch_init_in_memory(oe_degree);
    table_.resize(std::max(ie_num, oe_num));
    table_idx_.store(std::max(ie_num, oe_num));
  }

  void Open(const std::string& oe_name, const std::string& ie_name,
            const std::string& edata_name, const std::string& snapshot_dir,
            const std::string& work_dir,
            const std::vector<std::string>& col_names,
            const std::vector<PropertyType>& property_types) override {
    in_csr_->open(ie_name, snapshot_dir, work_dir);
    out_csr_->open(oe_name, snapshot_dir, work_dir);

    // fix me: storage_strategies_ is not used
    table_.open(edata_name, snapshot_dir, work_dir, col_names, property_types,
                {});
    table_idx_.store(table_.row_num());
    table_.resize(
        std::max(table_.row_num() + (table_.row_num() + 4) / 5, 4096ul));
  }

  void OpenInMemory(const std::string& oe_name, const std::string& ie_name,
                    const std::string& edata_name,
                    const std::string& snapshot_dir,
                    const std::vector<std::string>& col_names,
                    const std::vector<PropertyType>& property_types,
                    size_t src_vertex_cap, size_t dst_vertex_cap) override {
    in_csr_->open_in_memory(snapshot_dir + "/" + ie_name, dst_vertex_cap);
    out_csr_->open_in_memory(snapshot_dir + "/" + oe_name, src_vertex_cap);
    // fix me: storage_strategies_ is not used
    table_.open_in_memory(edata_name, snapshot_dir, col_names, property_types,
                          {});
    table_idx_.store(table_.row_num());
    table_.resize(
        std::max(table_.row_num() + (table_.row_num() + 4) / 5, 4096ul));
  }

  void OpenWithHugepages(const std::string& oe_name, const std::string& ie_name,
                         const std::string& edata_name,
                         const std::string& snapshot_dir,
                         const std::vector<std::string>& col_names,
                         const std::vector<PropertyType>& property_types,
                         size_t src_vertex_cap,
                         size_t dst_vertex_cap) override {
    LOG(FATAL) << "not supported...";
  }

  void Dump(const std::string& oe_name, const std::string& ie_name,
            const std::string& edata_name,
            const std::string& new_snapshot_dir) override {
    in_csr_->dump(ie_name, new_snapshot_dir);
    out_csr_->dump(oe_name, new_snapshot_dir);
    table_.resize(table_idx_.load());
  }

  CsrBase* GetInCsr() override { return in_csr_; }
  CsrBase* GetOutCsr() override { return out_csr_; }
  const CsrBase* GetInCsr() const override { return in_csr_; }
  const CsrBase* GetOutCsr() const override { return out_csr_; }

  void BatchPutEdge(vid_t src, vid_t dst, size_t row_id) {
    in_csr_->batch_put_edge_with_index(dst, src, row_id);
    out_csr_->batch_put_edge_with_index(src, dst, row_id);
  }

  Table& GetTable() { return table_; }

  const Table& GetTable() const { return table_; }
  void IngestEdge(vid_t src, vid_t dst, grape::OutArchive& oarc, timestamp_t ts,
                  Allocator& alloc) override {
    size_t row_id = table_idx_.fetch_add(1);
    size_t len;
    oarc >> len;
    table_.ingest(row_id, oarc);
    in_csr_->put_edge_with_index(dst, src, row_id, ts, alloc);
    out_csr_->put_edge_with_index(src, dst, row_id, ts, alloc);
  }

  void SortByEdgeData(timestamp_t ts) override {
    LOG(FATAL) << "Not implemented";
  }

  void UpdateEdge(vid_t src, vid_t dst, const Any& data, timestamp_t ts,
                  Allocator& alloc) override {
    auto oe_ptr = out_csr_->edge_iter_mut(src);
    grape::InArchive arc;
    Record r = data.AsRecord();
    for (size_t i = 0; i < r.len; ++i) {
      arc << r.props[i];
    }
    grape::OutArchive oarc;
    oarc.SetSlice(arc.GetBuffer(), arc.GetSize());
    auto oe = dynamic_cast<MutableCsrEdgeIter<RecordView>*>(oe_ptr.get());
    size_t index = std::numeric_limits<size_t>::max();
    while (oe != nullptr && oe->is_valid()) {
      if (oe->get_neighbor() == dst) {
        oe->set_timestamp(ts);
        index = oe->get_index();
        break;
      }
      oe->next();
    }
    auto ie_ptr = in_csr_->edge_iter_mut(dst);
    auto ie = dynamic_cast<MutableCsrEdgeIter<RecordView>*>(ie_ptr.get());
    while (ie != nullptr && ie->is_valid()) {
      if (ie->get_neighbor() == src) {
        ie->set_timestamp(ts);
        index = ie->get_index();
        break;
      }
      ie->next();
    }
    if (index != std::numeric_limits<size_t>::max()) {
      table_.ingest(index, oarc);
    } else {
      size_t row_id = table_idx_.fetch_add(1);
      table_.ingest(row_id, oarc);
      in_csr_->put_edge_with_index(dst, src, row_id, ts, alloc);
      out_csr_->put_edge_with_index(src, dst, row_id, ts, alloc);
    }
  }

  void BatchDeleteVertices(bool is_src,
                           const std::vector<vid_t>& vids) override {
    MutableCsr<RecordView>* casted_in_csr =
        dynamic_cast<MutableCsr<RecordView>*>(in_csr_);
    MutableCsr<RecordView>* casted_out_csr =
        dynamic_cast<MutableCsr<RecordView>*>(out_csr_);
    if (is_src) {
      if (casted_in_csr) {
        casted_in_csr->batch_delete_vertices(false, vids);
      }
      if (casted_out_csr) {
        casted_out_csr->batch_delete_vertices(true, vids);
      }
    } else {
      if (casted_in_csr) {
        casted_in_csr->batch_delete_vertices(true, vids);
      }
      if (casted_out_csr) {
        casted_out_csr->batch_delete_vertices(false, vids);
      }
    }
  }

  void BatchDeleteEdge(
      const std::vector<std::tuple<vid_t, vid_t>>& parsed_edges_vec) override {
    MutableCsr<RecordView>* casted_in_csr =
        dynamic_cast<MutableCsr<RecordView>*>(in_csr_);
    MutableCsr<RecordView>* casted_out_csr =
        dynamic_cast<MutableCsr<RecordView>*>(out_csr_);
    if (casted_in_csr) {
      casted_in_csr->batch_delete_edges(false, parsed_edges_vec);
    }
    if (casted_out_csr) {
      casted_out_csr->batch_delete_edges(true, parsed_edges_vec);
    }
  }

  void ResetTimestamp() override {
    in_csr_->reset_timestamp();
    out_csr_->reset_timestamp();
  }

  void CompactNbr(const std::string& work_dir, float reserve_ratio) override {
    in_csr_->compact_nbr(work_dir, reserve_ratio);
    out_csr_->compact_nbr(work_dir, reserve_ratio);
  }

  void Drop() override {
    Close();
    // TODO: delete file in work_dir
  }

  void Close() override {
    in_csr_->close();
    out_csr_->close();
  }

 private:
  TypedCsrBase<RecordView>* in_csr_;
  TypedCsrBase<RecordView>* out_csr_;
  std::atomic<size_t>& table_idx_;
  Table& table_;
};

}  // namespace gs

#endif  // STORAGES_RT_MUTABLE_GRAPH_DUAL_CSR_H_
