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

#include <third_party/libgrape-lite/grape/serialization/in_archive.h>
#include "src/storages/rt_mutable_graph/csr/immutable_csr.h"
#include "src/storages/rt_mutable_graph/csr/mutable_csr.h"
#include "src/utils/allocators.h"
#include "src/utils/property/table.h"

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
                    const std::string& work_dir) = 0;
  virtual void OpenInMemory(const std::string& oe_name,
                            const std::string& ie_name,
                            const std::string& edata_name,
                            const std::string& snapshot_dir,
                            size_t src_vertex_cap, size_t dst_vertex_cap) = 0;
  virtual void OpenWithHugepages(const std::string& oe_name,
                                 const std::string& ie_name,
                                 const std::string& edata_name,
                                 const std::string& snapshot_dir,
                                 size_t src_vertex_cap,
                                 size_t dst_vertex_cap) = 0;
  virtual void Dump(const std::string& oe_name, const std::string& ie_name,
                    const std::string& edata_name,
                    const std::string& new_snapshot_dir) = 0;

  virtual void IngestEdge(vid_t src, vid_t dst, grape::OutArchive& oarc,
                          timestamp_t timestamp, Allocator& alloc) = 0;

  virtual void SortByEdgeData(timestamp_t ts) = 0;

  virtual void UpdateEdge(vid_t src, vid_t dst, const Any& oarc,
                          timestamp_t timestamp, Allocator& alloc) = 0;

  void Resize(vid_t src_vertex_num, vid_t dst_vertex_num) {
    GetInCsr()->resize(dst_vertex_num);
    GetOutCsr()->resize(src_vertex_num);
  }

  void Warmup(int thread_num) {
    GetInCsr()->warmup(thread_num);
    GetOutCsr()->warmup(thread_num);
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
            const std::string& work_dir) override {
    in_csr_->open(ie_name, snapshot_dir, work_dir);
    out_csr_->open(oe_name, snapshot_dir, work_dir);
  }

  void OpenInMemory(const std::string& oe_name, const std::string& ie_name,
                    const std::string& edata_name,
                    const std::string& snapshot_dir, size_t src_vertex_cap,
                    size_t dst_vertex_cap) override {
    in_csr_->open_in_memory(snapshot_dir + "/" + ie_name, dst_vertex_cap);
    out_csr_->open_in_memory(snapshot_dir + "/" + oe_name, src_vertex_cap);
  }

  void OpenWithHugepages(const std::string& oe_name, const std::string& ie_name,
                         const std::string& edata_name,
                         const std::string& snapshot_dir, size_t src_vertex_cap,
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

  void BatchAppendEdge(vid_t src, vid_t dst, const EDATA_T& data) {
    in_csr_->batch_put_edge(dst, src, data);
    out_csr_->batch_put_edge(src, dst, data);
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
  DualCsr(EdgeStrategy oe_strategy, EdgeStrategy ie_strategy, uint16_t width,
          bool oe_mutable, bool ie_mutable)
      : in_csr_(nullptr),
        out_csr_(nullptr),
        column_(StorageStrategy::kMem, width) {
    if (ie_strategy == EdgeStrategy::kNone) {
      in_csr_ = new EmptyCsr<std::string_view>(column_);
    } else if (ie_strategy == EdgeStrategy::kMultiple) {
      if (ie_mutable) {
        in_csr_ = new MutableCsr<std::string_view>(column_);
      } else {
        in_csr_ = new ImmutableCsr<std::string_view>(column_);
      }
    } else if (ie_strategy == EdgeStrategy::kSingle) {
      if (ie_mutable) {
        in_csr_ = new SingleMutableCsr<std::string_view>(column_);
      } else {
        in_csr_ = new SingleImmutableCsr<std::string_view>(column_);
      }
    }
    if (oe_strategy == EdgeStrategy::kNone) {
      out_csr_ = new EmptyCsr<std::string_view>(column_);
    } else if (oe_strategy == EdgeStrategy::kMultiple) {
      if (oe_mutable) {
        out_csr_ = new MutableCsr<std::string_view>(column_);
      } else {
        out_csr_ = new ImmutableCsr<std::string_view>(column_);
      }
    } else if (oe_strategy == EdgeStrategy::kSingle) {
      if (oe_mutable) {
        out_csr_ = new SingleMutableCsr<std::string_view>(column_);
      } else {
        out_csr_ = new SingleImmutableCsr<std::string_view>(column_);
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
    column_.open(edata_name, "", work_dir);
    column_.resize(std::max(ie_num, oe_num));
    column_idx_.store(0);
  }

  void BatchInitInMemory(const std::string& edata_name,
                         const std::string& work_dir,
                         const std::vector<int>& oe_degree,
                         const std::vector<int>& ie_degree) override {
    size_t ie_num = in_csr_->batch_init_in_memory(ie_degree);
    size_t oe_num = out_csr_->batch_init_in_memory(oe_degree);
    column_.open(edata_name, "", work_dir);
    column_.resize(std::max(ie_num, oe_num));
    column_idx_.store(0);
  }

  void Open(const std::string& oe_name, const std::string& ie_name,
            const std::string& edata_name, const std::string& snapshot_dir,
            const std::string& work_dir) override {
    in_csr_->open(ie_name, snapshot_dir, work_dir);
    out_csr_->open(oe_name, snapshot_dir, work_dir);
    column_.open(edata_name, snapshot_dir, work_dir);
    column_idx_.store(column_.size());
    column_.resize(std::max(column_.size() + (column_.size() + 4) / 5, 4096ul));
  }

  void OpenInMemory(const std::string& oe_name, const std::string& ie_name,
                    const std::string& edata_name,
                    const std::string& snapshot_dir, size_t src_vertex_cap,
                    size_t dst_vertex_cap) override {
    in_csr_->open_in_memory(snapshot_dir + "/" + ie_name, dst_vertex_cap);
    out_csr_->open_in_memory(snapshot_dir + "/" + oe_name, src_vertex_cap);
    column_.open_in_memory(snapshot_dir + "/" + edata_name);
    column_idx_.store(column_.size());
    column_.resize(std::max(column_.size() + (column_.size() + 4) / 5, 4096ul));
  }

  void OpenWithHugepages(const std::string& oe_name, const std::string& ie_name,
                         const std::string& edata_name,
                         const std::string& snapshot_dir, size_t src_vertex_cap,
                         size_t dst_vertex_cap) override {
    LOG(FATAL) << "not supported...";
  }

  void Dump(const std::string& oe_name, const std::string& ie_name,
            const std::string& edata_name,
            const std::string& new_snapshot_dir) override {
    in_csr_->dump(ie_name, new_snapshot_dir);
    out_csr_->dump(oe_name, new_snapshot_dir);
    column_.resize(column_idx_.load());
    column_.dump_without_close(new_snapshot_dir + "/" + edata_name);
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
    column_.set_value(row_id, prop);
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
    if (index != std::numeric_limits<size_t>::max()) {
      column_.set_value(index, prop);
    } else {
      size_t row_id = column_idx_.fetch_add(1);
      column_.set_value(row_id, prop);
      in_csr_->put_edge_with_index(dst, src, row_id, ts, alloc);
      out_csr_->put_edge_with_index(src, dst, row_id, ts, alloc);
    }
  }

  void BatchPutEdge(vid_t src, vid_t dst, const std::string_view& data) {
    size_t row_id = column_idx_.fetch_add(1);
    column_.set_value(row_id, data);
    in_csr_->batch_put_edge_with_index(dst, src, row_id);
    out_csr_->batch_put_edge_with_index(src, dst, row_id);
  }

  void BatchPutEdge(vid_t src, vid_t dst, const std::string& data) {
    size_t row_id = column_idx_.fetch_add(1);
    column_.set_value(row_id, data);

    in_csr_->batch_put_edge_with_index(dst, src, row_id);
    out_csr_->batch_put_edge_with_index(src, dst, row_id);
  }

  void BatchAppendEdge(vid_t src, vid_t dst, const std::string_view& data) {
    size_t row_id = column_idx_.fetch_add(1);
    column_.set_value(row_id, data);
    in_csr_->batch_put_edge_with_index(dst, src, row_id);
    out_csr_->batch_put_edge_with_index(src, dst, row_id);
  }

  void BatchAppendEdge(vid_t src, vid_t dst, const std::string& data) {
    size_t row_id = column_idx_.fetch_add(1);
    column_.set_value(row_id, data);

    in_csr_->batch_put_edge_with_index(dst, src, row_id);
    out_csr_->batch_put_edge_with_index(src, dst, row_id);
  }

  void Close() override {
    in_csr_->close();
    out_csr_->close();
    column_.close();
  }

  TypedCsrBase<std::string_view>* take_in_csr() {
    auto in_csr = in_csr_;
    in_csr_ = nullptr;
    return in_csr;
  }

  TypedCsrBase<std::string_view>* take_out_csr() {
    auto out_csr = out_csr_;
    out_csr_ = nullptr;
    return out_csr;
  }

  StringColumn&& take_string_column() { return std::move(column_); }

 private:
  TypedCsrBase<std::string_view>* in_csr_;
  TypedCsrBase<std::string_view>* out_csr_;
  std::atomic<size_t> column_idx_;
  StringColumn column_;
};

template <>
class DualCsr<RecordView> : public DualCsrBase {
 public:
  DualCsr(EdgeStrategy oe_strategy, EdgeStrategy ie_strategy,
          const std::vector<std::string>& col_name,
          const std::vector<PropertyType>& property_types,
          const std::vector<StorageStrategy>& storage_strategies,
          bool oe_mutable, bool ie_mutable)
      : col_name_(col_name),
        property_types_(property_types),
        storage_strategies_(storage_strategies),
        in_csr_(nullptr),
        out_csr_(nullptr) {
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

  void add_property(const std::string& col_name,
                    const PropertyType& property_type,
                    std::shared_ptr<ColumnBase> column) {
    if (table_.col_num() != 0) {
      if (column->size() != table_.row_num()) {
        LOG(ERROR) << "The number of rows in the column is inconsistent with "
                      "the number of rows in the table.";
        return;
      }
    }
    if (std::find(col_name_.begin(), col_name_.end(), col_name) !=
        col_name_.end()) {
      LOG(ERROR) << "Column " << col_name << " already exists.";
      return;
    }
    col_name_.push_back(col_name);
    property_types_.push_back(property_type);
    storage_strategies_.push_back(StorageStrategy::kMem);
    table_.add_column(col_name, property_type, column);
  }

  void add_properties(const std::vector<std::string>& col_name,
                      const std::vector<PropertyType>& property_types) {
    CHECK(col_name.size() == property_types.size());
    for (auto& col : col_name) {
      if (std::find(col_name_.begin(), col_name_.end(), col) !=
          col_name_.end()) {
        LOG(ERROR) << "Column " << col << " already exists.";
        return;
      }
    }
    for (size_t i = 0; i < col_name.size(); ++i) {
      col_name_.push_back(col_name[i]);
      property_types_.push_back(property_types[i]);
      storage_strategies_.push_back(StorageStrategy::kMem);
    }

    table_.add_columns(col_name, property_types);
  }

  void delete_properties(const std::vector<std::string>& col_name) {
    for (const auto& col : col_name) {
      auto it = std::find(col_name_.begin(), col_name_.end(), col);
      if (it == col_name_.end()) {
        LOG(ERROR) << "Column " << col << " does not exist.";
        return;
      }
    }
    for (const auto& col : col_name) {
      auto it = std::find(col_name_.begin(), col_name_.end(), col);
      size_t index = std::distance(col_name_.begin(), it);
      col_name_.erase(it);
      property_types_.erase(property_types_.begin() + index);
      storage_strategies_.erase(storage_strategies_.begin() + index);
    }
    // Remove columns from the table
    for (const auto& col : col_name) {
      table_.delete_column(col);
    }
  }

  void rename_properties(const std::vector<std::string>& old_names,
                         const std::vector<std::string>& new_names) {
    CHECK_EQ(old_names.size(), new_names.size());
    for (size_t i = 0; i < old_names.size(); ++i) {
      auto it = std::find(col_name_.begin(), col_name_.end(), old_names[i]);
      if (it == col_name_.end()) {
        LOG(ERROR) << "Column " << old_names[i] << " does not exist.";
        return;
      }
      if (std::find(col_name_.begin(), col_name_.end(), new_names[i]) !=
          col_name_.end()) {
        LOG(ERROR) << "Column " << new_names[i] << " already exists.";
        return;
      }
      size_t index = std::distance(col_name_.begin(), it);
      col_name_[index] = new_names[i];
      table_.rename_column(old_names[i], new_names[i]);
    }
  }

  void InitTable(const std::string& edata_name, const std::string& work_dir) {
    table_.init(edata_name, work_dir, col_name_, property_types_,
                storage_strategies_);
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
            const std::string& work_dir) override {
    in_csr_->open(ie_name, snapshot_dir, work_dir);
    out_csr_->open(oe_name, snapshot_dir, work_dir);

    // fix me: storage_strategies_ is not used
    table_.open(edata_name, snapshot_dir, work_dir, col_name_, property_types_,
                {});
    table_idx_.store(table_.row_num());
    table_.resize(
        std::max(table_.row_num() + (table_.row_num() + 4) / 5, 4096ul));
  }

  void OpenInMemory(const std::string& oe_name, const std::string& ie_name,
                    const std::string& edata_name,
                    const std::string& snapshot_dir, size_t src_vertex_cap,
                    size_t dst_vertex_cap) override {
    in_csr_->open_in_memory(snapshot_dir + "/" + ie_name, dst_vertex_cap);
    out_csr_->open_in_memory(snapshot_dir + "/" + oe_name, src_vertex_cap);
    // fix me: storage_strategies_ is not used
    table_.open_in_memory(edata_name, snapshot_dir, col_name_, property_types_,
                          {});
    table_idx_.store(table_.row_num());
    table_.resize(
        std::max(table_.row_num() + (table_.row_num() + 4) / 5, 4096ul));
  }

  void OpenWithHugepages(const std::string& oe_name, const std::string& ie_name,
                         const std::string& edata_name,
                         const std::string& snapshot_dir, size_t src_vertex_cap,
                         size_t dst_vertex_cap) override {
    LOG(FATAL) << "not supported...";
  }

  void Dump(const std::string& oe_name, const std::string& ie_name,
            const std::string& edata_name,
            const std::string& new_snapshot_dir) override {
    in_csr_->dump(ie_name, new_snapshot_dir);
    out_csr_->dump(oe_name, new_snapshot_dir);
    table_.resize(table_idx_.load());
    table_.dump(edata_name, new_snapshot_dir);
  }

  CsrBase* GetInCsr() override { return in_csr_; }
  CsrBase* GetOutCsr() override { return out_csr_; }
  const CsrBase* GetInCsr() const override { return in_csr_; }
  const CsrBase* GetOutCsr() const override { return out_csr_; }

  void SetInCsr() {}

  void SetOutCsr() {}

  void BatchPutEdge(vid_t src, vid_t dst, size_t row_id) {
    in_csr_->batch_put_edge_with_index(dst, src, row_id);
    out_csr_->batch_put_edge_with_index(src, dst, row_id);
  }

  void BatchAppendEdge(vid_t src, vid_t dst, size_t row_id) {
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

  void Close() override {
    in_csr_->close();
    out_csr_->close();
    table_.close();
  }

  void SetInCsr(std::unique_ptr<TypedCsrBase<size_t>>&& in_csr) {
    in_csr_->set_csr(in_csr);
  }

  void SetOutCsr(std::unique_ptr<TypedCsrBase<size_t>>&& out_csr) {
    out_csr_->set_csr(out_csr);
  }

 private:
  std::vector<std::string> col_name_;
  std::vector<PropertyType> property_types_;
  std::vector<StorageStrategy> storage_strategies_;
  TypedCsrBase<RecordView>* in_csr_;
  TypedCsrBase<RecordView>* out_csr_;
  std::atomic<size_t> table_idx_;
  Table table_;
};

}  // namespace gs

#endif  // STORAGES_RT_MUTABLE_GRAPH_DUAL_CSR_H_
