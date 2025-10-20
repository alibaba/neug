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

#ifndef STORAGES_RT_MUTABLE_GRAPH_CSR_CSR_BASE_H_
#define STORAGES_RT_MUTABLE_GRAPH_CSR_CSR_BASE_H_

#include <string>
#include <vector>

#include "neug/storages/csr/generic_view.h"
#include "neug/storages/csr/nbr.h"
#include "neug/utils/allocators.h"
#include "neug/utils/property/types.h"

namespace gs {

enum class CsrType {
  kImmutable,
  kMutable,
  kSingleMutable,
  kSingleImmutable,
  kEmpty,
};

class CsrBase {
 public:
  CsrBase() = default;
  virtual ~CsrBase() = default;

  virtual CsrType csr_type() const = 0;

  virtual GenericView get_generic_view(timestamp_t ts) const = 0;

  virtual timestamp_t unsorted_since() const { return 0; }

  virtual size_t size() const = 0;
  // Returns the number of edges in the graph. Note that the returned value is
  // exactly the number of edges in this csr. Even if there may be some reserved
  // space, the reserved space will count as 0.
  virtual size_t edge_num() const = 0;

  virtual void open(const std::string& name, const std::string& snapshot_dir,
                    const std::string& work_dir) = 0;

  virtual void open_in_memory(const std::string& prefix, size_t v_cap) = 0;

  virtual void open_with_hugepages(const std::string& prefix,
                                   size_t v_cap = 0) = 0;

  virtual void dump(const std::string& name,
                    const std::string& new_snapshot_dir) = 0;

  virtual void warmup(int thread_num) const = 0;

  virtual void resize(vid_t vnum) = 0;

  virtual void close() = 0;

  virtual void batch_sort_by_edge_data(timestamp_t ts) {
    LOG(FATAL) << "not supported...";
  }

  virtual void batch_delete_vertices(const std::set<vid_t>& src_set,
                                     const std::set<vid_t>& dst_set) = 0;

  virtual void batch_delete_edges(const std::vector<vid_t>& src_list,
                                  const std::vector<vid_t>& dst_list) = 0;

  virtual void put_generic_edge(vid_t src, vid_t dst, const Prop& data,
                                timestamp_t ts, Allocator& alloc) = 0;

  virtual std::tuple<std::vector<vid_t>, std::vector<vid_t>> batch_export(
      std::shared_ptr<ColumnBase> prev_data_col) const = 0;
};

template <typename EDATA_T>
class TypedCsrBase : public CsrBase {
 public:
  virtual void batch_put_edges(const std::vector<vid_t>& src_list,
                               const std::vector<vid_t>& dst_list,
                               const std::vector<EDATA_T>& data_list,
                               timestamp_t ts = 0) = 0;

  virtual void put_edge(vid_t src, vid_t dst, const EDATA_T& data,
                        timestamp_t ts, Allocator& alloc) {
    LOG(FATAL) << "not supported...";
  }

  void put_generic_edge(vid_t src, vid_t dst, const Prop& data, timestamp_t ts,
                        Allocator& alloc) override {
    this->put_edge(src, dst, PropUtils<EDATA_T>::to_typed(data), ts, alloc);
  }
};

}  // namespace gs

#endif  // STORAGES_RT_MUTABLE_GRAPH_CSR_CSR_BASE_H_