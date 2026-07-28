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

#include <string>
#include <utility>
#include <vector>

#include "neug/common/types/value.h"
#include "neug/storages/allocators.h"
#include "neug/storages/csr/csr_view.h"
#include "neug/storages/csr/nbr.h"
#include "neug/storages/module/module.h"
#include "neug/utils/property/types.h"

#include <glog/logging.h>

namespace neug {

// CsrType is defined in csr_view.h to avoid a csr_base <-> csr_view
// include cycle (CsrView may need to know about CsrType in future).

class CsrBase : public Module {
 public:
  static constexpr size_t INFINITE_CAPACITY =
      std::numeric_limits<size_t>::max();
  CsrBase() = default;
  virtual ~CsrBase() = default;

  virtual CsrType csr_type() const = 0;

  virtual CsrView get_generic_view(timestamp_t ts) const = 0;

  virtual timestamp_t unsorted_since() const { return 0; }

  virtual size_t size() const = 0;
  // Returns the number of edges in the graph. Note that the returned value is
  // exactly the number of edges in this csr. Even if there may be some reserved
  // space, the reserved space will count as 0.
  virtual size_t edge_num() const = 0;

  virtual void compact() = 0;

  virtual void resize(vid_t vnum) = 0;

  virtual size_t capacity() const = 0;

  virtual void batch_sort_by_edge_data(timestamp_t ts) {
    LOG(FATAL) << "not supported...";
  }

  virtual void batch_delete_vertices(const std::set<vid_t>& src_set,
                                     const std::set<vid_t>& dst_set) = 0;

  /**
   * @brief Batch delete edges.
   * @param edges The edges to be deleted, represented as pairs of
   * (source vertex id, offset in the neighbor list).
   */
  virtual void batch_delete_edges(
      const std::vector<std::pair<vid_t, int32_t>>& edges) = 0;

  virtual void batch_delete_edges(const std::vector<vid_t>& src_list,
                                  const std::vector<vid_t>& dst_list) = 0;

  virtual void delete_edge(vid_t src, int32_t offset, timestamp_t ts) = 0;

  virtual void revert_delete_edge(vid_t src, vid_t nbr, int32_t offset,
                                  timestamp_t ts) = 0;

  virtual std::pair<int32_t, const void*> put_generic_edge(
      vid_t src, vid_t dst, const Value& data, timestamp_t ts,
      Allocator& alloc) = 0;

  virtual std::tuple<std::vector<vid_t>, std::vector<vid_t>> batch_export(
      ColumnBase* prev_data_col) const = 0;

  /// Detach the adjacency list of vertex vid so subsequent writes
  /// are isolated from the parent CSR.  Must only be called on a COW CSR
  /// for a vertex whose adjlist has not yet been detached.  The caller
  /// (e.g. PropertyGraphCowState) is responsible for tracking which
  /// adjlists have been detached.
  virtual void DetachVertex(vid_t vid, Allocator& alloc) = 0;
};

template <typename EDATA_T>
class TypedCsrBase : public CsrBase {
 public:
  virtual void batch_put_edges(const std::vector<vid_t>& src_list,
                               const std::vector<vid_t>& dst_list,
                               const std::vector<EDATA_T>& data_list,
                               timestamp_t ts = 0) = 0;

  virtual std::pair<int32_t, const void*> put_edge(vid_t src, vid_t dst,
                                                   const EDATA_T& data,
                                                   timestamp_t ts,
                                                   Allocator& alloc) {
    LOG(FATAL) << "not supported...";
    return {0, nullptr};
  }

  std::pair<int32_t, const void*> put_generic_edge(vid_t src, vid_t dst,
                                                   const Value& data,
                                                   timestamp_t ts,
                                                   Allocator& alloc) override {
    return this->put_edge(src, dst, data.GetValue<EDATA_T>(), ts, alloc);
  }
};

}  // namespace neug
