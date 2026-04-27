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

#include "neug/storages/csr/immutable_csr.h"

#include "neug/storages/module/module_factory.h"

#include <glog/logging.h>
#include <stdio.h>
#include <string.h>
#include <algorithm>
#include <atomic>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <map>
#include <memory>
#include <thread>
#include <utility>

#include "neug/storages/container/container_utils.h"
#include "neug/storages/container/i_container.h"
#include "neug/utils/property/types.h"

namespace neug {

template <typename EDATA_T>
void ImmutableCsr<EDATA_T>::Open(Checkpoint& ckp, const ModuleDescriptor& desc,
                                 MemoryLevel memory_level) {
  Close();
  unsorted_since_ = std::stoull(desc.get("unsorted_since").value_or("0"));
  edge_num_.store(std::stoull(desc.get("edge_num").value_or("0")));
  degree_list_buffer_ = ckp.OpenFile(
      desc.get_sub_module_or_default("degree_list").path, memory_level);
  nbr_list_buffer_ = ckp.OpenFile(
      desc.get_sub_module_or_default("nbr_list").path, memory_level);
  auto v_cap = size();
  adj_list_buffer_ = ckp.CreateRuntimeContainer(v_cap * sizeof(nbr_t*),
                                                MemoryLevel::kInMemory);
  auto adj_lists_ptr = reinterpret_cast<nbr_t**>(adj_list_buffer_->GetData());
  auto degree_list_ptr =
      reinterpret_cast<const int*>(degree_list_buffer_->GetData());
  auto nbr_list_ptr = reinterpret_cast<nbr_t*>(nbr_list_buffer_->GetData());
  nbr_t* cur_nbr_list_ptr = nbr_list_ptr;
  for (size_t i = 0; i < v_cap; ++i) {
    int deg = degree_list_ptr[i];
    if (deg != 0) {
      adj_lists_ptr[i] = cur_nbr_list_ptr;
    } else {
      adj_lists_ptr[i] = NULL;
    }
    cur_nbr_list_ptr += deg;
  }
}

template <typename EDATA_T>
ModuleDescriptor ImmutableCsr<EDATA_T>::Dump(Checkpoint& ckp) {
  ModuleDescriptor desc;
  desc.module_type = ModuleTypeName();
  desc.set("unsorted_since", std::to_string(unsorted_since_));
  desc.set("edge_num", std::to_string(edge_num_.load()));
  ModuleDescriptor degree_desc, nbr_desc;
  degree_desc.size = degree_list_buffer_->GetDataSize();
  nbr_desc.size = nbr_list_buffer_->GetDataSize();
  degree_desc.path = ckp.Commit(*degree_list_buffer_);
  nbr_desc.path = ckp.Commit(*nbr_list_buffer_);
  desc.set_sub_module("degree_list", std::move(degree_desc));
  desc.set_sub_module("nbr_list", std::move(nbr_desc));
  return desc;
}

template <typename EDATA_T>
void ImmutableCsr<EDATA_T>::reset_timestamp() {}

template <typename EDATA_T>
void ImmutableCsr<EDATA_T>::compact() {
  // For current adj_list where the dst vertex is invalid, swap it to the end.
  vid_t vnum = size();
  if (vnum <= 0) {
    return;
  }
  size_t removed = 0;
  auto** adj_arr = reinterpret_cast<nbr_t**>(adj_list_buffer_->GetData());
  auto* deg_arr = reinterpret_cast<int*>(degree_list_buffer_->GetData());
  nbr_t* write_ptr = adj_arr[0];
  for (vid_t i = 0; i < vnum; ++i) {
    int deg = deg_arr[i];
    if (deg == 0) {
      continue;
    }
    const nbr_t* read_ptr = adj_arr[i];
    const nbr_t* read_end = read_ptr + deg;
    while (read_ptr != read_end) {
      if (read_ptr->neighbor != std::numeric_limits<vid_t>::max()) {
        if (removed) {
          *write_ptr = *read_ptr;
        }
        ++write_ptr;
      } else {
        --deg_arr[i];
        ++removed;
      }
      ++read_ptr;
    }
  }
  nbr_list_buffer_->Resize(nbr_list_buffer_->GetDataSize() -
                           removed * sizeof(nbr_t));
  nbr_t* ptr = reinterpret_cast<nbr_t*>(nbr_list_buffer_->GetData());
  for (vid_t i = 0; i < vnum; ++i) {
    adj_arr[i] = ptr;
    ptr += deg_arr[i];
  }
}

template <typename EDATA_T>
void ImmutableCsr<EDATA_T>::resize(vid_t vnum) {
  auto old_v_cap = size();
  auto** adj_arr = reinterpret_cast<nbr_t**>(adj_list_buffer_->GetData());
  auto* deg_arr = reinterpret_cast<int*>(degree_list_buffer_->GetData());
  if (vnum > old_v_cap) {
    adj_list_buffer_->Resize(vnum * sizeof(nbr_t*));
    degree_list_buffer_->Resize(vnum * sizeof(int));
    adj_arr = reinterpret_cast<nbr_t**>(adj_list_buffer_->GetData());
    deg_arr = reinterpret_cast<int*>(degree_list_buffer_->GetData());
    for (vid_t i = old_v_cap; i < vnum; ++i) {
      adj_arr[i] = NULL;
      deg_arr[i] = 0;
    }
  } else {
    adj_list_buffer_->Resize(vnum * sizeof(nbr_t*));
    degree_list_buffer_->Resize(vnum * sizeof(int));
  }
}

template <typename EDATA_T>
size_t ImmutableCsr<EDATA_T>::capacity() const {
  // We assume the capacity of each csr is INFINITE.
  return CsrBase::INFINITE_CAPACITY;
}

template <typename EDATA_T>
void ImmutableCsr<EDATA_T>::Close() {
  if (adj_list_buffer_) {
    adj_list_buffer_->Close();
  }
  if (degree_list_buffer_) {
    degree_list_buffer_->Close();
  }
  if (nbr_list_buffer_) {
    nbr_list_buffer_->Close();
  }
}

template <typename EDATA_T>
void ImmutableCsr<EDATA_T>::batch_sort_by_edge_data(timestamp_t ts) {
  if (!degree_list_buffer_) {
    unsorted_since_ = ts;
    return;
  }
  vid_t vnum = size();
  auto** adj_arr = reinterpret_cast<nbr_t**>(adj_list_buffer_->GetData());
  const auto* deg_arr =
      reinterpret_cast<const int*>(degree_list_buffer_->GetData());
  for (size_t i = 0; i != vnum; ++i) {
    std::sort(
        adj_arr[i], adj_arr[i] + deg_arr[i],
        [](const nbr_t& lhs, const nbr_t& rhs) { return lhs.data < rhs.data; });
  }
  unsorted_since_ = ts;
}

template <typename EDATA_T>
void ImmutableCsr<EDATA_T>::batch_delete_vertices(
    const std::set<vid_t>& src_set, const std::set<vid_t>& dst_set) {
  vid_t vnum = size();
  auto** adj_arr = reinterpret_cast<nbr_t**>(adj_list_buffer_->GetData());
  auto* deg_arr = reinterpret_cast<int*>(degree_list_buffer_->GetData());
  size_t removed = 0;
  for (vid_t i = 0; i < vnum; ++i) {
    int deg = deg_arr[i];
    if (deg == 0) {
      continue;
    }
    if (src_set.find(i) != src_set.end()) {
      for (int j = 0; j < deg; ++j) {
        if (adj_arr[i][j].neighbor != std::numeric_limits<vid_t>::max()) {
          edge_num_.fetch_sub(1, std::memory_order_relaxed);
        }
      }
      removed += deg;
      deg_arr[i] = 0;
    } else {
      const nbr_t* old_ptr = adj_arr[i];
      const nbr_t* old_end = old_ptr + deg;
      nbr_t* new_ptr = adj_arr[i] - removed;
      while (old_ptr != old_end) {
        if (dst_set.find(old_ptr->neighbor) == dst_set.end()) {
          *new_ptr = *old_ptr;
          ++new_ptr;
        } else {
          --deg_arr[i];
          ++removed;
          if (old_ptr->neighbor != std::numeric_limits<vid_t>::max()) {
            edge_num_.fetch_sub(1, std::memory_order_relaxed);
          }
        }
        ++old_ptr;
      }
    }
  }
  nbr_list_buffer_->Resize(
      (nbr_list_buffer_->GetDataSize() - removed * sizeof(nbr_t)));
  nbr_t* ptr = reinterpret_cast<nbr_t*>(nbr_list_buffer_->GetData());
  for (vid_t i = 0; i < vnum; ++i) {
    adj_arr[i] = ptr;
    ptr += deg_arr[i];
  }
}

template <typename EDATA_T>
void ImmutableCsr<EDATA_T>::batch_delete_edges(
    const std::vector<vid_t>& src_list, const std::vector<vid_t>& dst_list) {
  std::map<vid_t, std::set<vid_t>> src_dst_map;
  vid_t vnum = size();
  for (size_t i = 0; i < src_list.size(); ++i) {
    if (src_list[i] >= vnum) {
      continue;
    }
    src_dst_map[src_list[i]].insert(dst_list[i]);
  }
  auto** adj_arr = reinterpret_cast<nbr_t**>(adj_list_buffer_->GetData());
  const auto* deg_arr =
      reinterpret_cast<const int*>(degree_list_buffer_->GetData());
  for (vid_t i = 0; i < vnum; ++i) {
    int deg = deg_arr[i];
    if (deg == 0) {
      continue;
    }
    auto iter = src_dst_map.find(i);
    if (iter != src_dst_map.end()) {
      const std::set<vid_t>& dst_set = iter->second;
      nbr_t* write_ptr = adj_arr[i];
      const nbr_t* read_end = write_ptr + deg_arr[i];
      while (write_ptr != read_end) {
        if (write_ptr->neighbor != std::numeric_limits<vid_t>::max() &&
            dst_set.find(write_ptr->neighbor) != dst_set.end()) {
          write_ptr->neighbor = std::numeric_limits<vid_t>::max();
          edge_num_.fetch_sub(1, std::memory_order_relaxed);
        }
        ++write_ptr;
      }
    }
  }
}

template <typename EDATA_T>
void ImmutableCsr<EDATA_T>::batch_delete_edges(
    const std::vector<std::pair<vid_t, int32_t>>& edges) {
  std::map<vid_t, std::set<int32_t>> src_offset_map;
  vid_t vnum = size();
  auto** adj_arr = reinterpret_cast<nbr_t**>(adj_list_buffer_->GetData());
  auto* deg_arr = reinterpret_cast<int*>(degree_list_buffer_->GetData());
  for (const auto& edge : edges) {
    if (edge.first >= vnum || edge.second >= deg_arr[edge.first]) {
      continue;
    }
    src_offset_map[edge.first].insert(edge.second);
  }
  for (vid_t i = 0; i < vnum; ++i) {
    int deg = deg_arr[i];
    if (deg == 0) {
      continue;
    }
    auto iter = src_offset_map.find(i);
    if (iter != src_offset_map.end()) {
      nbr_t* write_ptr = adj_arr[i];
      for (const auto& offset : iter->second) {
        write_ptr[offset].neighbor = std::numeric_limits<vid_t>::max();
        edge_num_.fetch_sub(1, std::memory_order_relaxed);
      }
    }
  }
}

template <typename EDATA_T>
void ImmutableCsr<EDATA_T>::delete_edge(vid_t src, int32_t offset,
                                        timestamp_t ts) {
  vid_t vnum = size();
  const auto* deg_arr =
      reinterpret_cast<const int*>(degree_list_buffer_->GetData());
  if (src >= vnum || offset >= deg_arr[src]) {
    THROW_INVALID_ARGUMENT_EXCEPTION("src out of bound or offset out of bound");
  }
  nbr_t* nbrs = reinterpret_cast<nbr_t**>(adj_list_buffer_->GetData())[src];
  if (nbrs[offset].neighbor == std::numeric_limits<vid_t>::max()) {
    LOG(ERROR) << "Fail to delete edge, already deleted.";
    return;
  }
  nbrs[offset].neighbor = std::numeric_limits<vid_t>::max();
  edge_num_.fetch_sub(1, std::memory_order_relaxed);
}

template <typename EDATA_T>
void ImmutableCsr<EDATA_T>::revert_delete_edge(vid_t src, vid_t nbr,
                                               int32_t offset, timestamp_t ts) {
  vid_t vnum = size();
  const auto* deg_arr =
      reinterpret_cast<const int*>(degree_list_buffer_->GetData());
  if (src >= vnum || offset >= deg_arr[src]) {
    THROW_INVALID_ARGUMENT_EXCEPTION("src out of bound or offset out of bound");
  }
  nbr_t* nbrs = reinterpret_cast<nbr_t**>(adj_list_buffer_->GetData())[src];
  if (nbrs[offset].neighbor != std::numeric_limits<vid_t>::max()) {
    THROW_INVALID_ARGUMENT_EXCEPTION(
        "Attempting to revert delete on edge that is not deleted.");
  }
  nbrs[offset].neighbor = nbr;
  edge_num_.fetch_add(1, std::memory_order_relaxed);
}

template <typename EDATA_T>
void ImmutableCsr<EDATA_T>::batch_put_edges(
    const std::vector<vid_t>& src_list, const std::vector<vid_t>& dst_list,
    const std::vector<EDATA_T>& data_list, timestamp_t ts) {
  auto old_edge_num = nbr_list_buffer_->GetDataSize() / sizeof(nbr_t);
  auto v_cap = size();
  auto** adj_arr = reinterpret_cast<nbr_t**>(adj_list_buffer_->GetData());
  auto* deg_arr = reinterpret_cast<int*>(degree_list_buffer_->GetData());
  std::vector<int> old_degree_list(v_cap);
  memcpy(old_degree_list.data(), degree_list_buffer_->GetData(),
         sizeof(int) * v_cap);
  for (size_t i = 0; i < src_list.size(); ++i) {
    ++deg_arr[src_list[i]];
  }
  size_t new_edge_num = old_edge_num + src_list.size();
  nbr_list_buffer_->Resize(new_edge_num * sizeof(nbr_t));

  size_t new_edge_offset = new_edge_num;
  size_t old_edge_offset = old_edge_num;
  auto* nbr_arr = reinterpret_cast<nbr_t*>(nbr_list_buffer_->GetData());
  for (int64_t i = v_cap - 1; i >= 0; --i) {
    new_edge_offset -= deg_arr[i];
    old_edge_offset -= old_degree_list[i];
    adj_arr[i] = nbr_arr + new_edge_offset;
    memmove(nbr_arr + new_edge_offset, nbr_arr + old_edge_offset,
            sizeof(nbr_t) * old_degree_list[i]);
  }
  for (size_t i = 0; i < src_list.size(); ++i) {
    vid_t src = src_list[i];
    auto& nbr = adj_arr[src][old_degree_list[src]++];
    nbr.neighbor = dst_list[i];
    nbr.data = data_list[i];
    edge_num_.fetch_add(1, std::memory_order_relaxed);
  }
}

template <typename EDATA_T>
void SingleImmutableCsr<EDATA_T>::Open(Checkpoint& ckp,
                                       const ModuleDescriptor& descriptor,
                                       MemoryLevel memory_level) {
  nbr_list_buffer_ = ckp.OpenFile(
      descriptor.get_sub_module_or_default("nbr_list").path, memory_level);
  edge_num_.store(std::stoull(descriptor.get("edge_num").value_or("0")));
}

template <typename EDATA_T>
ModuleDescriptor SingleImmutableCsr<EDATA_T>::Dump(Checkpoint& ckp) {
  ModuleDescriptor desc;
  desc.module_type = ModuleTypeName();
  ModuleDescriptor nbr_desc;
  nbr_desc.size = nbr_list_buffer_->GetDataSize();
  nbr_desc.path = ckp.Commit(*nbr_list_buffer_);
  desc.set_sub_module("nbr_list", std::move(nbr_desc));
  desc.set("edge_num", std::to_string(edge_num_.load()));
  return desc;
}

template <typename EDATA_T>
void SingleImmutableCsr<EDATA_T>::reset_timestamp() {}

template <typename EDATA_T>
void SingleImmutableCsr<EDATA_T>::compact() {}

template <typename EDATA_T>
void SingleImmutableCsr<EDATA_T>::resize(vid_t vnum) {
  auto old_size = size();
  nbr_list_buffer_->Resize(vnum * sizeof(nbr_t));
  if (vnum > old_size) {
    auto* nbr_arr = reinterpret_cast<nbr_t*>(nbr_list_buffer_->GetData());
    for (size_t k = old_size; k != vnum; ++k) {
      nbr_arr[k].neighbor = std::numeric_limits<vid_t>::max();
    }
  }
}

template <typename EDATA_T>
size_t SingleImmutableCsr<EDATA_T>::capacity() const {
  return size();
}

template <typename EDATA_T>
void SingleImmutableCsr<EDATA_T>::Close() {
  if (nbr_list_buffer_) {
    nbr_list_buffer_->Close();
  }
}

template <typename EDATA_T>
void SingleImmutableCsr<EDATA_T>::batch_sort_by_edge_data(timestamp_t ts) {}

template <typename EDATA_T>
void SingleImmutableCsr<EDATA_T>::batch_delete_vertices(
    const std::set<vid_t>& src_set, const std::set<vid_t>& dst_set) {
  vid_t vnum = size();
  auto* nbr_arr = reinterpret_cast<nbr_t*>(nbr_list_buffer_->GetData());
  for (auto src : src_set) {
    if (src >= vnum) {
      continue;
    }
    if (nbr_arr[src].neighbor != std::numeric_limits<vid_t>::max()) {
      edge_num_.fetch_sub(1, std::memory_order_relaxed);
      nbr_arr[src].neighbor = std::numeric_limits<vid_t>::max();
    }
  }
  for (vid_t i = 0; i < vnum; ++i) {
    auto nbr = nbr_arr[i].neighbor;
    if (nbr != std::numeric_limits<vid_t>::max() &&
        dst_set.find(nbr) != dst_set.end()) {
      nbr_arr[i].neighbor = std::numeric_limits<vid_t>::max();
      edge_num_.fetch_sub(1, std::memory_order_relaxed);
    }
  }
}

template <typename EDATA_T>
void SingleImmutableCsr<EDATA_T>::batch_delete_edges(
    const std::vector<vid_t>& src_list, const std::vector<vid_t>& dst_list) {
  vid_t vnum = size();
  auto* nbr_arr = reinterpret_cast<nbr_t*>(nbr_list_buffer_->GetData());
  for (size_t i = 0; i < src_list.size(); ++i) {
    vid_t src = src_list[i];
    if (src >= vnum) {
      continue;
    }
    vid_t dst = dst_list[i];
    if (nbr_arr[src].neighbor == dst) {
      if (nbr_arr[src].neighbor != std::numeric_limits<vid_t>::max()) {
        edge_num_.fetch_sub(1, std::memory_order_relaxed);
        nbr_arr[src].neighbor = std::numeric_limits<vid_t>::max();
      }
    }
  }
}

template <typename EDATA_T>
void SingleImmutableCsr<EDATA_T>::batch_delete_edges(
    const std::vector<std::pair<vid_t, int32_t>>& edges) {
  vid_t vnum = size();
  auto* nbr_arr = reinterpret_cast<nbr_t*>(nbr_list_buffer_->GetData());
  for (const auto& edge : edges) {
    vid_t src = edge.first;
    if (src >= vnum) {
      continue;
    }
    assert(edge.second == 0);
    if (nbr_arr[src].neighbor != std::numeric_limits<vid_t>::max()) {
      nbr_arr[src].neighbor = std::numeric_limits<vid_t>::max();
      edge_num_.fetch_sub(1, std::memory_order_relaxed);
    }
  }
}

template <typename EDATA_T>
void SingleImmutableCsr<EDATA_T>::delete_edge(vid_t src, int32_t offset,
                                              timestamp_t ts) {
  vid_t vnum = size();
  if (src >= vnum || offset != 0) {
    THROW_INVALID_ARGUMENT_EXCEPTION("src out of bound or offset out of bound");
    return;
  }
  auto* nbr_arr = reinterpret_cast<nbr_t*>(nbr_list_buffer_->GetData());
  if (nbr_arr[src].neighbor == std::numeric_limits<vid_t>::max()) {
    LOG(ERROR) << "Fail to delete edge, already deleted.";
    return;
  }
  nbr_arr[src].neighbor = std::numeric_limits<vid_t>::max();
  edge_num_.fetch_sub(1, std::memory_order_relaxed);
}

template <typename EDATA_T>
void SingleImmutableCsr<EDATA_T>::revert_delete_edge(vid_t src, vid_t nbr,
                                                     int32_t offset,
                                                     timestamp_t ts) {
  vid_t vnum = size();
  if (src >= vnum || offset != 0) {
    THROW_INVALID_ARGUMENT_EXCEPTION("src out of bound or offset out of bound");
  }
  auto* nbr_arr = reinterpret_cast<nbr_t*>(nbr_list_buffer_->GetData());
  if (nbr_arr[src].neighbor != std::numeric_limits<vid_t>::max()) {
    THROW_INVALID_ARGUMENT_EXCEPTION(
        "Attempting to revert delete on edge that is not deleted.");
  }
  nbr_arr[src].neighbor = nbr;
  edge_num_.fetch_add(1, std::memory_order_relaxed);
}

template <typename EDATA_T>
void SingleImmutableCsr<EDATA_T>::batch_put_edges(
    const std::vector<vid_t>& src_list, const std::vector<vid_t>& dst_list,
    const std::vector<EDATA_T>& data_list, timestamp_t) {
  vid_t vnum = size();
  auto* nbr_arr = reinterpret_cast<nbr_t*>(nbr_list_buffer_->GetData());
  for (size_t i = 0; i < src_list.size(); ++i) {
    vid_t src = src_list[i];
    if (src >= vnum) {
      continue;
    }
    auto& nbr = nbr_arr[src];
    if (nbr.neighbor != std::numeric_limits<vid_t>::max()) {
      LOG(ERROR) << "Fail to put edge, edge already exists.";
      continue;
    }
    nbr.neighbor = dst_list[i];
    nbr.data = data_list[i];
    edge_num_.fetch_add(1, std::memory_order_relaxed);
  }
}

template class ImmutableCsr<int32_t>;
template class ImmutableCsr<uint32_t>;
template class ImmutableCsr<int64_t>;
template class ImmutableCsr<uint64_t>;
template class ImmutableCsr<float>;
template class ImmutableCsr<double>;
template class ImmutableCsr<EmptyType>;
template class ImmutableCsr<Date>;
template class ImmutableCsr<DateTime>;
template class ImmutableCsr<Interval>;
template class ImmutableCsr<bool>;

template class SingleImmutableCsr<int32_t>;
template class SingleImmutableCsr<uint32_t>;
template class SingleImmutableCsr<int64_t>;
template class SingleImmutableCsr<uint64_t>;
template class SingleImmutableCsr<float>;
template class SingleImmutableCsr<double>;
template class SingleImmutableCsr<EmptyType>;
template class SingleImmutableCsr<Date>;
template class SingleImmutableCsr<DateTime>;
template class SingleImmutableCsr<Interval>;
template class SingleImmutableCsr<bool>;

NEUG_REGISTER_TEMPLATE_MODULE(ImmutableCsr, EmptyType);
NEUG_REGISTER_TEMPLATE_MODULE(ImmutableCsr, bool);
NEUG_REGISTER_TEMPLATE_MODULE(ImmutableCsr, int32_t);
NEUG_REGISTER_TEMPLATE_MODULE(ImmutableCsr, uint32_t);
NEUG_REGISTER_TEMPLATE_MODULE(ImmutableCsr, int64_t);
NEUG_REGISTER_TEMPLATE_MODULE(ImmutableCsr, uint64_t);
NEUG_REGISTER_TEMPLATE_MODULE(ImmutableCsr, float);
NEUG_REGISTER_TEMPLATE_MODULE(ImmutableCsr, double);
NEUG_REGISTER_TEMPLATE_MODULE(ImmutableCsr, Date);
NEUG_REGISTER_TEMPLATE_MODULE(ImmutableCsr, DateTime);
NEUG_REGISTER_TEMPLATE_MODULE(ImmutableCsr, Interval);

NEUG_REGISTER_TEMPLATE_MODULE(SingleImmutableCsr, EmptyType);
NEUG_REGISTER_TEMPLATE_MODULE(SingleImmutableCsr, bool);
NEUG_REGISTER_TEMPLATE_MODULE(SingleImmutableCsr, int32_t);
NEUG_REGISTER_TEMPLATE_MODULE(SingleImmutableCsr, uint32_t);
NEUG_REGISTER_TEMPLATE_MODULE(SingleImmutableCsr, int64_t);
NEUG_REGISTER_TEMPLATE_MODULE(SingleImmutableCsr, uint64_t);
NEUG_REGISTER_TEMPLATE_MODULE(SingleImmutableCsr, float);
NEUG_REGISTER_TEMPLATE_MODULE(SingleImmutableCsr, double);
NEUG_REGISTER_TEMPLATE_MODULE(SingleImmutableCsr, Date);
NEUG_REGISTER_TEMPLATE_MODULE(SingleImmutableCsr, DateTime);
NEUG_REGISTER_TEMPLATE_MODULE(SingleImmutableCsr, Interval);

}  // namespace neug