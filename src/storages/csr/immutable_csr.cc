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

#include <glog/logging.h>
#include <stdio.h>
#include <string.h>
#include <algorithm>
#include <atomic>
#include <cstdint>
#include <filesystem>
#include <map>
#include <memory>
#include <thread>
#include <utility>

#include "neug/storages/container/i_container.h"
#include "neug/storages/container_utils.h"
#include "neug/storages/file_names.h"
#include "neug/utils/property/types.h"

namespace neug {

template <typename NbrType>
void initialize_adj_lists(NbrType** adj_lists_ptr, const int* degree_list_ptr,
                          NbrType* nbr_list_ptr, size_t num_vertices) {
  NbrType* cur_nbr_list_ptr = nbr_list_ptr;
  for (size_t i = 0; i < num_vertices; ++i) {
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
void ImmutableCsr<EDATA_T>::open_internal(const std::string& snapshot_prefix,
                                          const std::string& tmp_prefix,
                                          MemoryLevel mem_level) {
  close();
  load_meta(snapshot_prefix);
  degree_list_buffer_ = prepare_and_open_container(
      snapshot_prefix + ".deg", tmp_prefix + ".deg", mem_level);
  nbr_list_buffer_ = prepare_and_open_container(snapshot_prefix + ".nbr",
                                                tmp_prefix + ".nbr", mem_level);
  if (mem_level == MemoryLevel::kSyncToFile) {
    adj_list_buffer_ =
        prepare_and_open_container("", tmp_prefix + ".adj", mem_level);
  } else {
    adj_list_buffer_ = prepare_and_open_container("", "", mem_level);
  }
  auto v_cap = size();
  adj_list_buffer_->Resize(v_cap * sizeof(nbr_t*));
  initialize_adj_lists(
      reinterpret_cast<nbr_t**>(adj_list_buffer_->GetData()),
      reinterpret_cast<const int*>(degree_list_buffer_->GetData()),
      reinterpret_cast<nbr_t*>(nbr_list_buffer_->GetData()), v_cap);
}

template <typename EDATA_T>
void ImmutableCsr<EDATA_T>::open(const std::string& name,
                                 const std::string& snapshot_dir,
                                 const std::string& work_dir) {
  // Changes made to the CSR will not be synchronized to the file
  // TODO(luoxiaojian): Implement the insert operation on ImmutableCsr.
  // Allow an empty or missing snapshot_dir: the underlying helpers already
  // handle absent files by falling back to empty containers.  A subsequent
  // resize() / insert call will allocate storage as needed.
  std::string snap_prefix =
      (!snapshot_dir.empty() && std::filesystem::exists(snapshot_dir))
          ? snapshot_dir + "/" + name
          : "";
  auto tmp_prefix = tmp_dir(work_dir) + "/" + name;
  open_internal(snap_prefix, tmp_prefix, MemoryLevel::kSyncToFile);
}

template <typename EDATA_T>
void ImmutableCsr<EDATA_T>::open_in_memory(const std::string& prefix) {
  open_internal(prefix, "", MemoryLevel::kInMemory);
}

template <typename EDATA_T>
void ImmutableCsr<EDATA_T>::open_with_hugepages(const std::string& prefix) {
  open_internal(prefix, "", MemoryLevel::kHugePagePrefered);
}

template <typename EDATA_T>
void ImmutableCsr<EDATA_T>::dump(const std::string& name,
                                 const std::string& new_snapshot_dir) {
  dump_meta(new_snapshot_dir + "/" + name);
  degree_list_buffer_->Dump(new_snapshot_dir + "/" + name + ".deg");
  nbr_list_buffer_->Dump(new_snapshot_dir + "/" + name + ".nbr");
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
void ImmutableCsr<EDATA_T>::close() {
  CloseAndReset(adj_list_buffer_);
  CloseAndReset(degree_list_buffer_);
  CloseAndReset(nbr_list_buffer_);
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
  }
}

template <typename EDATA_T>
void ImmutableCsr<EDATA_T>::load_meta(const std::string& prefix) {
  std::string meta_file_path = prefix + ".meta";
  if (std::filesystem::exists(meta_file_path)) {
    FILE* meta_file_fd = fopen(meta_file_path.c_str(), "r");
    CHECK_EQ(fread(&unsorted_since_, sizeof(timestamp_t), 1, meta_file_fd), 1);
    fclose(meta_file_fd);
  } else {
    unsorted_since_ = 0;
  }
}

template <typename EDATA_T>
void ImmutableCsr<EDATA_T>::dump_meta(const std::string& prefix) const {
  std::string meta_file_path = prefix + ".meta";
  FILE* meta_file_fd = fopen((prefix + ".meta").c_str(), "wb");
  CHECK_EQ(fwrite(&unsorted_since_, sizeof(timestamp_t), 1, meta_file_fd), 1);
  fflush(meta_file_fd);
  fclose(meta_file_fd);
}

template <typename EDATA_T>
void SingleImmutableCsr<EDATA_T>::open(const std::string& name,
                                       const std::string& snapshot_dir,
                                       const std::string& work_dir) {
  nbr_list_buffer_ = prepare_and_open_container(
      snapshot_dir + "/" + name + ".snbr",
      tmp_dir(work_dir) + "/" + name + ".snbr", MemoryLevel::kSyncToFile);
}

template <typename EDATA_T>
void SingleImmutableCsr<EDATA_T>::open_in_memory(const std::string& prefix) {
  nbr_list_buffer_ =
      OpenDataContainer(MemoryLevel::kInMemory, prefix + ".snbr");
}

template <typename EDATA_T>
void SingleImmutableCsr<EDATA_T>::open_with_hugepages(
    const std::string& prefix) {
  nbr_list_buffer_ =
      OpenDataContainer(MemoryLevel::kHugePagePrefered, prefix + ".snbr");
}

template <typename EDATA_T>
void SingleImmutableCsr<EDATA_T>::dump(const std::string& name,
                                       const std::string& new_snapshot_dir) {
  // TODO: opt with mv
  nbr_list_buffer_->Dump(new_snapshot_dir + "/" + name + ".snbr");
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
void SingleImmutableCsr<EDATA_T>::close() {
  CloseAndReset(nbr_list_buffer_);
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
    nbr_arr[src].neighbor = std::numeric_limits<vid_t>::max();
  }
  for (vid_t i = 0; i < vnum; ++i) {
    auto nbr = nbr_arr[i].neighbor;
    if (nbr != std::numeric_limits<vid_t>::max() &&
        dst_set.find(nbr) != dst_set.end()) {
      nbr_arr[i].neighbor = std::numeric_limits<vid_t>::max();
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
      nbr_arr[src].neighbor = std::numeric_limits<vid_t>::max();
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
    nbr_arr[src].neighbor = std::numeric_limits<vid_t>::max();
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
    nbr.neighbor = dst_list[i];
    nbr.data = data_list[i];
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

}  // namespace neug