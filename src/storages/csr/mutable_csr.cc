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

#include "neug/storages/csr/mutable_csr.h"

#include <errno.h>
#include <stdint.h>
#include <unistd.h>
#include <algorithm>
#include <cctype>
#include <cmath>
#include <cstdint>
#include <filesystem>
#include <map>
#include <memory>
#include <numeric>
#include <thread>
#include <utility>
#include <vector>

#include "neug/storages/container/container_utils.h"
#include "neug/storages/file_names.h"
#include "neug/utils/exception/exception.h"
#include "neug/utils/file_utils.h"
#include "neug/utils/property/types.h"
#include "neug/utils/spinlock.h"

namespace neug {

template <typename EDATA_T>
void MutableCsr<EDATA_T>::open_internal(const std::string& snapshot_prefix,
                                        const std::string& tmp_prefix,
                                        MemoryLevel mem_level) {
  close();
  load_meta(snapshot_prefix);

  auto deg_suffix = ".deg";
  auto cap_suffix = ".cap";

  nbr_list_ =
      OpenContainer(snapshot_prefix + ".nbr", tmp_prefix + ".nbr", mem_level);
  degree_list_ = OpenContainer(snapshot_prefix + deg_suffix,
                               tmp_prefix + deg_suffix, mem_level);
  adj_list_buffer_ = OpenContainer("", tmp_prefix + ".buf", mem_level);
  cap_list_ = OpenContainer(snapshot_prefix + cap_suffix,
                            tmp_prefix + cap_suffix, mem_level);

  auto v_cap = degree_list_->GetDataSize() / sizeof(int);
  if (cap_list_->GetDataSize() != degree_list_->GetDataSize()) {
    THROW_INTERNAL_EXCEPTION(
        "Capacity list size does not match degree list size");
  }
  adj_list_buffer_->Resize(v_cap * sizeof(nbr_t*));
  locks_ = new SpinLock[v_cap];

  // Build the adj_list pointer array from the contiguous nbr_list.
  const auto* deg_ptr = reinterpret_cast<const int*>(degree_list_->GetData());
  const auto* cap_ptr = reinterpret_cast<const int*>(cap_list_->GetData());
  auto* adj_lists_ptr = reinterpret_cast<nbr_t**>(adj_list_buffer_->GetData());
  auto* nbr_list_ptr = reinterpret_cast<nbr_t*>(nbr_list_->GetData());
  uint64_t edge_count = 0;
  for (size_t i = 0; i < v_cap; ++i) {
    adj_lists_ptr[i] = nbr_list_ptr;
    edge_count += deg_ptr[i];
    nbr_list_ptr += cap_ptr[i];
  }
  if (edge_num_.load() != edge_count) {
    LOG(WARNING) << "Edge count from meta (" << edge_num_.load()
                 << ") does not match count computed from degree list ("
                 << edge_count << "). Using computed count.";
    THROW_STORAGE_EXCEPTION(
        "Edge count mismatch: meta has " + std::to_string(edge_num_.load()) +
        " but degree list implies " + std::to_string(edge_count));
  }
}

template <typename EDATA_T>
void MutableCsr<EDATA_T>::open(const std::string& name,
                               const std::string& snapshot_dir,
                               const std::string& work_dir) {
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
void MutableCsr<EDATA_T>::open_in_memory(const std::string& prefix) {
  open_internal(prefix, "", MemoryLevel::kInMemory);
}

template <typename EDATA_T>
void MutableCsr<EDATA_T>::open_with_hugepages(const std::string& prefix) {
  open_internal(prefix, "", MemoryLevel::kHugePagePreferred);
}

template <typename EDATA_T>
void MutableCsr<EDATA_T>::dump(const std::string& name,
                               const std::string& new_snapshot_dir) {
  size_t vnum = vertex_capacity();
  dump_meta(new_snapshot_dir + "/" + name);
  degree_list_->Dump(new_snapshot_dir + "/" + name + ".deg");

  const int* caps = reinterpret_cast<const int*>(cap_list_->GetData());

  const nbr_t* const* lists =
      reinterpret_cast<const nbr_t* const*>(adj_list_buffer_->GetData());
  const std::string nbr_path = new_snapshot_dir + "/" + name + ".nbr";
  std::ofstream nbr_out(nbr_path, std::ios::binary);
  if (!nbr_out.is_open()) {
    THROW_IO_EXCEPTION("Failed to open file for writing: " + nbr_path);
  }
  FileHeader header{};
  nbr_out.write(reinterpret_cast<const char*>(&header), sizeof(header));

  MD5_CTX ctx;
  MD5_Init(&ctx);
  for (size_t i = 0; i < vnum; ++i) {
    const void* data = lists[i];
    size_t len = caps[i] * sizeof(nbr_t);
    nbr_out.write(reinterpret_cast<const char*>(data), len);
    MD5_Update(&ctx, data, len);
  }
  MD5_Final(header.data_md5, &ctx);
  // Update the header with the correct MD5 after writing all data
  nbr_out.seekp(0);
  nbr_out.write(reinterpret_cast<const char*>(&header), sizeof(header));
  nbr_out.flush();
  nbr_out.close();
  cap_list_->Dump(new_snapshot_dir + "/" + name + ".cap");
}

template <typename EDATA_T>
void MutableCsr<EDATA_T>::reset_timestamp() {
  size_t vnum = vertex_capacity();
  auto** buf_arr = reinterpret_cast<nbr_t**>(adj_list_buffer_->GetData());
  auto* sz_arr = reinterpret_cast<int*>(degree_list_->GetData());
  for (size_t i = 0; i != vnum; ++i) {
    nbr_t* nbrs = buf_arr[i];
    if (nbrs == nullptr) {
      continue;
    }
    size_t deg = sz_arr[i];
    for (size_t j = 0; j != deg; ++j) {
      if (nbrs[j].timestamp != INVALID_TIMESTAMP) {
        nbrs[j].timestamp.store(0, std::memory_order_relaxed);
      }
    }
  }
}

template <typename EDATA_T>
void MutableCsr<EDATA_T>::compact() {
  // We don't shrink the capacity of each adjacency list, but just remove the
  // deleted edges.
  size_t vnum = vertex_capacity();
  auto** buf_arr = reinterpret_cast<nbr_t**>(adj_list_buffer_->GetData());
  auto* sz_arr = reinterpret_cast<int*>(degree_list_->GetData());
  size_t total_edge_num = 0;
  for (size_t i = 0; i != vnum; ++i) {
    int sz = sz_arr[i];
    nbr_t* read_ptr = buf_arr[i];
    if (read_ptr == nullptr) {
      continue;
    }
    nbr_t* read_end = read_ptr + sz;
    nbr_t* write_ptr = buf_arr[i];
    int removed = 0;
    while (read_ptr != read_end) {
      if (read_ptr->timestamp != INVALID_TIMESTAMP) {
        if (removed) {
          *write_ptr = *read_ptr;
        }
        ++write_ptr;
      } else {
        ++removed;
      }
      ++read_ptr;
    }
    sz_arr[i] -= removed;
    total_edge_num += sz_arr[i];
  }
  if (total_edge_num != edge_num_.load()) {
    LOG(WARNING) << "Inconsistent edge count after compaction"
                 << ": expected " << edge_num_.load() << ", actual "
                 << total_edge_num;
    THROW_STORAGE_EXCEPTION(
        "Inconsistent edge count after compaction: expected " +
        std::to_string(edge_num_.load()) + ", actual " +
        std::to_string(total_edge_num));
  }
}

template <typename EDATA_T>
void MutableCsr<EDATA_T>::resize(vid_t vnum) {
  if (adj_list_buffer_ == nullptr || degree_list_ == nullptr ||
      cap_list_ == nullptr) {
    LOG(ERROR) << "Containers not initialized, cannot resize";
    THROW_RUNTIME_ERROR("Containers not initialized");
  }
  auto old_vnum = vertex_capacity();
  if (vnum > old_vnum) {
    adj_list_buffer_->Resize(vnum * sizeof(nbr_t*));
    degree_list_->Resize(vnum * sizeof(int));
    cap_list_->Resize(vnum * sizeof(int));
    auto** buf_arr = reinterpret_cast<nbr_t**>(adj_list_buffer_->GetData());
    auto* sz_arr = reinterpret_cast<int*>(degree_list_->GetData());
    auto* cap_arr = reinterpret_cast<int*>(cap_list_->GetData());
    for (vid_t i = old_vnum; i < vnum; ++i) {
      buf_arr[i] = nullptr;
      sz_arr[i] = 0;
      cap_arr[i] = 0;
    }
    delete[] locks_;
    locks_ = new SpinLock[vnum];
  } else {
    adj_list_buffer_->Resize(vnum * sizeof(nbr_t*));
    degree_list_->Resize(vnum * sizeof(int));
    cap_list_->Resize(vnum * sizeof(int));
  }
}

template <typename EDATA_T>
size_t MutableCsr<EDATA_T>::capacity() const {
  // We assume the capacity of each csr is INFINITE.
  return CsrBase::INFINITE_CAPACITY;
}

template <typename EDATA_T>
void MutableCsr<EDATA_T>::close() {
  if (locks_ != nullptr) {
    delete[] locks_;
    locks_ = nullptr;
  }
  if (adj_list_buffer_) {
    adj_list_buffer_->Close();
  }
  if (degree_list_) {
    degree_list_->Close();
  }
  if (cap_list_) {
    cap_list_->Close();
  }
  if (nbr_list_) {
    nbr_list_->Close();
  }
}

template <typename EDATA_T>
void MutableCsr<EDATA_T>::batch_sort_by_edge_data(timestamp_t ts) {
  if (adj_list_buffer_ != nullptr) {
    size_t vnum = vertex_capacity();
    auto** buf_arr = reinterpret_cast<nbr_t**>(adj_list_buffer_->GetData());
    auto* sz_arr = reinterpret_cast<int*>(degree_list_->GetData());
    for (size_t i = 0; i != vnum; ++i) {
      nbr_t* begin = buf_arr[i];
      if (begin == nullptr) {
        continue;
      }
      std::sort(begin, begin + sz_arr[i],
                [](const nbr_t& lhs, const nbr_t& rhs) {
                  return lhs.data < rhs.data;
                });
    }
  }
  unsorted_since_ = ts;
}

template <typename EDATA_T>
void MutableCsr<EDATA_T>::batch_delete_vertices(
    const std::set<vid_t>& src_set, const std::set<vid_t>& dst_set) {
  vid_t vnum = static_cast<vid_t>(vertex_capacity());
  auto** buf_arr = reinterpret_cast<nbr_t**>(adj_list_buffer_->GetData());
  auto* sz_arr = reinterpret_cast<int*>(degree_list_->GetData());
  for (vid_t src : src_set) {
    if (src < vnum) {
      auto* data = buf_arr[src];
      auto* end = data + sz_arr[src];
      for (auto* ptr = data; ptr != end; ++ptr) {
        if (ptr->timestamp.load() != std::numeric_limits<timestamp_t>::max()) {
          edge_num_.fetch_sub(1, std::memory_order_relaxed);
        }
      }
      sz_arr[src] = 0;
    }
  }
  for (vid_t src = 0; src < vnum; ++src) {
    if (sz_arr[src] == 0) {
      continue;
    }
    const nbr_t* read_ptr = buf_arr[src];
    if (read_ptr == nullptr) {
      continue;
    }
    const nbr_t* read_end = read_ptr + sz_arr[src];
    nbr_t* write_ptr = buf_arr[src];
    int removed = 0;
    while (read_ptr != read_end) {
      vid_t nbr = read_ptr->neighbor;
      if (dst_set.find(nbr) == dst_set.end()) {
        if (removed) {
          *write_ptr = *read_ptr;
        }
        ++write_ptr;
      } else {
        if (read_ptr->timestamp.load() !=
            std::numeric_limits<timestamp_t>::max()) {
          edge_num_.fetch_sub(1, std::memory_order_relaxed);
        }
        ++removed;
      }
      ++read_ptr;
    }

    sz_arr[src] -= removed;
  }
}

template <typename EDATA_T>
void MutableCsr<EDATA_T>::batch_delete_edges(
    const std::vector<vid_t>& src_list, const std::vector<vid_t>& dst_list) {
  std::map<vid_t, std::set<vid_t>> src_dst_map;
  vid_t vnum = static_cast<vid_t>(vertex_capacity());
  for (size_t i = 0; i < src_list.size(); ++i) {
    vid_t src = src_list[i];
    if (src >= vnum) {
      continue;
    }
    src_dst_map[src].insert(dst_list[i]);
  }
  auto** buf_arr = reinterpret_cast<nbr_t**>(adj_list_buffer_->GetData());
  auto* sz_arr = reinterpret_cast<int*>(degree_list_->GetData());
  for (const auto& pair : src_dst_map) {
    vid_t src = pair.first;
    nbr_t* write_ptr = buf_arr[src];
    if (write_ptr == nullptr) {
      continue;
    }
    const nbr_t* read_end = write_ptr + sz_arr[src];
    while (write_ptr != read_end) {
      if (pair.second.find(write_ptr->neighbor) != pair.second.end()) {
        write_ptr->timestamp.store(std::numeric_limits<timestamp_t>::max());
        edge_num_.fetch_sub(1, std::memory_order_relaxed);
      }
      ++write_ptr;
    }
  }
}

template <typename EDATA_T>
void MutableCsr<EDATA_T>::batch_delete_edges(
    const std::vector<std::pair<vid_t, int32_t>>& edges) {
  std::map<vid_t, std::set<int32_t>> src_offset_map;
  vid_t vnum = static_cast<vid_t>(vertex_capacity());
  const auto* sz_arr = reinterpret_cast<const int*>(degree_list_->GetData());
  auto** buf_arr = reinterpret_cast<nbr_t**>(adj_list_buffer_->GetData());
  for (const auto& edge : edges) {
    if (edge.first >= vnum || edge.second >= sz_arr[edge.first]) {
      continue;
    }
    src_offset_map[edge.first].insert(edge.second);
  }
  for (const auto& pair : src_offset_map) {
    vid_t src = pair.first;
    nbr_t* write_ptr = buf_arr[src];
    if (write_ptr == nullptr) {
      continue;
    }
    for (auto offset : pair.second) {
      if (write_ptr[offset].timestamp.load() !=
          std::numeric_limits<timestamp_t>::max()) {
        edge_num_.fetch_sub(1, std::memory_order_relaxed);
      }
      write_ptr[offset].timestamp.store(
          std::numeric_limits<timestamp_t>::max());
    }
  }
}

template <typename EDATA_T>
void MutableCsr<EDATA_T>::delete_edge(vid_t src, int32_t offset,
                                      timestamp_t ts) {
  vid_t vnum = static_cast<vid_t>(vertex_capacity());
  const auto* sz_arr = reinterpret_cast<const int*>(degree_list_->GetData());
  if (src >= vnum || offset >= sz_arr[src]) {
    THROW_INVALID_ARGUMENT_EXCEPTION("src out of bound or offset out of bound");
  }
  nbr_t* nbrs = reinterpret_cast<nbr_t**>(adj_list_buffer_->GetData())[src];
  if (nbrs == nullptr) {
    THROW_INVALID_ARGUMENT_EXCEPTION("adjacency buffer is null");
  }
  auto old_ts = nbrs[offset].timestamp.load();
  if (old_ts <= ts) {
    nbrs[offset].timestamp.store(std::numeric_limits<timestamp_t>::max());
    edge_num_.fetch_sub(1, std::memory_order_relaxed);
  } else if (old_ts == std::numeric_limits<timestamp_t>::max()) {
    LOG(ERROR) << "Attempting to delete already deleted edge.";
  } else {
    LOG(ERROR) << "Attempting to delete edge with timestamp " << old_ts
               << " using older timestamp " << ts;
  }
}

template <typename EDATA_T>
void MutableCsr<EDATA_T>::revert_delete_edge(vid_t src, vid_t nbr,
                                             int32_t offset, timestamp_t ts) {
  vid_t vnum = static_cast<vid_t>(vertex_capacity());
  const auto* sz_arr = reinterpret_cast<const int*>(degree_list_->GetData());
  if (src >= vnum || offset >= sz_arr[src]) {
    THROW_INVALID_ARGUMENT_EXCEPTION("src out of bound or offset out of bound");
  }
  nbr_t* nbrs = reinterpret_cast<nbr_t**>(adj_list_buffer_->GetData())[src];
  if (nbrs == nullptr) {
    THROW_INVALID_ARGUMENT_EXCEPTION("adjacency buffer is null");
  }
  if (nbrs[offset].neighbor != nbr) {
    THROW_INVALID_ARGUMENT_EXCEPTION("neighbor id not match");
  }
  auto old_ts = nbrs[offset].timestamp.load();
  if (old_ts == std::numeric_limits<timestamp_t>::max()) {
    assert(nbrs[offset].neighbor == nbr);
    nbrs[offset].timestamp.store(ts);
    edge_num_.fetch_add(1, std::memory_order_relaxed);
  } else {
    THROW_INVALID_ARGUMENT_EXCEPTION(
        "Attempting to revert delete on edge that is not deleted.");
  }
}

template <typename EDATA_T>
void MutableCsr<EDATA_T>::batch_put_edges(const std::vector<vid_t>& src_list,
                                          const std::vector<vid_t>& dst_list,
                                          const std::vector<EDATA_T>& data_list,
                                          timestamp_t ts) {
  vid_t vnum = static_cast<vid_t>(vertex_capacity());
  if (vnum == 0) {
    return;
  }
  std::vector<int> degree(vnum, 0);
  for (auto src : src_list) {
    if (src < vnum) {
      degree[src]++;
    }
  }

  auto** buf_arr = reinterpret_cast<nbr_t**>(adj_list_buffer_->GetData());
  auto* sz_arr = reinterpret_cast<int*>(degree_list_->GetData());
  auto* cap_arr = reinterpret_cast<int*>(cap_list_->GetData());
  size_t total_to_move = 0;
  size_t total_to_allocate = 0;
  for (vid_t i = 0; i < vnum; ++i) {
    int old_deg = sz_arr[i];
    total_to_move += old_deg;
    int new_degree = degree[i] + old_deg;
    int new_cap = std::ceil(new_degree * NeugDBConfig::DEFAULT_RESERVE_RATIO);
    cap_arr[i] = new_cap;
    total_to_allocate += new_cap;
  }

  std::vector<nbr_t> new_nbr_list(total_to_move);
  size_t offset = 0;
  for (vid_t i = 0; i < vnum; ++i) {
    int old_deg = sz_arr[i];
    if (old_deg > 0 && buf_arr[i] != nullptr) {
      memcpy(new_nbr_list.data() + offset, buf_arr[i], sizeof(nbr_t) * old_deg);
    }
    offset += old_deg;
  }
  nbr_list_->Resize(total_to_allocate * sizeof(nbr_t));
  auto base_ptr = reinterpret_cast<nbr_t*>(nbr_list_->GetData());
  offset = 0;
  size_t new_offset = 0;
  for (vid_t i = 0; i < vnum; ++i) {
    nbr_t* new_buffer = base_ptr != nullptr ? base_ptr + offset : nullptr;
    int old_deg = sz_arr[i];
    if (old_deg > 0 && new_buffer != nullptr) {
      memcpy(new_buffer, new_nbr_list.data() + new_offset,
             sizeof(nbr_t) * old_deg);
    }
    new_offset += old_deg;
    offset += cap_arr[i];
    buf_arr[i] = new_buffer;
    sz_arr[i] = old_deg;
  }
  size_t added_edge_num = 0;
  for (size_t i = 0; i < src_list.size(); ++i) {
    vid_t src = src_list[i];
    if (src >= vnum) {
      continue;
    }
    vid_t dst = dst_list[i];
    const EDATA_T& data = data_list[i];
    auto& nbr = buf_arr[src][sz_arr[src]++];
    nbr.neighbor = dst;
    nbr.data = data;
    nbr.timestamp.store(ts);
    added_edge_num++;
  }
  edge_num_.fetch_add(added_edge_num, std::memory_order_relaxed);
}

template <typename EDATA_T>
void MutableCsr<EDATA_T>::load_meta(const std::string& prefix) {
  std::string meta_file_path = prefix + ".meta";
  if (std::filesystem::exists(meta_file_path)) {
    timestamp_t ts;
    std::ifstream meta_in(meta_file_path, std::ios::binary);
    if (!meta_in.is_open()) {
      THROW_IO_EXCEPTION("Failed to open meta file: " + meta_file_path);
    }
    meta_in.read(reinterpret_cast<char*>(&ts), sizeof(ts));
    unsorted_since_ = ts;
    uint64_t edge_num;
    meta_in.read(reinterpret_cast<char*>(&edge_num), sizeof(edge_num));
    edge_num_.store(edge_num);
    meta_in.close();
  } else {
    unsorted_since_ = 0;
    edge_num_.store(0);
  }
}

template <typename EDATA_T>
void MutableCsr<EDATA_T>::dump_meta(const std::string& prefix) const {
  std::string meta_file_path = prefix + ".meta";
  std::ofstream meta_out(meta_file_path, std::ios::binary);
  if (!meta_out.is_open()) {
    THROW_IO_EXCEPTION("Failed to open meta file for writing: " +
                       meta_file_path);
  }

  timestamp_t ts = unsorted_since_;
  meta_out.write(reinterpret_cast<const char*>(&ts), sizeof(ts));
  uint64_t edge_num = edge_num_.load();
  meta_out.write(reinterpret_cast<const char*>(&edge_num), sizeof(edge_num));
  meta_out.close();
}

template <typename EDATA_T>
void SingleMutableCsr<EDATA_T>::open(const std::string& name,
                                     const std::string& snapshot_dir,
                                     const std::string& work_dir) {
  close();
  load_meta(snapshot_dir + "/" + name);
  nbr_list_ = OpenContainer(snapshot_dir + "/" + name + ".snbr",
                            tmp_dir(work_dir) + "/" + name + ".snbr",
                            MemoryLevel::kSyncToFile);
}

template <typename EDATA_T>
void SingleMutableCsr<EDATA_T>::open_in_memory(const std::string& prefix) {
  close();
  load_meta(prefix);
  nbr_list_ = OpenContainer(prefix + ".snbr", "", MemoryLevel::kInMemory);
}

template <typename EDATA_T>
void SingleMutableCsr<EDATA_T>::open_with_hugepages(const std::string& prefix) {
  close();
  load_meta(prefix);
  nbr_list_ =
      OpenContainer(prefix + ".snbr", "", MemoryLevel::kHugePagePreferred);
}

template <typename EDATA_T>
void SingleMutableCsr<EDATA_T>::dump(const std::string& name,
                                     const std::string& new_snapshot_dir) {
  // TODO: opt with mv
  if (!nbr_list_) {
    return;
  }
  dump_meta(new_snapshot_dir + "/" + name);
  nbr_list_->Dump(new_snapshot_dir + "/" + name + ".snbr");
}

template <typename EDATA_T>
void SingleMutableCsr<EDATA_T>::reset_timestamp() {
  if (!nbr_list_) {
    return;
  }
  nbr_t* data = reinterpret_cast<nbr_t*>(nbr_list_->GetData());
  size_t vnum = vertex_capacity();
  for (size_t i = 0; i != vnum; ++i) {
    if (data[i].timestamp != INVALID_TIMESTAMP) {
      data[i].timestamp.store(0, std::memory_order_relaxed);
    }
  }
}

template <typename EDATA_T>
void SingleMutableCsr<EDATA_T>::compact() {}

template <typename EDATA_T>
void SingleMutableCsr<EDATA_T>::resize(vid_t vnum) {
  size_t old_vnum = vertex_capacity();
  nbr_list_->Resize(vnum * sizeof(nbr_t));
  if (vnum > old_vnum) {
    auto* data = reinterpret_cast<nbr_t*>(nbr_list_->GetData());
    for (vid_t i = old_vnum; i < vnum; ++i) {
      data[i].timestamp.store(std::numeric_limits<timestamp_t>::max());
    }
  }
}

template <typename EDATA_T>
size_t SingleMutableCsr<EDATA_T>::capacity() const {
  return vertex_capacity();
}

template <typename EDATA_T>
void SingleMutableCsr<EDATA_T>::close() {
  if (nbr_list_) {
    nbr_list_->Close();
  }
}

template <typename EDATA_T>
void SingleMutableCsr<EDATA_T>::batch_sort_by_edge_data(timestamp_t ts) {}

template <typename EDATA_T>
void SingleMutableCsr<EDATA_T>::batch_delete_vertices(
    const std::set<vid_t>& src_set, const std::set<vid_t>& dst_set) {
  if (!nbr_list_) {
    return;
  }
  nbr_t* data = reinterpret_cast<nbr_t*>(nbr_list_->GetData());
  vid_t vnum = static_cast<vid_t>(vertex_capacity());
  for (auto src : src_set) {
    if (src < vnum) {
      if (data[src].timestamp.load() !=
          std::numeric_limits<timestamp_t>::max()) {
        edge_num_.fetch_sub(1, std::memory_order_relaxed);
      }
      data[src].timestamp.store(std::numeric_limits<timestamp_t>::max());
    }
  }
  for (vid_t v = 0; v < vnum; ++v) {
    auto& nbr = data[v];
    if (dst_set.find(nbr.neighbor) != dst_set.end()) {
      if (nbr.timestamp.load() != std::numeric_limits<timestamp_t>::max()) {
        edge_num_.fetch_sub(1, std::memory_order_relaxed);
      }
      nbr.timestamp.store(std::numeric_limits<timestamp_t>::max());
    }
  }
}

template <typename EDATA_T>
void SingleMutableCsr<EDATA_T>::batch_delete_edges(
    const std::vector<vid_t>& src_list, const std::vector<vid_t>& dst_list) {
  if (!nbr_list_) {
    return;
  }
  nbr_t* data = reinterpret_cast<nbr_t*>(nbr_list_->GetData());
  vid_t vnum = static_cast<vid_t>(vertex_capacity());
  for (size_t i = 0; i != src_list.size(); ++i) {
    vid_t src = src_list[i];
    vid_t dst = dst_list[i];
    if (src >= vnum) {
      continue;
    }
    auto& nbr = data[src];
    if (nbr.neighbor == dst) {
      if (nbr.timestamp.load() != std::numeric_limits<timestamp_t>::max()) {
        edge_num_.fetch_sub(1, std::memory_order_relaxed);
      }
      nbr.timestamp.store(std::numeric_limits<timestamp_t>::max());
    }
  }
}

template <typename EDATA_T>
void SingleMutableCsr<EDATA_T>::batch_delete_edges(
    const std::vector<std::pair<vid_t, int32_t>>& edge_list) {
  if (!nbr_list_) {
    return;
  }
  nbr_t* data = reinterpret_cast<nbr_t*>(nbr_list_->GetData());
  vid_t vnum = static_cast<vid_t>(vertex_capacity());
  for (const auto& edge : edge_list) {
    vid_t src = edge.first;
    if (src >= vnum) {
      continue;
    }
    auto& nbr = data[src];
    assert(edge.second == 0);
    nbr.timestamp.store(std::numeric_limits<timestamp_t>::max());
    edge_num_.fetch_sub(1, std::memory_order_relaxed);
  }
}

template <typename EDATA_T>
void SingleMutableCsr<EDATA_T>::delete_edge(vid_t src, int32_t offset,
                                            timestamp_t ts) {
  if (!nbr_list_) {
    return;
  }
  vid_t vnum = static_cast<vid_t>(vertex_capacity());
  if (src >= vnum) {
    THROW_INVALID_ARGUMENT_EXCEPTION(
        "src out of bound: " + std::to_string(src) +
        " >= " + std::to_string(vnum));
  }
  nbr_t* data = reinterpret_cast<nbr_t*>(nbr_list_->GetData());
  auto& nbr = data[src];
  assert(offset == 0);
  if (nbr.timestamp.load() <= ts) {
    nbr.timestamp.store(std::numeric_limits<timestamp_t>::max());
    edge_num_.fetch_sub(1, std::memory_order_relaxed);
  } else if (nbr.timestamp.load() == std::numeric_limits<timestamp_t>::max()) {
    LOG(ERROR) << "Fail to delete edge, already deleted.";
  } else {
    LOG(ERROR) << "Fail to delete edge, timestamp not satisfied.";
  }
}

template <typename EDATA_T>
void SingleMutableCsr<EDATA_T>::revert_delete_edge(vid_t src, vid_t nbr_vid,
                                                   int32_t offset,
                                                   timestamp_t ts) {
  if (!nbr_list_) {
    return;
  }
  vid_t vnum = static_cast<vid_t>(vertex_capacity());
  if (src >= vnum || offset != 0) {
    THROW_INVALID_ARGUMENT_EXCEPTION("src out of bound or offset out of bound");
  }
  nbr_t* data = reinterpret_cast<nbr_t*>(nbr_list_->GetData());
  auto& nbr = data[src];
  if (nbr.neighbor != nbr_vid) {
    THROW_INVALID_ARGUMENT_EXCEPTION("neighbor id not match");
  }
  if (nbr.timestamp.load() == std::numeric_limits<timestamp_t>::max()) {
    nbr.timestamp.store(ts);
    edge_num_.fetch_add(1, std::memory_order_relaxed);
  } else {
    THROW_INVALID_ARGUMENT_EXCEPTION(
        "Attempting to revert delete on edge that is not deleted.");
  }
}

template <typename EDATA_T>
void SingleMutableCsr<EDATA_T>::batch_put_edges(
    const std::vector<vid_t>& src_list, const std::vector<vid_t>& dst_list,
    const std::vector<EDATA_T>& data_list, timestamp_t ts) {
  if (!nbr_list_) {
    return;
  }
  vid_t vnum = static_cast<vid_t>(vertex_capacity());
  nbr_t* data = reinterpret_cast<nbr_t*>(nbr_list_->GetData());
  for (size_t i = 0; i != src_list.size(); ++i) {
    vid_t src = src_list[i];
    if (src >= vnum) {
      continue;
    }
    auto& nbr = data[src];
    nbr.neighbor = dst_list[i];
    nbr.data = data_list[i];
    nbr.timestamp.store(ts);
    edge_num_.fetch_add(1, std::memory_order_relaxed);
  }
}

template <typename EDATA_T>
void SingleMutableCsr<EDATA_T>::load_meta(const std::string& prefix) {
  std::string meta_file_path = prefix + ".meta";
  if (std::filesystem::exists(meta_file_path)) {
    std::ifstream meta_in(meta_file_path, std::ios::binary);
    uint64_t edge_num;
    meta_in.read(reinterpret_cast<char*>(&edge_num), sizeof(edge_num));
    edge_num_.store(edge_num);
    meta_in.close();
  } else {
    edge_num_.store(0);
  }
}

template <typename EDATA_T>
void SingleMutableCsr<EDATA_T>::dump_meta(const std::string& prefix) const {
  std::string meta_file_path = prefix + ".meta";
  std::ofstream meta_out(meta_file_path, std::ios::binary);
  uint64_t edge_num = edge_num_.load();
  meta_out.write(reinterpret_cast<const char*>(&edge_num), sizeof(edge_num));
  meta_out.close();
}

template class MutableCsr<EmptyType>;
template class MutableCsr<int32_t>;
template class MutableCsr<uint32_t>;
template class MutableCsr<Date>;
template class MutableCsr<int64_t>;
template class MutableCsr<uint64_t>;
template class MutableCsr<double>;
template class MutableCsr<float>;
template class MutableCsr<DateTime>;
template class MutableCsr<Interval>;
template class MutableCsr<bool>;

template class SingleMutableCsr<float>;
template class SingleMutableCsr<double>;
template class SingleMutableCsr<uint64_t>;
template class SingleMutableCsr<int64_t>;
template class SingleMutableCsr<Date>;
template class SingleMutableCsr<uint32_t>;
template class SingleMutableCsr<int32_t>;
template class SingleMutableCsr<EmptyType>;
template class SingleMutableCsr<DateTime>;
template class SingleMutableCsr<Interval>;
template class SingleMutableCsr<bool>;

}  // namespace neug
