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
#include <sstream>
#include <stdexcept>
#include <system_error>
#include <thread>
#include <utility>
#include <vector>

#include "neug/storages/column/anon_mmap_container.h"
#include "neug/storages/column/file_header.h"
#include "neug/storages/column/file_mmap_container.h"
#include "neug/storages/file_names.h"
#include "neug/utils/exception/exception.h"
#include "neug/utils/file_utils.h"
#include "neug/utils/property/types.h"
#include "neug/utils/spinlock.h"

namespace neug {

template <typename EDATA_T>
void MutableCsr<EDATA_T>::open(const std::string& name,
                               const std::string& snapshot_dir,
                               const std::string& work_dir) {
  if (snapshot_dir.empty() || !std::filesystem::exists(snapshot_dir)) {
    THROW_INVALID_ARGUMENT_EXCEPTION(
        "Snapshot directory is required for disk-backed open()");
  }
  close();
  std::string prefix = snapshot_dir + "/" + name;
  load_meta(prefix);

  auto deg_file_name = prefix + ".deg";
  auto cap_file_name = prefix + ".cap";
  auto snap_nbr_file_name = prefix + ".nbr";
  auto tmp_nbr_file_name = tmp_dir(work_dir) + "/" + name + ".nbr";

  std::shared_ptr<IDataContainer> degree_list =
      std::make_shared<FilePrivateMMap>();
  if (std::filesystem::exists(deg_file_name)) {
    degree_list->Open(deg_file_name);
  }

  std::shared_ptr<IDataContainer> cap_list = degree_list;
  if (std::filesystem::exists(cap_file_name)) {
    cap_list = std::make_shared<FilePrivateMMap>();
    cap_list->Open(cap_file_name);
  }
  // For nbr_list, we will copy it to tmp, and then open with mmap_shared.
  std::filesystem::create_directories(tmp_dir(work_dir));
  std::filesystem::remove(tmp_nbr_file_name);
  if (!std::filesystem::exists(snap_nbr_file_name)) {
    file_utils::create_file(tmp_nbr_file_name, sizeof(FileHeader));
  } else {
    file_utils::copy_file(snap_nbr_file_name, tmp_nbr_file_name, true);
  }
  nbr_list_ = std::make_unique<FileSharedMMap>();
  nbr_list_->Open(tmp_nbr_file_name);
  auto v_cap = degree_list->GetDataSize() / sizeof(int);

  adj_list_buffer_ = std::make_unique<FileSharedMMap>();
  adj_list_size_ = std::make_unique<FileSharedMMap>();
  adj_list_capacity_ = std::make_unique<FileSharedMMap>();
  // Should be ok even if the previous file exists.
  std::filesystem::remove(tmp_dir(work_dir) + "/" + name + ".buf");
  std::filesystem::remove(tmp_dir(work_dir) + "/" + name + ".size");
  std::filesystem::remove(tmp_dir(work_dir) + "/" + name + ".cap");
  file_utils::create_file(tmp_dir(work_dir) + "/" + name + ".buf",
                          sizeof(FileHeader));
  file_utils::create_file(tmp_dir(work_dir) + "/" + name + ".size",
                          sizeof(FileHeader));
  file_utils::create_file(tmp_dir(work_dir) + "/" + name + ".cap",
                          sizeof(FileHeader));
  adj_list_buffer_->Open(tmp_dir(work_dir) + "/" + name + ".buf");
  adj_list_size_->Open(tmp_dir(work_dir) + "/" + name + ".size");
  adj_list_capacity_->Open(tmp_dir(work_dir) + "/" + name + ".cap");
  adj_list_buffer_->Resize(v_cap * sizeof(nbr_t*));
  adj_list_size_->Resize(v_cap * sizeof(std::atomic<int>));
  adj_list_capacity_->Resize(v_cap * sizeof(int));

  locks_ = new SpinLock[v_cap];

  nbr_t* ptr = nbr_entries_ptr();
  auto degree_ptr = reinterpret_cast<const int*>(degree_list->GetData());
  auto cap_ptr = reinterpret_cast<const int*>(cap_list->GetData());
  for (size_t i = 0; i < v_cap; ++i) {
    int deg = degree_ptr[i];
    int cap = cap_ptr[i];
    adj_list_buffer_ptr()[i] = ptr;
    adj_list_size_ptr()[i].store(deg, std::memory_order_relaxed);
    adj_list_cap_ptr()[i] = cap;
    ptr += cap;
  }
}

template <typename EDATA_T>
void MutableCsr<EDATA_T>::open_in_memory(const std::string& prefix) {
  close();
  load_meta(prefix);

  auto degree_file_name = prefix + ".deg";
  auto cap_file_name = prefix + ".cap";
  auto snap_nbr_file_name = prefix + ".nbr";

  std::shared_ptr<IDataContainer> degree_list =
      std::make_shared<FilePrivateMMap>();
  if (std::filesystem::exists(degree_file_name)) {
    degree_list->Open(degree_file_name);
  }

  std::shared_ptr<IDataContainer> cap_list = degree_list;
  if (std::filesystem::exists(cap_file_name)) {
    cap_list = std::make_shared<FilePrivateMMap>();
    cap_list->Open(cap_file_name);
  }

  auto v_cap = degree_list->GetDataSize() / sizeof(int);
  nbr_list_ = std::make_unique<FilePrivateMMap>();
  if (std::filesystem::exists(snap_nbr_file_name)) {
    nbr_list_->Open(snap_nbr_file_name);
  }
  adj_list_buffer_ = std::make_unique<AnonMMap>();
  adj_list_size_ = std::make_unique<AnonMMap>();
  adj_list_capacity_ = std::make_unique<AnonMMap>();

  adj_list_buffer_->Resize(v_cap * sizeof(nbr_t*));
  adj_list_size_->Resize(v_cap * sizeof(std::atomic<int>));
  adj_list_capacity_->Resize(v_cap * sizeof(int));
  locks_ = new SpinLock[v_cap];

  auto* ptr = nbr_entries_ptr();

  auto* degree_ptr = reinterpret_cast<const int*>(degree_list->GetData());
  auto cap_ptr = reinterpret_cast<const int*>(cap_list->GetData());

  for (size_t i = 0; i < v_cap; ++i) {
    int deg = degree_ptr[i];
    int cap = cap_ptr[i];
    adj_list_buffer_ptr()[i] = ptr;
    adj_list_size_ptr()[i].store(deg, std::memory_order_relaxed);
    adj_list_cap_ptr()[i] = cap;
    ptr += cap;
  }
}

template <typename EDATA_T>
void MutableCsr<EDATA_T>::open_with_hugepages(const std::string& prefix) {
  close();
  load_meta(prefix);
  auto degree_file_name = prefix + ".deg";
  auto cap_file_name = prefix + ".cap";
  auto snap_nbr_file_name = prefix + ".nbr";

  // For tmp degree_list, we FilePrivateMap since we only need to read it once
  std::shared_ptr<IDataContainer> degree_list =
      std::make_shared<FilePrivateMMap>();
  if (std::filesystem::exists(degree_file_name)) {
    degree_list->Open(degree_file_name);
  }

  std::shared_ptr<IDataContainer> cap_list = degree_list;
  if (std::filesystem::exists(cap_file_name)) {
    cap_list = std::make_shared<FilePrivateMMap>();
    cap_list->Open(cap_file_name);
  }

  auto v_cap = degree_list->GetDataSize() / sizeof(int);
  nbr_list_ = std::make_unique<AnonHugeMMap>();
  if (std::filesystem::exists(snap_nbr_file_name)) {
    nbr_list_->Open(snap_nbr_file_name);
  }
  adj_list_buffer_ = std::make_unique<AnonHugeMMap>();
  adj_list_size_ = std::make_unique<AnonHugeMMap>();
  adj_list_capacity_ = std::make_unique<AnonHugeMMap>();

  adj_list_buffer_->Resize(v_cap * sizeof(nbr_t*));
  adj_list_size_->Resize(v_cap * sizeof(std::atomic<int>));
  adj_list_capacity_->Resize(v_cap * sizeof(int));
  locks_ = new SpinLock[v_cap];

  auto* ptr = nbr_entries_ptr();

  auto* degree_ptr = reinterpret_cast<const int*>(degree_list->GetData());
  auto cap_ptr = reinterpret_cast<const int*>(cap_list->GetData());

  for (size_t i = 0; i < v_cap; ++i) {
    int deg = degree_ptr[i];
    int cap = cap_ptr[i];
    adj_list_buffer_ptr()[i] = ptr;
    adj_list_size_ptr()[i].store(deg, std::memory_order_relaxed);
    adj_list_cap_ptr()[i] = cap;
    ptr += cap;
  }
}

template <typename EDATA_T>
void MutableCsr<EDATA_T>::dump(const std::string& name,
                               const std::string& new_snapshot_dir) {
  size_t vnum = vertex_capacity();
  dump_meta(new_snapshot_dir + "/" + name);

  auto degree_list = std::make_unique<AnonMMap>();
  degree_list->OpenAnonymous(vnum * sizeof(int));
  auto cap_list = std::make_unique<AnonMMap>();
  cap_list->OpenAnonymous(vnum * sizeof(int));
  bool need_cap_list = false;
  auto degree_ptr = reinterpret_cast<int*>(degree_list->GetData());
  auto cap_ptr = reinterpret_cast<int*>(cap_list->GetData());
  size_t offset = 0;
  for (size_t i = 0; i < vnum; ++i) {
    offset += adj_list_cap_ptr()[i];
    degree_ptr[i] = adj_list_size_ptr()[i].load(std::memory_order_relaxed);
    cap_ptr[i] = adj_list_cap_ptr()[i];

    if (degree_ptr[i] != cap_ptr[i]) {
      need_cap_list = true;
    }
  }

  auto cap_file = new_snapshot_dir + "/" + name + ".cap";
  if (need_cap_list) {
    cap_list->Dump(cap_file);
  } else if (std::filesystem::exists(cap_file)) {
    std::filesystem::remove(cap_file);
  }

  degree_list->Dump(new_snapshot_dir + "/" + name + ".deg");
  std::string filename = new_snapshot_dir + "/" + name + ".nbr";
  FILE* fout = fopen(filename.c_str(), "wb");
  if (fout == nullptr) {
    std::stringstream ss;
    ss << "Failed to open nbr list " << filename << ", " << strerror(errno);
    LOG(ERROR) << ss.str();
    throw std::runtime_error(ss.str());
  }

  // First write empty FileHeader, which will be updated after writing the data.
  FileHeader header{};
  if (fwrite(&header, sizeof(FileHeader), 1, fout) != 1) {
    fclose(fout);
    std::stringstream ss;
    ss << "Failed to write FileHeader for nbr list " << filename << ", "
       << strerror(errno);
    LOG(ERROR) << ss.str();
    throw std::runtime_error(ss.str());
  }
  MD5_CTX md5_ctx;
  MD5_Init(&md5_ctx);

  for (size_t i = 0; i < vnum; ++i) {
    size_t ret{};
    if ((ret = fwrite(adj_list_buffer_ptr()[i], sizeof(nbr_t),
                      adj_list_cap_ptr()[i], fout)) !=
        static_cast<size_t>(adj_list_cap_ptr()[i])) {
      fclose(fout);
      std::stringstream ss;
      ss << "Failed to write nbr list " << filename << ", expected "
         << adj_list_cap_ptr()[i] << ", got " << ret << ", " << strerror(errno);
      LOG(ERROR) << ss.str();
      throw std::runtime_error(ss.str());
    }
    MD5_Update(&md5_ctx, adj_list_buffer_ptr()[i],
               sizeof(nbr_t) * adj_list_cap_ptr()[i]);
  }

  MD5_Final(header.data_md5, &md5_ctx);

  // Update the FileHeader with the correct MD5.
  if (fseek(fout, 0, SEEK_SET) != 0) {
    fclose(fout);
    std::stringstream ss;
    ss << "Failed to seek to beginning of nbr list " << filename << ", "
       << strerror(errno);
    LOG(ERROR) << ss.str();
    throw std::runtime_error(ss.str());
  }
  if (fwrite(&header, sizeof(FileHeader), 1, fout) != 1) {
    fclose(fout);
    std::stringstream ss;
    ss << "Failed to write FileHeader for nbr list " << filename << ", "
       << strerror(errno);
    LOG(ERROR) << ss.str();
    throw std::runtime_error(ss.str());
  }

  int ret = 0;
  if ((ret = fflush(fout)) != 0) {
    fclose(fout);
    std::stringstream ss;
    ss << "Failed to flush nbr list " << filename << ", error code: " << ret
       << " " << strerror(errno);
    LOG(ERROR) << ss.str();
    throw std::runtime_error(ss.str());
  }
  if ((ret = fclose(fout)) != 0) {
    std::stringstream ss;
    ss << "Failed to close nbr list " << filename << ", error code: " << ret
       << " " << strerror(errno);
    LOG(ERROR) << ss.str();
    throw std::runtime_error(ss.str());
  }
}

template <typename EDATA_T>
void MutableCsr<EDATA_T>::reset_timestamp() {
  size_t vnum = vertex_capacity();
  for (size_t i = 0; i != vnum; ++i) {
    nbr_t* nbrs = adj_list_buffer_ptr()[i];
    if (nbrs == nullptr) {
      continue;
    }
    size_t deg = adj_list_size_ptr()[i].load(std::memory_order_relaxed);
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
  for (size_t i = 0; i != vnum; ++i) {
    int sz = adj_list_size_ptr()[i];
    nbr_t* read_ptr = adj_list_buffer_ptr()[i];
    if (read_ptr == nullptr) {
      continue;
    }
    nbr_t* read_end = read_ptr + sz;
    nbr_t* write_ptr = adj_list_buffer_ptr()[i];
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
    adj_list_size_ptr()[i] -= removed;
  }
}

template <typename EDATA_T>
void MutableCsr<EDATA_T>::resize(vid_t vnum) {
  if (adj_list_buffer_ == nullptr || adj_list_size_ == nullptr ||
      adj_list_capacity_ == nullptr) {
    LOG(ERROR) << "Containers not initialized, cannot resize";
    THROW_RUNTIME_ERROR("Containers not initialized");
  }
  auto old_vnum = vertex_capacity();
  if (vnum > old_vnum) {
    adj_list_buffer_->Resize(vnum * sizeof(nbr_t*));
    adj_list_size_->Resize(vnum * sizeof(std::atomic<int>));
    adj_list_capacity_->Resize(vnum * sizeof(int));
    for (vid_t i = old_vnum; i < vnum; ++i) {
      adj_list_buffer_ptr()[i] = nullptr;
      adj_list_size_ptr()[i].store(0, std::memory_order_relaxed);
      adj_list_cap_ptr()[i] = 0;
    }
    delete[] locks_;
    locks_ = new SpinLock[vnum];
  } else {
    adj_list_buffer_->Resize(vnum * sizeof(nbr_t*));
    adj_list_size_->Resize(vnum * sizeof(std::atomic<int>));
    adj_list_capacity_->Resize(vnum * sizeof(int));
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
  CloseAndReset(adj_list_buffer_);
  CloseAndReset(adj_list_size_);
  CloseAndReset(adj_list_capacity_);
  CloseAndReset(nbr_list_);
}

template <typename EDATA_T>
void MutableCsr<EDATA_T>::batch_sort_by_edge_data(timestamp_t ts) {
  if (adj_list_buffer_ != nullptr) {
    size_t vnum = vertex_capacity();
    for (size_t i = 0; i != vnum; ++i) {
      nbr_t* begin = adj_list_buffer_ptr()[i];
      if (begin == nullptr) {
        continue;
      }
      std::sort(begin,
                begin + adj_list_size_ptr()[i].load(std::memory_order_relaxed),
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
  for (vid_t src : src_set) {
    if (src < vnum) {
      adj_list_size_ptr()[src].store(0, std::memory_order_relaxed);
    }
  }
  for (vid_t src = 0; src < vnum; ++src) {
    if (adj_list_size_ptr()[src].load(std::memory_order_relaxed) == 0) {
      continue;
    }
    const nbr_t* read_ptr = adj_list_buffer_ptr()[src];
    if (read_ptr == nullptr) {
      continue;
    }
    const nbr_t* read_end =
        read_ptr + adj_list_size_ptr()[src].load(std::memory_order_relaxed);
    nbr_t* write_ptr = adj_list_buffer_ptr()[src];
    int removed = 0;
    while (read_ptr != read_end) {
      vid_t nbr = read_ptr->neighbor;
      if (dst_set.find(nbr) == dst_set.end()) {
        if (removed) {
          *write_ptr = *read_ptr;
        }
        ++write_ptr;
      } else {
        ++removed;
      }
      ++read_ptr;
    }
    adj_list_size_ptr()[src] -= removed;
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
  for (const auto& pair : src_dst_map) {
    vid_t src = pair.first;
    nbr_t* write_ptr = adj_list_buffer_ptr()[src];
    if (write_ptr == nullptr) {
      continue;
    }
    const nbr_t* read_end =
        write_ptr + adj_list_size_ptr()[src].load(std::memory_order_relaxed);
    while (write_ptr != read_end) {
      if (pair.second.find(write_ptr->neighbor) != pair.second.end()) {
        write_ptr->timestamp.store(std::numeric_limits<timestamp_t>::max());
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
  for (const auto& edge : edges) {
    if (edge.first >= vnum ||
        edge.second >=
            adj_list_size_ptr()[edge.first].load(std::memory_order_relaxed)) {
      continue;
    }
    src_offset_map[edge.first].insert(edge.second);
  }
  for (const auto& pair : src_offset_map) {
    vid_t src = pair.first;
    nbr_t* write_ptr = adj_list_buffer_ptr()[src];
    if (write_ptr == nullptr) {
      continue;
    }
    for (auto offset : pair.second) {
      write_ptr[offset].timestamp.store(
          std::numeric_limits<timestamp_t>::max());
    }
  }
}

template <typename EDATA_T>
void MutableCsr<EDATA_T>::delete_edge(vid_t src, int32_t offset,
                                      timestamp_t ts) {
  vid_t vnum = static_cast<vid_t>(vertex_capacity());
  if (src >= vnum ||
      offset >= adj_list_size_ptr()[src].load(std::memory_order_relaxed)) {
    THROW_INVALID_ARGUMENT_EXCEPTION("src out of bound or offset out of bound");
  }
  nbr_t* nbrs = adj_list_buffer_ptr()[src];
  if (nbrs == nullptr) {
    THROW_INVALID_ARGUMENT_EXCEPTION("adjacency buffer is null");
  }
  auto old_ts = nbrs[offset].timestamp.load();
  if (old_ts <= ts) {
    nbrs[offset].timestamp.store(std::numeric_limits<timestamp_t>::max());
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
  if (src >= vnum ||
      offset >= adj_list_size_ptr()[src].load(std::memory_order_relaxed)) {
    THROW_INVALID_ARGUMENT_EXCEPTION("src out of bound or offset out of bound");
  }
  nbr_t* nbrs = adj_list_buffer_ptr()[src];
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

  size_t total_to_move = 0;
  size_t total_to_allocate = 0;
  for (vid_t i = 0; i < vnum; ++i) {
    int old_deg = adj_list_size_ptr()[i].load(std::memory_order_relaxed);
    total_to_move += old_deg;
    int new_degree = degree[i] + old_deg;
    int new_cap = std::ceil(new_degree * NeugDBConfig::DEFAULT_RESERVE_RATIO);
    adj_list_cap_ptr()[i] = new_cap;
    total_to_allocate += new_cap;
  }

  std::vector<nbr_t> new_nbr_list(total_to_move);
  size_t offset = 0;
  for (vid_t i = 0; i < vnum; ++i) {
    int old_deg = adj_list_size_ptr()[i].load(std::memory_order_relaxed);
    if (old_deg > 0 && adj_list_buffer_ptr()[i] != nullptr) {
      memcpy(new_nbr_list.data() + offset, adj_list_buffer_ptr()[i],
             sizeof(nbr_t) * old_deg);
    }
    offset += old_deg;
  }
  nbr_list_->Resize(total_to_allocate * sizeof(nbr_t));
  auto base_ptr = reinterpret_cast<nbr_t*>(nbr_list_->GetData());
  offset = 0;
  size_t new_offset = 0;
  for (vid_t i = 0; i < vnum; ++i) {
    nbr_t* new_buffer = base_ptr != nullptr ? base_ptr + offset : nullptr;
    int old_deg = adj_list_size_ptr()[i].load(std::memory_order_relaxed);
    if (old_deg > 0 && new_buffer != nullptr) {
      memcpy(new_buffer, new_nbr_list.data() + new_offset,
             sizeof(nbr_t) * old_deg);
    }
    new_offset += old_deg;
    offset += adj_list_cap_ptr()[i];
    adj_list_buffer_ptr()[i] = new_buffer;
    adj_list_size_ptr()[i].store(old_deg, std::memory_order_relaxed);
  }

  for (size_t i = 0; i < src_list.size(); ++i) {
    vid_t src = src_list[i];
    if (src >= vnum) {
      continue;
    }
    vid_t dst = dst_list[i];
    const EDATA_T& data = data_list[i];
    auto& nbr = adj_list_buffer_ptr()[src][adj_list_size_ptr()[src].fetch_add(
        1, std::memory_order_relaxed)];
    nbr.neighbor = dst;
    nbr.data = data;
    nbr.timestamp.store(ts);
  }
}

template <typename EDATA_T>
void MutableCsr<EDATA_T>::load_meta(const std::string& prefix) {
  std::string meta_file_path = prefix + ".meta";
  if (std::filesystem::exists(meta_file_path)) {
    read_file(meta_file_path, &unsorted_since_, sizeof(timestamp_t), 1);
  } else {
    unsorted_since_ = 0;
  }
}

template <typename EDATA_T>
void MutableCsr<EDATA_T>::dump_meta(const std::string& prefix) const {
  std::string meta_file_path = prefix + ".meta";
  write_file(meta_file_path, &unsorted_since_, sizeof(timestamp_t), 1);
}

template <typename EDATA_T>
void SingleMutableCsr<EDATA_T>::open(const std::string& name,
                                     const std::string& snapshot_dir,
                                     const std::string& work_dir) {
  close();
  auto snapshot_file = snapshot_dir + "/" + name + ".snbr";
  auto tmp_file = tmp_dir(work_dir) + "/" + name + ".snbr";
  if (!std::filesystem::exists(snapshot_file)) {
    file_utils::create_file(tmp_file, sizeof(FileHeader));
  } else {
    file_utils::copy_file(snapshot_file, tmp_file, true);
  }
  nbr_list_ = std::make_unique<FileSharedMMap>();
  nbr_list_->Open(tmp_file);
}

template <typename EDATA_T>
void SingleMutableCsr<EDATA_T>::open_in_memory(const std::string& prefix) {
  close();

  auto snapshot_file = prefix + ".snbr";
  nbr_list_ = std::make_unique<FilePrivateMMap>();
  if (std::filesystem::exists(snapshot_file)) {
    nbr_list_->Open(snapshot_file);
  }
}

template <typename EDATA_T>
void SingleMutableCsr<EDATA_T>::open_with_hugepages(const std::string& prefix) {
  close();

  auto snapshot_file = prefix + ".snbr";
  nbr_list_ = std::make_unique<AnonHugeMMap>();
  if (std::filesystem::exists(snapshot_file)) {
    nbr_list_->Open(snapshot_file);
  }
}

template <typename EDATA_T>
void SingleMutableCsr<EDATA_T>::dump(const std::string& name,
                                     const std::string& new_snapshot_dir) {
  // TODO: opt with mv
  if (!nbr_list_) {
    return;
  }
  nbr_list_->Sync();
  nbr_list_->Dump(new_snapshot_dir + "/" + name + ".snbr");
}

template <typename EDATA_T>
void SingleMutableCsr<EDATA_T>::reset_timestamp() {
  if (!nbr_list_) {
    return;
  }
  nbr_t* data = nbr_entries();
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
    auto data = nbr_entries();
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
  CloseAndReset(nbr_list_);
}

template <typename EDATA_T>
void SingleMutableCsr<EDATA_T>::batch_sort_by_edge_data(timestamp_t ts) {}

template <typename EDATA_T>
void SingleMutableCsr<EDATA_T>::batch_delete_vertices(
    const std::set<vid_t>& src_set, const std::set<vid_t>& dst_set) {
  if (!nbr_list_) {
    return;
  }
  nbr_t* data = nbr_entries();
  vid_t vnum = static_cast<vid_t>(vertex_capacity());
  for (auto src : src_set) {
    if (src < vnum) {
      data[src].timestamp.store(std::numeric_limits<timestamp_t>::max());
    }
  }
  for (vid_t v = 0; v < vnum; ++v) {
    auto& nbr = data[v];
    if (dst_set.find(nbr.neighbor) != dst_set.end()) {
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
  nbr_t* data = nbr_entries();
  vid_t vnum = static_cast<vid_t>(vertex_capacity());
  for (size_t i = 0; i != src_list.size(); ++i) {
    vid_t src = src_list[i];
    vid_t dst = dst_list[i];
    if (src >= vnum) {
      continue;
    }
    auto& nbr = data[src];
    if (nbr.neighbor == dst) {
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
  nbr_t* data = nbr_entries();
  vid_t vnum = static_cast<vid_t>(vertex_capacity());
  for (const auto& edge : edge_list) {
    vid_t src = edge.first;
    if (src >= vnum) {
      continue;
    }
    auto& nbr = data[src];
    assert(edge.second == 0);
    nbr.timestamp.store(std::numeric_limits<timestamp_t>::max());
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
  nbr_t* data = nbr_entries();
  auto& nbr = data[src];
  assert(offset == 0);
  if (nbr.timestamp.load() <= ts) {
    nbr.timestamp.store(std::numeric_limits<timestamp_t>::max());
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
  nbr_t* data = nbr_entries();
  auto& nbr = data[src];
  if (nbr.neighbor != nbr_vid) {
    THROW_INVALID_ARGUMENT_EXCEPTION("neighbor id not match");
  }
  if (nbr.timestamp.load() == std::numeric_limits<timestamp_t>::max()) {
    nbr.timestamp.store(ts);
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
  nbr_t* data = nbr_entries();
  for (size_t i = 0; i != src_list.size(); ++i) {
    vid_t src = src_list[i];
    if (src >= vnum) {
      continue;
    }
    auto& nbr = data[src];
    nbr.neighbor = dst_list[i];
    nbr.data = data_list[i];
    nbr.timestamp.store(ts);
  }
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
