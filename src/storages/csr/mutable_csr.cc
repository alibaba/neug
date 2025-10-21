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
#include <cmath>
#include <cstdint>
#include <filesystem>
#include <map>
#include <memory>
#include <stdexcept>
#include <thread>
#include <utility>

#include "neug/storages/file_names.h"
#include "neug/utils/exception/exception.h"

namespace gs {

void read_file(const std::string& filename, void* buffer, size_t size,
               size_t num) {
  FILE* fin = fopen(filename.c_str(), "r");
  if (fin == nullptr) {
    std::stringstream ss;
    ss << "Failed to open file " << filename << ", " << strerror(errno);
    LOG(ERROR) << ss.str();
    THROW_RUNTIME_ERROR(ss.str());
  }
  size_t ret_len = 0;
  if ((ret_len = fread(buffer, size, num, fin)) != num) {
    std::stringstream ss;
    ss << "Failed to read file " << filename << ", expected " << num << ", got "
       << ret_len << ", " << strerror(errno);
    LOG(ERROR) << ss.str();
    THROW_RUNTIME_ERROR(ss.str());
  }
  int ret = 0;
  if ((ret = fclose(fin)) != 0) {
    std::stringstream ss;
    ss << "Failed to close file " << filename << ", error code: " << ret << " "
       << strerror(errno);
    LOG(ERROR) << ss.str();
    THROW_RUNTIME_ERROR(ss.str());
  }
}

void write_file(const std::string& filename, const void* buffer, size_t size,
                size_t num) {
  FILE* fout = fopen(filename.c_str(), "wb");
  if (fout == nullptr) {
    std::stringstream ss;
    ss << "Failed to open file " << filename << ", " << strerror(errno);
    LOG(ERROR) << ss.str();
    THROW_RUNTIME_ERROR(ss.str());
  }
  size_t ret_len = 0;
  if ((ret_len = fwrite(buffer, size, num, fout)) != num) {
    std::stringstream ss;
    ss << "Failed to write file " << filename << ", expected " << num
       << ", got " << ret_len << ", " << strerror(errno);
    LOG(ERROR) << ss.str();
  }
  int ret = 0;
  if ((ret = fclose(fout)) != 0) {
    std::stringstream ss;
    ss << "Failed to close file " << filename << ", error code: " << ret << " "
       << strerror(errno);
    LOG(ERROR) << ss.str();
    THROW_RUNTIME_ERROR(ss.str());
  }
}

template <typename EDATA_T>
void MutableCsr<EDATA_T>::open(const std::string& name,
                               const std::string& snapshot_dir,
                               const std::string& work_dir) {
  mmap_array<int> degree_list;
  mmap_array<int>* cap_list = &degree_list;
  if (snapshot_dir != "") {
    degree_list.open(snapshot_dir + "/" + name + ".deg", false);
    if (std::filesystem::exists(snapshot_dir + "/" + name + ".cap")) {
      cap_list = new mmap_array<int>();
      cap_list->open(snapshot_dir + "/" + name + ".cap", false);
    }
    nbr_list_.open(snapshot_dir + "/" + name + ".nbr", false);
    load_meta(snapshot_dir + "/" + name);
  }
  if (std::filesystem::exists(tmp_dir(work_dir) + "/" + name + ".nbr")) {
    std::filesystem::remove(tmp_dir(work_dir) + "/" + name + ".nbr");
  }
  nbr_list_.touch(tmp_dir(work_dir) + "/" + name + ".nbr");
  adj_list_buffer_.open(tmp_dir(work_dir) + "/" + name + ".adj_buffer", true);
  adj_list_buffer_.resize(degree_list.size());
  adj_list_size_.open(tmp_dir(work_dir) + "/" + name + ".adj_size", true);
  adj_list_size_.resize(degree_list.size());
  adj_list_capacity_.open(tmp_dir(work_dir) + "/" + name + ".adj_cap", true);
  adj_list_capacity_.resize(degree_list.size());
  locks_ = new grape::SpinLock[degree_list.size()];

  nbr_t* ptr = nbr_list_.data();
  for (size_t i = 0; i < degree_list.size(); ++i) {
    int degree = degree_list[i];
    int cap = (*cap_list)[i];
    adj_list_buffer_[i] = ptr;
    adj_list_capacity_[i] = cap;
    adj_list_size_[i] = degree;
    ptr += cap;
  }
  if (cap_list != &degree_list) {
    delete cap_list;
  }
}

template <typename EDATA_T>
void MutableCsr<EDATA_T>::open_in_memory(const std::string& prefix,
                                         size_t v_cap) {
  mmap_array<int> degree_list;
  degree_list.open(prefix + ".deg", false);
  load_meta(prefix);
  mmap_array<int>* cap_list = &degree_list;
  if (std::filesystem::exists(prefix + ".cap")) {
    cap_list = new mmap_array<int>();
    cap_list->open(prefix + ".cap", false);
  }

  nbr_list_.open(prefix + ".nbr", false);

  adj_list_buffer_.reset();
  adj_list_size_.reset();
  adj_list_capacity_.reset();
  v_cap = std::max(v_cap, degree_list.size());
  adj_list_buffer_.resize(v_cap);
  adj_list_size_.resize(v_cap);
  adj_list_capacity_.resize(v_cap);
  locks_ = new grape::SpinLock[v_cap];

  nbr_t* ptr = nbr_list_.data();
  for (size_t i = 0; i < degree_list.size(); ++i) {
    int degree = degree_list[i];
    int cap = (*cap_list)[i];
    adj_list_buffer_[i] = ptr;
    adj_list_capacity_[i] = cap;
    adj_list_size_[i] = degree;
    ptr += cap;
  }
  for (size_t i = degree_list.size(); i < v_cap; ++i) {
    adj_list_buffer_[i] = ptr;
    adj_list_capacity_[i] = 0;
    adj_list_size_[i] = 0;
  }

  if (cap_list != &degree_list) {
    delete cap_list;
  }
}

template <typename EDATA_T>
void MutableCsr<EDATA_T>::open_with_hugepages(const std::string& prefix,
                                              size_t v_cap) {
  mmap_array<int> degree_list;
  degree_list.open(prefix + ".deg", false);
  load_meta(prefix);
  mmap_array<int>* cap_list = &degree_list;
  if (std::filesystem::exists(prefix + ".cap")) {
    cap_list = new mmap_array<int>();
    cap_list->open(prefix + ".cap", false);
  }

  nbr_list_.open_with_hugepages(prefix + ".nbr");

  adj_list_buffer_.reset();
  adj_list_size_.reset();
  adj_list_capacity_.reset();
  v_cap = std::max(v_cap, degree_list.size());
  adj_list_buffer_.open_with_hugepages("");
  adj_list_buffer_.resize(v_cap);
  adj_list_size_.open_with_hugepages("");
  adj_list_size_.resize(v_cap);
  adj_list_capacity_.open_with_hugepages("");
  adj_list_capacity_.resize(v_cap);
  locks_ = new grape::SpinLock[v_cap];

  nbr_t* ptr = nbr_list_.data();
  for (size_t i = 0; i < degree_list.size(); ++i) {
    int degree = degree_list[i];
    int cap = (*cap_list)[i];
    adj_list_buffer_[i] = ptr;
    adj_list_capacity_[i] = cap;
    adj_list_size_[i] = degree;
    ptr += cap;
  }
  for (size_t i = degree_list.size(); i < v_cap; ++i) {
    adj_list_buffer_[i] = ptr;
    adj_list_capacity_[i] = 0;
    adj_list_size_[i] = 0;
  }

  if (cap_list != &degree_list) {
    delete cap_list;
  }
}

template <typename EDATA_T>
void MutableCsr<EDATA_T>::dump(const std::string& name,
                               const std::string& new_snapshot_dir) {
  size_t vnum = adj_list_buffer_.size();
  bool reuse_nbr_list = true;
  dump_meta(new_snapshot_dir + "/" + name);
  mmap_array<int> degree_list;
  std::vector<int> cap_list;
  degree_list.open("", false);
  degree_list.resize(vnum);
  cap_list.resize(vnum);
  bool need_cap_list = false;
  size_t offset = 0;
  for (size_t i = 0; i < vnum; ++i) {
    if (adj_list_size_[i] != 0) {
      if (!(adj_list_buffer_[i] == nbr_list_.data() + offset &&
            offset < nbr_list_.size())) {
        reuse_nbr_list = false;
      }
    }
    offset += adj_list_capacity_[i];

    degree_list[i] = adj_list_size_[i];
    cap_list[i] = adj_list_capacity_[i];
    if (degree_list[i] != cap_list[i]) {
      need_cap_list = true;
    }
  }

  if (need_cap_list) {
    write_file(new_snapshot_dir + "/" + name + ".cap", cap_list.data(),
               sizeof(int), cap_list.size());
  }

  degree_list.dump(new_snapshot_dir + "/" + name + ".deg");

  if (reuse_nbr_list && !nbr_list_.filename().empty() &&
      std::filesystem::exists(nbr_list_.filename())) {
    std::error_code errorCode;
    std::string link_filename = new_snapshot_dir + "/" + name + ".nbr";
    if (std::filesystem::exists(link_filename)) {
      std::filesystem::remove(link_filename);
    }
    std::filesystem::create_hard_link(nbr_list_.filename(),
                                      new_snapshot_dir + "/" + name + ".nbr",
                                      errorCode);
    if (errorCode) {
      std::stringstream ss;
      ss << "Failed to create hard link from " << nbr_list_.filename() << " to "
         << new_snapshot_dir + "/" + name + ".snbr"
         << ", error code: " << errorCode << " " << errorCode.message();
      LOG(ERROR) << ss.str();
      throw std::runtime_error(ss.str());
    }
  } else {
    FILE* fout = fopen((new_snapshot_dir + "/" + name + ".nbr").c_str(), "wb");
    std::string filename = new_snapshot_dir + "/" + name + ".nbr";
    if (fout == nullptr) {
      std::stringstream ss;
      ss << "Failed to open nbr list " << filename << ", " << strerror(errno);
      LOG(ERROR) << ss.str();
      throw std::runtime_error(ss.str());
    }

    for (size_t i = 0; i < vnum; ++i) {
      size_t ret{};
      if ((ret = fwrite(adj_list_buffer_[i], sizeof(nbr_t),
                        adj_list_capacity_[i], fout)) !=
          static_cast<size_t>(adj_list_capacity_[i])) {
        std::stringstream ss;
        ss << "Failed to write nbr list " << filename << ", expected "
           << adj_list_capacity_[i] << ", got " << ret << ", "
           << strerror(errno);
        LOG(ERROR) << ss.str();
        throw std::runtime_error(ss.str());
      }
    }
    int ret = 0;
    if ((ret = fflush(fout)) != 0) {
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
}

template <typename EDATA_T>
void MutableCsr<EDATA_T>::warmup(int thread_num) const {}

template <typename EDATA_T>
void MutableCsr<EDATA_T>::reset_timestamp() {
  size_t vnum = adj_list_buffer_.size();
  for (size_t i = 0; i != vnum; ++i) {
    nbr_t* nbrs = adj_list_buffer_[i];
    size_t deg = adj_list_size_[i].load(std::memory_order_relaxed);
    for (size_t j = 0; j != deg; ++j) {
      nbrs[j].timestamp.store(0, std::memory_order_relaxed);
    }
  }
}

template <typename EDATA_T>
void MutableCsr<EDATA_T>::resize(vid_t vnum) {
  if (vnum > adj_list_size_.size()) {
    size_t old_size = adj_list_size_.size();
    adj_list_buffer_.resize(vnum);
    adj_list_size_.resize(vnum);
    adj_list_capacity_.resize(vnum);
    for (size_t k = old_size; k != vnum; ++k) {
      adj_list_buffer_[k] = nullptr;
      adj_list_size_[k] = 0;
      adj_list_capacity_[k] = 0;
    }
    delete[] locks_;
    locks_ = new grape::SpinLock[vnum];
  } else {
    adj_list_buffer_.resize(vnum);
    adj_list_size_.resize(vnum);
    adj_list_capacity_.resize(vnum);
  }
}

template <typename EDATA_T>
void MutableCsr<EDATA_T>::close() {
  if (locks_ != nullptr) {
    delete[] locks_;
    locks_ = nullptr;
  }
  adj_list_buffer_.reset();
  adj_list_size_.reset();
  adj_list_capacity_.reset();
  nbr_list_.reset();
}

template <typename EDATA_T>
void MutableCsr<EDATA_T>::batch_sort_by_edge_data(timestamp_t ts) {
  size_t vnum = adj_list_buffer_.size();
  for (size_t i = 0; i != vnum; ++i) {
    std::sort(
        adj_list_buffer_[i],
        adj_list_buffer_[i] + adj_list_size_[i].load(std::memory_order_relaxed),
        [](const nbr_t& lhs, const nbr_t& rhs) { return lhs.data < rhs.data; });
  }
  unsorted_since_ = ts;
}

template <typename EDATA_T>
void MutableCsr<EDATA_T>::batch_delete_vertices(
    const std::set<vid_t>& src_set, const std::set<vid_t>& dst_set) {
  vid_t vnum = adj_list_size_.size();
  for (vid_t src : src_set) {
    if (src < vnum) {
      adj_list_size_[src] = 0;
    }
  }
  for (vid_t src = 0; src < vnum; ++src) {
    if (adj_list_size_[src] == 0) {
      continue;
    }
    const nbr_t* read_ptr = adj_list_buffer_[src];
    const nbr_t* read_end = read_ptr + adj_list_size_[src].load();
    nbr_t* write_ptr = adj_list_buffer_[src];
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
    adj_list_size_[src] -= removed;
  }
}

template <typename EDATA_T>
void MutableCsr<EDATA_T>::batch_delete_edges(
    const std::vector<vid_t>& src_list, const std::vector<vid_t>& dst_list) {
  std::map<vid_t, std::set<vid_t>> src_dst_map;
  vid_t vnum = adj_list_size_.size();
  for (size_t i = 0; i < src_list.size(); ++i) {
    vid_t src = src_list[i];
    if (src >= vnum) {
      continue;
    }
    src_dst_map[src].insert(dst_list[i]);
  }
  for (const auto& pair : src_dst_map) {
    vid_t src = pair.first;
    const nbr_t* read_ptr = adj_list_buffer_[src];
    const nbr_t* read_end = read_ptr + adj_list_size_[src].load();
    nbr_t* write_ptr = adj_list_buffer_[src];
    int removed = 0;
    while (read_ptr != read_end) {
      vid_t nbr = read_ptr->neighbor;
      if (pair.second.find(nbr) == pair.second.end()) {
        if (removed) {
          *write_ptr = *read_ptr;
        }
        ++write_ptr;
      } else {
        ++removed;
      }
      ++read_ptr;
    }
    adj_list_size_[src] -= removed;
  }
}

template <typename EDATA_T>
void MutableCsr<EDATA_T>::batch_put_edges(const std::vector<vid_t>& src_list,
                                          const std::vector<vid_t>& dst_list,
                                          const std::vector<EDATA_T>& data_list,
                                          timestamp_t ts) {
  vid_t vnum = adj_list_size_.size();
  std::vector<int> degree(vnum, 0);
  for (auto src : src_list) {
    if (src < vnum) {
      degree[src]++;
    }
  }

  size_t total_to_move = 0;
  size_t total_to_allocate = 0;
  for (vid_t i = 0; i < vnum; ++i) {
    int old_deg = adj_list_size_[i].load();
    total_to_move += old_deg;
    int new_degree = degree[i] + old_deg;
    int new_cap = std::ceil(new_degree * RESERVE_RATIO);
    adj_list_capacity_[i] = new_cap;
    total_to_allocate += new_cap;
  }

  std::vector<nbr_t> new_nbr_list(total_to_move);
  size_t offset = 0;
  for (vid_t i = 0; i < vnum; ++i) {
    int old_deg = adj_list_size_[i].load();
    memcpy(new_nbr_list.data() + offset, adj_list_buffer_[i],
           sizeof(nbr_t) * old_deg);
    offset += old_deg;
  }

  nbr_list_.resize(total_to_allocate);
  offset = 0;
  size_t new_offset = 0;
  for (vid_t i = 0; i < vnum; ++i) {
    nbr_t* new_buffer = nbr_list_.data() + offset;
    int old_deg = adj_list_size_[i].load();
    memcpy(new_buffer, new_nbr_list.data() + new_offset,
           sizeof(nbr_t) * old_deg);
    new_offset += old_deg;
    offset += adj_list_capacity_[i];
    adj_list_buffer_[i] = new_buffer;
    adj_list_size_[i].store(old_deg);
  }

  for (size_t i = 0; i < src_list.size(); ++i) {
    vid_t src = src_list[i];
    if (src >= vnum) {
      continue;
    }
    vid_t dst = dst_list[i];
    const EDATA_T& data = data_list[i];
    auto& nbr = adj_list_buffer_[src][adj_list_size_[src].fetch_add(1)];
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
  if (!std::filesystem::exists(tmp_dir(work_dir) + "/" + name + ".snbr")) {
    copy_file(snapshot_dir + "/" + name + ".snbr",
              tmp_dir(work_dir) + "/" + name + ".snbr");
  }
  nbr_list_.open(tmp_dir(work_dir) + "/" + name + ".snbr", true);
}

template <typename EDATA_T>
void SingleMutableCsr<EDATA_T>::open_in_memory(const std::string& prefix,
                                               size_t v_cap) {
  nbr_list_.open(prefix + ".snbr", false);
  if (nbr_list_.size() < v_cap) {
    size_t old_size = nbr_list_.size();
    nbr_list_.reset();
    nbr_list_.resize(v_cap);
    if (old_size > 0) {
      read_file(prefix + ".snbr", nbr_list_.data(), sizeof(nbr_t), old_size);
    }
    for (size_t k = old_size; k != v_cap; ++k) {
      nbr_list_[k].timestamp.store(std::numeric_limits<timestamp_t>::max());
    }
  }
}

template <typename EDATA_T>
void SingleMutableCsr<EDATA_T>::open_with_hugepages(const std::string& prefix,
                                                    size_t v_cap) {
  nbr_list_.open_with_hugepages(prefix + ".snbr", v_cap);
  size_t old_size = nbr_list_.size();
  if (old_size < v_cap) {
    nbr_list_.resize(v_cap);
    for (size_t k = old_size; k != v_cap; ++k) {
      nbr_list_[k].timestamp.store(std::numeric_limits<timestamp_t>::max());
    }
  }
}

template <typename EDATA_T>
void SingleMutableCsr<EDATA_T>::dump(const std::string& name,
                                     const std::string& new_snapshot_dir) {
  // TODO: opt with mv
  write_file(new_snapshot_dir + "/" + name + ".snbr", nbr_list_.data(),
             sizeof(nbr_t), nbr_list_.size());
}

template <typename EDATA_T>
void SingleMutableCsr<EDATA_T>::warmup(int thread_num) const {
  size_t vnum = nbr_list_.size();
  std::vector<std::thread> threads;
  std::atomic<size_t> v_i(0);
  std::atomic<size_t> output(0);
  const size_t chunk = 4096;
  for (int i = 0; i < thread_num; ++i) {
    threads.emplace_back([&]() {
      size_t ret = 0;
      while (true) {
        size_t begin = std::min(v_i.fetch_add(chunk), vnum);
        size_t end = std::min(begin + chunk, vnum);
        if (begin == end) {
          break;
        }
        while (begin < end) {
          auto& nbr = nbr_list_[begin];
          ret += nbr.neighbor;
          ++begin;
        }
      }
      output.fetch_add(ret);
    });
  }
  for (auto& thrd : threads) {
    thrd.join();
  }
  (void) output.load();
}

template <typename EDATA_T>
void SingleMutableCsr<EDATA_T>::reset_timestamp() {
  size_t vnum = nbr_list_.size();
  for (size_t i = 0; i != vnum; ++i) {
    nbr_list_[i].timestamp.store(0, std::memory_order_relaxed);
  }
}

template <typename EDATA_T>
void SingleMutableCsr<EDATA_T>::resize(vid_t vnum) {
  if (vnum > nbr_list_.size()) {
    size_t old_size = nbr_list_.size();
    nbr_list_.resize(vnum);
    for (size_t k = old_size; k != vnum; ++k) {
      nbr_list_[k].timestamp.store(std::numeric_limits<timestamp_t>::max());
    }
  } else {
    nbr_list_.resize(vnum);
  }
}

template <typename EDATA_T>
void SingleMutableCsr<EDATA_T>::close() {
  nbr_list_.reset();
}

template <typename EDATA_T>
void SingleMutableCsr<EDATA_T>::batch_sort_by_edge_data(timestamp_t ts) {}

template <typename EDATA_T>
void SingleMutableCsr<EDATA_T>::batch_delete_vertices(
    const std::set<vid_t>& src_set, const std::set<vid_t>& dst_set) {
  vid_t vnum = nbr_list_.size();
  for (auto src : src_set) {
    if (src < vnum) {
      nbr_list_[src].timestamp.store(std::numeric_limits<timestamp_t>::max());
    }
  }
  for (vid_t v = 0; v < vnum; ++v) {
    auto& nbr = nbr_list_[v];
    if (dst_set.find(nbr.neighbor) != dst_set.end()) {
      nbr.timestamp.store(std::numeric_limits<timestamp_t>::max());
    }
  }
}

template <typename EDATA_T>
void SingleMutableCsr<EDATA_T>::batch_delete_edges(
    const std::vector<vid_t>& src_list, const std::vector<vid_t>& dst_list) {
  vid_t vnum = nbr_list_.size();
  for (size_t i = 0; i != src_list.size(); ++i) {
    vid_t src = src_list[i];
    vid_t dst = dst_list[i];
    if (src >= vnum) {
      continue;
    }
    auto& nbr = nbr_list_[src];
    if (nbr.neighbor == dst) {
      nbr.timestamp.store(std::numeric_limits<timestamp_t>::max());
    }
  }
}

template <typename EDATA_T>
void SingleMutableCsr<EDATA_T>::batch_put_edges(
    const std::vector<vid_t>& src_list, const std::vector<vid_t>& dst_list,
    const std::vector<EDATA_T>& data_list, timestamp_t ts) {
  vid_t vnum = nbr_list_.size();
  for (size_t i = 0; i != src_list.size(); ++i) {
    vid_t src = src_list[i];
    if (src >= vnum) {
      continue;
    }
    auto& nbr = nbr_list_[src];
    nbr.neighbor = dst_list[i];
    nbr.data = data_list[i];
    nbr.timestamp.store(ts);
  }
}

template class MutableCsr<grape::EmptyType>;
template class MutableCsr<int32_t>;
template class MutableCsr<uint32_t>;
template class MutableCsr<Date>;
template class MutableCsr<int64_t>;
template class MutableCsr<uint64_t>;
template class MutableCsr<double>;
template class MutableCsr<float>;
template class MutableCsr<TimeStamp>;
template class MutableCsr<DateTime>;
template class MutableCsr<Interval>;

template class SingleMutableCsr<TimeStamp>;
template class SingleMutableCsr<float>;
template class SingleMutableCsr<double>;
template class SingleMutableCsr<uint64_t>;
template class SingleMutableCsr<int64_t>;
template class SingleMutableCsr<Date>;
template class SingleMutableCsr<uint32_t>;
template class SingleMutableCsr<int32_t>;
template class SingleMutableCsr<grape::EmptyType>;
template class SingleMutableCsr<DateTime>;
template class SingleMutableCsr<Interval>;

}  // namespace gs
