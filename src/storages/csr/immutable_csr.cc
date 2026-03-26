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
#include "neug/storages/column/anon_mmap_container.h"
#include "neug/storages/column/file_header.h"
#include "neug/storages/column/file_mmap_container.h"
#include "neug/storages/column/i_container.h"
#include "neug/storages/file_names.h"
#include "neug/utils/property/types.h"

namespace neug {

template <typename EDATA_T>
void ImmutableCsr<EDATA_T>::open(const std::string& name,
                                 const std::string& snapshot_dir,
                                 const std::string& work_dir) {
  // Changes made to the CSR will not be synchronized to the file
  // TODO(luoxiaojian): Implement the insert operation on ImmutableCsr.
  if (snapshot_dir.empty() || !std::filesystem::exists(snapshot_dir)) {
    THROW_INVALID_ARGUMENT_EXCEPTION(
        "Snapshot directory is required for disk-backed open()");
  }
  auto prefix = snapshot_dir + "/" + name;
  close();
  load_meta(prefix);
  auto degree_file_name = prefix + ".deg";
  auto nbr_file_name = prefix + ".nbr";
  auto tmp_nbr_file_name = tmp_dir(work_dir) + "/" + name + ".nbr";
  auto tmp_degree_file_name = tmp_dir(work_dir) + "/" + name + ".deg";

  if (!std::filesystem::exists(degree_file_name)) {
    file_utils::create_file(tmp_degree_file_name, sizeof(FileHeader));
  } else {
    file_utils::copy_file(degree_file_name, tmp_degree_file_name, true);
  }
  degree_list_buffer_ = std::make_unique<FileSharedMMap>();
  degree_list_buffer_->Open(tmp_degree_file_name);

  if (!std::filesystem::exists(nbr_file_name)) {
    file_utils::create_file(tmp_nbr_file_name, sizeof(FileHeader));
  } else {
    file_utils::copy_file(nbr_file_name, tmp_nbr_file_name, true);
  }
  nbr_list_buffer_ = std::make_unique<FileSharedMMap>();
  nbr_list_buffer_->Open(tmp_nbr_file_name);

  adj_list_buffer_ = std::make_unique<FileSharedMMap>();
  auto tmp_adj_list_file_name = tmp_dir(work_dir) + "/" + name + ".adj";
  std::filesystem::remove(tmp_adj_list_file_name);
  file_utils::create_file(tmp_adj_list_file_name, sizeof(FileHeader));
  adj_list_buffer_->Open(tmp_adj_list_file_name);
  auto v_cap = size();
  adj_list_buffer_->Resize(v_cap * sizeof(nbr_t*));

  auto cur_nbr_list_ptr = nbr_list_ptr();
  for (size_t i = 0; i < v_cap; ++i) {
    int deg = degree_list_ptr()[i];
    if (deg != 0) {
      adj_lists_ptr()[i] = cur_nbr_list_ptr;
    } else {
      adj_lists_ptr()[i] = NULL;
    }
    cur_nbr_list_ptr += deg;
  }
}

template <typename EDATA_T>
void ImmutableCsr<EDATA_T>::open_in_memory(const std::string& prefix) {
  close();
  load_meta(prefix);
  auto degree_file_name = prefix + ".deg";
  auto nbr_file_name = prefix + ".nbr";
  degree_list_buffer_ = std::make_unique<FilePrivateMMap>();
  if (std::filesystem::exists(degree_file_name)) {
    degree_list_buffer_->Open(degree_file_name);
  }

  nbr_list_buffer_ = std::make_unique<FilePrivateMMap>();
  if (std::filesystem::exists(nbr_file_name)) {
    nbr_list_buffer_->Open(nbr_file_name);
  }

  adj_list_buffer_ = std::make_unique<AnonMMap>();
  auto v_cap = size();
  adj_list_buffer_->Resize(v_cap * sizeof(nbr_t*));

  auto cur_nbr_list_ptr = nbr_list_ptr();
  for (size_t i = 0; i < v_cap; ++i) {
    int deg = degree_list_ptr()[i];
    if (deg != 0) {
      adj_lists_ptr()[i] = cur_nbr_list_ptr;
    } else {
      adj_lists_ptr()[i] = NULL;
    }
    cur_nbr_list_ptr += deg;
  }
}

template <typename EDATA_T>
void ImmutableCsr<EDATA_T>::open_with_hugepages(const std::string& prefix) {
  close();
  load_meta(prefix);
  auto degree_file_name = prefix + ".deg";
  auto nbr_file_name = prefix + ".nbr";
  degree_list_buffer_ = std::make_unique<AnonHugeMMap>();
  if (std::filesystem::exists(degree_file_name)) {
    degree_list_buffer_->Open(degree_file_name);
  }

  nbr_list_buffer_ = std::make_unique<AnonHugeMMap>();
  if (std::filesystem::exists(nbr_file_name)) {
    nbr_list_buffer_->Open(nbr_file_name);
  }

  auto v_cap = size();
  adj_list_buffer_ = std::make_unique<AnonHugeMMap>();
  adj_list_buffer_->Resize(v_cap * sizeof(nbr_t*));
  auto cur_nbr_list_ptr = nbr_list_ptr();

  for (size_t i = 0; i < v_cap; ++i) {
    int deg = degree_list_ptr()[i];
    if (deg != 0) {
      adj_lists_ptr()[i] = cur_nbr_list_ptr;
    } else {
      adj_lists_ptr()[i] = NULL;
    }
    cur_nbr_list_ptr += deg;
  }
}

template <typename EDATA_T>
void ImmutableCsr<EDATA_T>::dump(const std::string& name,
                                 const std::string& new_snapshot_dir) {
  dump_meta(new_snapshot_dir + "/" + name);
  degree_list_buffer_->Sync();
  nbr_list_buffer_->Sync();
  degree_list_buffer_->Dump(new_snapshot_dir + "/" + name + ".deg");
  auto vnum = size();
  FILE* fout = fopen((new_snapshot_dir + "/" + name + ".nbr").c_str(), "wb");
  FileHeader header{};
  if (fwrite(&header, sizeof(FileHeader), 1, fout) != 1) {
    fclose(fout);
    std::stringstream ss;
    ss << "Failed to fwrite file [ " << new_snapshot_dir + "/" + name + ".nbr"
       << " ], " << strerror(errno);
    LOG(ERROR) << ss.str();
    THROW_RUNTIME_ERROR(ss.str());
  }
  MD5_CTX md5_ctx;
  MD5_Init(&md5_ctx);
  for (size_t k = 0; k < vnum; ++k) {
    int deg = degree_list_ptr()[k];
    if (deg != 0 && adj_lists_ptr()[k] != NULL) {
      const nbr_t* nbrs = adj_lists_ptr()[k];
      if (fwrite(nbrs, sizeof(nbr_t), deg, fout) != static_cast<size_t>(deg)) {
        std::stringstream ss;
        ss << "Failed to fwrite file [ "
           << new_snapshot_dir + "/" + name + ".nbr"
           << " ], " << strerror(errno);
        LOG(ERROR) << ss.str();
        THROW_RUNTIME_ERROR(ss.str());
      }
      MD5_Update(&md5_ctx, nbrs, sizeof(nbr_t) * deg);
    }
  }
  MD5_Final(header.data_md5, &md5_ctx);
  if (fseek(fout, 0, SEEK_SET) != 0) {
    fclose(fout);
    std::stringstream ss;
    ss << "Failed to fseek file [ " << new_snapshot_dir + "/" + name + ".nbr"
       << " ], " << strerror(errno);
    LOG(ERROR) << ss.str();
    THROW_RUNTIME_ERROR(ss.str());
  }
  if (fwrite(&header, sizeof(FileHeader), 1, fout) != 1) {
    fclose(fout);
    std::stringstream ss;
    ss << "Failed to fwrite file [ " << new_snapshot_dir + "/" + name + ".nbr"
       << " ], " << strerror(errno);
    LOG(ERROR) << ss.str();
    THROW_RUNTIME_ERROR(ss.str());
  }
  fflush(fout);
  fclose(fout);
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
  nbr_t* write_ptr = adj_lists_ptr()[0];
  for (vid_t i = 0; i < vnum; ++i) {
    int deg = degree_list_ptr()[i];
    if (deg == 0) {
      continue;
    }
    const nbr_t* read_ptr = adj_lists_ptr()[i];
    const nbr_t* read_end = read_ptr + deg;
    while (read_ptr != read_end) {
      if (read_ptr->neighbor != std::numeric_limits<vid_t>::max()) {
        if (removed) {
          *write_ptr = *read_ptr;
        }
        ++write_ptr;
      } else {
        --degree_list_ptr()[i];
        ++removed;
      }
      ++read_ptr;
    }
  }
  nbr_list_buffer_->Resize(nbr_list_buffer_->GetDataSize() -
                           removed * sizeof(nbr_t));
  nbr_t* ptr = nbr_list_ptr();
  for (vid_t i = 0; i < vnum; ++i) {
    adj_lists_ptr()[i] = ptr;
    ptr += degree_list_ptr()[i];
  }
}

template <typename EDATA_T>
void ImmutableCsr<EDATA_T>::resize(vid_t vnum) {
  auto old_v_cap = size();
  if (vnum > old_v_cap) {
    adj_list_buffer_->Resize(vnum * sizeof(nbr_t*));
    degree_list_buffer_->Resize(vnum * sizeof(int));
    for (vid_t i = old_v_cap; i < vnum; ++i) {
      adj_lists_ptr()[i] = NULL;
      degree_list_ptr()[i] = 0;
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
  for (size_t i = 0; i != vnum; ++i) {
    std::sort(
        adj_lists_ptr()[i], adj_lists_ptr()[i] + degree_list_ptr()[i],
        [](const nbr_t& lhs, const nbr_t& rhs) { return lhs.data < rhs.data; });
  }
  unsorted_since_ = ts;
}

template <typename EDATA_T>
void ImmutableCsr<EDATA_T>::batch_delete_vertices(
    const std::set<vid_t>& src_set, const std::set<vid_t>& dst_set) {
  vid_t vnum = size();
  size_t removed = 0;
  for (vid_t i = 0; i < vnum; ++i) {
    int deg = degree_list_ptr()[i];
    if (deg == 0) {
      continue;
    }
    if (src_set.find(i) != src_set.end()) {
      removed += deg;
      degree_list_ptr()[i] = 0;
    } else {
      const nbr_t* old_ptr = adj_lists_ptr()[i];
      const nbr_t* old_end = old_ptr + deg;
      nbr_t* new_ptr = adj_lists_ptr()[i] - removed;
      while (old_ptr != old_end) {
        if (dst_set.find(old_ptr->neighbor) == dst_set.end()) {
          *new_ptr = *old_ptr;
          ++new_ptr;
        } else {
          --degree_list_ptr()[i];
          ++removed;
        }
        ++old_ptr;
      }
    }
  }
  nbr_list_buffer_->Resize(
      (nbr_list_buffer_->GetDataSize() - removed * sizeof(nbr_t)));
  nbr_t* ptr = nbr_list_ptr();
  for (vid_t i = 0; i < vnum; ++i) {
    adj_lists_ptr()[i] = ptr;
    ptr += degree_list_ptr()[i];
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
  for (vid_t i = 0; i < vnum; ++i) {
    int deg = degree_list_ptr()[i];
    if (deg == 0) {
      continue;
    }
    auto iter = src_dst_map.find(i);
    if (iter != src_dst_map.end()) {
      const std::set<vid_t>& dst_set = iter->second;
      nbr_t* write_ptr = adj_lists_ptr()[i];
      const nbr_t* read_end = write_ptr + degree_list_ptr()[i];
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
  for (const auto& edge : edges) {
    if (edge.first >= vnum || edge.second >= degree_list_ptr()[edge.first]) {
      continue;
    }
    src_offset_map[edge.first].insert(edge.second);
  }
  for (vid_t i = 0; i < vnum; ++i) {
    int deg = degree_list_ptr()[i];
    if (deg == 0) {
      continue;
    }
    auto iter = src_offset_map.find(i);
    if (iter != src_offset_map.end()) {
      nbr_t* write_ptr = adj_lists_ptr()[i];
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
  if (src >= vnum || offset >= degree_list_ptr()[src]) {
    THROW_INVALID_ARGUMENT_EXCEPTION("src out of bound or offset out of bound");
  }
  nbr_t* nbrs = adj_lists_ptr()[src];
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
  if (src >= vnum || offset >= degree_list_ptr()[src]) {
    THROW_INVALID_ARGUMENT_EXCEPTION("src out of bound or offset out of bound");
  }
  nbr_t* nbrs = adj_lists_ptr()[src];
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
  std::vector<int> old_degree_list(v_cap);
  memcpy(old_degree_list.data(), degree_list_buffer_->GetData(),
         sizeof(int) * v_cap);
  for (size_t i = 0; i < src_list.size(); ++i) {
    ++degree_list_ptr()[src_list[i]];
  }
  size_t new_edge_num = old_edge_num + src_list.size();
  nbr_list_buffer_->Resize(new_edge_num * sizeof(nbr_t));

  size_t new_edge_offset = new_edge_num;
  size_t old_edge_offset = old_edge_num;
  for (int64_t i = v_cap - 1; i >= 0; --i) {
    new_edge_offset -= degree_list_ptr()[i];
    old_edge_offset -= old_degree_list[i];
    adj_lists_ptr()[i] = nbr_list_ptr() + new_edge_offset;
    memmove(nbr_list_ptr() + new_edge_offset, nbr_list_ptr() + old_edge_offset,
            sizeof(nbr_t) * old_degree_list[i]);
  }
  for (size_t i = 0; i < src_list.size(); ++i) {
    vid_t src = src_list[i];
    auto& nbr = adj_lists_ptr()[src][old_degree_list[src]++];
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
  auto tmp_file = tmp_dir(work_dir) + "/" + name + ".snbr";
  auto snapshot_file = snapshot_dir + "/" + name + ".snbr";
  if (std::filesystem::exists(snapshot_file)) {
    file_utils::copy_file(snapshot_file, tmp_file, true);
  } else {
    file_utils::create_file(tmp_file, sizeof(FileHeader));
  }
  nbr_list_buffer_ = std::make_unique<FileSharedMMap>();
  nbr_list_buffer_->Open(tmp_file);
}

template <typename EDATA_T>
void SingleImmutableCsr<EDATA_T>::open_in_memory(const std::string& prefix) {
  auto snapshot_file = prefix + ".snbr";
  nbr_list_buffer_ = std::make_unique<FilePrivateMMap>();
  if (std::filesystem::exists(snapshot_file)) {
    nbr_list_buffer_->Open(snapshot_file);
  }
}

template <typename EDATA_T>
void SingleImmutableCsr<EDATA_T>::open_with_hugepages(
    const std::string& prefix) {
  auto snapshot_file = prefix + ".snbr";
  nbr_list_buffer_ = std::make_unique<AnonHugeMMap>();
  if (std::filesystem::exists(snapshot_file)) {
    nbr_list_buffer_->Open(snapshot_file);
  }
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
    for (size_t k = old_size; k != vnum; ++k) {
      nbr_list_ptr()[k].neighbor = std::numeric_limits<vid_t>::max();
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
  for (auto src : src_set) {
    if (src >= vnum) {
      continue;
    }
    nbr_list_ptr()[src].neighbor = std::numeric_limits<vid_t>::max();
  }
  for (vid_t i = 0; i < vnum; ++i) {
    auto nbr = nbr_list_ptr()[i].neighbor;
    if (nbr != std::numeric_limits<vid_t>::max() &&
        dst_set.find(nbr) != dst_set.end()) {
      nbr_list_ptr()[i].neighbor = std::numeric_limits<vid_t>::max();
    }
  }
}

template <typename EDATA_T>
void SingleImmutableCsr<EDATA_T>::batch_delete_edges(
    const std::vector<vid_t>& src_list, const std::vector<vid_t>& dst_list) {
  vid_t vnum = size();
  for (size_t i = 0; i < src_list.size(); ++i) {
    vid_t src = src_list[i];
    if (src >= vnum) {
      continue;
    }
    vid_t dst = dst_list[i];
    if (nbr_list_ptr()[src].neighbor == dst) {
      nbr_list_ptr()[src].neighbor = std::numeric_limits<vid_t>::max();
    }
  }
}

template <typename EDATA_T>
void SingleImmutableCsr<EDATA_T>::batch_delete_edges(
    const std::vector<std::pair<vid_t, int32_t>>& edges) {
  vid_t vnum = size();
  for (const auto& edge : edges) {
    vid_t src = edge.first;
    if (src >= vnum) {
      continue;
    }
    assert(edge.second == 0);
    nbr_list_ptr()[src].neighbor = std::numeric_limits<vid_t>::max();
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
  if (nbr_list_ptr()[src].neighbor == std::numeric_limits<vid_t>::max()) {
    LOG(ERROR) << "Fail to delete edge, already deleted.";
    return;
  }
  nbr_list_ptr()[src].neighbor = std::numeric_limits<vid_t>::max();
}

template <typename EDATA_T>
void SingleImmutableCsr<EDATA_T>::revert_delete_edge(vid_t src, vid_t nbr,
                                                     int32_t offset,
                                                     timestamp_t ts) {
  vid_t vnum = size();
  if (src >= vnum || offset != 0) {
    THROW_INVALID_ARGUMENT_EXCEPTION("src out of bound or offset out of bound");
  }
  if (nbr_list_ptr()[src].neighbor != std::numeric_limits<vid_t>::max()) {
    THROW_INVALID_ARGUMENT_EXCEPTION(
        "Attempting to revert delete on edge that is not deleted.");
  }
  nbr_list_ptr()[src].neighbor = nbr;
}

template <typename EDATA_T>
void SingleImmutableCsr<EDATA_T>::batch_put_edges(
    const std::vector<vid_t>& src_list, const std::vector<vid_t>& dst_list,
    const std::vector<EDATA_T>& data_list, timestamp_t) {
  vid_t vnum = size();
  for (size_t i = 0; i < src_list.size(); ++i) {
    vid_t src = src_list[i];
    if (src >= vnum) {
      continue;
    }
    auto& nbr = nbr_list_ptr()[src];
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