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
#include "neug/storages/file_names.h"
#include "neug/utils/property/types.h"

namespace neug {

template <typename EDATA_T>
void ImmutableCsr<EDATA_T>::open(const std::string& name,
                                 const std::string& snapshot_dir,
                                 const std::string& work_dir) {
  // Changes made to the CSR will not be synchronized to the file
  // TODO(luoxiaojian): Implement the insert operation on ImmutableCsr.
  if (snapshot_dir != "") {
    degree_list_.open(snapshot_dir + "/" + name + ".deg", false);
    nbr_list_.open(snapshot_dir + "/" + name + ".nbr", false);
    load_meta(snapshot_dir + "/" + name);
  }

  adj_lists_.open(tmp_dir(work_dir) + "/" + name + ".adj", true);
  adj_lists_.resize(degree_list_.size());

  nbr_t* ptr = nbr_list_.data();
  for (size_t i = 0; i < degree_list_.size(); ++i) {
    int deg = degree_list_[i];
    adj_lists_[i] = ptr;
    ptr += deg;
  }
}

template <typename EDATA_T>
void ImmutableCsr<EDATA_T>::open_in_memory(const std::string& prefix,
                                           size_t v_cap) {
  degree_list_.open(prefix + ".deg", false);
  load_meta(prefix);
  nbr_list_.open(prefix + ".nbr", false);
  adj_lists_.reset();
  v_cap = std::max(v_cap, degree_list_.size());
  adj_lists_.resize(v_cap);
  size_t old_degree_size = degree_list_.size();
  degree_list_.resize(v_cap);

  nbr_t* ptr = nbr_list_.data();
  for (size_t i = 0; i < old_degree_size; ++i) {
    int deg = degree_list_[i];
    if (deg != 0) {
      adj_lists_[i] = ptr;
    } else {
      adj_lists_[i] = NULL;
    }
    ptr += deg;
  }
  for (size_t i = old_degree_size; i < degree_list_.size(); ++i) {
    degree_list_[i] = 0;
    adj_lists_[i] = NULL;
  }
}

template <typename EDATA_T>
void ImmutableCsr<EDATA_T>::open_with_hugepages(const std::string& prefix,
                                                size_t v_cap) {
  degree_list_.open_with_hugepages(prefix + ".deg", v_cap);
  load_meta(prefix);
  nbr_list_.open_with_hugepages(prefix + ".nbr");
  adj_lists_.reset();
  v_cap = std::max(v_cap, degree_list_.size());
  adj_lists_.resize(v_cap);
  size_t old_degree_size = degree_list_.size();
  degree_list_.resize(v_cap);

  nbr_t* ptr = nbr_list_.data();
  for (size_t i = 0; i < old_degree_size; ++i) {
    int deg = degree_list_[i];
    if (deg != 0) {
      adj_lists_[i] = ptr;
    } else {
      adj_lists_[i] = NULL;
    }
    ptr += deg;
  }
  for (size_t i = old_degree_size; i < degree_list_.size(); ++i) {
    degree_list_[i] = 0;
    adj_lists_[i] = NULL;
  }
}

template <typename EDATA_T>
void ImmutableCsr<EDATA_T>::dump(const std::string& name,
                                 const std::string& new_snapshot_dir) {
  dump_meta(new_snapshot_dir + "/" + name);
  size_t vnum = adj_lists_.size();
  {
    FILE* fout = fopen((new_snapshot_dir + "/" + name + ".deg").c_str(), "wb");
    fwrite(degree_list_.data(), sizeof(int), vnum, fout);
    fflush(fout);
    fclose(fout);
  }
  {
    FILE* fout = fopen((new_snapshot_dir + "/" + name + ".nbr").c_str(), "wb");
    for (size_t k = 0; k < vnum; ++k) {
      if (adj_lists_[k] != NULL && degree_list_[k] != 0) {
        fwrite(adj_lists_[k], sizeof(nbr_t), degree_list_[k], fout);
      }
    }
    fflush(fout);
    fclose(fout);
  }
}

template <typename EDATA_T>
void ImmutableCsr<EDATA_T>::reset_timestamp() {}

template <typename EDATA_T>
void ImmutableCsr<EDATA_T>::compact() {
  // For current adj_list where the dst vertex is invalid, swap it to the end.
  vid_t vnum = adj_lists_.size();
  if (vnum <= 0) {
    return;
  }
  size_t removed = 0;
  nbr_t* write_ptr = adj_lists_[0];
  for (vid_t i = 0; i < vnum; ++i) {
    int deg = degree_list_[i];
    if (deg == 0) {
      continue;
    }
    const nbr_t* read_ptr = adj_lists_[i];
    const nbr_t* read_end = read_ptr + deg;
    while (read_ptr != read_end) {
      if (read_ptr->neighbor != std::numeric_limits<vid_t>::max()) {
        if (removed) {
          *write_ptr = *read_ptr;
        }
        ++write_ptr;
      } else {
        --degree_list_[i];
        ++removed;
      }
      ++read_ptr;
    }
  }
  nbr_list_.resize(nbr_list_.size() - removed);
  nbr_t* ptr = nbr_list_.data();
  for (vid_t i = 0; i < vnum; ++i) {
    adj_lists_[i] = ptr;
    ptr += degree_list_[i];
  }
}

template <typename EDATA_T>
void ImmutableCsr<EDATA_T>::resize(vid_t vnum) {
  if (vnum > adj_lists_.size()) {
    size_t old_size = adj_lists_.size();
    adj_lists_.resize(vnum);
    degree_list_.resize(vnum);
    for (size_t k = old_size; k != vnum; ++k) {
      adj_lists_[k] = NULL;
      degree_list_[k] = 0;
    }
  } else {
    adj_lists_.resize(vnum);
    degree_list_.resize(vnum);
  }
}

template <typename EDATA_T>
void ImmutableCsr<EDATA_T>::close() {
  adj_lists_.reset();
  degree_list_.reset();
  nbr_list_.reset();
}

template <typename EDATA_T>
void ImmutableCsr<EDATA_T>::batch_sort_by_edge_data(timestamp_t ts) {
  size_t vnum = adj_lists_.size();
  for (size_t i = 0; i != vnum; ++i) {
    std::sort(
        adj_lists_[i], adj_lists_[i] + degree_list_[i],
        [](const nbr_t& lhs, const nbr_t& rhs) { return lhs.data < rhs.data; });
  }
  unsorted_since_ = ts;
}

template <typename EDATA_T>
void ImmutableCsr<EDATA_T>::batch_delete_vertices(
    const std::set<vid_t>& src_set, const std::set<vid_t>& dst_set) {
  vid_t vnum = adj_lists_.size();
  size_t removed = 0;
  for (vid_t i = 0; i < vnum; ++i) {
    int deg = degree_list_[i];
    if (deg == 0) {
      continue;
    }
    if (src_set.find(i) != src_set.end()) {
      removed += deg;
      degree_list_[i] = 0;
    } else {
      const nbr_t* old_ptr = adj_lists_[i];
      const nbr_t* old_end = old_ptr + deg;
      nbr_t* new_ptr = adj_lists_[i] - removed;
      while (old_ptr != old_end) {
        if (dst_set.find(old_ptr->neighbor) == dst_set.end()) {
          *new_ptr = *old_ptr;
          ++new_ptr;
        } else {
          --degree_list_[i];
          ++removed;
        }
        ++old_ptr;
      }
    }
  }
  nbr_list_.resize(nbr_list_.size() - removed);
  nbr_t* ptr = nbr_list_.data();
  for (vid_t i = 0; i < vnum; ++i) {
    adj_lists_[i] = ptr;
    ptr += degree_list_[i];
  }
}

template <typename EDATA_T>
void ImmutableCsr<EDATA_T>::batch_delete_edges(
    const std::vector<vid_t>& src_list, const std::vector<vid_t>& dst_list) {
  std::map<vid_t, std::set<vid_t>> src_dst_map;
  vid_t vnum = adj_lists_.size();
  for (size_t i = 0; i < src_list.size(); ++i) {
    if (src_list[i] >= vnum) {
      continue;
    }
    src_dst_map[src_list[i]].insert(dst_list[i]);
  }
  for (vid_t i = 0; i < vnum; ++i) {
    int deg = degree_list_[i];
    if (deg == 0) {
      continue;
    }
    auto iter = src_dst_map.find(i);
    if (iter != src_dst_map.end()) {
      const std::set<vid_t>& dst_set = iter->second;
      nbr_t* write_ptr = adj_lists_[i];
      const nbr_t* read_end = write_ptr + degree_list_[i];
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
  vid_t vnum = adj_lists_.size();
  for (const auto& edge : edges) {
    if (edge.first >= vnum || edge.second >= degree_list_[edge.first]) {
      continue;
    }
    src_offset_map[edge.first].insert(edge.second);
  }
  for (vid_t i = 0; i < vnum; ++i) {
    int deg = degree_list_[i];
    if (deg == 0) {
      continue;
    }
    auto iter = src_offset_map.find(i);
    if (iter != src_offset_map.end()) {
      nbr_t* write_ptr = adj_lists_[i];
      for (const auto& offset : iter->second) {
        write_ptr[offset].neighbor = std::numeric_limits<vid_t>::max();
      }
    }
  }
}

template <typename EDATA_T>
void ImmutableCsr<EDATA_T>::delete_edge(vid_t src, int32_t offset,
                                        timestamp_t ts) {
  vid_t vnum = adj_lists_.size();
  if (src >= vnum || offset >= degree_list_[src]) {
    THROW_INVALID_ARGUMENT_EXCEPTION("src out of bound or offset out of bound");
  }
  nbr_t* nbrs = adj_lists_[src];
  if (nbrs[offset].neighbor == std::numeric_limits<vid_t>::max()) {
    LOG(ERROR) << "Fail to delete edge, already deleted.";
    return;
  }
  nbrs[offset].neighbor = std::numeric_limits<vid_t>::max();
}

template <typename EDATA_T>
void ImmutableCsr<EDATA_T>::revert_delete_edge(vid_t src, vid_t nbr,
                                               int32_t offset, timestamp_t ts) {
  vid_t vnum = adj_lists_.size();
  if (src >= vnum || offset >= degree_list_[src]) {
    THROW_INVALID_ARGUMENT_EXCEPTION("src out of bound or offset out of bound");
  }
  nbr_t* nbrs = adj_lists_[src];
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
  std::vector<int> old_degree_list(degree_list_.size());
  memcpy(old_degree_list.data(), degree_list_.data(),
         sizeof(int) * degree_list_.size());
  for (size_t i = 0; i < src_list.size(); ++i) {
    ++degree_list_[src_list[i]];
  }
  size_t old_edge_num = nbr_list_.size();
  size_t new_edge_num = old_edge_num + src_list.size();
  nbr_list_.resize(new_edge_num);
  vid_t vnum = degree_list_.size();
  size_t new_edge_offset = new_edge_num;
  size_t old_edge_offset = old_edge_num;
  for (int64_t i = vnum - 1; i >= 0; --i) {
    new_edge_offset -= degree_list_[i];
    old_edge_offset -= old_degree_list[i];
    adj_lists_[i] = nbr_list_.data() + new_edge_offset;
    memmove(nbr_list_.data() + new_edge_offset,
            nbr_list_.data() + old_edge_offset,
            sizeof(nbr_t) * old_degree_list[i]);
  }
  for (size_t i = 0; i < src_list.size(); ++i) {
    vid_t src = src_list[i];
    auto& nbr = adj_lists_[src][old_degree_list[src]++];
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
  if (std::filesystem::exists(tmp_file)) {
    std::filesystem::remove(tmp_file);
  }
  if (!std::filesystem::exists(tmp_file)) {
    if (std::filesystem::exists(snapshot_file)) {
      copy_file(snapshot_file, tmp_file);
    }
  }
  nbr_list_.open(tmp_file, true);
}

template <typename EDATA_T>
void SingleImmutableCsr<EDATA_T>::open_in_memory(const std::string& prefix,
                                                 size_t v_cap) {
  nbr_list_.open(prefix + ".snbr", false);
  if (nbr_list_.size() < v_cap) {
    size_t old_size = nbr_list_.size();
    nbr_list_.reset();
    nbr_list_.resize(v_cap);
    if (old_size > 0) {
      FILE* fin = fopen((prefix + ".snbr").c_str(), "r");
      CHECK_EQ(fread(nbr_list_.data(), sizeof(nbr_t), old_size, fin), old_size);
      fclose(fin);
    }
    for (size_t k = old_size; k != v_cap; ++k) {
      nbr_list_[k].neighbor = std::numeric_limits<vid_t>::max();
    }
  }
}

template <typename EDATA_T>
void SingleImmutableCsr<EDATA_T>::open_with_hugepages(const std::string& prefix,
                                                      size_t v_cap) {
  nbr_list_.open_with_hugepages(prefix + ".snbr", v_cap);
  size_t old_size = nbr_list_.size();
  if (old_size < v_cap) {
    nbr_list_.resize(v_cap);
    for (size_t k = old_size; k != v_cap; ++k) {
      nbr_list_[k].neighbor = std::numeric_limits<vid_t>::max();
    }
  }
}

template <typename EDATA_T>
void SingleImmutableCsr<EDATA_T>::dump(const std::string& name,
                                       const std::string& new_snapshot_dir) {
  // TODO: opt with mv
  FILE* fp = fopen((new_snapshot_dir + "/" + name + ".snbr").c_str(), "wb");
  fwrite(nbr_list_.data(), sizeof(nbr_t), nbr_list_.size(), fp);
  fflush(fp);
  fclose(fp);
}

template <typename EDATA_T>
void SingleImmutableCsr<EDATA_T>::reset_timestamp() {}

template <typename EDATA_T>
void SingleImmutableCsr<EDATA_T>::compact() {}

template <typename EDATA_T>
void SingleImmutableCsr<EDATA_T>::resize(vid_t vnum) {
  if (vnum > nbr_list_.size()) {
    size_t old_size = nbr_list_.size();
    nbr_list_.resize(vnum);
    for (size_t k = old_size; k != vnum; ++k) {
      nbr_list_[k].neighbor = std::numeric_limits<vid_t>::max();
    }
  } else {
    nbr_list_.resize(vnum);
  }
}

template <typename EDATA_T>
void SingleImmutableCsr<EDATA_T>::close() {
  nbr_list_.reset();
}

template <typename EDATA_T>
void SingleImmutableCsr<EDATA_T>::batch_sort_by_edge_data(timestamp_t ts) {}

template <typename EDATA_T>
void SingleImmutableCsr<EDATA_T>::batch_delete_vertices(
    const std::set<vid_t>& src_set, const std::set<vid_t>& dst_set) {
  vid_t vnum = nbr_list_.size();
  for (auto src : src_set) {
    if (src >= vnum) {
      continue;
    }
    nbr_list_[src].neighbor = std::numeric_limits<vid_t>::max();
  }
  for (vid_t i = 0; i < vnum; ++i) {
    auto nbr = nbr_list_[i].neighbor;
    if (nbr != std::numeric_limits<vid_t>::max() &&
        dst_set.find(nbr) != dst_set.end()) {
      nbr_list_[i].neighbor = std::numeric_limits<vid_t>::max();
    }
  }
}

template <typename EDATA_T>
void SingleImmutableCsr<EDATA_T>::batch_delete_edges(
    const std::vector<vid_t>& src_list, const std::vector<vid_t>& dst_list) {
  vid_t vnum = nbr_list_.size();
  for (size_t i = 0; i < src_list.size(); ++i) {
    vid_t src = src_list[i];
    if (src >= vnum) {
      continue;
    }
    vid_t dst = dst_list[i];
    if (nbr_list_[src].neighbor == dst) {
      nbr_list_[src].neighbor = std::numeric_limits<vid_t>::max();
    }
  }
}

template <typename EDATA_T>
void SingleImmutableCsr<EDATA_T>::batch_delete_edges(
    const std::vector<std::pair<vid_t, int32_t>>& edges) {
  vid_t vnum = nbr_list_.size();
  for (const auto& edge : edges) {
    vid_t src = edge.first;
    if (src >= vnum) {
      continue;
    }
    assert(edge.second == 0);
    nbr_list_[src].neighbor = std::numeric_limits<vid_t>::max();
  }
}

template <typename EDATA_T>
void SingleImmutableCsr<EDATA_T>::delete_edge(vid_t src, int32_t offset,
                                              timestamp_t ts) {
  vid_t vnum = nbr_list_.size();
  if (src >= vnum || offset != 0) {
    THROW_INVALID_ARGUMENT_EXCEPTION("src out of bound or offset out of bound");
    return;
  }
  if (nbr_list_[src].neighbor == std::numeric_limits<vid_t>::max()) {
    LOG(ERROR) << "Fail to delete edge, already deleted.";
    return;
  }
  nbr_list_[src].neighbor = std::numeric_limits<vid_t>::max();
}

template <typename EDATA_T>
void SingleImmutableCsr<EDATA_T>::revert_delete_edge(vid_t src, vid_t nbr,
                                                     int32_t offset,
                                                     timestamp_t ts) {
  vid_t vnum = nbr_list_.size();
  if (src >= vnum || offset != 0) {
    THROW_INVALID_ARGUMENT_EXCEPTION("src out of bound or offset out of bound");
  }
  if (nbr_list_[src].neighbor != std::numeric_limits<vid_t>::max()) {
    THROW_INVALID_ARGUMENT_EXCEPTION(
        "Attempting to revert delete on edge that is not deleted.");
  }
  nbr_list_[src].neighbor = nbr;
}

template <typename EDATA_T>
void SingleImmutableCsr<EDATA_T>::batch_put_edges(
    const std::vector<vid_t>& src_list, const std::vector<vid_t>& dst_list,
    const std::vector<EDATA_T>& data_list, timestamp_t) {
  vid_t vnum = nbr_list_.size();
  for (size_t i = 0; i < src_list.size(); ++i) {
    vid_t src = src_list[i];
    if (src >= vnum) {
      continue;
    }
    auto& nbr = nbr_list_[src];
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