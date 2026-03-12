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

#include "neug/storages/graph/vertex_timestamp.h"
#include <filesystem>

#include "neug/utils/serialization/in_archive.h"
#include "neug/utils/serialization/out_archive.h"

namespace neug {

void VertexTimestamp::Open(const std::string& tracker_file_prefix) {
  std::string ts_filename = tracker_file_prefix + ".ts";
  std::string meta_filename = tracker_file_prefix + ".meta";
  if (!meta_filename.empty() || std::filesystem::exists(meta_filename)) {
    load_meta(meta_filename);
  } else {
    Init(0, 4096);
  }
  if (std::filesystem::exists(ts_filename)) {
    load_ts(ts_filename);
  }
}

void VertexTimestamp::Dump(const std::string& tracker_file_prefix) {
  std::string ts_filename = tracker_file_prefix + ".ts";
  std::string meta_filename = tracker_file_prefix + ".meta";
  // Before dump, reset the timestamp of modified vertices
  vid_t num = max_vertex_num_ - init_vertex_num_;
  for (vid_t v = 0; v < num; ++v) {
    if (inserted_vertices_[v].load() != DELETED_TIMESTAMP) {
      inserted_vertices_[v].store(0);
    }
  }
  Compact();
  dump_meta(meta_filename);
  dump_ts(ts_filename);
}

void VertexTimestamp::Init(vid_t init_vertex_num, vid_t max_vertex_num) {
  if (init_vertex_num_ == init_vertex_num &&
      max_vertex_num_ == max_vertex_num) {
    return;
  }
  Reset();
  init_vertex_num_ = init_vertex_num;
  max_vertex_num_ = std::max(init_vertex_num, max_vertex_num);
  vid_t capacity = max_vertex_num_ - init_vertex_num_;
  if (capacity) {
    inserted_vertices_ = std::make_unique<std::atomic<timestamp_t>[]>(capacity);
    for (vid_t i = 0; i < capacity; ++i) {
      inserted_vertices_[i].store(std::numeric_limits<timestamp_t>::max());
    }
  }
}

void VertexTimestamp::Reserve(size_t new_size) {
  if (max_vertex_num_ >= new_size && new_size > init_vertex_num_) {
    return;
  }
  if (new_size <= init_vertex_num_) {
    inserted_vertices_.reset();
    max_vertex_num_ = new_size;
    init_vertex_num_ = new_size;
    if (removed_vertices_) {
      auto it = removed_vertices_->begin();
      while (it != removed_vertices_->end()) {
        if (*it >= new_size) {
          it = removed_vertices_->erase(it);
        } else {
          ++it;
        }
      }
    }
    return;
  }

  resize_inserted_vertices(new_size - init_vertex_num_);
  max_vertex_num_ = new_size;
}

void VertexTimestamp::Swap(VertexTimestamp& other) {
  std::swap(init_vertex_num_, other.init_vertex_num_);
  std::swap(inserted_vertices_, other.inserted_vertices_);
  std::swap(max_vertex_num_, other.max_vertex_num_);
  std::swap(removed_vertices_, other.removed_vertices_);
}

void VertexTimestamp::Reset() {
  init_vertex_num_ = 0;
  if (inserted_vertices_) {
    inserted_vertices_.reset();
  }
  max_vertex_num_ = 0;
  if (removed_vertices_) {
    removed_vertices_.reset();
  }
}

void VertexTimestamp::Clear() { Reset(); }

timestamp_t VertexTimestamp::RemoveVertex(vid_t v) {
  if (v >= max_vertex_num_) {
    THROW_INVALID_ARGUMENT_EXCEPTION("Vertex " + std::to_string(v) +
                                     " is out of range.");
  }
  if (v >= init_vertex_num_) {
    timestamp_t old_ts = inserted_vertices_[v - init_vertex_num_].load();
    if (old_ts == DELETED_TIMESTAMP) {
      THROW_INVALID_ARGUMENT_EXCEPTION("Vertex " + std::to_string(v) +
                                       " has been deleted.");
    }
    if (!inserted_vertices_[v - init_vertex_num_].compare_exchange_strong(
            old_ts, DELETED_TIMESTAMP)) {
      THROW_INTERNAL_EXCEPTION("Fail to delete vertex " + std::to_string(v));
    }
    return old_ts;
  } else {
    if (!removed_vertices_) {
      removed_vertices_ = std::make_unique<std::set<vid_t>>();
    }
    assert(removed_vertices_->find(v) == removed_vertices_->end());
    removed_vertices_->insert(v);
    return 0;
  }
}

void VertexTimestamp::RevertRemoveVertex(vid_t v, timestamp_t old_ts) {
  assert(v < max_vertex_num_);
  if (v >= init_vertex_num_) {
    inserted_vertices_[v - init_vertex_num_].store(old_ts);
  } else {
    if (!removed_vertices_) {
      THROW_INTERNAL_EXCEPTION("Vertex " + std::to_string(v) +
                               " is not removed.");
    }
    removed_vertices_->erase(v);
  }
}

void VertexTimestamp::Compact() {
  ResetTimestamps();
  // For the begining vertices which has timestamp 0, we can remove their
  // entries
  if (inserted_vertices_) {
    vid_t num = max_vertex_num_ - init_vertex_num_;
    vid_t first_non_zero = num;
    for (vid_t v = 0; v < num; ++v) {
      if (inserted_vertices_[v].load() != 0) {
        first_non_zero = v;
        break;
      }
    }
    if (first_non_zero > 0) {
      resize_inserted_vertices(num - first_non_zero, false);
      init_vertex_num_ += first_non_zero;
    }
  }
}

void VertexTimestamp::ResetTimestamps() {
  vid_t num = max_vertex_num_ - init_vertex_num_;
  for (vid_t v = 0; v < num; ++v) {
    if (inserted_vertices_[v].load() != DELETED_TIMESTAMP) {
      inserted_vertices_[v].store(0);
    }
  }
}

void VertexTimestamp::load_meta(const std::string& meta_filename) {
  if (!std::filesystem::exists(meta_filename)) {
    Reset();
    return;
  }

  FILE* file = fopen(meta_filename.c_str(), "r");
  size_t file_size = std::filesystem::file_size(meta_filename);
  std::vector<char> buffer(file_size);
  auto ret = fread(buffer.data(), sizeof(char), file_size, file);
  if (ret != file_size) {
    THROW_INTERNAL_EXCEPTION("Failed to read meta file: " + meta_filename);
  }
  OutArchive arc;
  arc.SetSlice(buffer.data(), file_size);
  arc >> init_vertex_num_ >> max_vertex_num_;
  Init(init_vertex_num_, max_vertex_num_);
  uint32_t removed_size;
  arc >> removed_size;
  if (removed_size > 0) {
    if (!removed_vertices_) {
      removed_vertices_ = std::make_unique<std::set<vid_t>>();
    }
    for (uint32_t i = 0; i < removed_size; ++i) {
      vid_t v;
      arc >> v;
      removed_vertices_->insert(v);
    }
  }
  fclose(file);
}

void VertexTimestamp::dump_meta(const std::string& meta_filename) {
  InArchive arc;
  arc << init_vertex_num_ << max_vertex_num_;
  if (removed_vertices_) {
    arc << static_cast<uint32_t>(removed_vertices_->size());
    for (const auto& v : *removed_vertices_) {
      arc << v;
    }
  } else {
    arc << static_cast<uint32_t>(0);
  }
  FILE* file = fopen(meta_filename.c_str(), "wb");
  fwrite(arc.GetBuffer(), sizeof(char), arc.GetSize(), file);
  fflush(file);
  fclose(file);
}

void VertexTimestamp::load_ts(const std::string& ts_filename) {
  if (!std::filesystem::exists(ts_filename)) {
    return;
  }

  FILE* file = fopen(ts_filename.c_str(), "r");
  size_t file_size = std::filesystem::file_size(ts_filename);
  std::vector<char> buffer(file_size);
  auto ret = fread(buffer.data(), sizeof(char), file_size, file);
  if (ret != file_size) {
    THROW_INTERNAL_EXCEPTION("Failed to read ts file: " + ts_filename);
  }
  OutArchive arc;
  arc.SetSlice(buffer.data(), file_size);
  uint32_t vec_size;
  arc >> vec_size;
  resize_inserted_vertices(vec_size);
  assert(vec_size == max_vertex_num_ - init_vertex_num_);
  for (vid_t i = 0; i < vec_size; ++i) {
    timestamp_t ts;
    arc >> ts;
    assert(inserted_vertices_);
    assert(i < max_vertex_num_ - init_vertex_num_);
    inserted_vertices_[i].store(ts);
  }

  assert(arc.Empty());
  fclose(file);
}

void VertexTimestamp::dump_ts(const std::string& ts_filename) {
  InArchive arc;
  vid_t vec_size = max_vertex_num_ - init_vertex_num_;
  arc << static_cast<uint32_t>(vec_size);
  for (vid_t i = 0; i < vec_size; ++i) {
    arc << inserted_vertices_[i].load();
  }
  FILE* file = fopen(ts_filename.c_str(), "wb");
  fwrite(arc.GetBuffer(), sizeof(char), arc.GetSize(), file);
  fflush(file);
  fclose(file);
}

// Keep_front is true: keep the front part when resizing
// Keep_front is false: keep the back part when resizing
void VertexTimestamp::resize_inserted_vertices(size_t new_size,
                                               bool keep_front) {
  auto new_inserted_vertices =
      std::make_unique<std::atomic<timestamp_t>[]>(new_size);
  if (!inserted_vertices_) {
    for (vid_t i = 0; i < new_size; ++i) {
      new_inserted_vertices[i].store(DELETED_TIMESTAMP);
    }
    inserted_vertices_.swap(new_inserted_vertices);
    return;
  }
  vid_t min_size = std::min(
      new_size, static_cast<size_t>(max_vertex_num_ - init_vertex_num_));
  if (keep_front) {
    for (vid_t i = 0; i < min_size; ++i) {
      new_inserted_vertices[i].store(inserted_vertices_[i].load());
    }
  } else {
    vid_t offset =
        static_cast<vid_t>(max_vertex_num_ - init_vertex_num_) - min_size;
    for (vid_t i = 0; i < min_size; ++i) {
      new_inserted_vertices[i].store(inserted_vertices_[i + offset].load());
    }
  }
  for (vid_t i = min_size; i < new_size; ++i) {
    new_inserted_vertices[i].store(DELETED_TIMESTAMP);
  }
  inserted_vertices_.swap(new_inserted_vertices);
}
}  // namespace neug