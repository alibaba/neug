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

#include <stdlib.h>

#include <filesystem>
#include <fstream>
#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "neug/config.h"
#include "neug/storages/container/file_header.h"
#include "neug/storages/container/mmap_container.h"
#include "neug/utils/file_utils.h"
#include "neug/utils/property/types.h"

namespace neug {

class ArenaAllocator {
  static constexpr size_t batch_size = 16 * 1024 * 1024;

 public:
  ArenaAllocator(MemoryLevel strategy, const std::string& prefix)
      : strategy_(strategy),
        prefix_(prefix),
        cur_buffer_(nullptr),
        cur_loc_(0),
        cur_size_(0),
        allocated_memory_(0),
        allocated_batches_(0) {}
  ~ArenaAllocator() {}

  void reserve(size_t cap) {
    if (cur_size_ - cur_loc_ >= cap) {
      return;
    }
    cap = (cap + batch_size - 1) ^ (batch_size - 1);
    cur_buffer_ = allocate_batch(cap);
    cur_loc_ = 0;
    cur_size_ = cap;
  }

  void* allocate(size_t size) {
    allocated_memory_ += size;
    if (cur_size_ - cur_loc_ >= size) {
      void* ret = reinterpret_cast<char*>(cur_buffer_) + cur_loc_;
      cur_loc_ += size;
      return ret;
    } else if (size >= batch_size / 2) {
      return allocate_batch(size);
    } else {
      cur_buffer_ = allocate_batch(batch_size);
      void* ret = cur_buffer_;
      cur_loc_ = size;
      cur_size_ = batch_size;
      return ret;
    }
  }

  size_t allocated_memory() const { return allocated_memory_; }

 private:
  void* allocate_batch(size_t size) {
    allocated_batches_ += size;
    std::string file_name;
    if (strategy_ == MemoryLevel::kSyncToFile) {
      file_name = prefix_ + std::to_string(mmap_buffers_.size());
      if (!std::filesystem::exists(file_name)) {
        CreateEmptyContainerFile(file_name);
      }
    }
    auto buf = OpenDataContainer(strategy_, file_name);
    buf->Resize(size);
    mmap_buffers_.push_back(std::move(buf));
    return mmap_buffers_.back()->GetData();
  }

  MemoryLevel strategy_;
  std::string prefix_;
  std::vector<std::unique_ptr<IDataContainer>> mmap_buffers_;

  void* cur_buffer_;
  size_t cur_loc_;
  size_t cur_size_;

  size_t allocated_memory_;
  size_t allocated_batches_;
};

using Allocator = ArenaAllocator;

}  // namespace neug
