/** Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <atomic>
#include <cstdint>
#include <limits>
#include <memory>
#include <unordered_map>

#include "neug/config.h"
#include "neug/storages/container/i_container.h"
#include "neug/storages/module/module.h"
#include "neug/utils/property/types.h"
#include "neug/utils/result.h"

namespace neug {

using index_id_t = uint32_t;
static constexpr index_id_t INVALID_INDEX_ID =
    std::numeric_limits<index_id_t>::max();
static constexpr vid_t INVALID_VID = std::numeric_limits<vid_t>::max();

class IndexIDAccessor : public Module {
 public:
  ~IndexIDAccessor() override = default;

  virtual index_id_t GetIndexIDByVID(vid_t vid) const = 0;
  virtual vid_t GetVIDByIndexID(index_id_t index_id) const = 0;
  virtual index_id_t UpsertVID(vid_t vid) = 0;
  virtual Status DeleteVID(vid_t vid) = 0;

  void Open(Checkpoint& ckp, const ModuleDescriptor& descriptor,
            MemoryLevel level) override = 0;
  void Dump(Checkpoint& ckp, CheckpointManifest& meta,
            const std::string& key) override = 0;
  std::unique_ptr<Module> Clone() const override = 0;
  void Detach(Checkpoint& ckp, MemoryLevel level) override = 0;
};

class DefaultIndexIDAccessor final : public IndexIDAccessor {
 public:
  static constexpr size_t kDefaultCapacity = 1024;

  DefaultIndexIDAccessor() = default;
  ~DefaultIndexIDAccessor() override = default;

  size_t size() const { return next_index_id_.load(std::memory_order_relaxed); }
  size_t capacity() const {
    return index_id_to_vid_ ? index_id_to_vid_->GetDataSize() / sizeof(vid_t)
                            : 0;
  }

  index_id_t GetIndexIDByVID(vid_t vid) const override;
  vid_t GetVIDByIndexID(index_id_t index_id) const override;
  index_id_t UpsertVID(vid_t vid) override;
  Status DeleteVID(vid_t vid) override;

  void Open(Checkpoint& ckp, const ModuleDescriptor& descriptor,
            MemoryLevel level) override;
  void Dump(Checkpoint& ckp, CheckpointManifest& meta,
            const std::string& key) override;
  std::unique_ptr<Module> Clone() const override;
  void Detach(Checkpoint& ckp, MemoryLevel level) override;
  std::string ModuleTypeName() const override {
    return "default_index_id_accessor";
  }

 private:
  void Resize(size_t new_capacity);
  void RebuildVIDToIndexID();

  std::shared_ptr<IDataContainer> index_id_to_vid_;
  std::unordered_map<vid_t, index_id_t> vid_to_index_id_;
  std::atomic<index_id_t> next_index_id_{0};
};

}  // namespace neug
