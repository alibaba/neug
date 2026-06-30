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

#include "neug/config.h"
#include "neug/storages/container/i_container.h"
#include "neug/storages/module/module.h"

namespace neug {

using doc_id_t = uint32_t;
using vid_t = uint32_t;
static constexpr vid_t INVALID_VID = std::numeric_limits<vid_t>::max();

/**
 * @brief Maps doc_id (internal index identifier) to vid (vertex id).
 *
 * DocIDMap inherits Module and uses IDataContainer for persistent storage.
 * It supports COW (Copy-On-Write) via Fork() for transaction isolation.
 *
 * Append-Only design: deleted entries are marked with INVALID_VID.
 */
class DocIDMap : public Module {
 public:
  static constexpr size_t kDefaultCapacity = 1024;

  DocIDMap();
  ~DocIDMap() override = default;

  size_t size() const { return next_doc_id_.load(std::memory_order_relaxed); }
  size_t capacity() const {
    return buffer_ ? buffer_->GetDataSize() / sizeof(vid_t) : 0;
  }

  // --- Module interface ---
  void Open(Checkpoint& ckp, const ModuleDescriptor& descriptor,
            MemoryLevel level) override;
  void Dump(Checkpoint& ckp, CheckpointManifest& meta,
            const std::string& key) override;
  std::string ModuleTypeName() const override { return "doc_id_map"; }

  /**
   * @brief Allocate a new doc_id and map it to vid.
   * @return The newly allocated doc_id.
   */
  doc_id_t Insert(vid_t vid);

  /**
   * @brief Mark a vid as erased (set to INVALID_VID).
   *
   * Linear scan to find matching vid. Only marks, does not compact.
   */
  void Erase(vid_t vid);

  /**
   * @brief O(1) lookup: get vid for a given doc_id.
   * @return The vid, or INVALID_VID if doc_id is out of range or erased.
   */
  vid_t GetVID(doc_id_t doc_id) const;

  /**
   * @brief Deep copy without Checkpoint (uses anonymous mmap).
   */
  std::unique_ptr<Module> Clone() const override;

  void Detach(Checkpoint& ckp, MemoryLevel level) override;

  /**
   * @brief Grow the buffer to accommodate at least new_capacity entries.
   */
  void Resize(size_t new_capacity);

 private:
  std::shared_ptr<IDataContainer> buffer_;
  std::atomic<doc_id_t> next_doc_id_{0};
};

}  // namespace neug
