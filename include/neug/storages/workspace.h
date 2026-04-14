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

#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <string>

#include "neug/storages/checkpoint.h"

namespace neug {

// ---------------------------------------------------------------------------

/**
 * @brief Manages a database directory that holds multiple numbered checkpoints.
 *
 * Directory layout:
 * ```
 * db_dir/
 * ├── checkpoint-00001/
 * │   ├── meta
 * │   ├── snapshot/
 * │   ├── runtime/
 * │   └── wal/
 * ├── checkpoint-00002/
 * └── ...
 * ```
 *
 * `Workspace` does **not** inherit `Module`; it is a directory-level
 * manager, not a data module itself.
 */
class Workspace {
 public:
  Workspace();
  ~Workspace();

  /**
   * @brief Open a database directory.
   * @param db_dir Path to the database directory
   */
  void Open(const std::string& db_dir);

  /**
   * @brief Close the workspace and release resources.
   */
  void Close();

  /**
   * @brief Get the number of checkpoints in the workspace.
   */
  size_t NumCheckpoints() const;

  /**
   * @brief Get the ID of the most recent checkpoint.
   * @return -1 if no checkpoints exist
   */
  int HeadId() const;

  /**
   * @brief Create a new checkpoint.
   * @return The ID of the new checkpoint
   */
  int CreateCheckpoint();

  /**
   * @brief Get a checkpoint by ID (const version).
   */
  Checkpoint& GetCheckpoint(int id) const;

  /**
   * @brief Get a checkpoint by ID.
   */
  Checkpoint& GetCheckpoint(int id);

  Checkpoint& GetLatestCheckpoint() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (checkpoints_.empty()) {
      throw std::runtime_error("No checkpoints available");
    }
    return *checkpoints_.rbegin()->second;
  }

  std::string db_dir() const { return db_dir_; }

 private:
  std::string db_dir_;
  std::map<int, std::unique_ptr<Checkpoint>> checkpoints_;
  mutable std::mutex mutex_;
};

}  // namespace neug
