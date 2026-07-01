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

#include <stdint.h>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "neug/transaction/wal/wal.h"

namespace neug {

class PendingWalRotation {
 public:
  PendingWalRotation() = default;
  PendingWalRotation(const PendingWalRotation&) = delete;
  PendingWalRotation& operator=(const PendingWalRotation&) = delete;
  PendingWalRotation(PendingWalRotation&& other) noexcept;
  PendingWalRotation& operator=(PendingWalRotation&& other) noexcept;

  bool valid() const { return valid_; }
  const std::string& staging_wal_dir() const { return staging_wal_dir_; }
  const std::string& final_wal_dir() const { return final_wal_dir_; }

 private:
  friend class WalManager;

  PendingWalRotation(std::string staging_wal_dir, std::string final_wal_dir,
                     size_t writer_num);

  void clear() noexcept;

  std::string staging_wal_dir_;
  std::string final_wal_dir_;
  std::vector<std::string> slot_wal_dirs_;
  size_t writer_num_ = 0;
  bool valid_ = false;
};

/**
 * @brief Session-local WAL writer slot managed by WalManager.
 *
 * Each session holds a WalWriterSlot that owns a lazy-initialized IWalWriter.
 * When a checkpoint is published, WalManager installs a prepared rotation and
 * updates all slots' WAL directories, causing each writer to re-open lazily in
 * the new checkpoint's wal/ directory.
 *
 * @note **Breaking change**: This class replaces the raw `IWalWriter&`
 * parameter previously passed to Insert/Update/CompactTransaction constructors.
 */
class WalWriterSlot {
 public:
  IWalWriter& Writer();

 private:
  friend class WalManager;

  WalWriterSlot(std::string wal_dir, int32_t thread_id);

  void Rotate(const std::string& wal_dir) noexcept;
  void InstallRotation(std::string wal_dir) noexcept;
  void Close() noexcept;

  std::string wal_dir_;
  int32_t thread_id_;
  std::unique_ptr<IWalWriter> writer_;
};

/**
 * @brief Manages a pool of WalWriterSlots and supports WAL rotation.
 *
 * @note **Breaking change**: This class is new in the checkpoint GC refactor.
 * It centralizes WAL writer lifecycle management, enabling atomic rotation
 * of all writers when a checkpoint is published.
 */
class WalManager {
 public:
  WalManager(std::string wal_dir, int32_t writer_num);
  ~WalManager();

  WalWriterSlot& WriterSlot(int32_t thread_id);

  IWalWriter& Writer(int32_t thread_id);

  const std::string& wal_dir() const { return wal_dir_; }

  void Rotate(const std::string& wal_dir) noexcept;
  PendingWalRotation PrepareRotation(const std::string& staging_wal_dir);
  void InstallPreparedRotation(PendingWalRotation&& rotation,
                               const std::string& final_wal_dir) noexcept;
  void AbortPreparedRotation(PendingWalRotation&& rotation) noexcept;

 private:
  void CloseAllLocked();
  void ValidateThreadId(int32_t thread_id) const;

  std::string wal_dir_;
  std::vector<std::unique_ptr<WalWriterSlot>> writer_slots_;
  std::mutex mutex_;
};

}  // namespace neug
