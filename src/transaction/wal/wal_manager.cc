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

#include "neug/transaction/wal/wal_manager.h"

#include <glog/logging.h>
#include <stdint.h>
#include <exception>
#include <filesystem>
#include <string_view>
#include <utility>

#include "neug/utils/exception/exception.h"

namespace neug {

namespace {
std::string PreparedFinalWalDir(const std::string& staging_wal_dir) {
  auto wal_path = std::filesystem::path(staging_wal_dir);
  if (wal_path.filename() != "wal") {
    THROW_INVALID_ARGUMENT_EXCEPTION(
        "Pending WAL rotation dir must end with wal/: " + staging_wal_dir);
  }

  auto staging_checkpoint_path = wal_path.parent_path();
  auto staging_name = staging_checkpoint_path.filename().string();
  constexpr std::string_view kNextSuffix = ".next";
  if (staging_name.size() <= kNextSuffix.size() ||
      staging_name.compare(staging_name.size() - kNextSuffix.size(),
                           kNextSuffix.size(), kNextSuffix) != 0) {
    THROW_INVALID_ARGUMENT_EXCEPTION(
        "Pending WAL rotation dir must be under a .next checkpoint: " +
        staging_wal_dir);
  }

  staging_name.resize(staging_name.size() - kNextSuffix.size());
  return (staging_checkpoint_path.parent_path() / staging_name / "wal")
      .string();
}
}  // namespace

PendingWalRotation::PendingWalRotation(std::string staging_wal_dir,
                                       std::string final_wal_dir,
                                       size_t writer_num)
    : staging_wal_dir_(std::move(staging_wal_dir)),
      final_wal_dir_(std::move(final_wal_dir)),
      slot_wal_dirs_(writer_num, final_wal_dir_),
      writer_num_(writer_num),
      valid_(true) {}

PendingWalRotation::PendingWalRotation(PendingWalRotation&& other) noexcept
    : staging_wal_dir_(std::move(other.staging_wal_dir_)),
      final_wal_dir_(std::move(other.final_wal_dir_)),
      slot_wal_dirs_(std::move(other.slot_wal_dirs_)),
      writer_num_(other.writer_num_),
      valid_(other.valid_) {
  other.clear();
}

PendingWalRotation& PendingWalRotation::operator=(
    PendingWalRotation&& other) noexcept {
  if (this != &other) {
    staging_wal_dir_ = std::move(other.staging_wal_dir_);
    final_wal_dir_ = std::move(other.final_wal_dir_);
    slot_wal_dirs_ = std::move(other.slot_wal_dirs_);
    writer_num_ = other.writer_num_;
    valid_ = other.valid_;
    other.clear();
  }
  return *this;
}

void PendingWalRotation::clear() noexcept {
  staging_wal_dir_.clear();
  final_wal_dir_.clear();
  slot_wal_dirs_.clear();
  writer_num_ = 0;
  valid_ = false;
}

WalWriterSlot::WalWriterSlot(std::string wal_dir, int32_t thread_id)
    : wal_dir_(std::move(wal_dir)), thread_id_(thread_id) {}

IWalWriter& WalWriterSlot::Writer() {
  if (!writer_) {
    writer_ = WalWriterFactory::CreateWalWriter(wal_dir_, thread_id_);
    writer_->open();
  }
  return *writer_;
}

void WalWriterSlot::Rotate(const std::string& wal_dir) noexcept {
  try {
    InstallRotation(wal_dir);
  } catch (const std::exception& e) {
    LOG(FATAL) << "Failed to rotate WAL writer slot: " << e.what();
  } catch (...) { LOG(FATAL) << "Failed to rotate WAL writer slot."; }
}

void WalWriterSlot::InstallRotation(std::string wal_dir) noexcept {
  Close();
  wal_dir_.swap(wal_dir);
}

void WalWriterSlot::Close() noexcept {
  if (!writer_) {
    return;
  }
  try {
    writer_->close();
  } catch (const std::exception& e) {
    LOG(WARNING) << "Failed to close WAL writer during rotation: " << e.what();
  } catch (...) {
    LOG(WARNING) << "Failed to close WAL writer during rotation.";
  }
  writer_.reset();
}

WalManager::WalManager(std::string wal_dir, int32_t writer_num)
    : wal_dir_(std::move(wal_dir)) {
  if (writer_num <= 0) {
    THROW_INVALID_ARGUMENT_EXCEPTION("WalManager writer_num must be positive.");
  }
  WalWriterFactory::Init();
  writer_slots_.reserve(writer_num);
  for (int32_t i = 0; i < writer_num; ++i) {
    writer_slots_.push_back(std::unique_ptr<WalWriterSlot>(
        new WalWriterSlot(wal_dir_, /*thread_id=*/i)));
  }
}

WalManager::~WalManager() {
  std::lock_guard<std::mutex> lock(mutex_);
  CloseAllLocked();
}

WalWriterSlot& WalManager::WriterSlot(int32_t thread_id) {
  std::lock_guard<std::mutex> lock(mutex_);
  ValidateThreadId(thread_id);
  return *writer_slots_[thread_id];
}

IWalWriter& WalManager::Writer(int32_t thread_id) {
  return WriterSlot(thread_id).Writer();
}

void WalManager::Rotate(const std::string& wal_dir) noexcept {
  try {
    std::lock_guard<std::mutex> lock(mutex_);
    if (wal_dir == wal_dir_) {
      return;
    }
    for (auto& writer_slot : writer_slots_) {
      writer_slot->Rotate(wal_dir);
    }
    wal_dir_ = wal_dir;
  } catch (const std::exception& e) {
    LOG(FATAL) << "Failed to rotate WAL manager: " << e.what();
  } catch (...) { LOG(FATAL) << "Failed to rotate WAL manager."; }
}

PendingWalRotation WalManager::PrepareRotation(
    const std::string& staging_wal_dir) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (staging_wal_dir.empty()) {
    THROW_INVALID_ARGUMENT_EXCEPTION(
        "Pending WAL rotation dir cannot be empty.");
  }
  auto final_wal_dir = PreparedFinalWalDir(staging_wal_dir);
  std::filesystem::create_directories(staging_wal_dir);
  return PendingWalRotation(staging_wal_dir, std::move(final_wal_dir),
                            writer_slots_.size());
}

void WalManager::InstallPreparedRotation(
    PendingWalRotation&& rotation, const std::string& final_wal_dir) noexcept {
  try {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!rotation.valid()) {
      LOG(FATAL) << "Invalid pending WAL rotation.";
    }
    if (rotation.writer_num_ != writer_slots_.size()) {
      LOG(FATAL) << "Pending WAL rotation writer count mismatch.";
    }
    if (rotation.slot_wal_dirs_.size() != writer_slots_.size()) {
      LOG(FATAL) << "Pending WAL rotation slot state mismatch.";
    }
    if (final_wal_dir.empty()) {
      LOG(FATAL) << "Final WAL rotation dir cannot be empty.";
    }
    if (final_wal_dir != rotation.final_wal_dir_) {
      LOG(FATAL) << "Final WAL rotation dir mismatch. expected="
                 << rotation.final_wal_dir_ << ", actual=" << final_wal_dir;
    }
    if (final_wal_dir == wal_dir_) {
      rotation.clear();
      return;
    }
    for (size_t i = 0; i < writer_slots_.size(); ++i) {
      writer_slots_[i]->InstallRotation(std::move(rotation.slot_wal_dirs_[i]));
    }
    wal_dir_.swap(rotation.final_wal_dir_);
    rotation.clear();
  } catch (const std::exception& e) {
    LOG(FATAL) << "Failed to install prepared WAL rotation: " << e.what();
  } catch (...) { LOG(FATAL) << "Failed to install prepared WAL rotation."; }
}

void WalManager::AbortPreparedRotation(PendingWalRotation&& rotation) noexcept {
  rotation.clear();
}

void WalManager::CloseAllLocked() {
  for (auto& writer_slot : writer_slots_) {
    writer_slot->Close();
  }
}

void WalManager::ValidateThreadId(int32_t thread_id) const {
  if (thread_id < 0 ||
      thread_id >= static_cast<int32_t>(writer_slots_.size())) {
    THROW_INVALID_ARGUMENT_EXCEPTION("Invalid WAL writer thread id: " +
                                     std::to_string(thread_id));
  }
}

}  // namespace neug
