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

#include "neug/storages/checkpoint_file_manager.h"

#include <fcntl.h>
#ifndef _WIN32
#include <unistd.h>
#else
#include <io.h>
#define open _open
#define close _close
#endif

#include <cerrno>
#include <cstring>
#include <filesystem>
#include <system_error>
#include <utility>

#include "neug/utils/io/file/file_utils.h"

#include <glog/logging.h>

namespace neug {

std::shared_ptr<const RuntimeWorkspace> RuntimeWorkspace::Create(
    std::string path) {
  auto workspace =
      std::shared_ptr<RuntimeWorkspace>(new RuntimeWorkspace(std::move(path)));
  std::error_code ec;
  std::filesystem::create_directories(workspace->path_, ec);
  if (ec) {
    THROW_IO_EXCEPTION("RuntimeWorkspace: failed to create " +
                       workspace->path_ + ": " + ec.message());
  }
  return workspace;
}

RuntimeWorkspace::RuntimeWorkspace(std::string path) : path_(std::move(path)) {}

RuntimeWorkspace::~RuntimeWorkspace() {
  std::error_code ec;
  std::filesystem::remove_all(path_, ec);
  if (ec) {
    LOG(WARNING) << "RuntimeWorkspace: failed to remove " << path_ << ": "
                 << ec.message();
  }
}

namespace {

void sync_file(const std::string& path) {
#ifdef _WIN32
  const int fd = ::_open(path.c_str(), _O_RDWR);
#else
  const int fd = ::open(path.c_str(), O_RDONLY);
#endif
  if (fd < 0) {
    THROW_IO_EXCEPTION("Checkpoint object open failed: " + path);
  }
#ifdef _WIN32
  const int sync_result = ::_commit(fd);
#else
  const int sync_result = ::fsync(fd);
#endif
  if (sync_result != 0) {
    const auto message = std::string(std::strerror(errno));
    ::close(fd);
    THROW_IO_EXCEPTION("Checkpoint object fsync failed: " + path + ": " +
                       message);
  }
  ::close(fd);
}

}  // namespace

struct CheckpointFileManager::RuntimeFileCleanupContext {
  explicit RuntimeFileCleanupContext(
      std::shared_ptr<const RuntimeWorkspace> runtime_workspace)
      : runtime_workspace(std::move(runtime_workspace)) {}

  bool IsRuntimeFile(const std::string& path) const {
    if (path.empty()) {
      return false;
    }
    auto parent = std::filesystem::path(path).parent_path().string();
    return parent == runtime_workspace->path();
  }

  void RemoveIfRuntimeFile(const std::string& path) {
    if (!IsRuntimeFile(path)) {
      return;
    }
    std::error_code ec;
    std::filesystem::remove(path, ec);
    if (ec) {
      LOG(WARNING) << "RuntimeFileCleanup: failed to remove " << path << ": "
                   << ec.message();
    }
  }

  std::shared_ptr<const RuntimeWorkspace> runtime_workspace;
};

CheckpointFileManager::RuntimeFileHandle::RuntimeFileHandle(
    std::shared_ptr<RuntimeFileCleanupContext> cleanup, std::string path)
    : cleanup_(std::move(cleanup)), path_(std::move(path)) {}

CheckpointFileManager::RuntimeFileHandle::~RuntimeFileHandle() {
  if (cleanup_) {
    cleanup_->RemoveIfRuntimeFile(path_);
  }
}

CheckpointFileManager::RuntimeFileHandle::RuntimeFileHandle(
    RuntimeFileHandle&& other) noexcept
    : cleanup_(std::move(other.cleanup_)), path_(std::move(other.path_)) {}

void CheckpointFileManager::RuntimeFileHandle::Release() {
  cleanup_.reset();
  path_.clear();
}

CheckpointFileManager::CheckpointFileManager(
    const std::string& object_dir,
    std::shared_ptr<const RuntimeWorkspace> runtime_workspace)
    : object_dir_(object_dir),
      runtime_dir_(runtime_workspace->path()),
      runtime_cleanup_(std::make_shared<RuntimeFileCleanupContext>(
          std::move(runtime_workspace))) {}

CheckpointFileManager::~CheckpointFileManager() = default;

std::shared_ptr<IDataContainer> CheckpointFileManager::OpenFile(
    const std::string& file_path, MemoryLevel level) {
  std::string runtime_path;
  if (level == MemoryLevel::kSyncToFile) {
    runtime_path = CreateRuntimeContainerPath();
  }
  try {
    auto container = OpenContainer(file_path, runtime_path, level);
    return WrapWithRuntimeCleanup(std::move(container));
  } catch (...) {
    runtime_cleanup_->RemoveIfRuntimeFile(runtime_path);
    throw;
  }
}

std::shared_ptr<IDataContainer> CheckpointFileManager::CreateRuntimeContainer(
    size_t size, MemoryLevel level) {
  std::string path;
  if (level == MemoryLevel::kSyncToFile) {
    path = CreateRuntimeContainerPath();
  }
  try {
    auto ret = OpenContainer("", path, level);
    if (ret == nullptr) {
      return nullptr;
    }
    ret->Resize(size);
    return WrapWithRuntimeCleanup(std::move(ret));
  } catch (...) {
    runtime_cleanup_->RemoveIfRuntimeFile(path);
    throw;
  }
}

std::shared_ptr<IDataContainer> CheckpointFileManager::WrapWithRuntimeCleanup(
    std::unique_ptr<IDataContainer> container) const {
  if (container == nullptr) {
    return nullptr;
  }
  auto cleanup = runtime_cleanup_;
  auto initial_path = container->GetPath();
  auto* raw = container.get();
  auto shared = std::shared_ptr<IDataContainer>(
      raw, [cleanup, initial_path](IDataContainer* ptr) {
        std::string path;
        if (ptr != nullptr) {
          path = ptr->GetPath();
        }
        delete ptr;
        cleanup->RemoveIfRuntimeFile(path);
        if (initial_path != path) {
          cleanup->RemoveIfRuntimeFile(initial_path);
        }
      });
  container.release();
  return shared;
}

static bool is_file_in_dir(const std::string& path, const std::string& dir) {
  if (path.empty()) {
    return false;
  }
  auto parent = std::filesystem::path(path).parent_path().string();
  return parent == dir;
}

std::string CheckpointFileManager::Commit(IDataContainer& buffer) {
  auto original_path = buffer.GetPath();
  if (!buffer.IsDirty() && is_file_in_dir(original_path, object_dir_)) {
    buffer.Close();
    return original_path;
  }

  auto runtime_file = CreateRuntimeFile();
  buffer.Dump(runtime_file.path());
  runtime_cleanup_->RemoveIfRuntimeFile(original_path);
  return CommitRuntimeFile(std::move(runtime_file));
}

CheckpointFileManager::RuntimeFileHandle
CheckpointFileManager::CreateRuntimeFile() {
  auto path = CreateRuntimeContainerPath();
  return RuntimeFileHandle(runtime_cleanup_, std::move(path));
}

std::string CheckpointFileManager::CreateRuntimeContainerPath() {
  std::lock_guard<std::mutex> lock(mutex_);
  while (true) {
    auto uuid = UUIDGenerator::Generate();
    auto runtime_path = runtime_dir_ + "/" + uuid;
    auto object_path = object_dir_ + "/" + uuid;
    if (std::filesystem::exists(object_path)) {
      continue;
    }

    const int fd =
        ::open(runtime_path.c_str(), O_WRONLY | O_CREAT | O_EXCL, 0644);
    if (fd >= 0) {
      ::close(fd);
      return runtime_path;
    }
    const int open_errno = errno;
    if (open_errno == EEXIST || open_errno == EINTR) {
      continue;
    }
    const auto error = std::error_code(open_errno, std::generic_category());
    THROW_IO_EXCEPTION("Failed to reserve runtime file " + runtime_path + ": " +
                       error.message());
  }
}

std::string CheckpointFileManager::CreateRuntimeObjectNameLocked() const {
  while (true) {
    std::string uuid = UUIDGenerator::Generate();
    auto runtime_path = runtime_dir_ + "/" + uuid;
    auto object_path = object_dir_ + "/" + uuid;
    if (!std::filesystem::exists(runtime_path) &&
        !std::filesystem::exists(object_path)) {
      return uuid;
    }
  }
}

std::string CheckpointFileManager::CommitRuntimeFile(RuntimeFileHandle&& file) {
  if (!file.valid()) {
    THROW_INVALID_ARGUMENT_EXCEPTION(
        "CheckpointFileManager::CommitRuntimeFile: invalid runtime file");
  }
  std::lock_guard<std::mutex> lock(mutex_);
  const auto uuid = std::filesystem::path(file.path_).filename().string();
  auto dst = commitRuntimeFileLocked(uuid, file.path_);
  file.Release();
  return dst;
}

std::string CheckpointFileManager::materializeObjectLocked(
    const std::string& abs_path) {
  auto parent = std::filesystem::path(abs_path).parent_path().string();
  if (parent == object_dir_) {
    return abs_path;
  }

  if (!std::filesystem::exists(abs_path)) {
    THROW_IO_EXCEPTION("CopyToSnapshot: source file does not exist: " +
                       abs_path);
  }

  std::string new_uuid = CreateRuntimeObjectNameLocked();
  auto dst = object_dir_ + "/" + new_uuid;
  file_utils::copy_file(abs_path, dst, false);
  sync_file(dst);
  VLOG(1) << "CopyToSnapshot: " << abs_path << " copied to object_dir: " << dst;
  return dst;
}

std::string CheckpointFileManager::commitRuntimeFileLocked(
    const std::string& uuid, const std::string& abs_path) {
  auto parent = std::filesystem::path(abs_path).parent_path().string();
  if (parent != runtime_dir_) {
    THROW_INVALID_ARGUMENT_EXCEPTION(
        "CommitRuntimeFile: path is not in checkpoint runtime_dir: " +
        abs_path);
  }
  if (!std::filesystem::exists(abs_path)) {
    THROW_IO_EXCEPTION("CommitRuntimeFile: source file does not exist: " +
                       abs_path);
  }

  auto dst = object_dir_ + "/" + uuid;
  while (std::filesystem::exists(dst)) {
    dst = object_dir_ + "/" + CreateRuntimeObjectNameLocked();
  }
  std::error_code ec;
  std::filesystem::rename(abs_path, dst, ec);
  if (!ec) {
    sync_file(dst);
    VLOG(1) << "CommitRuntimeFile: " << abs_path << " moved to " << dst;
    return dst;
  }

  VLOG(1) << "CommitRuntimeFile: rename failed (" << ec.message()
          << "), falling back to copy for " << abs_path;
  file_utils::copy_file(abs_path, dst, false);
  sync_file(dst);
  std::filesystem::remove(abs_path, ec);
  if (ec) {
    LOG(WARNING) << "CommitRuntimeFile: failed to remove " << abs_path
                 << " after copy fallback: " << ec.message();
  }
  return dst;
}

std::string CheckpointFileManager::MaterializeObject(
    const std::string& abs_path) {
  std::lock_guard<std::mutex> lock(mutex_);
  // Runtime files may be backed by a live MAP_SHARED container. Materialize a
  // new immutable object instead of aliasing future writes. External paths use
  // the same safe copy path.
  return materializeObjectLocked(abs_path);
}

bool CheckpointFileManager::SyncObjectDirectory() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return file_utils::fsync_directory(object_dir_);
}

}  // namespace neug
