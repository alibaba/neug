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

#include "neug/transaction/wal/local_wal_writer.h"

#include "neug/utils/exception/exception.h"

#include <errno.h>
#include <fcntl.h>
#include <glog/logging.h>
#include <string.h>
#include <algorithm>
#include <array>
#include <limits>
#ifdef _WIN32
#include <io.h>
#else
#include <unistd.h>
#endif
#include <charconv>
#include <exception>
#include <filesystem>
#include <ostream>
#include <string_view>

#include "neug/transaction/wal/wal.h"
#include "neug/utils/io/file/file_utils.h"
#include "neug/utils/likely.h"

namespace neug {
namespace {

constexpr uint32_t kMaxWalVersions = 65536;

bool ParseWalVersion(const std::filesystem::path& path, int slot_id,
                     uint32_t& version) {
  const auto name = path.filename().string();
  const auto prefix = "thread_" + std::to_string(slot_id) + "_";
  constexpr std::string_view suffix = ".wal";
  if (!name.starts_with(prefix) || !name.ends_with(suffix)) {
    return false;
  }
  const auto version_text = std::string_view(name).substr(
      prefix.size(), name.size() - prefix.size() - suffix.size());
  if (version_text.empty()) {
    return false;
  }
  const auto result = std::from_chars(
      version_text.data(), version_text.data() + version_text.size(), version);
  return result.ec == std::errc() &&
         result.ptr == version_text.data() + version_text.size();
}

int64_t WriteSome(int fd, const char* data, size_t length, size_t offset) {
#ifdef _WIN32
  if (_lseeki64(fd, static_cast<__int64>(offset), SEEK_SET) == -1) {
    THROW_IO_EXCEPTION("Failed to seek wal file: " +
                       std::string(strerror(errno)));
  }
  const auto chunk = static_cast<unsigned int>(
      std::min<size_t>(length, std::numeric_limits<unsigned int>::max()));
  return _write(fd, data, chunk);
#else
  return ::pwrite(fd, data, length, static_cast<off_t>(offset));
#endif
}

void WriteAllAt(int fd, const char* data, size_t length, size_t offset) {
  size_t written = 0;
  while (written < length) {
    const auto ret =
        WriteSome(fd, data + written, length - written, offset + written);
    if (ret < 0) {
      if (errno == EINTR) {
        continue;
      }
      THROW_IO_EXCEPTION("Failed to write wal file: " +
                         std::string(strerror(errno)));
    }
    if (ret == 0) {
      THROW_IO_EXCEPTION("Failed to write wal file: zero-byte write");
    }
    written += static_cast<size_t>(ret);
  }
}

void SyncFile(int fd) {
#ifdef _WIN32
  if (_commit(fd) != 0) {
    THROW_IO_EXCEPTION("Failed to sync wal file: " +
                       std::string(strerror(errno)));
  }
#elif defined(F_FULLFSYNC)
  if (::fcntl(fd, F_FULLFSYNC) != 0) {
    THROW_IO_EXCEPTION("Failed to sync wal file: " +
                       std::string(strerror(errno)));
  }
#else
  if (::fdatasync(fd) != 0) {
    THROW_IO_EXCEPTION("Failed to sync wal file: " +
                       std::string(strerror(errno)));
  }
#endif
}

}  // namespace

std::unique_ptr<IWalWriter> LocalWalWriter::Make(const std::string& wal_uri,
                                                 int slot_id) {
  return std::unique_ptr<IWalWriter>(new LocalWalWriter(wal_uri, slot_id));
}

LocalWalWriter::~LocalWalWriter() noexcept {
  try {
    close();
  } catch (const std::exception& e) {
    LOG(ERROR) << "Failed to close WAL writer during destruction: " << e.what();
  } catch (...) {
    LOG(ERROR) << "Failed to close WAL writer during destruction.";
  }
}

void LocalWalWriter::open(const std::string& wal_uri) {
  close();
  wal_uri_ = wal_uri;
  file_used_ = 0;
  opened_ = true;
}

void LocalWalWriter::create_file() {
  const auto prefix = get_wal_uri_path(wal_uri_);
  std::filesystem::create_directories(prefix);
  uint32_t next_version = 0;
  for (const auto& entry : std::filesystem::directory_iterator(prefix)) {
    uint32_t version = 0;
    if (entry.symlink_status().type() == std::filesystem::file_type::regular &&
        ParseWalVersion(entry.path(), slot_id_, version)) {
      next_version =
          version >= kMaxWalVersions - 1
              ? kMaxWalVersions
              : std::max(next_version, static_cast<uint32_t>(version + 1));
    }
  }
  while (next_version < kMaxWalVersions) {
    // Keep the historical on-disk prefix for WAL replay compatibility. The
    // numeric component identifies a logical execution slot.
    const auto wal_path = prefix + "/thread_" + std::to_string(slot_id_) + "_" +
                          std::to_string(next_version++) + ".wal";
#ifdef _WIN32
    fd_ = _open(wal_path.c_str(), O_RDWR | O_CREAT | O_EXCL | O_BINARY,
                _S_IREAD | _S_IWRITE);
#else
    fd_ = ::open(wal_path.c_str(), O_RDWR | O_CREAT | O_EXCL, 0644);
#endif
    if (fd_ != -1) {
      directory_sync_pending_ = true;
      return;
    }
    if (errno != EEXIST) {
      THROW_IO_EXCEPTION("Failed to create wal file " + wal_path + ": " +
                         std::string(strerror(errno)));
    }
  }
  THROW_IO_EXCEPTION(
      "Failed to create wal file: exhausted 65536 versions for " +
      std::to_string(slot_id_));
}

void LocalWalWriter::close() {
  opened_ = false;
  directory_sync_pending_ = false;
  if (fd_ != -1) {
    // Retire the descriptor before calling close(). Retrying close() after an
    // error is unsafe because the descriptor may already have been released
    // and reused by another thread.
    const int fd = fd_;
    fd_ = -1;
    file_used_ = 0;
#ifdef _WIN32
    const int close_result = _close(fd);
#else
    const int close_result = ::close(fd);
#endif
    if (close_result != 0) {
      THROW_IO_EXCEPTION("Failed to close file" + std::string(strerror(errno)));
    }
  }
}

bool LocalWalWriter::append(const char* data, size_t length) {
  if (NEUG_UNLIKELY(!opened_)) {
    return false;
  }
  if (length == 0) {
    return true;
  }
  if (data == nullptr) {
    THROW_INVALID_ARGUMENT_EXCEPTION("Cannot append a null WAL buffer");
  }
  if (length >
      std::numeric_limits<size_t>::max() - file_used_ - sizeof(WalHeader)) {
    THROW_OVERFLOW_EXCEPTION("WAL file size overflow");
  }

  if (fd_ == -1) {
    create_file();
  }

  const std::array<char, sizeof(WalHeader)> terminator{};
  WriteAllAt(fd_, data, length, file_used_);
  WriteAllAt(fd_, terminator.data(), terminator.size(), file_used_ + length);
  SyncFile(fd_);
  if (directory_sync_pending_ &&
      !file_utils::fsync_directory(get_wal_uri_path(wal_uri_))) {
    THROW_IO_EXCEPTION("Failed to sync wal directory " +
                       get_wal_uri_path(wal_uri_));
  }
  directory_sync_pending_ = false;
  file_used_ += length;
  return true;
}

const bool LocalWalWriter::registered_ = WalWriterFactory::RegisterWalWriter(
    "file", static_cast<WalWriterFactory::wal_writer_initializer_t>(
                &LocalWalWriter::Make));

}  // namespace neug
