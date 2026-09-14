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
#include <utility>

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

void WriteAllAt(int fd, const char* data, size_t length, size_t offset) {
  size_t written = 0;
  while (written < length) {
#ifdef _WIN32
    if (_lseeki64(fd, static_cast<__int64>(offset + written), SEEK_SET) == -1) {
      THROW_IO_EXCEPTION("Failed to seek wal file: " +
                         std::string(strerror(errno)));
    }
    const auto chunk = static_cast<unsigned int>(std::min<size_t>(
        length - written, std::numeric_limits<unsigned int>::max()));
    const auto ret = _write(fd, data + written, chunk);
#else
    const auto ret = ::pwrite(fd, data + written, length - written,
                              static_cast<off_t>(offset + written));
#endif
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

int CloseFd(int fd) noexcept {
#ifdef _WIN32
  return _close(fd);
#else
  return ::close(fd);
#endif
}

void ResizeFile(int fd, size_t size) {
#ifdef _WIN32
  if (_chsize_s(fd, size) != 0) {
#else
  if (::ftruncate(fd, static_cast<off_t>(size)) != 0) {
#endif
    THROW_IO_EXCEPTION("Failed to resize WAL file: " +
                       std::string(strerror(errno)));
  }
}  // namespace

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
      // Make the directory entry durable before WAL contents can become
      // recoverable. If this fails, append() must not write the transaction.
      if (!file_utils::fsync_directory(prefix)) {
        (void) CloseFd(std::exchange(fd_, -1));
        THROW_IO_EXCEPTION("Failed to sync wal directory " + prefix);
      }
      try {
        ensure_file_size(initial_file_size_);
      } catch (...) {
        (void) CloseFd(std::exchange(fd_, -1));
        throw;
      }
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

void LocalWalWriter::ensure_file_size(size_t required_size) {
  if (required_size <= file_size_) {
    return;
  }

  size_t next_size = file_size_;
  while (next_size < required_size) {
    if (file_growth_size_ == 0 ||
        next_size > std::numeric_limits<size_t>::max() - file_growth_size_) {
      next_size = required_size;
      break;
    }
    next_size += file_growth_size_;
  }
  ResizeFile(fd_, next_size);
  file_size_ = next_size;
}

void LocalWalWriter::close() {
  opened_ = false;
  file_size_ = 0;
  file_used_ = 0;
  if (fd_ == -1) {
    return;
  }

  // Retire the descriptor before calling close(). Retrying close() after an
  // error is unsafe because the descriptor may already have been released and
  // reused by another thread.
  if (CloseFd(std::exchange(fd_, -1)) != 0) {
    THROW_IO_EXCEPTION("Failed to close WAL file: " +
                       std::string(strerror(errno)));
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

  try {
    if (fd_ == -1) {
      create_file();
    }

    const std::array<char, sizeof(WalHeader)> terminator{};
    // The zero-filled tail remains the recovery terminator. Preallocation
    // keeps ordinary commits from extending i_size and flushing inode metadata.
    // TODO(W1): LocalWalParser currently uses EOF to identify an interrupted
    // payload. A crash during the payload write can therefore be mistaken for
    // a complete record when the preallocated zero-filled tail supplies the
    // missing bytes. Replace this framing with a validated commit trailer
    // before relying on preallocation for crash recovery.
    ensure_file_size(file_used_ + length + terminator.size());
    WriteAllAt(fd_, data, length, file_used_);
    WriteAllAt(fd_, terminator.data(), terminator.size(), file_used_ + length);
    SyncFile(fd_);
    file_used_ += length;
    return true;
  } catch (...) {
    // A failed append may leave an interrupted tail. Do not reuse its offset;
    // recovery will retain only the previously committed prefix.
    opened_ = false;
    throw;
  }
}

const bool LocalWalWriter::registered_ = WalWriterFactory::RegisterWalWriter(
    "file", static_cast<WalWriterFactory::wal_writer_initializer_t>(
                &LocalWalWriter::Make));

}  // namespace neug
