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
#ifdef _WIN32
#include <io.h>
#else
#include <unistd.h>
#endif
#include <algorithm>
#include <cerrno>
#include <exception>
#include <filesystem>
#include <limits>
#include <ostream>

#include "neug/transaction/wal/wal.h"
#include "neug/utils/io/file/file_utils.h"
#include "neug/utils/likely.h"

namespace neug {

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
  if (poisoned_) {
    THROW_IO_EXCEPTION(
        "Cannot reopen WAL writer after an uncertain append failure");
  }
  close();
  wal_uri_ = wal_uri;
  opened_ = true;
}

void LocalWalWriter::create_file() {
  const auto prefix = get_wal_uri_path(wal_uri_);
  const int max_version = 65536;
  for (int version = 0; version != max_version; ++version) {
    // Keep the historical on-disk prefix for WAL replay compatibility. The
    // numeric component now identifies a logical execution slot, not a
    // physical pthread.
    std::string path = prefix + "/thread_" + std::to_string(slot_id_) + "_" +
                       std::to_string(version) + ".wal";
    if (std::filesystem::exists(path)) {
      continue;
    }
#ifdef _WIN32
    fd_ = _open(path.c_str(), O_RDWR | O_CREAT | O_TRUNC, _S_IREAD | _S_IWRITE);
#else
    fd_ = ::open(path.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0644);
#endif
    break;
  }
  if (fd_ == -1) {
    THROW_IO_EXCEPTION("Failed to open wal file " +
                       std::string(strerror(errno)));
  }
  // Persist the new file name before preallocation. Directory-sync failures
  // then leave an empty file rather than an unused 1 GiB WAL.
  if (!file_utils::fsync_directory(prefix)) {
    THROW_IO_EXCEPTION("Failed to fsync WAL directory " + prefix);
  }
#ifdef _WIN32
  const errno_t trunc_err = _chsize_s(fd_, TRUNC_SIZE);
#else
  const int trunc_err = ftruncate(fd_, TRUNC_SIZE);
#endif
  if (trunc_err != 0) {
#ifdef _WIN32
    errno = static_cast<int>(trunc_err);
#endif
    THROW_IO_EXCEPTION("Failed to truncate wal file " +
                       std::string(strerror(errno)));
  }
  file_size_ = TRUNC_SIZE;
  file_used_ = 0;
}

int LocalWalWriter::close_file() noexcept {
  // Retire the descriptor before close(): an error does not mean it is safe
  // to retry closing a descriptor that another thread may already have reused.
  const int fd = fd_;
  fd_ = -1;
  file_size_ = 0;
  file_used_ = 0;
  if (fd == -1) {
    return 0;
  }
#ifdef _WIN32
  return _close(fd);
#else
  return ::close(fd);
#endif
}

[[noreturn]] void LocalWalWriter::retire_after_append_failure(
    const std::string& operation, int error_number) {
  poisoned_ = true;
  const std::string message = operation + ": " + strerror(error_number);
  if (close_file() != 0) {
    LOG(ERROR) << "Failed to close WAL file after append failure: "
               << strerror(errno);
  }
  THROW_IO_EXCEPTION(message);
}

void LocalWalWriter::write_all_at(const char* data, size_t length,
                                  size_t offset) {
  size_t total = 0;
  while (total < length) {
#ifdef _WIN32
    if (_lseeki64(fd_, static_cast<__int64>(offset + total), SEEK_SET) == -1) {
      retire_after_append_failure("Failed to seek WAL file", errno);
    }
    const auto remaining = length - total;
    const auto chunk = static_cast<unsigned int>(
        std::min<size_t>(remaining, std::numeric_limits<unsigned int>::max()));
    const auto written = _write(fd_, data + total, chunk);
#else
    const auto written =
        pwrite(fd_, data + total, length - total, offset + total);
#endif
    if (written < 0) {
      if (errno == EINTR) {
        continue;
      }
      retire_after_append_failure("Failed to write WAL file", errno);
    }
    if (written == 0) {
      retire_after_append_failure("Failed to write WAL file", EIO);
    }
    total += static_cast<size_t>(written);
  }
}

void LocalWalWriter::close() {
  opened_ = false;
  if (close_file() != 0) {
    THROW_IO_EXCEPTION("Failed to close WAL file: " +
                       std::string(strerror(errno)));
  }
}

bool LocalWalWriter::append(const char* data, size_t length) {
  if (NEUG_UNLIKELY(poisoned_)) {
    THROW_IO_EXCEPTION(
        "WAL writer is disabled after an uncertain append failure");
  }
  if (NEUG_UNLIKELY(!opened_)) {
    return false;
  }
  if (length == 0) {
    return true;
  }
  if (fd_ == -1) {
    try {
      create_file();
    } catch (const std::exception& e) {
      LOG(ERROR) << "Failed to create WAL file before append: " << e.what();
      if (close_file() != 0) {
        LOG(ERROR) << "Failed to close WAL file after creation failure: "
                   << strerror(errno);
      }
      return false;
    }
  }
  const size_t expected_size = file_used_ + length + sizeof(WalHeader);
  if (expected_size > file_size_) {
    size_t new_file_size = (expected_size / TRUNC_SIZE + 1) * TRUNC_SIZE;
#ifdef _WIN32
    const errno_t resize_err = _chsize_s(fd_, new_file_size);
#else
    const int resize_err = ftruncate(fd_, new_file_size);
#endif
    if (resize_err != 0) {
#ifdef _WIN32
      errno = static_cast<int>(resize_err);
#endif
      retire_after_append_failure("Failed to resize WAL file", errno);
    }
    file_size_ = new_file_size;
  }

  write_all_at(data, length, file_used_);
  const WalHeader terminator{};
  write_all_at(reinterpret_cast<const char*>(&terminator), sizeof(terminator),
               file_used_ + length);

#ifdef _WIN32
  if (_commit(fd_) != 0) {
    retire_after_append_failure("Failed to sync WAL file", errno);
  }
#elif defined(F_FULLFSYNC)
  if (fcntl(fd_, F_FULLFSYNC) != 0) {
    retire_after_append_failure("Failed to sync WAL file", errno);
  }
#else
  if (fdatasync(fd_) != 0) {
    retire_after_append_failure("Failed to sync WAL file", errno);
  }
#endif
  file_used_ += length;
  return true;
}

const bool LocalWalWriter::registered_ = WalWriterFactory::RegisterWalWriter(
    "file", static_cast<WalWriterFactory::wal_writer_initializer_t>(
                &LocalWalWriter::Make));

}  // namespace neug
