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
#include <unistd.h>
#include <filesystem>
#include <ostream>

#include "neug/transaction/wal/wal.h"
#include "neug/utils/likely.h"

namespace neug {

std::unique_ptr<IWalWriter> LocalWalWriter::Make(const std::string& wal_uri,
                                                 int thread_id) {
  return std::unique_ptr<IWalWriter>(new LocalWalWriter(wal_uri, thread_id));
}

void LocalWalWriter::open() {
  auto prefix = get_wal_uri_path(wal_uri_);
  if (!std::filesystem::exists(prefix)) {
    std::filesystem::create_directories(prefix);
  }
  const int max_version = 65536;
  for (int version = 0; version != max_version; ++version) {
    std::string path = prefix + "/thread_" + std::to_string(thread_id_) + "_" +
                       std::to_string(version) + ".wal";
    if (std::filesystem::exists(path)) {
      continue;
    }
    fd_ = ::open(path.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0644);
    break;
  }
  if (fd_ == -1) {
    THROW_IO_EXCEPTION("Failed to open wal file " + std::string(strerror(errno)));
  }
  if (ftruncate(fd_, TRUNC_SIZE) != 0) {
    THROW_IO_EXCEPTION("Failed to truncate wal file " + std::string(strerror(errno)));
  }
  file_size_ = TRUNC_SIZE;
  file_used_ = 0;
}

void LocalWalWriter::close() {
  if (fd_ != -1) {
    if (::close(fd_) != 0) {
      THROW_IO_EXCEPTION("Failed to close file" + std::string(strerror(errno)));
    }
    fd_ = -1;
    file_size_ = 0;
    file_used_ = 0;
  }
}

bool LocalWalWriter::append(const char* data, size_t length) {
  if (NEUG_UNLIKELY(fd_ == -1)) {
    return false;
  }
  size_t expected_size = file_used_ + length;
  if (expected_size > file_size_) {
    size_t new_file_size = (expected_size / TRUNC_SIZE + 1) * TRUNC_SIZE;
    if (ftruncate(fd_, new_file_size) != 0) {
      THROW_IO_EXCEPTION("Failed to truncate wal file " + std::string(strerror(errno)));
    }
    file_size_ = new_file_size;
  }

  file_used_ += length;

  if (static_cast<size_t>(write(fd_, data, length)) != length) {
    THROW_IO_EXCEPTION("Failed to write wal file " + std::string(strerror(errno)));
  }

#if 1
#ifdef F_FULLFSYNC
  if (fcntl(fd_, F_FULLFSYNC) != 0) {
#ifdef __APPLE__
    THROW_IO_EXCEPTION("Failed to fcntl sync wal file " + std::string(strerror(errno)));
#else
    THROW_IO_EXCEPTION("Failed to fcntl sync wal file " + std::string(strerrno(errno)));
#endif
  }
#else
  // if (fsync(fd_) != 0) {
  if (fdatasync(fd_) != 0) {
    THROW_IO_EXCEPTION("Failed to fsync wal file " + std::string(strerror(errno)));
  }
#endif
#endif
  return true;
}

const bool LocalWalWriter::registered_ = WalWriterFactory::RegisterWalWriter(
    "file", static_cast<WalWriterFactory::wal_writer_initializer_t>(
                &LocalWalWriter::Make));

}  // namespace neug
