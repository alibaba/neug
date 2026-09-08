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

#include "neug/transaction/wal/local_wal_parser.h"

#include <fcntl.h>
#ifndef _WIN32
#include <sys/mman.h>
#include <unistd.h>
#else
#include <io.h>
#endif
#include <algorithm>
#include <cerrno>
#include <cstring>
#include <filesystem>
#include <ostream>

#include "neug/transaction/wal/wal.h"
#include "neug/utils/exception/exception.h"
#include "neug/utils/io/file/file_utils.h"

namespace neug {

LocalWalParser::LocalWalParser(const std::string& wal_uri) {
  LocalWalParser::open(wal_uri);
}

void LocalWalParser::open(const std::string& wal_uri) {
  close();
  auto wal_dir = get_wal_uri_path(wal_uri);
  if (!std::filesystem::exists(wal_dir)) {
    std::filesystem::create_directory(wal_dir);
  }

  std::vector<std::string> paths;
  for (const auto& entry : std::filesystem::directory_iterator(wal_dir)) {
    paths.push_back(entry.path().string());
  }
  std::vector<std::string> mapped_paths;
  for (auto path : paths) {
    size_t file_size = std::filesystem::file_size(path);
    if (file_size == 0) {
      continue;
    }
#ifdef _WIN32
    int fd = _open(path.c_str(), O_RDONLY, 0);
#else
    int fd = ::open(path.c_str(), O_RDONLY);
#endif
    if (fd == -1) {
      close();
      THROW_IO_EXCEPTION("Failed to open wal file: " + path + ": " +
                         strerror(errno));
    }
    void* mmapped_buffer =
        ::mmap(NULL, file_size, PROT_READ, MAP_PRIVATE, fd, 0);
    if (mmapped_buffer == MAP_FAILED) {
#ifdef _WIN32
      _close(fd);
#else
      ::close(fd);
#endif
      close();
      THROW_IO_EXCEPTION("Failed to mmap wal file: " + path + ": " +
                         strerror(errno));
    }

    fds_.push_back(fd);
    mmapped_ptrs_.push_back(mmapped_buffer);
    mmapped_size_.push_back(file_size);
    mapped_paths.push_back(path);
  }

  try {
    insert_wal_list_.resize(4096);
    for (size_t i = 0; i < mmapped_ptrs_.size(); ++i) {
      char* ptr = static_cast<char*>(mmapped_ptrs_[i]);
      const char* end = ptr + mmapped_size_[i];
      while (true) {
        if (static_cast<size_t>(end - ptr) < sizeof(WalHeader)) {
          THROW_IO_EXCEPTION("Corrupt WAL file " + mapped_paths[i] +
                             ": truncated header or missing terminator");
        }
        WalHeader header{};
        std::memcpy(&header, ptr, sizeof(header));
        ptr += sizeof(WalHeader);
        const uint32_t ts = header.timestamp;
        if (ts == 0) {
          break;
        }
        if (header.length < 0) {
          THROW_IO_EXCEPTION("Corrupt WAL file " + mapped_paths[i] +
                             ": negative record length");
        }
        const auto length = static_cast<size_t>(header.length);
        if (length > static_cast<size_t>(end - ptr)) {
          THROW_IO_EXCEPTION("Corrupt WAL file " + mapped_paths[i] +
                             ": record payload exceeds file size");
        }
        if (header.type) {
          UpdateWalUnit unit;
          unit.timestamp = ts;
          unit.ptr = ptr;
          unit.size = length;
          update_wal_list_.push_back(unit);
        } else {
          if (ts >= insert_wal_list_.size()) {
            insert_wal_list_.resize(ts + 1);
          }
          insert_wal_list_[ts].ptr = ptr;
          insert_wal_list_[ts].size = length;
        }
        ptr += length;
        last_ts_ = std::max(ts, last_ts_);
      }
    }

    if (!update_wal_list_.empty()) {
      std::sort(update_wal_list_.begin(), update_wal_list_.end(),
                [](const UpdateWalUnit& lhs, const UpdateWalUnit& rhs) {
                  return lhs.timestamp < rhs.timestamp;
                });
    }
  } catch (...) {
    close();
    throw;
  }
}

void LocalWalParser::close() {
  insert_wal_list_.clear();
  size_t ptr_num = mmapped_ptrs_.size();
  for (size_t i = 0; i < ptr_num; ++i) {
    munmap(mmapped_ptrs_[i], mmapped_size_[i]);
  }
  for (auto fd : fds_) {
#ifdef _WIN32
    _close(fd);
#else
    ::close(fd);
#endif
  }
  fds_.clear();
  mmapped_ptrs_.clear();
  mmapped_size_.clear();
  update_wal_list_.clear();
  last_ts_ = 0;
}

uint32_t LocalWalParser::last_ts() const { return last_ts_; }

const WalContentUnit& LocalWalParser::get_insert_wal(uint32_t ts) const {
  return insert_wal_list_[ts];
}

const std::vector<UpdateWalUnit>& LocalWalParser::get_update_wals() const {
  return update_wal_list_;
}

const bool LocalWalParser::registered_ = WalParserFactory::RegisterWalParser(
    "file", static_cast<WalParserFactory::wal_parser_initializer_t>(
                &LocalWalParser::Make));

}  // namespace neug
